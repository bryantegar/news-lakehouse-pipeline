"""
scraper.py — pulls new/updated articles.

Two modes, controlled by SCRAPER_MODE env var, each a concrete
BaseScraper implementation:

  - "fixture" (default) -> FixtureScraper: generates realistic fake
    articles locally so the whole pipeline runs end-to-end with zero
    external dependencies.

  - "live" -> KumparanScraper: the real kumparan.com GraphQL scraper
    (persisted-query, cursor-paginated, rate-limited), lifted from the
    earlier kumparan-de-final assessment project and trimmed to this
    project's schema (id, title, content, author_id, author_name,
    category, published_at, created_at, updated_at, deleted_at).

Both implement the same BaseScraper.fetch_new_or_updated(since)
contract, so nothing downstream (the DAGs, the Spark job) needs to
know which one is active — that's decided once, at get_scraper().

Adding a third source later (e.g. a second news portal) means adding
one more BaseScraper subclass here, not touching either of these two
or the DAG that calls them (Open/Closed Principle).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Iterator

from faker import Faker

log = logging.getLogger(__name__)
fake = Faker("id_ID")
CATEGORIES = ["nasional", "ekonomi", "olahraga", "teknologi", "hiburan"]

CHANNEL_SLUG_MAP = {
    "1": "nasional", "2": "hiburan", "3": "woman", "4": "mom",
    "5": "olahraga", "6": "teknologi", "7": "otomotif",
    "8": "food-travel", "9": "bolanita", "10": "ekonomi",
}


# ============================================================
# Pure helpers — no state, no I/O side effects beyond parsing.
# Kept as plain functions rather than methods: wrapping them in a
# class would add indirection without adding any actual behavior,
# since none of them depend on scraper configuration or connection
# state. They're shared by KumparanScraper's parsing step below.
# ============================================================

def _channel_to_category(channel_id, channel_slug) -> str:
    if channel_slug:
        return channel_slug
    return CHANNEL_SLUG_MAP.get(str(channel_id or ""), "UNKNOWN")


def _safe_str(val, max_len: int | None = None):
    if val is None:
        return None
    s = str(val).strip()
    return (s[:max_len] if max_len else s) or None


def _safe_int(val):
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _parse_dt(val):
    if not val:
        return None
    try:
        return dt.datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


# ============================================================
# Contract every article source must satisfy.
# ============================================================

class BaseScraper(ABC):
    """
    Every concrete scraper pulls articles created/updated since a
    given watermark and returns them in this project's flat article
    schema. Callers (the DAGs) only ever depend on this interface —
    never on FixtureScraper or KumparanScraper directly.
    """

    @abstractmethod
    def fetch_new_or_updated(self, since: dt.datetime) -> list[dict]:
        """Return articles with published_at/updated_at >= since."""
        raise NotImplementedError


# ============================================================
# Fixture mode — offline, deterministic-ish fake data
# ============================================================

class FixtureScraper(BaseScraper):
    """
    Local fake-data generator. Configurable at construction so tests
    can request a small, fast batch instead of the production default.
    """

    def __init__(self, categories: list[str] = CATEGORIES, articles_per_run: int = 50):
        self.categories = categories
        self.articles_per_run = articles_per_run

    def fetch_new_or_updated(self, since: dt.datetime) -> list[dict]:
        now = dt.datetime.utcnow()
        if since >= now:
            since = now - dt.timedelta(hours=1)  # safety: never let the window collapse/invert

        rows = []
        for _ in range(self.articles_per_run):
            offset_minutes = random.randint(1, 500)
            created = since + dt.timedelta(minutes=offset_minutes)
            created = min(created, now)  # never generate a future-published article

            row = {
                "id": random.randint(1, 999_999),
                "title": fake.sentence(nb_words=8),
                "content": fake.paragraph(nb_sentences=10),
                "author_id": random.randint(1, 30),
                "author_name": fake.name() if random.random() > 0.05 else None,  # -> completeness
                "category": (
                    random.choice(self.categories) if random.random() > 0.03 else "UNKNOWN"
                ),  # -> validity
                "published_at": created,
                "created_at": created,
                "updated_at": min(created if random.random() > 0.1 else now, now),
                "deleted_at": None,
            }
            rows.append(row)
        return rows


# ============================================================
# Live mode — real kumparan.com GraphQL scraper
# ============================================================

class KumparanScraper(BaseScraper):
    """
    Cursor-paginated, rate-limited GraphQL client for kumparan.com's
    persisted-query API. Pagination/rate-limit/retry knobs are
    constructor params (not module constants) so a test can spin up
    an instance with rate_limit_s=0 and max_pages=1 without touching
    production behavior.
    """

    GRAPHQL_URL = "https://cdn-graphql-v4.kumparan.com/query"
    QUERY_HASH = "eb503c3f2ef2f7f7ffb36ce34b1c928bdefdc87e6f178527f388ce4b5e3ceb16"
    OPERATION = "FindAllActiveHeadlines"

    def __init__(self, page_size: int = 20, rate_limit_s: float = 1.2, max_pages: int = 50):
        self.page_size = page_size
        self.rate_limit_s = rate_limit_s
        self.max_pages = max_pages
        self._session = self._make_session()

    def fetch_new_or_updated(self, since: dt.datetime) -> list[dict]:
        return list(self._scrape_headlines(since=since))

    def _make_session(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retry = Retry(
            total=3, backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; news-lakehouse-pipeline/1.0)",
            "Accept": "application/json",
            "Referer": "https://kumparan.com/",
        })
        return session

    def _fetch_page(self, cursor: str) -> dict:
        params = {
            "operationName": self.OPERATION,
            "variables": json.dumps(
                {"size": self.page_size, "placement": "HOMEPAGE", "cursor": cursor}
            ),
            "extensions": json.dumps(
                {"persistedQuery": {"version": 1, "sha256Hash": self.QUERY_HASH}}
            ),
            "deduplicate": "1",
        }
        resp = self._session.get(self.GRAPHQL_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _extract_story(self, edge: dict) -> dict | None:
        """Map one GraphQL edge -> flat dict matching this project's articles schema."""
        story = edge.get("story")
        if not story:
            return None

        author = story.get("author") or {}
        channel = story.get("channel") or {}

        lead_text = _safe_str(story.get("leadText"), max_len=2000)
        meta_desc = _safe_str(story.get("metaDescription"), max_len=1000)
        content = "\n\n".join(filter(None, [lead_text, meta_desc])) or None

        return {
            "id": _safe_int(story.get("id")),
            "title": _safe_str(story.get("title"), max_len=500),
            "content": content,
            "author_id": _safe_int(author.get("id")),
            "author_name": _safe_str(author.get("name"), max_len=200),
            "category": _channel_to_category(channel.get("id"), channel.get("slug")),
            "published_at": _parse_dt(story.get("publishedAt")),
            "created_at": _parse_dt(story.get("createdAt")) or _parse_dt(story.get("publishedAt")),
            "updated_at": _parse_dt(story.get("updatedAt")) or _parse_dt(story.get("publishedAt")),
            "deleted_at": _parse_dt(story.get("deletedAt")),
        }

    def _scrape_headlines(self, since: dt.datetime | None = None) -> Iterator[dict]:
        cursor = "1"
        page_num = 0
        seen_ids = set()

        while page_num < self.max_pages:
            log.info("Fetching page %d (cursor=%s)", page_num + 1, cursor)
            try:
                data = self._fetch_page(cursor)
            except Exception as e:
                log.error("Request failed on page %d: %s", page_num + 1, e)
                break

            edges = data.get("data", {}).get(self.OPERATION, {}).get("edges", [])
            if not edges:
                break

            stop = False
            for edge in edges:
                row = self._extract_story(edge)
                if row is None or row["id"] in seen_ids or row["id"] is None:
                    continue
                seen_ids.add(row["id"])

                if since and row["published_at"] and row["published_at"] < since:
                    stop = True
                    break

                yield row

            if stop:
                break

            last_id = edges[-1].get("story", {}).get("id") if edges else None
            if not last_id:
                break
            cursor = str(last_id)
            page_num += 1
            time.sleep(self.rate_limit_s)


# ============================================================
# Factory — the one place SCRAPER_MODE is read.
# ============================================================

def get_scraper() -> BaseScraper:
    """
    Reads SCRAPER_MODE and returns the matching BaseScraper instance.
    Callers depend only on BaseScraper.fetch_new_or_updated() from
    here on — this is the single point where the concrete class is
    decided.
    """
    mode = os.environ.get("SCRAPER_MODE", "fixture")
    if mode == "live":
        return KumparanScraper()
    return FixtureScraper()
