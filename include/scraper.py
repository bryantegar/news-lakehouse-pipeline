"""
scraper.py — pulls new/updated articles.

Two modes, controlled by SCRAPER_MODE env var:

  - "fixture" (default): generates realistic fake articles locally so the
    whole pipeline runs end-to-end with zero external dependencies.

  - "live": the real kumparan.com GraphQL scraper (persisted-query,
    cursor-paginated, rate-limited), lifted from the earlier
    kumparan-de-final assessment project and trimmed to this project's
    schema (id, title, content, author_id, author_name, category,
    published_at, created_at, updated_at, deleted_at).

Either mode returns the same shape, so nothing downstream (the DAGs,
the Spark job) needs to know which one is active.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import random
import time
from typing import Iterator

from faker import Faker

log = logging.getLogger(__name__)
fake = Faker("id_ID")
CATEGORIES = ["nasional", "ekonomi", "olahraga", "teknologi", "hiburan"]


# ============================================================
# Fixture mode — offline, deterministic-ish fake data
# ============================================================

def _fetch_fixture(since: dt.datetime, n: int = 50) -> list[dict]:
    now = dt.datetime.utcnow()
    if since >= now:
        since = now - dt.timedelta(hours=1)  # safety: never let the window collapse/invert

    rows = []
    for _ in range(n):
        offset_minutes = random.randint(1, 500)
        created = since + dt.timedelta(minutes=offset_minutes)
        created = min(created, now)  # never generate a future-published article

        row = {
            "id": random.randint(1, 999_999),
            "title": fake.sentence(nb_words=8),
            "content": fake.paragraph(nb_sentences=10),
            "author_id": random.randint(1, 30),
            "author_name": fake.name() if random.random() > 0.05 else None,  # inject nulls -> completeness
            "category": random.choice(CATEGORIES) if random.random() > 0.03 else "UNKNOWN",  # -> validity
            "published_at": created,
            "created_at": created,
            "updated_at": min(created if random.random() > 0.1 else now, now),  # some late-arriving edits, still capped
            "deleted_at": None,
        }
        rows.append(row)
    return rows


# ============================================================
# Live mode — real kumparan.com GraphQL scraper
# ============================================================

GRAPHQL_URL  = "https://cdn-graphql-v4.kumparan.com/query"
QUERY_HASH   = "eb503c3f2ef2f7f7ffb36ce34b1c928bdefdc87e6f178527f388ce4b5e3ceb16"
OPERATION    = "FindAllActiveHeadlines"
PAGE_SIZE    = 20
RATE_LIMIT_S = 1.2
MAX_PAGES    = 50

CHANNEL_SLUG_MAP = {
    "1": "nasional", "2": "hiburan", "3": "woman", "4": "mom",
    "5": "olahraga", "6": "teknologi", "7": "otomotif",
    "8": "food-travel", "9": "bolanita", "10": "ekonomi",
}


def _channel_to_category(channel_id, channel_slug) -> str:
    if channel_slug:
        return channel_slug
    return CHANNEL_SLUG_MAP.get(str(channel_id or ""), "UNKNOWN")


def _make_session():
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


def _fetch_page(session, cursor: str) -> dict:
    params = {
        "operationName": OPERATION,
        "variables": json.dumps({"size": PAGE_SIZE, "placement": "HOMEPAGE", "cursor": cursor}),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": QUERY_HASH}}),
        "deduplicate": "1",
    }
    resp = session.get(GRAPHQL_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _safe_str(val, max_len: int = None):
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


def _extract_story(edge: dict) -> dict | None:
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


def _scrape_headlines(since: dt.datetime | None = None, max_pages: int = MAX_PAGES) -> Iterator[dict]:
    session = _make_session()
    cursor = "1"
    page_num = 0
    seen_ids = set()

    while page_num < max_pages:
        log.info("Fetching page %d (cursor=%s)", page_num + 1, cursor)
        try:
            data = _fetch_page(session, cursor)
        except Exception as e:
            log.error("Request failed on page %d: %s", page_num + 1, e)
            break

        edges = data.get("data", {}).get(OPERATION, {}).get("edges", [])
        if not edges:
            break

        stop = False
        for edge in edges:
            row = _extract_story(edge)
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
        time.sleep(RATE_LIMIT_S)


def _fetch_live(since: dt.datetime) -> list[dict]:
    return list(_scrape_headlines(since=since))


# ============================================================
# Public entrypoint used by the DAGs
# ============================================================

def fetch_new_or_updated(since: dt.datetime) -> list[dict]:
    mode = os.environ.get("SCRAPER_MODE", "fixture")
    if mode == "fixture":
        return _fetch_fixture(since)
    return _fetch_live(since)
