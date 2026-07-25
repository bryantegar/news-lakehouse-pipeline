"""test_scraper.py — unit tests for include/scraper.py (post-OOP-refactor)."""
import datetime as dt

import pytest
from scraper import (
    BaseScraper,
    FixtureScraper,
    KumparanScraper,
    _channel_to_category,
    _parse_dt,
    _safe_int,
    _safe_str,
    get_scraper,
)

# ============================================================
# Pure helpers
# ============================================================

def test_channel_to_category_prefers_slug():
    assert _channel_to_category("1", "custom-slug") == "custom-slug"


def test_channel_to_category_falls_back_to_id_map():
    assert _channel_to_category("5", None) == "olahraga"


def test_channel_to_category_unknown_id_returns_unknown():
    assert _channel_to_category("999", None) == "UNKNOWN"


@pytest.mark.parametrize(
    "val,max_len,expected",
    [
        (None, None, None),
        ("  hello  ", None, "hello"),
        ("", None, None),
        ("this is long", 4, "this"),
    ],
)
def test_safe_str(val, max_len, expected):
    assert _safe_str(val, max_len) == expected


@pytest.mark.parametrize(
    "val,expected",
    [(None, None), ("42", 42), ("not a number", None), (7, 7)],
)
def test_safe_int(val, expected):
    assert _safe_int(val) == expected


def test_parse_dt_handles_z_suffix():
    result = _parse_dt("2026-01-15T10:30:00Z")
    assert result == dt.datetime(2026, 1, 15, 10, 30, 0)


def test_parse_dt_none_or_empty_returns_none():
    assert _parse_dt(None) is None
    assert _parse_dt("") is None


def test_parse_dt_malformed_returns_none_instead_of_raising():
    assert _parse_dt("not-a-date") is None


# ============================================================
# FixtureScraper
# ============================================================

def test_fixture_scraper_is_a_base_scraper():
    assert isinstance(FixtureScraper(), BaseScraper)


def test_fixture_scraper_returns_requested_article_count():
    scraper = FixtureScraper(articles_per_run=5)
    since = dt.datetime.utcnow() - dt.timedelta(hours=1)

    rows = scraper.fetch_new_or_updated(since)

    assert len(rows) == 5


def test_fixture_scraper_rows_match_expected_schema():
    scraper = FixtureScraper(articles_per_run=1)
    since = dt.datetime.utcnow() - dt.timedelta(hours=1)

    row = scraper.fetch_new_or_updated(since)[0]

    expected_keys = {
        "id", "title", "content", "author_id", "author_name", "category",
        "published_at", "created_at", "updated_at", "deleted_at",
    }
    assert expected_keys == set(row.keys())


def test_fixture_scraper_never_generates_future_published_at():
    scraper = FixtureScraper(articles_per_run=30)
    since = dt.datetime.utcnow() - dt.timedelta(hours=1)

    rows = scraper.fetch_new_or_updated(since)
    now = dt.datetime.utcnow()  # captured after the call, so it's a valid upper bound

    assert all(row["published_at"] <= now for row in rows)


def test_fixture_scraper_handles_since_in_the_future_without_inverting_window():
    """Regression guard: since >= now must not collapse the generation window."""
    scraper = FixtureScraper(articles_per_run=5)
    since_in_future = dt.datetime.utcnow() + dt.timedelta(days=1)

    rows = scraper.fetch_new_or_updated(since_in_future)

    assert len(rows) == 5


def test_fixture_scraper_respects_custom_categories():
    scraper = FixtureScraper(categories=["only-category"], articles_per_run=20)
    since = dt.datetime.utcnow() - dt.timedelta(hours=1)

    rows = scraper.fetch_new_or_updated(since)

    assert all(row["category"] in ("only-category", "UNKNOWN") for row in rows)


# ============================================================
# KumparanScraper — network calls mocked, never hits the real API
# ============================================================

def test_kumparan_scraper_is_a_base_scraper():
    assert isinstance(KumparanScraper(), BaseScraper)


def test_kumparan_scraper_extract_story_maps_graphql_edge_to_schema():
    scraper = KumparanScraper()
    edge = {
        "story": {
            "id": "123",
            "title": "  Some Headline  ",
            "leadText": "Lead paragraph.",
            "metaDescription": "Meta description.",
            "author": {"id": "7", "name": "Jane Doe"},
            "channel": {"id": "5", "slug": None},
            "publishedAt": "2026-01-15T10:00:00Z",
            "createdAt": "2026-01-15T09:55:00Z",
            "updatedAt": "2026-01-15T10:05:00Z",
            "deletedAt": None,
        }
    }

    row = scraper._extract_story(edge)

    assert row["id"] == 123
    assert row["title"] == "Some Headline"
    assert row["content"] == "Lead paragraph.\n\nMeta description."
    assert row["author_id"] == 7
    assert row["category"] == "olahraga"
    assert row["published_at"] == dt.datetime(2026, 1, 15, 10, 0, 0)
    assert row["deleted_at"] is None


def test_kumparan_scraper_extract_story_returns_none_when_no_story():
    scraper = KumparanScraper()

    assert scraper._extract_story({}) is None


def test_kumparan_scraper_scrape_headlines_stops_at_since_boundary(mocker):
    """
    A page with articles older than `since` should stop pagination —
    without this, an hourly scraper would re-walk the entire site
    history on every run.
    """
    scraper = KumparanScraper(rate_limit_s=0, max_pages=5)
    since = dt.datetime(2026, 1, 15, 9, 0, 0)

    page_response = {
        "data": {
            "FindAllActiveHeadlines": {
                "edges": [
                    {"story": {"id": "1", "publishedAt": "2026-01-15T10:00:00Z"}},
                    {"story": {"id": "2", "publishedAt": "2026-01-15T08:00:00Z"}},  # before since
                ]
            }
        }
    }
    mocker.patch.object(scraper, "_fetch_page", return_value=page_response)

    rows = scraper.fetch_new_or_updated(since)

    assert len(rows) == 1
    assert rows[0]["id"] == 1


def test_kumparan_scraper_deduplicates_repeated_ids(mocker):
    scraper = KumparanScraper(rate_limit_s=0, max_pages=1)

    page_response = {
        "data": {
            "FindAllActiveHeadlines": {
                "edges": [
                    {"story": {"id": "1", "publishedAt": "2026-01-15T10:00:00Z"}},
                    {"story": {"id": "1", "publishedAt": "2026-01-15T10:00:00Z"}},
                ]
            }
        }
    }
    mocker.patch.object(scraper, "_fetch_page", return_value=page_response)

    rows = scraper.fetch_new_or_updated(dt.datetime(2020, 1, 1))

    assert len(rows) == 1


def test_kumparan_scraper_stops_gracefully_on_fetch_error(mocker):
    scraper = KumparanScraper(rate_limit_s=0, max_pages=5)
    mocker.patch.object(scraper, "_fetch_page", side_effect=ConnectionError("boom"))

    rows = scraper.fetch_new_or_updated(dt.datetime(2020, 1, 1))

    assert rows == []


# ============================================================
# get_scraper() factory
# ============================================================

def test_get_scraper_defaults_to_fixture(monkeypatch):
    monkeypatch.delenv("SCRAPER_MODE", raising=False)

    assert isinstance(get_scraper(), FixtureScraper)


def test_get_scraper_returns_fixture_explicitly(monkeypatch):
    monkeypatch.setenv("SCRAPER_MODE", "fixture")

    assert isinstance(get_scraper(), FixtureScraper)


def test_get_scraper_returns_kumparan_for_live_mode(monkeypatch):
    monkeypatch.setenv("SCRAPER_MODE", "live")

    assert isinstance(get_scraper(), KumparanScraper)
