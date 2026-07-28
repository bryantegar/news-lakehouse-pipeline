"""
provision.py — auto-provisions Metabase via its REST API on container
startup: creates the admin account, connects to postgres-dwh, waits
for schema sync, then creates a set of SQL-question cards and wires
them into dashboards.

Metabase's open-source edition has no declarative "dashboard as code"
config format (that's an Enterprise feature) — the REST API is the
only programmatic path, so this script is the config-as-code layer
instead. It's the same one-shot-init pattern this project already
uses for airflow-init in docker-compose.yml.

Idempotent by design: `docker compose up` can be re-run any number of
times (container restarts, laptop reboots, etc.) without creating
duplicate databases, cards, or dashboards — every "ensure_*" function
checks for an existing match by name before creating anything.
"""
from __future__ import annotations

import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metabase-provision")

MB_URL = os.environ["MB_URL"].rstrip("/")
ADMIN_EMAIL = os.environ["MB_ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["MB_ADMIN_PASSWORD"]
ADMIN_FIRST_NAME = os.environ.get("MB_ADMIN_FIRST_NAME", "Admin")
ADMIN_LAST_NAME = os.environ.get("MB_ADMIN_LAST_NAME", "User")

DWH_DB_HOST = os.environ["DWH_DB_HOST"]
DWH_DB_PORT = int(os.environ.get("DWH_DB_PORT", 5432))
DWH_DB_NAME = os.environ["DWH_DB_NAME"]
DWH_DB_USER = os.environ["DWH_DB_USER"]
DWH_DB_PASSWORD = os.environ["DWH_DB_PASSWORD"]
DATABASE_DISPLAY_NAME = "news_dwh"

HEALTH_TIMEOUT_S = 180
SYNC_TIMEOUT_S = 120
REQUEST_TIMEOUT_S = 15


def wait_for_metabase() -> None:
    """Block until Metabase's own health endpoint reports OK."""
    deadline = time.monotonic() + HEALTH_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{MB_URL}/api/health", timeout=5)
            if resp.status_code == 200:
                log.info("Metabase is healthy.")
                return
        except requests.RequestException:
            pass
        log.info("Waiting for Metabase to become healthy...")
        time.sleep(5)
    raise TimeoutError(f"Metabase did not become healthy within {HEALTH_TIMEOUT_S}s")


def get_session() -> str:
    """
    Returns a Metabase session token, valid for use as the
    X-Metabase-Session header on all subsequent requests.

    First run: Metabase has no admin user yet, so /api/session/properties
    exposes a one-time `setup-token` — we use it to create the admin
    account via /api/setup, which returns a session directly.

    Re-runs: the setup token is gone (admin already exists), so we log
    in with the same fixed credentials instead.
    """
    props = requests.get(f"{MB_URL}/api/session/properties", timeout=REQUEST_TIMEOUT_S).json()
    setup_token = props.get("setup-token")

    if setup_token:
        log.info("No admin user yet — running initial setup.")
        resp = requests.post(
            f"{MB_URL}/api/setup",
            json={
                "token": setup_token,
                "user": {
                    "first_name": ADMIN_FIRST_NAME,
                    "last_name": ADMIN_LAST_NAME,
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                },
                "prefs": {"site_name": "news-lakehouse-pipeline", "allow_tracking": False},
            },
            timeout=REQUEST_TIMEOUT_S,
        )
        resp.raise_for_status()
        return resp.json()["id"]

    log.info("Admin user already exists — logging in.")
    resp = requests.post(
        f"{MB_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def _headers(session_id: str) -> dict:
    return {"X-Metabase-Session": session_id, "Content-Type": "application/json"}


def ensure_database(session_id: str) -> int:
    """Adds the postgres-dwh connection if it isn't already registered, returns its id."""
    resp = requests.get(f"{MB_URL}/api/database", headers=_headers(session_id), timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    for db in resp.json().get("data", []):
        if db["name"] == DATABASE_DISPLAY_NAME:
            log.info("Database '%s' already connected (id=%s).", DATABASE_DISPLAY_NAME, db["id"])
            return db["id"]

    log.info("Connecting Metabase to %s...", DATABASE_DISPLAY_NAME)
    resp = requests.post(
        f"{MB_URL}/api/database",
        headers=_headers(session_id),
        json={
            "engine": "postgres",
            "name": DATABASE_DISPLAY_NAME,
            "details": {
                "host": DWH_DB_HOST,
                "port": DWH_DB_PORT,
                "dbname": DWH_DB_NAME,
                "user": DWH_DB_USER,
                "password": DWH_DB_PASSWORD,
                "ssl": False,
            },
        },
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def wait_for_sync(session_id: str, database_id: int) -> None:
    """
    Metabase syncs schema/table metadata asynchronously after a database
    is added. Card creation below needs that metadata to exist, so poll
    until at least one table with columns shows up.
    """
    deadline = time.monotonic() + SYNC_TIMEOUT_S
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{MB_URL}/api/database/{database_id}/metadata",
            headers=_headers(session_id),
            timeout=REQUEST_TIMEOUT_S,
        )
        resp.raise_for_status()
        tables = resp.json().get("tables", [])
        if any(t.get("fields") for t in tables):
            log.info("Schema sync complete (%d tables visible).", len(tables))
            return
        log.info("Waiting for Metabase schema sync...")
        time.sleep(5)
    raise TimeoutError(f"Metabase schema sync did not complete within {SYNC_TIMEOUT_S}s")


def ensure_card(session_id: str, database_id: int, spec: dict) -> int:
    """Creates a native-SQL question if a card with this name doesn't already exist."""
    resp = requests.get(f"{MB_URL}/api/card", headers=_headers(session_id), timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    for card in resp.json():
        if card["name"] == spec["name"]:
            log.info("Card '%s' already exists (id=%s).", spec["name"], card["id"])
            return card["id"]

    log.info("Creating card '%s'...", spec["name"])
    resp = requests.post(
        f"{MB_URL}/api/card",
        headers=_headers(session_id),
        json={
            "name": spec["name"],
            "description": spec.get("description", ""),
            "display": spec.get("display", "table"),
            "visualization_settings": spec.get("visualization_settings", {}),
            "dataset_query": {
                "type": "native",
                "native": {"query": spec["sql"]},
                "database": database_id,
            },
        },
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def ensure_dashboard(session_id: str, name: str, description: str = "") -> int:
    resp = requests.get(f"{MB_URL}/api/dashboard", headers=_headers(session_id), timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    for dash in resp.json():
        if dash["name"] == name:
            log.info("Dashboard '%s' already exists (id=%s).", name, dash["id"])
            return dash["id"]

    log.info("Creating dashboard '%s'...", name)
    resp = requests.post(
        f"{MB_URL}/api/dashboard",
        headers=_headers(session_id),
        json={"name": name, "description": description},
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def get_dashboard_cards(session_id: str, dashboard_id: int) -> list[dict]:
    """Existing dashcards on this dashboard, with their real ids/positions."""
    resp = requests.get(
        f"{MB_URL}/api/dashboard/{dashboard_id}", headers=_headers(session_id), timeout=REQUEST_TIMEOUT_S
    )
    resp.raise_for_status()
    return resp.json().get("dashcards", [])


def add_cards_to_dashboard(session_id: str, dashboard_id: int, card_ids: list[int]) -> None:
    existing_dashcards = get_dashboard_cards(session_id, dashboard_id)
    existing_card_ids = {dc["card_id"] for dc in existing_dashcards}
    missing = [cid for cid in card_ids if cid not in existing_card_ids]
    if not missing:
        log.info("Dashboard already has all %d card(s).", len(card_ids))
        return

    # Simple vertical stack layout: each new card full-width, 8 rows tall,
    # placed below whatever's already there. Fine for a starter dashboard —
    # rearrange freely in the Metabase UI afterwards, this only runs once
    # per card.
    next_row = max((dc["row"] + dc["size_y"] for dc in existing_dashcards), default=0)
    new_dashcards = [
        {
            "id": -(i + 1),  # negative temp ids, per Metabase's bulk-update API convention
            "card_id": cid,
            "row": next_row + i * 8,
            "col": 0,
            "size_x": 18,
            "size_y": 8,
        }
        for i, cid in enumerate(missing)
    ]

    # PUT /api/dashboard/{id}/cards replaces the dashboard's entire card
    # layout — it's not additive. Sending only the new cards would wipe
    # out everything already placed, so existing dashcards must be
    # resent unchanged alongside the new ones.
    unchanged_dashcards = [
        {
            "id": dc["id"],
            "card_id": dc["card_id"],
            "row": dc["row"],
            "col": dc["col"],
            "size_x": dc["size_x"],
            "size_y": dc["size_y"],
        }
        for dc in existing_dashcards
    ]

    resp = requests.put(
        f"{MB_URL}/api/dashboard/{dashboard_id}/cards",
        headers=_headers(session_id),
        json={"cards": unchanged_dashcards + new_dashcards},
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    log.info("Added %d new card(s) to dashboard.", len(missing))


# ============================================================
# Dashboard definitions
# ============================================================

DQ_MONITORING_CARDS = [
    {
        "name": "DQ Overall Score Over Time",
        "description": (
            "Overall data quality score per pipeline run — the same series "
            "check_dq_anomaly monitors for anomalies."
        ),
        "display": "line",
        "sql": """
            SELECT run_at, overall_score
            FROM news_dwh.dq_scorecard
            ORDER BY run_at
        """,
    },
    {
        "name": "DQ Dimension Breakdown (Latest Run)",
        "description": (
            "Completeness/accuracy/consistency/timeliness/validity/uniqueness "
            "for the most recent scorecard."
        ),
        "display": "bar",
        "sql": """
            SELECT 'completeness' AS dimension, completeness AS score
                FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
            UNION ALL
            SELECT 'accuracy', accuracy FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
            UNION ALL
            SELECT 'consistency', consistency FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
            UNION ALL
            SELECT 'timeliness', timeliness FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
            UNION ALL
            SELECT 'validity', validity FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
            UNION ALL
            SELECT 'uniqueness', uniqueness FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT 1
        """,
    },
    {
        "name": "DQ Scorecard History (Table)",
        "description": "Raw scorecard history, most recent first.",
        "display": "table",
        "sql": """
            SELECT run_at, table_name, completeness, accuracy, consistency,
                   timeliness, validity, uniqueness, overall_score
            FROM news_dwh.dq_scorecard
            ORDER BY run_at DESC
            LIMIT 50
        """,
    },
]


def main() -> None:
    wait_for_metabase()
    session_id = get_session()
    database_id = ensure_database(session_id)
    wait_for_sync(session_id, database_id)

    dq_dashboard_id = ensure_dashboard(
        session_id,
        "Data Quality Monitoring",
        "DQ scorecard trend and breakdown — sourced from news_dwh.dq_scorecard.",
    )
    dq_card_ids = [ensure_card(session_id, database_id, spec) for spec in DQ_MONITORING_CARDS]
    add_cards_to_dashboard(session_id, dq_dashboard_id, dq_card_ids)

    log.info("Provisioning complete. Open %s to view dashboards.", MB_URL)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Provisioning failed.")
        sys.exit(1)
