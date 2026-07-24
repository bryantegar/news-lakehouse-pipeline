"""
news_scraper_hourly.py

Ingest step, stage 1: scrape (live kumparan.com or local fixture) and
UPSERT into the source OLTP DB. This mirrors the earlier kumparan-de-final
design — the scraper is a source system in its own right, separate from
the ETL that later reads FROM that source DB into the lake. Keeping this
split (instead of scraping straight into the lake) is what makes the
source DB / hard-delete trigger / watermark-on-updated_at pattern
actually meaningful.
"""
from __future__ import annotations

import datetime as dt
import sys

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow/include")
from alerts import notify_failure  # noqa: E402
from db import get_conn, get_watermark, set_watermark  # noqa: E402
from scraper import fetch_new_or_updated  # noqa: E402

PIPELINE_NAME = "kumparan_scraper"
default_args = {"on_failure_callback": notify_failure}


@dag(
    dag_id="news_scraper_hourly",
    default_args=default_args,
    schedule="@hourly",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "scraper"],
)
def news_scraper_hourly():

    @task
    def scrape_and_upsert():
        since = get_watermark(PIPELINE_NAME)
        rows = fetch_new_or_updated(since)
        if not rows:
            return 0

        with get_conn("source") as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO articles
                        (id, title, content, author_id, author_name, category,
                         published_at, created_at, updated_at, deleted_at)
                        VALUES (%(id)s, %(title)s, %(content)s, %(author_id)s, %(author_name)s,
                                %(category)s, %(published_at)s, %(created_at)s, %(updated_at)s,
                                %(deleted_at)s)
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            content = EXCLUDED.content,
                            author_id = EXCLUDED.author_id,
                            author_name = EXCLUDED.author_name,
                            category = EXCLUDED.category,
                            updated_at = EXCLUDED.updated_at,
                            deleted_at = EXCLUDED.deleted_at
                        """,
                        r,
                    )

        max_published = max(r["published_at"] for r in rows if r.get("published_at"))
        set_watermark(PIPELINE_NAME, max_published)
        return len(rows)

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_etl",
        trigger_dag_id="news_ingestion_hourly",
        wait_for_completion=False,
    )

    scrape_and_upsert() >> trigger_ingestion


news_scraper_hourly()
