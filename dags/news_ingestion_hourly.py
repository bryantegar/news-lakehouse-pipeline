"""
news_ingestion_hourly.py

Ingest -> Store (lake) -> Transform (PySpark) -> Load (DWH) -> Transform (dbt) -> Observe (DQ)

This is the "Medallion"-style flow from the portfolio-building material:
raw lands untouched, PySpark does row-level cleaning (bronze -> silver),
dbt does business-logic modeling (silver -> gold marts).
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/include")
import dwh  # noqa: E402  — DWH operations, backend-agnostic (postgres or bigquery)
from alerts import notify_dq_anomaly, notify_failure, notify_scrape_report  # noqa: E402
from anomaly import detect as detect_dq_anomaly  # noqa: E402
from db import get_conn  # noqa: E402  — SOURCE DB only; always Postgres, never swapped
from lake_storage import download_bytes, upload_bytes  # noqa: E402

LOCAL_LANDING = "/tmp/lake/landing_latest"
LOCAL_CLEANED = "/tmp/lake/cleaned_latest"

PIPELINE_NAME = "articles_etl_hourly"
BUCKET = os.environ.get("LAKE_BUCKET", "news-lakehouse")

default_args = {"on_failure_callback": notify_failure}


@dag(
    dag_id="news_ingestion_hourly",
    default_args=default_args,
    schedule=None,  # triggered by news_scraper_hourly once scraping finishes, not on its own clock
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "pyspark", "dbt"],
)
def news_ingestion_hourly():

    @task
    def extract_and_land() -> str:
        """
        Ingest: pull rows from the SOURCE DB where updated_at is in
        [last_watermark, extraction_ts) — the window-based incremental
        strategy from the original assessment doc. Store: write raw
        parquet to the lake.
        """
        since = dwh.get_watermark(PIPELINE_NAME)
        extraction_ts = dt.datetime.utcnow()

        with get_conn("source") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, title, content, author_id, author_name, category,
                           published_at, created_at, updated_at, deleted_at
                    FROM articles
                    WHERE updated_at >= %s AND updated_at < %s
                    """,
                    (since, extraction_ts),
                )
                rows = cur.fetchall()

        if not rows:
            return ""

        table = pa.Table.from_pylist([dict(r) for r in rows])
        buf = io.BytesIO()
        pq.write_table(table, buf)

        run_ts = dt.datetime.utcnow()
        key = f"landing/articles/dt={run_ts:%Y-%m-%d}/{run_ts:%H%M%S}.parquet"
        upload_bytes(BUCKET, key, buf.getvalue())

        # Watermark only advances after the extraction window closes (idempotent re-runs).
        dwh.set_watermark(PIPELINE_NAME, extraction_ts)
        return key

    @task
    def download_for_spark(key: str) -> str:
        """Pull the just-landed object back down so local Spark can read it as a file."""
        if not key:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("No new/updated rows since last watermark.")
        os.makedirs(LOCAL_LANDING, exist_ok=True)
        data = download_bytes(BUCKET, key)
        with open(f"{LOCAL_LANDING}/part.parquet", "wb") as f:
            f.write(data)
        return LOCAL_LANDING

    spark_clean = BashOperator(
        task_id="spark_clean_transform",
        bash_command=(
            f"python /opt/airflow/include/spark_jobs/clean_transform.py "
            f"--in {LOCAL_LANDING} "
            f"--out {LOCAL_CLEANED} "
            f"--scorecard /tmp/lake/dq_scorecard.json"
        ),
    )

    @task
    def upload_cleaned_to_lake():
        """Store: write the cleaned (silver) parquet back to the lake for lineage/reprocessing."""
        run_ts = dt.datetime.utcnow()
        for fname in os.listdir(LOCAL_CLEANED):
            if fname.endswith(".parquet"):
                with open(f"{LOCAL_CLEANED}/{fname}", "rb") as f:
                    key = f"cleaned/articles/dt={run_ts:%Y-%m-%d}/{run_ts:%H%M%S}_{fname}"
                    upload_bytes(BUCKET, key, f.read())

    @task
    def load_to_dwh_and_dq() -> dict:
        """Load cleaned parquet + scorecard into the DWH (news_raw schema)."""
        table = pq.read_table(LOCAL_CLEANED)
        rows = table.to_pylist()

        dwh.insert_articles_raw(rows)

        with open("/tmp/lake/dq_scorecard.json") as f:
            scorecard = json.load(f)
        dwh.insert_dq_scorecard(scorecard)

        batch_counts: dict = {}
        for r in rows:
            cat = r.get("category") or "(tanpa kategori)"
            batch_counts[cat] = batch_counts.get(cat, 0) + 1
        return batch_counts

    @task
    def check_dq_anomaly():
        """
        Observe: compare this run's DQ score against the rolling history
        of past runs. Separate from the hard `< 0.85` dbt test — this
        catches a score that's still technically fine but unusual
        compared to recent runs (e.g. a sharp drop that hasn't crossed
        the failure line yet).
        """
        rows = dwh.get_dq_history(limit=21)
        if not rows:
            return
        latest, history = rows[0], rows[1:]

        is_anomaly, z_score = detect_dq_anomaly(history, latest)
        notify_dq_anomaly(
            {"overall_score": latest, "table_name": "articles"}, is_anomaly, z_score
        )

    dbt_run = BashOperator(
        task_id="dbt_run_and_test",
        bash_command="cd /opt/airflow/dbt && dbt run && dbt snapshot && dbt test",
    )

    @task(trigger_rule="all_done")  # send the report even if dbt_test only WARNed/failed on a non-blocking test
    def send_scrape_report(batch_counts: dict | None):
        batch_counts = batch_counts or {}
        total_counts = dwh.get_category_totals()
        notify_scrape_report(batch_counts, total_counts, dt.datetime.utcnow())

    landed_key = extract_and_land()
    local_path = download_for_spark(landed_key)
    dwh_result = load_to_dwh_and_dq()
    (
        local_path
        >> spark_clean
        >> upload_cleaned_to_lake()
        >> dwh_result
        >> check_dq_anomaly()
        >> dbt_run
        >> send_scrape_report(dwh_result)
    )


news_ingestion_hourly()