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
import sys

import os
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/include")
from db import get_conn, get_watermark, set_watermark  # noqa: E402
from lake_storage import upload_bytes, download_bytes  # noqa: E402
from alerts import notify_failure  # noqa: E402

LOCAL_LANDING = "/tmp/lake/landing_latest"
LOCAL_CLEANED = "/tmp/lake/cleaned_latest"

PIPELINE_NAME = "articles_etl_hourly"
BUCKET = "news-lakehouse"

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
        since = get_watermark(PIPELINE_NAME)
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
        set_watermark(PIPELINE_NAME, extraction_ts)
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
    def load_to_dwh_and_dq():
        """Load cleaned parquet + scorecard into the DWH (news_raw schema)."""
        table = pq.read_table(LOCAL_CLEANED)
        rows = table.to_pylist()

        with get_conn("dwh") as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO news_raw.articles_raw
                        (id, title, content, author_id, author_name, category,
                         published_at, created_at, updated_at, deleted_at, is_hard_deleted)
                        VALUES (%(id)s, %(title)s, %(content)s, %(author_id)s, %(author_name)s,
                                %(category)s, %(published_at)s, %(created_at)s, %(updated_at)s,
                                %(deleted_at)s, %(is_hard_deleted)s)
                        """,
                        r,
                    )

            with open("/tmp/lake/dq_scorecard.json") as f:
                s = json.load(f)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO news_dwh.dq_scorecard
                    (data_source, table_name, completeness, accuracy, consistency,
                     timeliness, validity, uniqueness, overall_score)
                    VALUES ('lake', %(table_name)s, %(completeness)s, %(accuracy)s,
                            %(consistency)s, %(timeliness)s, %(validity)s, %(uniqueness)s,
                            %(overall_score)s)
                    """,
                    s,
                )

    dbt_run = BashOperator(
        task_id="dbt_run_and_test",
        bash_command="cd /opt/airflow/dbt && dbt run && dbt snapshot && dbt test",
    )

    landed_key = extract_and_land()
    local_path = download_for_spark(landed_key)
    local_path >> spark_clean >> upload_cleaned_to_lake() >> load_to_dwh_and_dq() >> dbt_run


news_ingestion_hourly()
