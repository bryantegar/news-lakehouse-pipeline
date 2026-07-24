"""
news_hard_delete_sync.py

The hourly ETL is watermark-based on `updated_at`, which a hard DELETE
never touches. This daily job closes that gap by reading `article_deleted`
(populated by the trigger in sql/01_source_ddl.sql) and flagging the
matching rows in the DWH as deleted, instead of silently keeping stale
"ghost" rows forever.
"""
from __future__ import annotations

import datetime as dt
import sys

from airflow.decorators import dag, task

sys.path.append("/opt/airflow/include")
import dwh  # noqa: E402  — DWH operations, backend-agnostic (postgres or bigquery)
from alerts import notify_failure  # noqa: E402
from db import get_conn  # noqa: E402  — SOURCE DB only; always Postgres

default_args = {"on_failure_callback": notify_failure}


@dag(
    dag_id="news_hard_delete_sync",
    default_args=default_args,
    schedule="@daily",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    tags=["reconciliation"],
)
def news_hard_delete_sync():

    @task
    def reconcile():
        with get_conn("source") as src_conn:
            with src_conn.cursor() as cur:
                cur.execute("SELECT article_id, deleted_at FROM article_deleted")
                deleted_rows = cur.fetchall()

        dwh.mark_hard_deleted([dict(r) for r in deleted_rows])
        return len(deleted_rows)

    reconcile()


news_hard_delete_sync()
