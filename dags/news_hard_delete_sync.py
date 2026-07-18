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
from db import get_conn  # noqa: E402
from alerts import notify_failure  # noqa: E402

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

        if not deleted_rows:
            return 0

        with get_conn("dwh") as dwh_conn:
            with dwh_conn.cursor() as cur:
                for row in deleted_rows:
                    cur.execute(
                        """
                        UPDATE news_raw.articles_raw
                        SET is_hard_deleted = TRUE, deleted_at = %(deleted_at)s
                        WHERE id = %(article_id)s
                        """,
                        row,
                    )
        return len(deleted_rows)

    reconcile()


news_hard_delete_sync()
