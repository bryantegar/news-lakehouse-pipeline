"""
db.py — thin connection helpers.

Kept deliberately dumb: one function per database, so swapping the
local Postgres DWH for real BigQuery later means editing *this file
only* (or just setting env vars), never the DAGs or the Spark job.
"""
import datetime as dt
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


@contextmanager
def get_conn(target: str):
    """
    target: "source" | "dwh"
    Reads connection info from environment variables (see .env.example).
    """
    prefix = "SOURCE" if target == "source" else "DWH"
    conn = psycopg2.connect(
        host=os.environ[f"{prefix}_DB_HOST"],
        port=os.environ.get(f"{prefix}_DB_PORT", 5432),
        dbname=os.environ[f"{prefix}_DB_NAME"],
        user=os.environ[f"{prefix}_DB_USER"],
        password=os.environ[f"{prefix}_DB_PASSWORD"],
        cursor_factory=RealDictCursor,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_watermark(pipeline_name: str) -> dt.datetime:
    """
    Returns the last watermark for this pipeline, auto-seeding a
    default (2016-01-01) row on first read so any new pipeline_name
    just works without needing a matching DDL seed row.
    """
    default = dt.datetime(2016, 1, 1)
    with get_conn("dwh") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO news_dwh.etl_watermark (pipeline_name, last_updated_at)
                VALUES (%s, %s)
                ON CONFLICT (pipeline_name) DO NOTHING
                """,
                (pipeline_name, default),
            )
            cur.execute(
                "SELECT last_updated_at FROM news_dwh.etl_watermark WHERE pipeline_name = %s",
                (pipeline_name,),
            )
            row = cur.fetchone()
            return row["last_updated_at"] if row else default


def set_watermark(pipeline_name: str, ts) -> None:
    with get_conn("dwh") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO news_dwh.etl_watermark (pipeline_name, last_updated_at)
                VALUES (%s, %s)
                ON CONFLICT (pipeline_name)
                DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at
                """,
                (pipeline_name, ts),
            )
