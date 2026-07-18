"""
db.py — connection pool for the API layer.

Separate from include/db.py on purpose: the API is a different runtime
(FastAPI/uvicorn, not Airflow) and should only ever need read access to
the DWH marts — it has no reason to know about the source DB or the
lake. A real production setup would point DWH_DB_* at a read replica.
"""
import os
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host=os.environ["DWH_DB_HOST"],
    port=os.environ.get("DWH_DB_PORT", 5432),
    dbname=os.environ["DWH_DB_NAME"],
    user=os.environ["DWH_DB_USER"],
    password=os.environ["DWH_DB_PASSWORD"],
    cursor_factory=RealDictCursor,
)


@contextmanager
def get_cursor():
    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)
