"""
dwh.py — data warehouse operations (watermark, raw load, DQ scorecard,
hard-delete marking), abstracted over the backend.

Two backends, selected by DWH_BACKEND env var — same pattern as
include/lake_storage.py:

  - "postgres" (default) — local Postgres, zero external dependencies.
  - "bigquery" — real BigQuery. Uses the same Application Default
    Credentials already set up for GCS (GOOGLE_APPLICATION_CREDENTIALS).

Callers (the DAGs) only use the functions exported here — they never
write raw SQL against a specific backend, so nothing else changes when
DWH_BACKEND flips. dbt is switched separately via DBT_TARGET
(dev=postgres, prod=bigquery) in dbt/profiles.yml.example — dbt's own
adapter handles staging/intermediate/marts either way.
"""
import datetime as dt
import os

BACKEND = os.environ.get("DWH_BACKEND", "postgres").lower()
DEFAULT_WATERMARK = dt.datetime(2016, 1, 1)


if BACKEND == "bigquery":
    from google.cloud import bigquery

    _client = None

    def _bq_client():
        global _client
        if _client is None:
            _client = bigquery.Client(project=os.environ.get("GCP_PROJECT") or None)
        return _client

    def _project():
        return os.environ.get("GCP_PROJECT")

    def _ensure_schema():
        """Idempotent dataset/table setup — safe to call on every task run."""
        client = _bq_client()
        project = _project()

        for dataset_id in ("news_raw", "news_intermediate", "news_dwh", "news_mart"):
            ref = bigquery.DatasetReference(project, dataset_id)
            try:
                client.get_dataset(ref)
            except Exception:
                ds = bigquery.Dataset(ref)
                ds.location = os.environ.get("GCP_LOCATION", "asia-southeast2")
                client.create_dataset(ds, exists_ok=True)

        articles_raw_schema = [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("author_id", "INT64"),
            bigquery.SchemaField("author_name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP"),
            bigquery.SchemaField("is_hard_deleted", "BOOL"),
            bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
        ]
        watermark_schema = [
            bigquery.SchemaField("pipeline_name", "STRING"),
            bigquery.SchemaField("last_updated_at", "TIMESTAMP"),
        ]
        dq_schema = [
            bigquery.SchemaField("run_at", "TIMESTAMP"),
            bigquery.SchemaField("data_source", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("completeness", "FLOAT64"),
            bigquery.SchemaField("accuracy", "FLOAT64"),
            bigquery.SchemaField("consistency", "FLOAT64"),
            bigquery.SchemaField("timeliness", "FLOAT64"),
            bigquery.SchemaField("validity", "FLOAT64"),
            bigquery.SchemaField("uniqueness", "FLOAT64"),
            bigquery.SchemaField("overall_score", "FLOAT64"),
        ]
        for table_id, schema in [
            (f"{project}.news_raw.articles_raw", articles_raw_schema),
            (f"{project}.news_dwh.etl_watermark", watermark_schema),
            (f"{project}.news_dwh.dq_scorecard", dq_schema),
        ]:
            try:
                client.get_table(table_id)
            except Exception:
                client.create_table(bigquery.Table(table_id, schema=schema))

    def get_watermark(pipeline_name: str) -> dt.datetime:
        _ensure_schema()
        client = _bq_client()
        project = _project()
        query = (
            f"SELECT last_updated_at FROM `{project}.news_dwh.etl_watermark` "
            f"WHERE pipeline_name = @pipeline_name"
        )
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)
            ]),
        )
        rows = list(job.result())
        if rows:
            return rows[0]["last_updated_at"]

        # DML INSERT, not insert_rows_json (streaming insert). A row created
        # via streaming insert lands in the streaming buffer and can't be
        # UPDATE/DELETE'd for up to ~90 min — set_watermark()'s MERGE would
        # then fail every time it hits WHEN MATCHED THEN UPDATE. DML INSERT
        # commits immediately with no such restriction, and this table is
        # low-volume (one row per pipeline) so there's no throughput reason
        # to prefer streaming here.
        insert_query = (
            f"INSERT INTO `{project}.news_dwh.etl_watermark` (pipeline_name, last_updated_at) "
            f"VALUES (@pipeline_name, @last_updated_at)"
        )
        client.query(
            insert_query,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
                bigquery.ScalarQueryParameter("last_updated_at", "TIMESTAMP", DEFAULT_WATERMARK),
            ]),
        ).result()
        return DEFAULT_WATERMARK

    def set_watermark(pipeline_name: str, ts: dt.datetime) -> None:
        client = _bq_client()
        project = _project()
        query = (
            f"MERGE `{project}.news_dwh.etl_watermark` T "
            f"USING (SELECT @pipeline_name AS pipeline_name, @ts AS last_updated_at) S "
            f"ON T.pipeline_name = S.pipeline_name "
            f"WHEN MATCHED THEN UPDATE SET last_updated_at = S.last_updated_at "
            f"WHEN NOT MATCHED THEN INSERT (pipeline_name, last_updated_at) "
            f"VALUES (S.pipeline_name, S.last_updated_at)"
        )
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
                bigquery.ScalarQueryParameter("ts", "TIMESTAMP", ts),
            ]),
        ).result()

    def insert_articles_raw(rows: list[dict]) -> None:
        _ensure_schema()
        client = _bq_client()
        project = _project()
        loaded_at = dt.datetime.utcnow().isoformat()
        payload = []
        for r in rows:
            row = dict(r)
            row.setdefault("is_hard_deleted", False)
            row["_loaded_at"] = loaded_at
            for k in ("published_at", "created_at", "updated_at", "deleted_at"):
                if row.get(k) is not None and hasattr(row[k], "isoformat"):
                    row[k] = row[k].isoformat()
            payload.append(row)
        errors = client.insert_rows_json(f"{project}.news_raw.articles_raw", payload)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    def insert_dq_scorecard(scorecard: dict) -> None:
        _ensure_schema()
        client = _bq_client()
        project = _project()
        row = {
            "run_at": dt.datetime.utcnow().isoformat(),
            "data_source": "lake",
            "table_name": scorecard.get("table_name"),
            "completeness": scorecard.get("completeness"),
            "accuracy": scorecard.get("accuracy"),
            "consistency": scorecard.get("consistency"),
            "timeliness": scorecard.get("timeliness"),
            "validity": scorecard.get("validity"),
            "uniqueness": scorecard.get("uniqueness"),
            "overall_score": scorecard.get("overall_score"),
        }
        errors = client.insert_rows_json(f"{project}.news_dwh.dq_scorecard", [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    def get_dq_history(limit: int = 21) -> list[float]:
        client = _bq_client()
        project = _project()
        query = (
            f"SELECT overall_score FROM `{project}.news_dwh.dq_scorecard` "
            f"ORDER BY run_at DESC LIMIT {int(limit)}"
        )
        return [r["overall_score"] for r in client.query(query).result() if r["overall_score"] is not None]

    def mark_hard_deleted(deleted_rows: list[dict]) -> None:
        if not deleted_rows:
            return
        client = _bq_client()
        project = _project()
        for row in deleted_rows:
            query = (
                f"UPDATE `{project}.news_raw.articles_raw` "
                f"SET is_hard_deleted = TRUE, deleted_at = @deleted_at "
                f"WHERE id = @article_id"
            )
            client.query(
                query,
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("deleted_at", "TIMESTAMP", row["deleted_at"]),
                    bigquery.ScalarQueryParameter("article_id", "INT64", row["article_id"]),
                ]),
            ).result()

    def get_category_totals() -> dict:
        """All-time article count per category, from the dbt-built mart (post-dedup)."""
        client = _bq_client()
        project = _project()
        query = f"SELECT category, COUNT(*) AS c FROM `{project}.news_mart.fct_articles` GROUP BY category"
        try:
            return {r["category"]: r["c"] for r in client.query(query).result()}
        except Exception:
            return {}  # mart may not exist yet on a brand-new project

else:
    # Local Postgres — thin wrappers around include/db.py, same behavior
    # as before this module existed.
    from db import get_conn
    from db import get_watermark as _pg_get_watermark
    from db import set_watermark as _pg_set_watermark

    def get_watermark(pipeline_name: str) -> dt.datetime:
        return _pg_get_watermark(pipeline_name)

    def set_watermark(pipeline_name: str, ts: dt.datetime) -> None:
        _pg_set_watermark(pipeline_name, ts)

    def insert_articles_raw(rows: list[dict]) -> None:
        with get_conn("dwh") as conn:
            with conn.cursor() as cur:
                for r in rows:
                    row = dict(r)
                    row.setdefault("is_hard_deleted", False)
                    cur.execute(
                        """
                        INSERT INTO news_raw.articles_raw
                        (id, title, content, author_id, author_name, category,
                         published_at, created_at, updated_at, deleted_at, is_hard_deleted)
                        VALUES (%(id)s, %(title)s, %(content)s, %(author_id)s, %(author_name)s,
                                %(category)s, %(published_at)s, %(created_at)s, %(updated_at)s,
                                %(deleted_at)s, %(is_hard_deleted)s)
                        """,
                        row,
                    )

    def insert_dq_scorecard(scorecard: dict) -> None:
        with get_conn("dwh") as conn:
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
                    scorecard,
                )

    def get_dq_history(limit: int = 21) -> list[float]:
        with get_conn("dwh") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT overall_score FROM news_dwh.dq_scorecard ORDER BY run_at DESC LIMIT %s",
                    (limit,),
                )
                return [r["overall_score"] for r in cur.fetchall() if r["overall_score"] is not None]

    def mark_hard_deleted(deleted_rows: list[dict]) -> None:
        if not deleted_rows:
            return
        with get_conn("dwh") as conn:
            with conn.cursor() as cur:
                for row in deleted_rows:
                    cur.execute(
                        """
                        UPDATE news_raw.articles_raw
                        SET is_hard_deleted = TRUE, deleted_at = %(deleted_at)s
                        WHERE id = %(article_id)s
                        """,
                        row,
                    )

    def get_category_totals() -> dict:
        """All-time article count per category, from the dbt-built mart (post-dedup)."""
        try:
            with get_conn("dwh") as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT category, COUNT(*) AS c FROM news_mart.fct_articles GROUP BY category")
                    return {r["category"]: r["c"] for r in cur.fetchall()}
        except Exception:
            return {}  # mart may not exist yet on a brand-new setup
