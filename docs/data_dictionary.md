# Data Dictionary

Explicit table-by-table reference — what each table is, where it sits
in the pipeline, and every column's meaning. Complements the diagram in
[`architecture.md`](architecture.md); that doc shows the *flow*, this
one shows the *structure*.

## Source DB (`postgres-source`, OLTP)

### `articles`
The system of record for scraped articles. Written only by
`news_scraper_hourly`; never written to by the ETL or the API.

| Column | Type | Meaning |
|---|---|---|
| `id` | BIGINT, PK | External article ID from kumparan.com (or fixture) |
| `title` | TEXT | Article headline |
| `content` | TEXT | Lead text + meta description (kumparan's public API doesn't expose full body text) |
| `author_id` | BIGINT | External author ID |
| `author_name` | TEXT | Author display name, nullable (some articles are unattributed) |
| `category` | TEXT | Channel/category slug, e.g. `nasional`, `ekonomi` |
| `published_at` | TIMESTAMP | When the article went live |
| `created_at` | TIMESTAMP | When this row was first scraped |
| `updated_at` | TIMESTAMP | Last time this row changed — the watermark column for incremental extraction |
| `deleted_at` | TIMESTAMP | Soft-delete timestamp reported by the source, if any |

### `article_deleted`
Populated automatically by a trigger (`trg_article_hard_delete`) that
fires on every `DELETE FROM articles`. Exists because a hard delete
removes the row entirely — `updated_at`-based watermarking can never
see it, so this table is the only record that it happened.

| Column | Type | Meaning |
|---|---|---|
| `article_id` | BIGINT, PK | ID of the row that was hard-deleted |
| `deleted_at` | TIMESTAMP | When the delete happened |

## Lake (`MinIO` ~ GCS)

| Path | Format | Contents |
|---|---|---|
| `landing/articles/dt=YYYY-MM-DD/HHMMSS.parquet` | Parquet | Raw extract from `articles`, unmodified |
| `cleaned/articles/dt=YYYY-MM-DD/HHMMSS_*.parquet` | Parquet | Same rows after the PySpark cleaning job (dedupe, category validity, whitespace normalization) |

## DWH (`postgres-dwh` ~ BigQuery)

### `news_raw.articles_raw`
Append-only landing table for the DWH — one row per (article, load), not
deduplicated. This is the audit trail: if a downstream mart ever looks
wrong, this table shows exactly what was loaded and when.

### `news_dwh.etl_watermark`
One row per pipeline (`kumparan_scraper`, `articles_etl_hourly`),
tracking how far each has progressed. Read/written by
`include/db.py::get_watermark` / `set_watermark`.

### `news_dwh.dq_scorecard`
One row per PySpark run, six DQ dimension scores (0–1) plus an overall
average. Source for `news_mart.mart_dq_scorecard`.

### `news_intermediate.stg_articles`
1:1 staging view over `articles_raw`. Light casting/renaming only, no
business logic — the "don't repeat yourself" layer every other model
builds on.

### `news_intermediate.int_articles_deduped`
Latest version of each article by `article_id` (dbt `row_number()`
dedup), plus derived fields (`published_date`, `content_length`).

### `news_intermediate.int_author_activity`
Per-author rollup (article count, first/last published date) — computed
once here instead of recomputed inside `dim_author`.

### `news_intermediate.snap_articles`
SCD Type 2 history of `title` and `category` per article, via `dbt
snapshot`. Columns `dbt_valid_from` / `dbt_valid_to` mark each version's
active window — query `WHERE dbt_valid_to IS NULL` for the current
version, or a specific timestamp for "what did this look like on date X".

### `news_mart.fct_articles`
Fact table, grain = one row per article. FKs to `dim_author` and
`dim_date`.

### `news_mart.dim_author` / `news_mart.dim_date`
Standard dimension tables for the star schema.

### `news_mart.mart_dq_scorecard`
BI-facing exposure of `news_dwh.dq_scorecard` — what the DQ scorecard
dashboard slide from the bootcamp material would query directly.

## API layer (read-only, `api/`)

Exposes `news_mart.fct_articles`, `news_mart.dim_author`, and
`news_mart.mart_dq_scorecard` as JSON over HTTP. Full request/response
schema: `http://localhost:8000/docs` (auto-generated OpenAPI spec, see
`api/schemas.py`). The API never writes — all mutation happens through
the Airflow pipeline.
