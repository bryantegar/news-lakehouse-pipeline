-- ============================================================
-- DWH — local Postgres standing in for BigQuery.
--
-- Naming mirrors BigQuery's Project -> Dataset -> Table hierarchy:
--   schema  news_raw          ~= BigQuery dataset "raw"
--   schema  news_intermediate ~= BigQuery dataset "intermediate"
--   schema  news_dwh          ~= BigQuery dataset "dwh" (star schema)
--   schema  news_mart         ~= BigQuery dataset "mart"
--
-- To go to production: point profiles.yml at the bigquery adapter and
-- keep every schema/model name identical — no dbt model needs to change.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS news_raw;
CREATE SCHEMA IF NOT EXISTS news_intermediate;
CREATE SCHEMA IF NOT EXISTS news_dwh;
CREATE SCHEMA IF NOT EXISTS news_mart;

-- Landing table: append-only copy of whatever the PySpark batch job
-- cleaned and wrote back from the lake (MinIO/GCS) this run.
CREATE TABLE IF NOT EXISTS news_raw.articles_raw (
    id              BIGINT,
    title           TEXT,
    content         TEXT,
    author_id       BIGINT,
    author_name     TEXT,
    category        TEXT,
    published_at    TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    deleted_at      TIMESTAMP,
    is_hard_deleted BOOLEAN DEFAULT FALSE,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    _dq_ok          BOOLEAN,
    _dq_score       NUMERIC
);

-- Incremental watermark bookkeeping (one row per pipeline).
CREATE TABLE IF NOT EXISTS news_dwh.etl_watermark (
    pipeline_name    TEXT PRIMARY KEY,
    last_updated_at  TIMESTAMP NOT NULL DEFAULT '2016-01-01'
);

INSERT INTO news_dwh.etl_watermark (pipeline_name, last_updated_at)
VALUES ('articles_etl_hourly', '2016-01-01')
ON CONFLICT (pipeline_name) DO NOTHING;

-- Data Quality scorecard — one row per DQ run per table/column,
-- mirroring the 6 dimensions from the bootcamp's Data Quality scorecard.
CREATE TABLE IF NOT EXISTS news_dwh.dq_scorecard (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    data_source     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    completeness    NUMERIC,
    accuracy        NUMERIC,
    consistency     NUMERIC,
    timeliness      NUMERIC,
    validity        NUMERIC,
    uniqueness      NUMERIC,
    overall_score   NUMERIC
);
