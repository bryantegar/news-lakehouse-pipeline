# Architecture

```
 kumparan.com GraphQL (or Faker fixture)
        │  news_scraper_hourly: UPSERT
        ▼
 Source DB (Postgres, OLTP)
        │  news_ingestion_hourly: extract (watermark on updated_at)
        ▼
 Lake / landing (MinIO ~ GCS)      raw, as-scraped parquet
        │  PySpark batch job (clean_transform.py)
        │  - dedupe, validity/consistency fixes
        │  - computes 6-dimension DQ scorecard
        ▼
 Lake / cleaned (MinIO ~ GCS)      silver parquet
        │  load
        ▼
 DWH raw schema (Postgres ~ BigQuery)   news_raw.articles_raw
        │  dbt snapshot  (SCD2 history: snap_articles)
        │  dbt run  (staging -> intermediate -> marts)
        ▼
 DWH marts (star schema)           fct_articles, dim_author, dim_date,
                                    mart_dq_scorecard
        │  dbt docs generate -> DataHub ingest (optional)
        ▼
 Data catalog / lineage (DataHub)
```

Daily side pipeline: `news_hard_delete_sync` reconciles hard deletes that
the watermark-based hourly job can never see (trigger-logged in
`article_deleted`).

## Swap-to-production checklist

| Local (this repo) | Production | Where to change |
|---|---|---|
| MinIO | Google Cloud Storage | `include/lake_storage.py` — swap boto3 client for `google-cloud-storage` |
| Postgres DWH | BigQuery | `dbt/profiles.yml.example` — switch target to `type: bigquery` (models unchanged) |
| Fixture scraper | Real news API/GraphQL | `include/scraper.py::_fetch_live()` |
| Local Spark (`local[*]`) | YARN/Dataproc/K8s cluster | `include/spark_jobs/clean_transform.py::cluster_config()` |
| No governance stack running | DataHub | `include/datahub_ingest.py` + a separate DataHub docker-compose (not bundled by default — see README) |

## Bootcamp curriculum coverage

| Bootcamp module | Where it lives in this repo |
|---|---|
| SQL (DDL/DML, CTEs, window functions) | `sql/01_source_ddl.sql`, `sql/02_dwh_ddl.sql`, all dbt model SQL |
| Data Warehouse modeling (star schema, Kimball, Medallion) | `dbt/models/marts` (fct/dim), raw→intermediate→mart layering |
| **SCD Type 2** | `dbt/snapshots/snap_articles.sql` — tracks title/category history |
| ETL / ELT | `dags/news_scraper_hourly.py`, `dags/news_ingestion_hourly.py` |
| Web Scraping | `include/scraper.py` (`_fetch_live`) — real kumparan.com GraphQL client |
| Data Quality (6 dimensions, profiling, GX-style checks) | `include/spark_jobs/clean_transform.py::dq_scorecard`, dbt tests, table below |
| dbt (models, tests, docs, snapshots) | `dbt/` in full |
| Airflow (DAGs, dynamic tasks, monitoring) | `dags/` — TaskFlow API, `TriggerDagRunOperator` chaining |
| Docker & Bash | `Dockerfile.airflow`, `docker-compose.yml`, `Makefile` |
| Google Cloud Storage | `include/lake_storage.py` (MinIO locally, GCS swap documented) |
| BigQuery / Data Warehousing on Cloud | `dbt/profiles.yml.example` (Postgres locally, BigQuery swap documented) |
| PySpark (SparkSession, DataFrame ops, batch processing) | `include/spark_jobs/clean_transform.py` |
| Data Governance (catalog, lineage) | `include/datahub_ingest.py` (optional, documented, not bundled by default) |
| How to Build a DE Portfolio | This README's structure — 5-min-read top section, architecture diagram, visible tests, one-command repro |

Not covered, on purpose:

- **Hadoop / MapReduce** — conceptual only; nothing in this project's
  data volume needs HDFS-style distributed storage, and standing up a
  Hadoop cluster for a demo this size would be architecture cosplay,
  not a real requirement. Worth knowing the concepts, not worth building.
- **Real multi-node Spark cluster** — `local[*]` is enough for this data
  size; `cluster_config()` in the Spark job documents what changes for
  YARN/Dataproc/K8s when the data actually grows past what one laptop
  handles comfortably.

## Why watermark + trigger, not CDC

A full CDC setup (Debezium, WAL streaming) is overkill for this data
volume and adds a Kafka dependency to a portfolio project. Watermark
covers 95% of change tracking; the delete-trigger table covers the one
case watermarks structurally can't (hard deletes). This is a documented
trade-off, not an oversight — call it out like this in interviews.

## Data Quality: where each dimension is actually enforced

| Dimension | Enforced in | Mechanism |
|---|---|---|
| Completeness | dbt (`stg_articles`) | `not_null` tests |
| Uniqueness | PySpark + dbt | `dropDuplicates` in Spark, `unique` test in dbt |
| Validity | PySpark + dbt | category allow-list in Spark, `accepted_values` warn-test in dbt |
| Accuracy | PySpark + dbt custom test | `published_at <= updated_at` check, `assert_no_future_published_articles` |
| Timeliness | dbt source freshness | `freshness` block on the `news_raw.articles_raw` source |
| Consistency | dbt (`int_articles_deduped`) | dedupe by `article_id`, whitespace normalization |

The PySpark job's 6-dimension score is also written to
`news_dwh.dq_scorecard` every run and gated in CI via
`assert_dq_score_above_threshold` — a bad batch fails `dbt test` instead
of silently landing in the marts.
