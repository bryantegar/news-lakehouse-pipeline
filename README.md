# news-lakehouse-pipeline

A lakehouse-style ETL pipeline for a news portal: **GCS-style landing
zone → PySpark batch cleaning → BigQuery-style warehouse (star schema)
→ dbt marts → REST API (Data-as-a-Service)**, orchestrated by Airflow,
with an enforced Data Quality scorecard, Slack alerting, and an optional
data-catalog (DataHub) integration.

Everything below runs fully offline on a laptop (fixture data + MinIO
standing in for GCS + local Postgres standing in for BigQuery). Swapping
in real GCP services is a config change, not a rewrite — see
[`docs/architecture.md`](docs/architecture.md).

## Why this exists

This is a rebuild of an earlier ETL assessment project
([`kumparan-de-final`](https://github.com/bryantegar/kumparan-de-v2)),
extended to cover topics that project didn't touch yet: a proper lake
layer, PySpark batch processing, SCD Type 2 history, a Data-as-a-Service
API layer, alerting, and metadata governance — while reusing the real
kumparan.com GraphQL scraper from that project and keeping the parts
that already worked well (watermark incremental loads, hard-delete
reconciliation via trigger, dbt-tested star schema).

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full diagram
and the local-to-cloud swap checklist.

```
kumparan.com (or fixture) -> Source DB (Postgres)          [news_scraper_hourly]
   -> Lake landing (MinIO) -> PySpark clean -> Lake cleaned  [news_ingestion_hourly]
   -> DWH raw (Postgres) -> dbt snapshot (SCD2) -> dbt run (staging/intermediate/marts)
   -> DQ scorecard -> REST API (FastAPI, read-only)
```

Three DAGs. The first two run hourly and are chained
(`news_scraper_hourly` triggers `news_ingestion_hourly` on completion,
instead of racing each other on the same clock); the third runs daily:

1. **`news_scraper_hourly`** — scrapes kumparan.com (or generates fixture
   data) and UPSERTs into the source OLTP DB. This is what makes the
   source DB / hard-delete trigger pattern meaningful — the scraper is a
   real source system, not just a passthrough into the lake.
   <img width="1917" height="871" alt="Screenshot 2026-07-18 194123" src="https://github.com/user-attachments/assets/b126dec4-9fed-43ec-bb00-40f55f93824d" />

2. **`news_ingestion_hourly`** — reads new/updated rows from the source
   DB (watermark on `updated_at`), lands them in the lake, cleans with
   PySpark, loads the DWH, then runs `dbt run && dbt snapshot && dbt test`.
   <img width="1917" height="868" alt="image" src="https://github.com/user-attachments/assets/4f26b582-b300-4796-80a2-94f14df1dd0a" />

3. **`news_hard_delete_sync`** — runs daily, not hourly. Reconciles hard
   deletes the watermark-based hourly job can never see: a trigger on the
   source DB logs every `DELETE FROM articles` to `article_deleted`, and
   this DAG marks the matching DWH rows as deleted instead of leaving
   stale "ghost" rows behind.
   <img width="1917" height="870" alt="image" src="https://github.com/user-attachments/assets/c9dfcf2e-c443-4303-8430-1c62eb47e9d9" />


## Real scraper vs fixture

`SCRAPER_MODE` env var (set in `docker-compose.yml`):

- `fixture` (default) — Faker-generated fake articles, zero external
  calls, safe for `make up` / first-time testing.
- `live` — the real kumparan.com GraphQL scraper (`include/scraper.py`),
  ported from the earlier `kumparan-de-final` submission. Respects rate
  limiting (1.2s between requests) and retries with backoff. Switch to
  it once the base stack works with fixtures.

## REST API (Data-as-a-Service)

`api/` is a small FastAPI service, read-only, sitting on top of the DWH
marts — the "bridge between big data systems and other product/tech
teams" pattern: consumers hit a documented HTTP contract instead of
running ad-hoc SQL against the warehouse. It starts automatically with
`make up` / `docker compose up -d --build`.

```bash
open http://localhost:8000/docs   # interactive Swagger / OpenAPI spec
```

Endpoints: `GET /articles`, `GET /articles/{id}`, `GET /authors`,
`GET /authors/{id}`, `GET /dq-scorecard/latest`,
`GET /dq-scorecard/history`, `GET /health`.

Built with the repository pattern (`api/repositories.py`) specifically
so the data-access layer is swappable: routes depend on an
`ArticleRepository` abstraction, not on Postgres directly — adding
`BigQueryArticleRepository` later is a few lines in
`api/dependencies.py`, no route changes.

## Monitoring / alerting

Every DAG has a Slack `on_failure_callback` (`include/alerts.py`). Unset
`SLACK_WEBHOOK_URL` (the local default) — it just logs what it would
have sent instead of failing the task. Set it to a real Slack incoming
webhook to get actual notifications, including dbt test failures (a bad
DQ batch fails `dbt test`, which fails the task, which fires the alert).

## Stack

| Layer | Local | Production target |
|---|---|---|
| Orchestration | Airflow (LocalExecutor) | same |
| Scraper | kumparan.com GraphQL (or Faker fixture) | same |
| Source of truth | Postgres | same |
| Object storage / lake | MinIO | Google Cloud Storage |
| Batch processing | PySpark (`local[*]`) | PySpark on Dataproc/K8s |
| Warehouse | Postgres | BigQuery |
| Transform | dbt-core + dbt-postgres | dbt-core + dbt-bigquery |
| History (SCD2) | dbt snapshots | same |
| Data quality | PySpark checks + dbt tests | same |
| Catalog / lineage | DataHub (optional, separate compose) | same |
| API / DaaS | FastAPI (repository pattern) | same |
| Alerting | Slack webhook (optional) | same |

## Getting started

Requirements: Docker + Docker Compose, ~4GB RAM free.

```bash
git clone <this-repo>
cd news-lakehouse-pipeline
make up          # builds the custom Airflow image (includes Java + PySpark) and starts everything
```

- Airflow UI: http://localhost:8081 (`admin` / `admin`)
- MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`)

Unpause both DAGs, then trigger the scraper (it triggers ingestion
automatically once it's done):

```bash
make airflow-trigger
```

Check results:

```bash
docker compose exec postgres-dwh psql -U news -d news_dwh -c "select * from news_mart.fct_articles limit 5;"
docker compose exec postgres-dwh psql -U news -d news_dwh -c "select * from news_mart.mart_dq_scorecard order by run_at desc limit 3;"
docker compose exec postgres-dwh psql -U news -d news_dwh -c "select article_id, title, category, dbt_valid_from, dbt_valid_to from news_intermediate.snap_articles order by article_id limit 10;"
```

## Running dbt directly

```bash
make dbt-run
make dbt-test
make dbt-docs   # generates lineage docs, also feeds the optional DataHub ingestion
```

## Project layout

```
dags/
  news_scraper_hourly.py     scrape kumparan.com (or fixture) -> upsert source DB, then triggers ingestion
  news_ingestion_hourly.py   source DB -> lake -> PySpark clean -> DWH -> dbt snapshot/run/test
  news_hard_delete_sync.py   daily reconciliation via the source-DB delete trigger
include/
  db.py                      Postgres connection helpers (source + DWH)
  lake_storage.py             MinIO/GCS wrapper
  scraper.py                  real kumparan GraphQL client + fixture generator
  utils.py                    channel->category mapping + misc transform helpers
  alerts.py                   Slack on_failure_callback
  spark_jobs/                 PySpark batch cleaning job
  datahub_ingest.py           optional metadata-to-catalog push
api/                         read-only FastAPI Data-as-a-Service layer (repository pattern)
dbt/
  models/                     staging -> intermediate -> marts, schema tests, 2 custom tests
  snapshots/                  SCD Type 2 on article title/category (dbt snapshot)
sql/                          source + DWH DDL (runs automatically via docker-compose)
docs/
  architecture.md              diagram + local-to-cloud swap checklist + curriculum coverage map
  data_dictionary.md           table-by-table column reference
  cost_optimization.md         cloud cost decisions and reasoning
```

## Data quality

Six dimensions (completeness, uniqueness, validity, accuracy,
timeliness, consistency) are each enforced somewhere concrete — table
in [`docs/architecture.md`](docs/architecture.md#data-quality-where-each-dimension-is-actually-enforced).
A batch that drops the DQ score below 0.85 fails `dbt test`, it doesn't
silently reach the marts.

## Coverage vs. the bootcamp curriculum

Most modules (SQL, DDL, DWH modeling, dbt, ETL, Airflow, Docker, GCS,
BigQuery, PySpark, Data Quality, portfolio structure) map directly onto
something in this repo — see the table in
[`docs/architecture.md`](docs/architecture.md#bootcamp-curriculum-coverage).
Hadoop/MapReduce and a real multi-node Spark cluster are the two
intentionally-out-of-scope items — reasoning in that doc.

![CI](https://github.com/bryantegar/news-lakehouse-pipeline/actions/workflows/ci.yml/badge.svg)

## What's intentionally not included

- **CDC** (Debezium/WAL streaming) — watermark + delete-trigger covers
  this data's actual change patterns without a Kafka dependency. See
  architecture doc for the trade-off reasoning.
- **Full DataHub stack** — not bundled in the default `docker-compose.yml`
  because it needs Kafka + Elasticsearch + MySQL and isn't worth the RAM
  for a demo. `include/datahub_ingest.py` documents how to wire it up.

## Status / next steps

- [ ] CI (GitHub Actions): lint + `dbt test` on push
- [ ] Slack alert on DAG/DQ failure
- [ ] Extra kumparan fields (engagement stats, publisher info) — dropped
      for now to keep the schema aligned across scraper/Spark/dbt; add
      them to `sql/01_source_ddl.sql` + `include/scraper.py` + the dbt
      models together if you want them
