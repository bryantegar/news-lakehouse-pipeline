# DataHub — Metadata Governance & Lineage

Optional data-catalog integration for this project. Once running, it
shows every table across the three medallion layers (news_raw →
news_dwh → news_mart), the dbt lineage between them (which staging
model feeds which mart), column-level descriptions, and dbt test
results — searchable in one place instead of scattered across
`schema.yml`, the source DB, and this README.

## Why DataHub runs as its own stack, not inside this project's docker-compose.yml

DataHub's full stack is Kafka + Zookeeper + Elasticsearch + MySQL +
two DataHub services (GMS + frontend) — heavy enough that DataHub's
own docs recommend at least 7GB of free RAM just for it, on top of
whatever this project's Airflow/Postgres/MinIO/Metabase stack is
already using.

More importantly: DataHub's quickstart `docker-compose.yml` is
explicitly CLI-managed — their own docs state it's fetched fresh from
GitHub on each `datahub docker quickstart` run, version-matched to
whichever DataHub release you're pulling. Hand-copying that file into
this project's `docker-compose.yml` would drift out of sync the next
time DataHub ships a new release and this project's copy doesn't
follow. Running it through their CLI instead means it's always
correctly matched — same reasoning as not vendoring a library's source
instead of installing it as a dependency.

## Setup

```bash
# 1. Install the DataHub CLI + connector plugins used by the recipes below
make -f datahub/Makefile datahub-install

# 2. Start the DataHub stack (Kafka, ES, MySQL, GMS, frontend).
#    First run pulls several images — expect a few minutes.
make -f datahub/Makefile datahub-up

# 3. Once it's up, log in at http://localhost:9002 (default: datahub / datahub)

# 4. Push this project's metadata in
make -f datahub/Makefile datahub-ingest-all
```

Re-running `datahub-ingest-all` any time is safe — DataHub upserts
entities by URN, so it won't create duplicates, just refreshes
whatever changed (new tables, updated dbt docs, etc.).

## What gets ingested

| Recipe | Source | What it adds in DataHub |
|---|---|---|
| `recipes/source_postgres.yml` | `postgres-source` (`news_source`) | Raw OLTP table schema |
| `recipes/dwh_postgres.yml` | `postgres-dwh` (`news_raw`/`news_dwh`/`news_mart`) | Warehouse table schema across all three layers |
| `recipes/dbt.yml` | dbt build artifacts | Model lineage, column descriptions, test results |

Swapping `DWH_BACKEND` to `bigquery` in production means swapping
`dwh_postgres.yml`'s `source.type` to `bigquery` with matching
credentials — the recipe shape stays the same, same pattern this
project already uses for `include/dwh.py`'s backend switch.

## Shutting it down

```bash
make -f datahub/Makefile datahub-down
```

This stops the containers but keeps DataHub's own data (under
`~/.datahub/`) so ingested metadata survives a restart.
