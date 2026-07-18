# Cost Optimization

Local dev has no cloud bill, so this doc is the actual deliverable for
that part of the job description — what would change, and why, once
this runs on real GCS + BigQuery.

## Storage (GCS)

| Decision | Why |
|---|---|
| Partition landing/cleaned prefixes by `dt=YYYY-MM-DD/HH` | Lets any reprocessing job or lifecycle rule target a narrow date range instead of scanning the whole bucket |
| Lifecycle rule: move `landing/` objects to Nearline after 30 days, Coldline after 90 | Raw landing data is rarely re-read after the cleaned/DWH copy exists — no reason to pay Standard-class rates for it indefinitely |
| Parquet (not JSON/CSV) everywhere in the lake | Columnar + compressed: smaller storage footprint and cheaper/faster scans for any engine reading it (Spark, BigQuery external tables) |

## Compute (BigQuery)

| Decision | Why |
|---|---|
| Partition `fct_articles` by `date_day`, cluster by `category` | BigQuery bills by bytes scanned — a query filtered to a date range and category only scans the relevant partitions/blocks instead of the whole table |
| `dbt run` materializes marts as tables, not views, on top of already-filtered intermediate models | Recomputing full aggregations on every downstream query (view semantics) costs more than paying once for a table build |
| `dbt source freshness` + `dbt test` fail fast on bad batches | A bad batch caught at the DWH-raw layer never gets multiplied through every downstream mart rebuild |
| On-demand pricing (not a reserved slot commitment) at this data volume | Slot reservations only pay off past a query-volume threshold this project doesn't reach — reassess if/when concurrent query volume grows |

## Compute (PySpark / Dataproc, if the batch job ever outgrows local `[*]`)

| Decision | Why |
|---|---|
| Ephemeral clusters per job, not an always-on cluster | An hourly batch job that runs in under a minute doesn't justify paying for idle cluster time between runs |
| `spark.dynamicAllocation.enabled=true` | Scales executors to the actual data volume of each run instead of a fixed worst-case size |

## Orchestration (Airflow)

| Decision | Why |
|---|---|
| `max_active_runs=1` on every DAG | Prevents overlapping runs from doubling compute cost if a run takes longer than the schedule interval |
| Watermark-based incremental extraction, not full reload | Every run processes only what changed, not the full historical dataset — this is the single biggest cost lever in the whole pipeline |

## Monitoring cost itself

Not built here (would need real GCP billing export access), but the
concrete next step: export GCP billing data to BigQuery, build a small
dbt model + Metabase/Looker Studio dashboard tracking cost per
pipeline run — same DQ-scorecard pattern already used in this project,
applied to cost instead of data quality.
