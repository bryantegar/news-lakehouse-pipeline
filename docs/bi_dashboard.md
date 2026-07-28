# BI Dashboard (Metabase)

Metabase turns the DQ scorecard and marts this project already builds
into something a non-technical stakeholder can actually look at,
instead of a table only accessible via SQL or the API.

## First-time setup

1. `docker compose up -d` (metabase starts alongside everything else)
2. Open **http://localhost:3000**
3. Metabase's first-run wizard:
   - Create an admin account (local only — this is your own instance)
   - **"Add your data"** → choose **PostgreSQL**
   - Connection details:
     | Field | Value |
     |---|---|
     | Display name | `news_dwh` |
     | Host | `postgres-dwh` |
     | Port | `5432` |
     | Database name | `news_dwh` |
     | Username | `news` |
     | Password | `news` |
   - Finish the wizard

Metabase auto-discovers every schema (`news_mart`, `news_intermediate`,
`news_raw`) and table in the connection — no manual schema mapping
needed.

## Dashboards worth building

Four charts is enough to make the point — this isn't about maximizing
chart count, it's about proving the DQ/mart data is actually usable
downstream.

### 1. DQ score trend (line chart)
- **Data**: `news_mart.mart_dq_scorecard`
- **X-axis**: `run_at`
- **Y-axis**: `overall_score`
- Add a goal line at `0.85` (the hard failure threshold) — makes the
  anomaly-detection story visible at a glance, not just in logs/email.

### 2. Articles by category (pie or bar chart)
- **Data**: `news_mart.fct_articles`
- **Group by**: `category`
- **Metric**: Count of rows
- This is the same breakdown the scrape-report email sends — the
  dashboard is the "always current" version of that snapshot.

### 3. Articles over time (bar chart)
- **Data**: `news_mart.fct_articles`
- **X-axis**: `date_day` (bucket by week or month once there's enough
  history)
- **Y-axis**: Count

### 4. Top authors (table or bar chart)
- **Data**: `news_mart.dim_author`
- **Sort by**: `total_articles` descending
- **Limit**: 10

## Combine into one dashboard

Metabase → **+ New** → **Dashboard** → add all four questions to it.
This single dashboard is the thing worth screenshotting for the
portfolio — it's the visual proof that the DQ framework and star schema
aren't just backend plumbing nobody ever looks at.

## Swap note (Postgres → BigQuery)

If `DWH_BACKEND=bigquery` is active, add a second Metabase data source
using the **BigQuery** connector instead of Postgres — same
`GCP_ADC_HOST_PATH` credentials already set up for the pipeline itself.
Metabase supports both connected simultaneously, so this doesn't
require removing the Postgres one.