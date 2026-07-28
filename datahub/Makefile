# datahub/Makefile
#
# DataHub is deployed as its own stack via the official `datahub` CLI
# (Kafka, Zookeeper, Elasticsearch, MySQL, GMS, frontend) rather than
# hand-copied into the project's own docker-compose.yml. DataHub's own
# docs are explicit that the quickstart compose file is CLI-managed
# and version-pinned — copying it by hand risks drifting out of sync
# with the images it pulls. Run these targets from the repo root:
#   make -f datahub/Makefile <target>

.PHONY: datahub-install datahub-up datahub-down datahub-status \
        datahub-ingest-source datahub-ingest-dwh datahub-ingest-dbt datahub-ingest-all

# Installs the DataHub CLI + the source connector plugins these
# recipes need (postgres, dbt), plus the REST sink used to talk to
# the GMS instance the CLI stands up.
datahub-install:
	pip install --break-system-packages 'acryl-datahub[postgres,dbt,datahub-rest]'

# Starts the full DataHub stack (Kafka, Zookeeper, Elasticsearch,
# MySQL, GMS, frontend). Needs ~7GB free RAM per DataHub's own docs —
# expect this to take several minutes on first run while images pull.
# UI ends up at http://localhost:9002 once it's up.
datahub-up:
	datahub docker quickstart

datahub-down:
	datahub docker quickstart --stop

datahub-status:
	datahub docker check

# Each target below pushes one recipe's metadata into the running
# DataHub instance. Safe to re-run — DataHub upserts by urn, it
# doesn't duplicate entities on repeated ingestion.
datahub-ingest-source:
	datahub ingest -c datahub/recipes/source_postgres.yml

datahub-ingest-dwh:
	datahub ingest -c datahub/recipes/dwh_postgres.yml

# Requires dbt build artifacts to exist first — `dbt compile` (or a
# full `dbt run`) must have produced dbt/target/manifest.json.
datahub-ingest-dbt:
	cd dbt && dbt compile
	datahub ingest -c datahub/recipes/dbt.yml

datahub-ingest-all: datahub-ingest-source datahub-ingest-dwh datahub-ingest-dbt
	@echo "All metadata ingested. Browse it at http://localhost:9002"
