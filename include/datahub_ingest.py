"""
datahub_ingest.py — pushes dbt's catalog + lineage metadata into DataHub.

This is the "Ingesting Metadata to DataHub" piece of the Data Governance
material. It's kept OUT of the default docker-compose because a full
DataHub stack (Kafka, Elasticsearch, MySQL, GMS...) is heavy for a
laptop demo. To use it:

    1. docker compose -f docker-compose.datahub.yml up -d   (see README)
    2. cd dbt && dbt docs generate                            (produces catalog.json/manifest.json)
    3. python datahub_ingest.py

It relies on `acryl-datahub`'s dbt source, which reads dbt's own
manifest/catalog artifacts — so no metadata is hand-maintained here;
it's generated straight from the dbt project, same as a real data
catalog should be.
"""
import subprocess

RECIPE = """
source:
  type: dbt
  config:
    manifest_path: "./dbt/target/manifest.json"
    catalog_path: "./dbt/target/catalog.json"
    target_platform: postgres

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
"""


def main():
    with open("datahub_dbt_recipe.yml", "w") as f:
        f.write(RECIPE)
    subprocess.run(["datahub", "ingest", "-c", "datahub_dbt_recipe.yml"], check=True)


if __name__ == "__main__":
    main()
