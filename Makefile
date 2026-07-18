.PHONY: up down logs ps dbt-run dbt-test dbt-snapshot dbt-docs airflow-trigger reset

up:
	docker compose up -d --build
	@echo "Airflow UI:  http://localhost:8081  (admin/admin)"
	@echo "API docs:    http://localhost:8000/docs"
	@echo "MinIO UI:    http://localhost:9001  (minioadmin/minioadmin)"

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

ps:
	docker compose ps

# Trigger the scraper DAG manually — it triggers the ingestion DAG when done,
# instead of waiting for the hourly schedule
airflow-trigger:
	docker compose exec airflow-webserver airflow dags unpause news_scraper_hourly
	docker compose exec airflow-webserver airflow dags unpause news_ingestion_hourly
	docker compose exec airflow-webserver airflow dags unpause news_hard_delete_sync
	docker compose exec airflow-webserver airflow dags trigger news_scraper_hourly

dbt-snapshot:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt snapshot"

dbt-run:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run"

dbt-test:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test"

dbt-docs:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt docs generate"
	@echo "Then: docker compose exec airflow-webserver bash -c 'cd /opt/airflow/dbt && dbt docs serve --port 8082'"
