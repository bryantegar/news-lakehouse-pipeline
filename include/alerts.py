"""
alerts.py — monitoring/alerting hook for Airflow DAGs.

Kept intentionally simple: one function, one env var. If
SLACK_WEBHOOK_URL isn't set (the default for local dev), this silently
does nothing instead of raising — a demo environment shouldn't need a
real Slack workspace to run.
"""
import os
import logging
import requests

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """Airflow on_failure_callback signature: takes the task context dict."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url

    message = (
        f":red_circle: *Airflow task failed*\n"
        f"DAG: `{dag_id}`  Task: `{task_id}`  Run: `{run_id}`\n"
        f"<{log_url}|View logs>"
    )

    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL not set — skipping alert. Would have sent:\n%s", message)
        return

    try:
        requests.post(webhook_url, json={"text": message}, timeout=5)
    except Exception as e:
        log.error("Failed to send Slack alert: %s", e)
