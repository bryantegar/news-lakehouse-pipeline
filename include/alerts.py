"""
alerts.py — monitoring/alerting hooks for Airflow DAGs.

Three channels, each fully independent and optional via env vars. None
of them are required for local dev — every send_* function silently
no-ops (and logs what it would have sent) if its channel isn't
configured, so a demo environment never needs real credentials to run.

  - Slack:    SLACK_WEBHOOK_URL
  - Telegram: TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  - Email:    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO

notify_failure() is the Airflow on_failure_callback — wire it into any
DAG's default_args. It fans out to every configured channel.
"""
import logging
import os
import smtplib
from email.mime.text import MIMEText

import requests

log = logging.getLogger(__name__)


def _send_slack(text: str) -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.info("SLACK_WEBHOOK_URL not set — skipping Slack alert.")
        return
    try:
        requests.post(webhook_url, json={"text": text}, timeout=5)
    except Exception as e:
        log.error("Failed to send Slack alert: %s", e)


def _send_telegram(text: str) -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        log.info("TELEGRAM_BOT_TOKEN/CHAT_ID not set — skipping Telegram alert.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=5,
        )
    except Exception as e:
        log.error("Failed to send Telegram alert: %s", e)


def _send_email(subject: str, body: str) -> None:
    host = os.environ.get("SMTP_HOST")
    to_addr = os.environ.get("ALERT_EMAIL_TO")
    if not host or not to_addr:
        log.info("SMTP_HOST/ALERT_EMAIL_TO not set — skipping email alert.")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = os.environ.get("SMTP_USER", "airflow@localhost")
        msg["To"] = to_addr

        with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", 587))) as server:
            server.starttls()
            user, password = os.environ.get("SMTP_USER"), os.environ.get("SMTP_PASSWORD")
            if user and password:
                server.login(user, password)
            server.sendmail(msg["From"], [to_addr], msg.as_string())
    except Exception as e:
        log.error("Failed to send email alert: %s", e)


def notify_failure(context: dict) -> None:
    """Airflow on_failure_callback signature: takes the task context dict."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url

    slack_text = (
        f":red_circle: *Airflow task failed*\n"
        f"DAG: `{dag_id}`  Task: `{task_id}`  Run: `{run_id}`\n"
        f"<{log_url}|View logs>"
    )
    telegram_text = (
        f"🔴 *Airflow task failed*\n"
        f"DAG: `{dag_id}`  Task: `{task_id}`\n"
        f"Run: `{run_id}`\n{log_url}"
    )
    email_subject = f"[news-lakehouse-pipeline] Task failed: {dag_id}.{task_id}"
    email_body = f"DAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\n\nLogs: {log_url}"

    # Email leads — the enterprise-standard channel; Slack/Telegram are
    # optional extras on top, not the primary story.
    _send_email(email_subject, email_body)
    _send_slack(slack_text)
    _send_telegram(telegram_text)


def notify_scrape_report(batch_counts: dict, total_counts: dict, run_ts) -> None:
    """
    Sent once per successful ingestion run: how many articles came in
    this run broken down by category, plus the running grand total per
    category in the warehouse. Email only by design — this is a
    routine report, not an alert, and email is the channel that reads
    naturally as a report instead of a ping.
    """
    batch_counts = batch_counts or {}
    total_counts = total_counts or {}
    if not batch_counts and not total_counts:
        return

    batch_total = sum(batch_counts.values())
    grand_total = sum(total_counts.values())

    lines = [f"Laporan scraping — {run_ts:%Y-%m-%d %H:%M UTC}", ""]
    lines.append(f"Artikel baru masuk run ini: {batch_total}")
    for category, count in sorted(batch_counts.items(), key=lambda kv: -kv[1]):
        lines.append(f"  - {category or '(tanpa kategori)'}: {count}")

    lines.append("")
    lines.append(f"Total keseluruhan di database: {grand_total}")
    for category, count in sorted(total_counts.items(), key=lambda kv: -kv[1]):
        lines.append(f"  - {category or '(tanpa kategori)'}: {count}")

    body = "\n".join(lines)
    subject = f"[news-lakehouse-pipeline] Laporan scraping — {batch_total} artikel baru, total {grand_total}"
    _send_email(subject, body)


def notify_dq_anomaly(scorecard: dict, is_anomaly: bool, z_score: float) -> None:
    """
    Separate from notify_failure: this fires on a statistical DQ
    anomaly even when the score is still above the hard 0.85 threshold
    (so dbt test still passes) — an early warning before it becomes an
    actual pipeline failure. See include/anomaly.py for the detection logic.
    """
    if not is_anomaly:
        return

    text = (
        f"DQ score anomaly detected: overall_score={scorecard['overall_score']:.3f} "
        f"(z-score={z_score:.2f} vs. the last 20 runs) for {scorecard['table_name']}. "
        f"Still above the hard failure threshold, but trending unusual — worth a look."
    )
    _send_email("[news-lakehouse-pipeline] DQ score anomaly detected", text)
    _send_slack(f":large_orange_circle: *DQ anomaly*\n{text}")
    _send_telegram(f"🟠 *DQ anomaly*\n{text}")
