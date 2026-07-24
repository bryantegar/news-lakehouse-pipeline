"""
test_alerts.py

Regression suite for include/alerts.py.

The batch_counts=None cases below reproduce the exact production
failure from 2026-07-23: when load_to_dwh_and_dq() is skipped
(no new/updated rows since the last watermark), send_scrape_report
still runs (trigger_rule="all_done") but its XCom-pulled batch_counts
argument comes back as None. notify_scrape_report crashed with
`AttributeError: 'NoneType' object has no attribute 'values'` on
`sum(batch_counts.values())`. Fixed by treating None as {} at both the
call site (dags/news_ingestion_hourly.py) and here (defense in depth,
since this function may get reused by other DAGs later).
"""
import datetime as dt

from alerts import notify_dq_anomaly, notify_scrape_report


def test_notify_scrape_report_handles_none_batch_counts(mocker):
    send_email = mocker.patch("alerts._send_email")

    notify_scrape_report(None, {"news": 10}, dt.datetime(2026, 1, 1))

    send_email.assert_called_once()


def test_notify_scrape_report_handles_none_total_counts(mocker):
    send_email = mocker.patch("alerts._send_email")

    notify_scrape_report({"news": 3}, None, dt.datetime(2026, 1, 1))

    send_email.assert_called_once()


def test_notify_scrape_report_noop_when_both_empty(mocker):
    send_email = mocker.patch("alerts._send_email")

    notify_scrape_report({}, {}, dt.datetime(2026, 1, 1))

    send_email.assert_not_called()


def test_notify_scrape_report_email_body_includes_batch_and_grand_total(mocker):
    send_email = mocker.patch("alerts._send_email")

    notify_scrape_report(
        {"news": 3, "sport": 1},
        {"news": 100, "sport": 50},
        dt.datetime(2026, 1, 1, 15, 0),
    )

    subject, body = send_email.call_args[0]
    assert "4 artikel baru" in subject
    assert "total 150" in subject
    assert "news: 3" in body
    assert "news: 100" in body


def test_notify_dq_anomaly_noop_when_not_anomaly(mocker):
    send_email = mocker.patch("alerts._send_email")
    send_slack = mocker.patch("alerts._send_slack")
    send_telegram = mocker.patch("alerts._send_telegram")

    notify_dq_anomaly({"overall_score": 0.9, "table_name": "articles"}, False, 0.5)

    send_email.assert_not_called()
    send_slack.assert_not_called()
    send_telegram.assert_not_called()


def test_notify_dq_anomaly_fires_on_all_channels_when_anomaly(mocker):
    send_email = mocker.patch("alerts._send_email")
    send_slack = mocker.patch("alerts._send_slack")
    send_telegram = mocker.patch("alerts._send_telegram")

    notify_dq_anomaly({"overall_score": 0.7, "table_name": "articles"}, True, -3.2)

    send_email.assert_called_once()
    send_slack.assert_called_once()
    send_telegram.assert_called_once()
