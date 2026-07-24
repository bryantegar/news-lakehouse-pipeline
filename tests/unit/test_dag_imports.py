"""
test_dag_imports.py

Regression guard for "Broken DAG" failures — a DAG module that raises
on import (missing function in include/, typo'd name, syntax error)
doesn't fail loudly; it silently disappears from the Airflow UI with a
collapsed error banner instead, and any DAG that triggers it
(TriggerDagRunOperator) fails with a confusing DagNotFound error with
no obvious link back to the real cause.

This is exactly how `notify_dq_anomaly` shipped broken previously:
news_ingestion_hourly.py imported it from include/alerts.py before the
function existed there. `airflow dags list-import-errors` would have
caught it, but nothing ran that command until someone noticed the
DAG missing from the UI. Running the same check here, in CI, on every
push, closes that gap.

Requires the airflow package (see .github/workflows/ci.yml — this
suite runs inside the same image built from Dockerfile.airflow, not
on a bare runner).
"""
import importlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

DAG_MODULES = [
    "news_ingestion_hourly",
    "news_scraper_hourly",
    "news_hard_delete_sync",
]


@pytest.mark.parametrize("module_name", DAG_MODULES)
def test_dag_module_imports_without_error(module_name):
    importlib.import_module(module_name)


def test_dag_ids_match_expected_names():
    """
    Each DAG factory function calls itself at module level (e.g.
    `news_ingestion_hourly()`), which registers the DAG under the
    dag_id set in the @dag(...) decorator. Import-only success (above)
    doesn't prove the DAG actually registered — this confirms it did.
    """
    from airflow.models import DagBag

    # DagBag re-parses every .py file under dags/ using Airflow's own
    # loader, which is the same mechanism the scheduler and webserver
    # use — closer to production behavior than importlib alone.
    dagbag = DagBag(dag_folder=str(REPO_ROOT / "dags"), include_examples=False)

    assert not dagbag.import_errors, (
        f"DAG import errors found: {dagbag.import_errors}"
    )
    for expected_dag_id in DAG_MODULES:
        assert expected_dag_id in dagbag.dags, (
            f"Expected DAG id '{expected_dag_id}' not found in DagBag. "
            f"Found: {list(dagbag.dags)}"
        )
