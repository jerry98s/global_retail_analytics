import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestration.airflow.plugins import catalog_dependency as cd

pytestmark = pytest.mark.unit

_DAGS = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_scheduled_run_waits_for_the_catalog_run_of_the_same_day() -> None:
    finance = SimpleNamespace(run_type="scheduled", start_date=_utc(2026, 11, 22, 0, 15))
    assert cd.catalog_logical_date(_utc(2026, 11, 21, 0, 15), dag_run=finance) == _utc(2026, 11, 21)


def test_manual_run_waits_for_the_catalog_run_that_started_last_midnight() -> None:
    restatement = SimpleNamespace(run_type="manual", start_date=_utc(2026, 11, 22, 9, 30))
    assert cd.catalog_logical_date(_utc(2026, 11, 22, 9, 30), dag_run=restatement) == _utc(2026, 11, 21)


def test_schedules_put_the_catalog_run_before_finance() -> None:
    catalog = (_DAGS / f"{cd.CATALOG_DAG_ID}.py").read_text(encoding="utf-8")
    warehouse = (_DAGS / "warehouse_daily_batch_pipeline.py").read_text(encoding="utf-8")
    assert 'schedule_interval="0 0 * * *"' in catalog
    assert re.search(r'schedule_interval\s*=\s*"15 1 \* \* \*"', warehouse)
    assert f'task_id="{cd.CATALOG_PUBLISH_TASK_ID}"' in catalog
    assert cd.WAIT_TIMEOUT == timedelta(hours=2)


def test_finance_waits_before_the_wap_clone_without_failing_on_a_late_catalog() -> None:
    warehouse = (_DAGS / "warehouse_daily_batch_pipeline.py").read_text(encoding="utf-8")
    sensor = warehouse.split("ExternalTaskSensor(", 1)[1]
    assert "soft_fail         = True" in sensor
    assert "external_dag_id   = CATALOG_DAG_ID" in sensor
    assert "metadata_start >> wait_for_catalog >> catalog_publish_check >> wap_clone" in warehouse
    assert "trigger_rule    = TriggerRule.ALL_DONE" in warehouse.split('task_id         = "catalog_publish_check"')[1]


def test_stale_message_names_the_missing_publish() -> None:
    subject, message = cd.stale_catalog_message("skipped", "scheduled__2026-11-21T00:15:00+00:00")
    assert "dim_product not refreshed" in subject
    assert "skipped" in message
    assert f"{cd.CATALOG_DAG_ID}.{cd.CATALOG_PUBLISH_TASK_ID}" in message
