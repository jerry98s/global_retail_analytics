"""Spark mode split, the Airflow pool, and the finance publish set (ADR-012)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.kafka.topics import PARTITIONS_BY_TOPIC, TOPIC_INVENTORY
from orchestration.airflow.plugins.spark_steps import (
    INVENTORY_HOLD_EXCLUDE,
    POOL,
    SPARK_PACKAGES,
    active_spark_steps,
    held_inventory_message,
    inventory_hold_exclude,
    spark_step,
    submit_when_idle,
)
from orchestration.airflow.plugins.wap_publish import FINANCE_SUMMARY_TABLES, WAP_TABLES
from spark.identity_resolution.kafka_frontier import target_hour_exclusive

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_JOB = _REPO / "spark/identity_resolution/identity_resolution_job.py"
_WAREHOUSE = _REPO / "orchestration/airflow/dags/warehouse_daily_batch_pipeline.py"
_MARKETING = _REPO / "orchestration/airflow/dags/marketing_hourly_customer_360_pipeline.py"


def test_job_modes_match_the_topic_contract() -> None:
    src = _JOB.read_text(encoding="utf-8")
    assert "choices=('all', 'hourly', 'inventory')" in src
    assert f"default='{TOPIC_INVENTORY}'" in src
    assert f"default={PARTITIONS_BY_TOPIC[TOPIC_INVENTORY]}" in src
    assert "--kafka-bootstrap-servers is required when inventory coverage is written" in src
    # Inventory mode returns before the identity graph. Customer 360 is hourly only.
    assert src.index("if not run_identity:") < src.index("resolution = assign_resolution(")
    assert "return ('clickstream',)" in src
    assert "return ('inventory',)" in src
    assert "'--kafka-security-protocol', default='PLAINTEXT'" in src
    assert "'--acknowledge-kafka-loss', default=''" in src
    # The previous frontier is the published one, read from the export root.
    assert "_previous_cutoff(spark, _consumer_root(args))" in src
    assert "read_published_frontier(spark, root)" in src


def test_spark_step_rejects_all_and_uses_the_pool() -> None:
    with pytest.raises(ValueError, match="hourly or inventory"):
        spark_step("all")
    hourly = spark_step("hourly")
    command = hourly["HadoopJarStep"]["Args"][-1]
    assert "--mode hourly" in command
    assert "--pos-parquet-path" in command
    assert "--kafka-bootstrap-servers" not in command
    assert "spark-sql-kafka" in SPARK_PACKAGES
    assert POOL == "emr_spark"
    inventory = spark_step("inventory")["HadoopJarStep"]["Args"][-1]
    assert "--mode inventory" in inventory
    assert "--kafka-bootstrap-servers {{ var.value.msk_bootstrap_brokers }}" in inventory
    assert "--pos-parquet-path" not in inventory


def test_inventory_step_signs_into_msk_with_iam() -> None:
    inventory = spark_step("inventory")["HadoopJarStep"]["Args"][-1]
    assert "--kafka-security-protocol SASL_SSL --kafka-sasl-mechanism AWS_MSK_IAM" in inventory
    assert "software.amazon.msk:aws-msk-iam-auth:1.1.9" in SPARK_PACKAGES
    assert "--acknowledge-kafka-loss \"{{ params.acknowledge_kafka_loss | join(',') }}\"" in inventory
    hourly = spark_step("hourly")["HadoopJarStep"]["Args"][-1]
    assert "acknowledge_kafka_loss" not in hourly


def _chain(src: str, first: str) -> str:
    """The dependency chain block that starts with ``metadata_start >> <first>``."""
    start = src.index(f"metadata_start\n        >> {first}")
    return src[start:src.index(")", start)]


def test_warehouse_dag_runs_inventory_beside_pos_and_holds_on_failure() -> None:
    src = _WAREHOUSE.read_text(encoding="utf-8")
    assert 'spark_step("inventory")' in src
    assert "submit_spark_step_task" in src and "EmrAddStepsOperator" not in src
    inventory = _chain(src, "wait_spark_idle")
    assert inventory.split() == [
        "metadata_start", ">>", "wait_spark_idle", ">>", "spark_inventory", ">>",
        "wait_spark_inventory", ">>", "inventory_outcome", ">>", "dbt_staging",
    ]
    pos = _chain(src, "generate_pos_parquet")
    assert "spark_inventory" not in pos
    assert pos.index("register_pos_partition") < pos.index("dbt_staging")
    outcome = src.split("inventory_outcome = PythonOperator(", 1)[1].split(")", 1)[0]
    assert "TriggerRule.ALL_DONE" in outcome
    assert 'OWNED_SELECT + INVENTORY_HOLD' in src
    assert 'TEST_SELECT + INVENTORY_HOLD' in src
    assert "identity_graph" not in src.split("OWNED_SELECT", 1)[1].split("TEST_SELECT", 1)[0]


def test_warehouse_dag_closes_the_whole_previous_day() -> None:
    src = _WAREHOUSE.read_text(encoding="utf-8")
    assert 'schedule_interval= "15 1 * * *"' in src
    # 01:15 minus the 60s watermark and 3600s lateness is 00:14, floored to 00:00.
    started = datetime(2026, 11, 22, 1, 15, tzinfo=timezone.utc)
    assert target_hour_exclusive(started, 60, 3600) == datetime(2026, 11, 22, 0, 0)


def test_warehouse_dag_promotes_the_frontier_after_publish() -> None:
    src = _WAREHOUSE.read_text(encoding="utf-8")
    assert ">> [save_baseline, promote_frontier, dbt_serving, redshift_analyze, cost_check]" in src
    assert src.index(">> wap_publish\n") < src.index(">> [save_baseline, promote_frontier")
    assert '"acknowledge_kafka_loss": Param(' in src


def test_marketing_dag_uses_hourly() -> None:
    src = _MARKETING.read_text(encoding="utf-8")
    assert 'spark_step("hourly")' in src
    assert "SPARK_POOL" in src
    assert "submit_spark_step_task" in src and "EmrAddStepsOperator" not in src
    assert src.index(">> wait_spark_idle") < src.index(">> spark_identity")
    assert 'spark_step("inventory")' not in src
    assert 'spark_step("all")' not in src


class _FakeEmr:
    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages
        self.requests: list[dict] = []
        self.added: list[dict] = []

    def list_steps(self, **request):
        self.requests.append(dict(request))
        return self.pages[len(self.requests) - 1]

    def add_job_flow_steps(self, JobFlowId: str, Steps: list[dict]):
        self.added.append({"JobFlowId": JobFlowId, "Steps": Steps})
        return {"StepIds": ["s-NEW"]}


def test_active_spark_steps_reads_every_page_and_ignores_other_steps() -> None:
    client = _FakeEmr([
        {"Steps": [{"Name": "flink-inventory", "Id": "s-1"}], "Marker": "m1"},
        {"Steps": [{"Name": "spark-hourly", "Id": "s-2"}]},
    ])
    assert active_spark_steps(client, "j-1") == ["spark-hourly (s-2)"]
    assert client.requests[0]["StepStates"] == ["PENDING", "RUNNING", "CANCEL_PENDING"]
    assert client.requests[1]["Marker"] == "m1"


def test_submit_refuses_while_a_timed_out_step_still_runs() -> None:
    client = _FakeEmr([{"Steps": [{"Name": "spark-inventory", "Id": "s-OLD"}]}])
    with pytest.raises(RuntimeError, match="spark-inventory \\(s-OLD\\)"):
        submit_when_idle(client, "j-1", [spark_step("hourly")])
    assert client.added == []


def test_submit_adds_the_step_when_the_cluster_is_idle() -> None:
    client = _FakeEmr([{"Steps": []}])
    assert submit_when_idle(client, "j-1", [spark_step("hourly")]) == ["s-NEW"]
    assert client.added[0]["JobFlowId"] == "j-1"


def test_hold_excludes_inventory_marts_and_their_audits_only() -> None:
    assert inventory_hold_exclude("complete") == ""
    assert inventory_hold_exclude(None) == ""
    held = inventory_hold_exclude("held")
    assert held == f" --exclude {INVENTORY_HOLD_EXCLUDE}"
    for node in ("fact_inventory_snapshot+", "inventory_publication+", "inventory_canonical_run_is_one"):
        assert node in held
    assert "fact_sales" not in held and "fact_returns" not in held


def test_held_inventory_alert_names_the_wait_state() -> None:
    subject, message = held_inventory_message("upstream_failed", "scheduled__2026-11-21T01:15:00+00:00")
    assert "inventory held" in subject
    assert "wait_spark_inventory_canonical ended upstream_failed" in message
    assert "The POS facts publish" in message


def test_control_record_publishes_with_finance() -> None:
    assert ("control", "inventory_publication") in FINANCE_SUMMARY_TABLES
    assert ("control", "inventory_publication") in WAP_TABLES
    finance = FINANCE_SUMMARY_TABLES.index(("finance", "fact_inventory_snapshot"))
    control = FINANCE_SUMMARY_TABLES.index(("control", "inventory_publication"))
    assert control == finance + 1
