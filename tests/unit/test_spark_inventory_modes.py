"""Spark mode split, the Airflow pool, and the finance publish set (ADR-012)."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.kafka.topics import PARTITIONS_BY_TOPIC, TOPIC_INVENTORY
from orchestration.airflow.plugins.spark_steps import POOL, SPARK_PACKAGES, spark_step
from orchestration.airflow.plugins.wap_publish import FINANCE_SUMMARY_TABLES, WAP_TABLES

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


def test_warehouse_dag_runs_inventory_spark_before_dbt() -> None:
    src = _WAREHOUSE.read_text(encoding="utf-8")
    assert 'spark_step("inventory")' in src
    assert "pool" in src and "SPARK_POOL" in src
    assert src.index(">> spark_inventory") < src.index(">> wait_spark_inventory")
    assert src.index(">> wait_spark_inventory") < src.index(">> dbt_staging")
    assert "identity_graph" not in src.split("OWNED_SELECT", 1)[1].split("TEST_SELECT", 1)[0]


def test_marketing_dag_uses_hourly() -> None:
    src = _MARKETING.read_text(encoding="utf-8")
    assert 'spark_step("hourly")' in src
    assert "SPARK_POOL" in src
    assert 'spark_step("inventory")' not in src
    assert 'spark_step("all")' not in src


def test_control_record_publishes_with_finance() -> None:
    assert ("control", "inventory_publication") in FINANCE_SUMMARY_TABLES
    assert ("control", "inventory_publication") in WAP_TABLES
    finance = FINANCE_SUMMARY_TABLES.index(("finance", "fact_inventory_snapshot"))
    control = FINANCE_SUMMARY_TABLES.index(("control", "inventory_publication"))
    assert control == finance + 1
