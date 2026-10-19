"""Unit tests for scripts/common/metadata_observer.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts.common import metadata_observer as mo

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


def test_layer_catalog_yaml_has_summary_and_gold() -> None:
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "layer_catalog.yml").read_text(
            encoding="utf-8"
        )
    )
    fqns = {o["object_fqn"] for o in raw["objects"]}
    assert "summary.sales_daily_store" in fqns
    assert "finance.fact_sales" in fqns
    assert "bronze.clickstream_events" in fqns
    for obj in raw["objects"]:
        assert obj["platform_layer"] in {
            "bronze",
            "silver",
            "staging",
            "intermediate",
            "gold",
            "summary",
            "serving",
        }


def test_metric_catalog_yaml_non_empty() -> None:
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "metric_catalog.yml").read_text(
            encoding="utf-8"
        )
    )
    assert len(raw["metrics"]) >= 3
    names = {m["metric_name"] for m in raw["metrics"]}
    assert "net_revenue_daily_store" in names


def test_metadata_ddl_files_exist() -> None:
    root = _REPO / "transformation" / "redshift" / "metadata"
    assert (root / "00_create_database.sql").is_file()
    schema = (root / "01_meta_schema.sql").read_text(encoding="utf-8")
    for table in (
        "layer_catalog",
        "metric_catalog",
        "pipeline_run",
        "table_freshness",
        "dq_check_result",
    ):
        assert f"meta.{table}" in schema


def test_parse_dbt_run_results_success_and_failure(tmp_path: Path) -> None:
    payload = {
        "results": [
            {
                "unique_id": "model.global_retail_analytics.fact_sales",
                "status": "success",
                "execution_time": 1.2,
                "relation_name": "finance.fact_sales",
            },
            {
                "unique_id": "test.global_retail_analytics.not_null_fact_sales_date_key",
                "status": "fail",
                "failures": 3,
                "execution_time": 0.4,
                "message": "Got 3 results, configured to fail if != 0",
            },
        ]
    }
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    rows = mo.parse_dbt_run_results(path)
    assert len(rows) == 2
    assert rows[0].status == "pass"
    assert rows[1].status == "fail"
    assert rows[1].failed_count == 3


def test_idempotent_pipeline_and_dq_writes(tmp_path: Path) -> None:
    # Avoid naming the file meta.duckdb — DuckDB uses the stem as catalog name
    # and then CREATE SCHEMA meta becomes ambiguous (meta.meta).
    db = tmp_path / "local_metadata.duckdb"
    writer = mo.DuckDBMetadataWriter(db)
    writer.ensure_schema()
    eid = "exec-1"
    mo.start_pipeline_run(
        writer,
        execution_id=eid,
        pipeline_name="local_quality",
        environment="local",
        trigger_type="manual",
    )
    mo.start_pipeline_run(
        writer,
        execution_id=eid,
        pipeline_name="local_quality",
        environment="local",
        trigger_type="manual",
    )
    mo.record_dq_results(
        writer,
        execution_id=eid,
        results=[
            mo.DqCheckResult(
                check_system="dbt",
                check_name="test.a",
                target_object="finance.fact_sales",
                status="pass",
            )
        ],
    )
    mo.record_dq_results(
        writer,
        execution_id=eid,
        results=[
            mo.DqCheckResult(
                check_system="dbt",
                check_name="test.a",
                target_object="finance.fact_sales",
                status="fail",
                failed_count=1,
            )
        ],
    )
    mo.finish_pipeline_run(writer, execution_id=eid, status="FAILED", error_text="boom")
    runs = writer.fetchone(
        "SELECT COUNT(*), MAX(status) FROM meta.pipeline_run WHERE execution_id = ?",
        (eid,),
    )
    assert runs[0] == 1
    assert runs[1] == "FAILED"
    dq = writer.fetchone(
        "SELECT COUNT(*), MAX(status) FROM meta.dq_check_result WHERE execution_id = ?",
        (eid,),
    )
    assert dq[0] == 1
    assert dq[1] == "fail"
    writer.close()


def test_seed_catalogs_upsert(tmp_path: Path) -> None:
    db = tmp_path / "local_metadata.duckdb"
    writer = mo.DuckDBMetadataWriter(db)
    writer.ensure_schema()
    mo.seed_catalogs(writer, _REPO / "metadata" / "catalog")
    mo.seed_catalogs(writer, _REPO / "metadata" / "catalog")
    row = writer.fetchone("SELECT COUNT(*) FROM meta.layer_catalog")
    assert row[0] >= 10
    mrow = writer.fetchone("SELECT COUNT(*) FROM meta.metric_catalog")
    assert mrow[0] >= 3
    writer.close()


def test_fail_open_wrapper_swallows() -> None:
    def _boom() -> None:
        raise RuntimeError("nope")

    assert mo.fail_open(_boom) is None


def test_cli_new_id() -> None:
    assert mo.main(["new-id"]) == 0


def test_summary_models_exist_and_single_fact_source() -> None:
    summary_dir = (
        _REPO / "transformation" / "dbt_project" / "models" / "marts" / "summary"
    )
    expected = {
        "sales_daily_store.sql": "fact_sales",
        "inventory_daily_product_store.sql": "fact_inventory_snapshot",
        "sessions_daily_platform.sql": "fact_customer_session",
    }
    for fname, fact in expected.items():
        text = (summary_dir / fname).read_text(encoding="utf-8")
        assert f"ref('{fact}')" in text
        # No fact-to-fact joins: only one fact ref.
        assert text.count("ref('fact_") == 1
    assert (_REPO / "transformation" / "dbt_project" / "tests" / "summary.yml").is_file()
