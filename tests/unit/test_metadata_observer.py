"""Unit tests for scripts/common/metadata_observer.py."""

from __future__ import annotations

import json
import re
from datetime import datetime
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
    assert "silver.clickstream_events" in fqns
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


def _model_unique_keys() -> dict[str, set[str]]:
    keys: dict[str, set[str]] = {}
    for path in (_REPO / "transformation" / "dbt_project" / "models").rglob("*.sql"):
        match = re.search(r"unique_key\s*=\s*(\[[^\]]*\]|'[^']*')", path.read_text(encoding="utf-8"))
        if match:
            keys[path.stem] = set(re.findall(r"'([^']+)'", match.group(1)))
    return keys


def test_layer_catalog_grain_matches_model_unique_key() -> None:
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "layer_catalog.yml").read_text(encoding="utf-8")
    )
    unique_keys = _model_unique_keys()
    checked = 0
    for obj in raw["objects"]:
        table = obj["object_fqn"].split(".", 1)[1]
        grain = obj.get("grain") or ""
        if table not in unique_keys:
            continue
        assert grain.startswith("("), (obj["object_fqn"], "grain must be the unique-key tuple")
        columns = {c.strip() for c in grain[1:grain.index(")")].split(",")}
        assert columns == unique_keys[table], (obj["object_fqn"], columns, unique_keys[table])
        checked += 1
    assert checked >= 4


def test_layer_catalog_lists_inventory_lambda_views() -> None:
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "layer_catalog.yml").read_text(encoding="utf-8")
    )
    fqns = {o["object_fqn"] for o in raw["objects"]}
    assert "intermediate.int_inventory_hourly" in fqns
    assert "intermediate.int_inventory_batch_coverage" in fqns


def test_layer_catalog_includes_critical_spark_outputs() -> None:
    """P1: Catalog must track identity resolution and other critical Spark outputs."""
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "layer_catalog.yml").read_text(encoding="utf-8")
    )
    fqns = {o["object_fqn"] for o in raw["objects"]}
    # Spark identity resolution outputs (ADR-010)
    assert "silver.identity_resolution" in fqns, "Missing identity_resolution table"
    assert "silver.identity_edges" in fqns, "Missing identity_edges table"


def test_metric_expressions_run_on_duckdb_and_redshift() -> None:
    # Redshift has no aggregate FILTER clause; integer / integer truncates.
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "metric_catalog.yml").read_text(encoding="utf-8")
    )
    for met in raw["metrics"]:
        expr = met["expression"]
        assert "FILTER" not in expr.upper(), met["metric_name"]
        assert "::" not in expr, met["metric_name"]
        # Every metric aggregates. Semi-additive ones (non_additive_dimension in
        # the semantic layer) roll up across keys but not across days.
        assert re.match(r"^(SUM|COUNT|CAST\(SUM)\(", expr), met["metric_name"]
        if met["unit"] == "ratio":
            assert "AS DOUBLE PRECISION" in expr.upper(), met["metric_name"]


def test_metric_catalog_yaml_non_empty() -> None:
    raw = yaml.safe_load(
        (_REPO / "metadata" / "catalog" / "metric_catalog.yml").read_text(
            encoding="utf-8"
        )
    )
    names = {m["metric_name"] for m in raw["metrics"]}
    assert "pipeline_success_rate" in names


def test_metadata_ddl_files_exist() -> None:
    schema = (
        _REPO / "scripts" / "common" / "metadata_observer.py"
    ).read_text(encoding="utf-8")
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
    writer.execute(
        "INSERT INTO meta.metric_catalog (metric_name, source_relation, expression) "
        "VALUES ('retired_metric', 'summary.gone', 'SUM(x)')"
    )
    mo.seed_catalogs(writer, _REPO / "metadata" / "catalog")
    mo.seed_catalogs(writer, _REPO / "metadata" / "catalog")
    row = writer.fetchone("SELECT COUNT(*) FROM meta.layer_catalog")
    assert row[0] >= 10
    names = set(writer.fetchone("SELECT list(metric_name) FROM meta.metric_catalog")[0])
    assert {"pipeline_success_rate", "net_revenue", "ending_on_hand_qty"} <= names
    assert "retired_metric" not in names
    writer.close()


def test_seed_catalogs_refuses_partial_sources(tmp_path: Path) -> None:
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    mo.seed_catalogs(writer, _REPO / "metadata" / "catalog")
    before = writer.fetchone("SELECT COUNT(*) FROM meta.metric_catalog")[0]
    with pytest.raises(FileNotFoundError):
        mo.seed_catalogs(writer, _REPO / "metadata" / "catalog", tmp_path / "no_semantic")
    assert writer.fetchone("SELECT COUNT(*) FROM meta.metric_catalog")[0] == before
    writer.close()


def test_freshness_targets_come_from_layer_catalog() -> None:
    targets = {
        f"{schema}.{table}": (ts, sla)
        for schema, table, ts, sla in mo.freshness_targets(_REPO / "metadata" / "catalog")
    }
    assert targets["bronze.clickstream_records"] == ("ingested_at", 60)
    assert targets["summary.inventory_position_daily"] == ("snapshot_date_key", 1440)
    assert "bronze.clickstream_events" not in targets
    # No SLA, no freshness probe.
    assert "finance.dim_store" not in targets


def test_freshness_reads_date_keys_as_end_of_day(tmp_path: Path) -> None:
    import duckdb

    analytics = duckdb.connect(str(tmp_path / "analytics.duckdb"))
    analytics.execute("CREATE SCHEMA summary")
    analytics.execute("CREATE TABLE summary.daily AS SELECT 20261023 AS date_key")
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    mo.collect_table_freshness(
        writer,
        analytics,
        execution_id="e1",
        targets=[("summary", "daily", "date_key", 1440)],
        measured_at=datetime(2026, 10, 24, 12, 0),
    )
    lag, status = writer.fetchone(
        "SELECT lag_minutes, sla_status FROM meta.table_freshness WHERE execution_id = 'e1'"
    )
    assert (lag, status) == (720, "ok")
    writer.close()
    analytics.close()


def test_freshness_breaches_lists_only_breached_tables_of_the_run(tmp_path: Path) -> None:
    import duckdb

    analytics = duckdb.connect(str(tmp_path / "analytics.duckdb"))
    analytics.execute("CREATE SCHEMA summary")
    analytics.execute("CREATE TABLE summary.fresh AS SELECT 20261024 AS date_key")
    analytics.execute("CREATE TABLE summary.stale AS SELECT 20261020 AS date_key")
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    for execution_id in ("e1", "e2"):
        mo.collect_table_freshness(
            writer,
            analytics,
            execution_id=execution_id,
            targets=[
                ("summary", "fresh", "date_key", 1440),
                ("summary", "stale", "date_key", 1440),
            ],
            measured_at=datetime(2026, 10, 24, 12, 0),
        )
    breaches = mo.freshness_breaches(writer, execution_id="e1")
    assert len(breaches) == 1
    assert breaches[0].startswith("summary.stale (lag ")
    writer.close()
    analytics.close()


def test_hourly_inventory_freshness_with_stale_data_breaches_sla(tmp_path: Path) -> None:
    """P0: Hourly tables with date keys must measure actual event time, not assume end of day."""
    import duckdb

    analytics = duckdb.connect(str(tmp_path / "analytics.duckdb"))
    analytics.execute("CREATE SCHEMA silver")
    analytics.execute(
        "CREATE TABLE silver.inventory_hourly AS "
        "SELECT 20261024 AS snapshot_date_key, 7 AS snapshot_hour"
    )
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    
    # Measured at 4 PM (16:00) same day. Last data is from hour 7 (7-8 AM).
    # Gap is from 8:00 AM to 4:00 PM = 8 hours = 480 minutes, which exceeds 120-minute SLA.
    mo.collect_table_freshness(
        writer,
        analytics,
        execution_id="e1",
        targets=[("silver", "inventory_hourly", "snapshot_date_key", 120)],
        measured_at=datetime(2026, 10, 24, 16, 0),
    )
    lag, status = writer.fetchone(
        "SELECT lag_minutes, sla_status FROM meta.table_freshness WHERE execution_id = 'e1'"
    )
    # Should breach because 480 minutes > 120-minute SLA
    assert lag == 480, f"Expected lag = 480 min (8 hours), got {lag}"
    assert status == "breach", f"Expected breach, got {status}"
    
    breaches = mo.freshness_breaches(writer, execution_id="e1")
    assert len(breaches) == 1
    writer.close()
    analytics.close()


def test_freshness_unknown_status_is_observable(tmp_path: Path) -> None:
    """P1: Unknown freshness (metadata failures) should be trackable separately."""
    import duckdb

    analytics = duckdb.connect(str(tmp_path / "analytics.duckdb"))
    analytics.execute("CREATE SCHEMA summary")
    analytics.execute("CREATE TABLE summary.good AS SELECT 20261024 AS date_key")
    # Missing table will result in unknown status
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    
    mo.collect_table_freshness(
        writer,
        analytics,
        execution_id="e1",
        targets=[
            ("summary", "good", "date_key", 1440),
            ("summary", "missing", "date_key", 1440),
        ],
        measured_at=datetime(2026, 10, 24, 12, 0),
    )
    
    # Query for unknown status count
    unknown_count = writer.fetchone(
        "SELECT COUNT(*) FROM meta.table_freshness "
        "WHERE execution_id = 'e1' AND sla_status = 'unknown'"
    )[0]
    assert unknown_count == 1, "Should have 1 table with unknown status"
    
    # Verify metric can be computed
    metric_value = writer.fetchone(
        "SELECT SUM(CASE WHEN sla_status = 'unknown' THEN 1 ELSE 0 END) "
        "FROM meta.table_freshness WHERE execution_id = 'e1'"
    )[0]
    assert metric_value == 1
    
    writer.close()
    analytics.close()


def test_publish_and_gate_outcomes_are_recorded(tmp_path: Path) -> None:
    writer = mo.DuckDBMetadataWriter(tmp_path / "local_metadata.duckdb")
    writer.ensure_schema()
    mo.record_wap_publish(writer, execution_id="e1", published=["finance.fact_sales"])
    mo.record_reconciliation_results(
        writer,
        execution_id="e1",
        deltas={
            "finance.fact_sales": {"status": "WARN"},
            "summary.sales_daily_store": {"status": "WARN"},
            "finance.dim_date": {"status": "ok"},
        },
        warned=True,
        blocked=["finance.fact_sales"],
    )
    rows = dict(
        writer.fetchall(
            "SELECT check_name, status FROM meta.dq_check_result WHERE execution_id = ?",
            ("e1",),
        )
    )
    assert rows == {
        "publish:finance.fact_sales": "pass",
        "row_count:finance.fact_sales": "fail",
        "row_count:summary.sales_daily_store": "warn",
        "row_count:finance.dim_date": "pass",
        "row_count_reconciliation_summary": "fail",
    }
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
        "inventory_position_daily.sql": "fact_inventory_snapshot",
        "sessions_daily_platform.sql": "fact_customer_session",
    }
    for fname, fact in expected.items():
        text = (summary_dir / fname).read_text(encoding="utf-8")
        assert f"ref('{fact}')" in text
        # No fact-to-fact joins: only one fact ref.
        assert text.count("ref('fact_") == 1
    assert (_REPO / "transformation" / "dbt_project" / "tests" / "summary.yml").is_file()
