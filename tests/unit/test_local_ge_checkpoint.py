"""Contracts for local DuckDB Great Expectations checkpoint."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_GE = _REPO / "quality" / "great_expectations"
_STACK = (_REPO / "scripts" / "local" / "run_local_stack.ps1").read_text(encoding="utf-8")
_RUNNER = (_REPO / "scripts" / "local" / "run_ge_local.py").read_text(encoding="utf-8")
_LOCAL_CP = (_GE / "checkpoints" / "gold_layer_local.yml").read_text(encoding="utf-8")
_GE_YML = (_GE / "great_expectations.yml").read_text(encoding="utf-8")


class TestLocalGeCheckpoint:
    def test_duckdb_datasource_configured(self) -> None:
        assert "duckdb_local:" in _GE_YML
        assert "pandas_local:" in _GE_YML
        assert "DUCKDB_SQLALCHEMY_URL" in _GE_YML

    def test_local_checkpoint_uses_duckdb_not_redshift(self) -> None:
        assert "datasource_name: duckdb_local" in _LOCAL_CP
        assert "redshift_gold" not in _LOCAL_CP

    def test_local_checkpoint_sql_is_duckdb_portable(self) -> None:
        # Strip comments so the "no TO_CHAR" note in the header does not trip.
        sql_body = "\n".join(
            line for line in _LOCAL_CP.splitlines() if not line.lstrip().startswith("#")
        )
        assert "TO_CHAR" not in sql_body
        assert "DATEADD" not in sql_body
        assert "strftime" in sql_body
        assert "INTERVAL" in sql_body

    def test_local_checkpoint_covers_core_suites(self) -> None:
        for suite in (
            "fact_sales",
            "dim_product_scd2",
            "identity_graph",
            "fact_customer_session",
            "dim_date_local",
            "dim_store",
            "fact_inventory_snapshot",
            "dim_customer",
            "customer_360_view",
            "inventory_bronze",
            "clickstream_bronze",
        ):
            assert f"expectation_suite_name: {suite}" in _LOCAL_CP

    def test_dim_date_local_suite_exists(self) -> None:
        path = _GE / "expectations" / "dim_date_local.json"
        assert path.is_file()
        text = path.read_text(encoding="utf-8")
        assert "dim_date_local" in text
        assert "365" in text

    def test_stack_quality_invokes_ge_local(self) -> None:
        assert "run_ge_local.py" in _STACK
        assert "--pending-tables" in _STACK
        assert "gold_layer_local" in _RUNNER

    def test_runner_points_at_local_duckdb(self) -> None:
        assert "gold_layer_local" in _RUNNER
        assert "local_retail.duckdb" in _RUNNER
        assert "pandas_local" in _RUNNER
        assert "fetchdf" in _RUNNER
