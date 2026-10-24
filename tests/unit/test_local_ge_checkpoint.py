"""Contracts for the Great Expectations Silver event checkpoints."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_GE = _REPO / "quality" / "great_expectations"
_STACK = (_REPO / "scripts" / "local" / "run_local_stack.ps1").read_text(encoding="utf-8")
_RUNNER = (_REPO / "scripts" / "local" / "run_ge_local.py").read_text(encoding="utf-8")
_LOCAL_CP = (_GE / "checkpoints" / "silver_events_local.yml").read_text(encoding="utf-8")
_CLOUD_CP = (_GE / "checkpoints" / "silver_events_daily.yml").read_text(encoding="utf-8")
_GE_YML = (_GE / "great_expectations.yml").read_text(encoding="utf-8")
_SUITES = ("inventory_bronze", "clickstream_bronze")


class TestLocalGeCheckpoint:
    def test_duckdb_datasource_configured(self) -> None:
        assert "duckdb_local:" in _GE_YML
        assert "pandas_local:" in _GE_YML
        assert "DUCKDB_SQLALCHEMY_URL" in _GE_YML

    def test_local_checkpoint_uses_duckdb_not_redshift(self) -> None:
        assert "datasource_name: duckdb_local" in _LOCAL_CP
        assert "redshift_gold" not in _LOCAL_CP

    def test_local_checkpoint_sql_is_duckdb_portable(self) -> None:
        sql_body = "\n".join(
            line for line in _LOCAL_CP.splitlines() if not line.lstrip().startswith("#")
        )
        assert "TO_CHAR" not in sql_body
        assert "DATEADD" not in sql_body
        assert "INTERVAL" in sql_body

    def test_checkpoints_cover_only_silver_event_suites(self) -> None:
        # Gold is audited by dbt tests in the WAP gate; GE must not duplicate it.
        for checkpoint in (_LOCAL_CP, _CLOUD_CP):
            for suite in _SUITES:
                assert f"expectation_suite_name: {suite}" in checkpoint
            assert checkpoint.count("expectation_suite_name:") == len(_SUITES)
            for gold_schema in ("finance.", "marketing.", "summary."):
                assert gold_schema not in checkpoint
        suites = {p.stem for p in (_GE / "expectations").glob("*.json")}
        assert suites == set(_SUITES)

    def test_hourly_dag_uses_shared_runner_not_ge_cli(self) -> None:
        dag = (
            _REPO / "orchestration" / "airflow" / "dags" / "quality_hourly_ge_checkpoint.py"
        ).read_text(encoding="utf-8")
        assert "run_ge_checkpoint.py" in dag
        assert "--checkpoint silver_events_daily" in dag
        assert "great_expectations checkpoint run" not in dag

    def test_stack_runs_ge_without_a_pending_gate(self) -> None:
        assert "run_ge_local.py" in _STACK
        assert "--pending-tables" not in _STACK

    def test_runner_points_at_local_duckdb(self) -> None:
        assert "silver_events_local" in _RUNNER
        assert "local_retail.duckdb" in _RUNNER
        assert "pandas_local" in _RUNNER
        assert "fetchdf" in _RUNNER
