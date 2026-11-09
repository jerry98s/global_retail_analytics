"""Local stack runs the same POS restatement and row-count gate as the owning DAGs."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import duckdb
import pytest

from orchestration.airflow.plugins.row_count_reconciliation import (
    BASELINE_VAR_NAME,
    baseline_var_name,
)
from orchestration.airflow.plugins.wap_publish import WAP_SCOPES

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_STACK = (_REPO / "scripts" / "local" / "run_local_stack.ps1").read_text(encoding="utf-8")


def _gate():
    spec = importlib.util.spec_from_file_location(
        "row_count_gate", _REPO / "scripts" / "local" / "row_count_gate.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pending_db(path: Path, **rows: int) -> None:
    """Every WAP table's pending twin with 10 rows, overridden by table name."""
    con = duckdb.connect(str(path))
    for tables in WAP_SCOPES.values():
        for schema, table in tables:
            con.execute(f"create schema if not exists {schema}_pending")
            con.execute(
                f"create or replace table {schema}_pending.{table} as "
                f"select range as id from range({rows.get(table, 10)})"
            )
    con.close()


def _variables(state: Path) -> dict:
    return json.loads(state.read_text())["variables"]


def _baseline(state: Path, scope: str) -> dict:
    return json.loads(_variables(state)[baseline_var_name(scope)])


def _audit(gate, db: Path, state: Path, *ack: str, scope: str | None = None) -> int:
    argv = ["audit", "--duckdb", str(db), "--state", str(state)]
    if scope:
        argv += ["--scope", scope]
    for table in ack:
        argv += ["--acknowledge-shrink", table]
    return gate.main(argv)


class TestRowCountGate:
    def test_audit_saves_one_baseline_per_scope_only_on_save(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, fact_sales=100, dim_customer=50)

        assert _audit(gate, db, state) == 0
        assert not any(k.startswith(BASELINE_VAR_NAME) for k in _variables(state))

        assert gate.main(["save", "--state", str(state)]) == 0
        assert _baseline(state, "finance")["finance.fact_sales"] == 100
        assert _baseline(state, "marketing")["marketing.dim_customer"] == 50
        assert set(_baseline(state, "catalog")) == {"marketing.dim_product"}

    def test_marketing_shrink_blocks_until_acknowledged(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, dim_customer=100)
        _audit(gate, db, state)
        gate.main(["save", "--state", str(state)])

        _pending_db(db, dim_customer=20)
        assert _audit(gate, db, state) == 1
        gate.main(["save", "--state", str(state)])
        assert _baseline(state, "marketing")["marketing.dim_customer"] == 100

        assert _audit(gate, db, state, "marketing.dim_customer") == 0
        gate.main(["save", "--state", str(state)])
        assert _baseline(state, "marketing")["marketing.dim_customer"] == 20

    def test_growth_does_not_freeze_the_baseline(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, fact_sales=100)
        _audit(gate, db, state)
        gate.main(["save", "--state", str(state)])

        _pending_db(db, fact_sales=1_000)
        assert _audit(gate, db, state) == 0
        gate.main(["save", "--state", str(state)])
        assert _baseline(state, "finance")["finance.fact_sales"] == 1_000

    def test_legacy_shared_baseline_seeds_each_scope(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        legacy = {"marketing.dim_customer": 1_000, "finance.fact_sales": 10}
        state.write_text(json.dumps({"variables": {BASELINE_VAR_NAME: json.dumps(legacy)}}))
        _pending_db(db, dim_customer=100)
        assert _audit(gate, db, state) == 1

    def test_a_blocked_scope_does_not_block_another(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, dim_customer=100, fact_sales=100)
        _audit(gate, db, state)
        gate.main(["save", "--state", str(state)])

        _pending_db(db, dim_customer=20, fact_sales=100)
        assert _audit(gate, db, state) == 1
        assert _audit(gate, db, state, scope="marketing") == 1
        assert _audit(gate, db, state, scope="finance") == 0
        gate.main(["save", "--state", str(state)])
        assert _baseline(state, "finance")["finance.fact_sales"] == 100
        assert _baseline(state, "marketing")["marketing.dim_customer"] == 100

    def test_an_acknowledgement_for_another_scope_is_rejected(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db)
        assert _audit(gate, db, state, "finance.fact_sales", scope="marketing") == 2

    def test_unknown_acknowledgement_is_rejected(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db)
        assert _audit(gate, db, state, "finance.dim_date") == 2

    def test_missing_pending_table_blocks(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        duckdb.connect(str(db)).close()
        assert _audit(gate, db, state) == 1


class TestStackWiring:
    def test_pending_vars_come_from_the_dag_plugin(self):
        assert "from orchestration.airflow.plugins.pos_restatement import pos_dbt_vars" in _STACK
        pending_uses = re.findall(r"'--vars', \(Get-PendingDbtVars\)", _STACK)
        assert len(pending_uses) == 3  # iceberg pending run + both pending audits
        assert "'restate_empty_dates': empty" in _STACK

    def test_restatement_skips_the_pos_generator(self):
        body = _STACK.split("function Invoke-PosParquetLocal {", 1)[1]
        assert body.index("Test-PosRestatement") < body.index("generate_pos_parquet")
        assert "-Task pos-parquet -RestateFrom $RestateFrom -RestateTo $RestateTo" in _STACK

    def test_gate_runs_before_publish_and_baseline_after(self):
        for task in ('"quality" {', '"all" {'):
            body = _STACK.split(task, 1)[1]
            gate = body.index("Invoke-RowCountGateLocal")
            publish = body.index("Invoke-WapPublishLocal")
            save = body.index("Save-RowCountBaselineLocal")
            assert gate < publish < save, task

    def test_checked_command_uses_the_exit_code_not_stderr(self):
        body = _STACK.split("function Invoke-CheckedCommand {", 1)[1].split("\nfunction ", 1)[0]
        assert body.index('$ErrorActionPreference = "Continue"') < body.index("Invoke-Expression")
        assert "$code = $LASTEXITCODE" in body
        assert "if ($code -ne 0)" in body

    def test_acknowledge_shrink_reaches_the_gate(self):
        assert "[string[]]$AcknowledgeShrink" in _STACK
        body = _STACK.split("function Invoke-RowCountGateLocal {", 1)[1].split("\nfunction ", 1)[0]
        assert "'--acknowledge-shrink', $table" in body
        assert "'--scope', $Scope" in body

    def test_quality_scope_publishes_only_that_dag(self):
        assert 'throw "-Scope is only valid with -Task quality."' in _STACK
        quality = _STACK.split('Pipeline "local_quality"', 1)[1].split('Pipeline "local_e2e"', 1)[0]
        assert "Get-ScopeTestArgs -Scope $Scope" in quality
        assert "Invoke-RowCountGateLocal -Scope $Scope" in quality
        assert "Invoke-WapPublishLocal -Scope $Scope" in quality
        publish = _STACK.split("function Invoke-WapPublishLocal {", 1)[1].split("\nfunction ", 1)[0]
        assert "WAP_TABLES if scope == 'all' else WAP_SCOPES[scope]" in publish
