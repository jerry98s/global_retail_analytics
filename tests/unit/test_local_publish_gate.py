"""Local stack runs the same POS restatement and row-count gate as the warehouse DAG."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import duckdb
import pytest

from orchestration.airflow.plugins.row_count_reconciliation import BASELINE_VAR_NAME
from orchestration.airflow.plugins.wap_publish import FINANCE_SUMMARY_TABLES

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


def _pending_db(path: Path, fact_sales_rows: int) -> None:
    con = duckdb.connect(str(path))
    for schema, table in FINANCE_SUMMARY_TABLES:
        rows = fact_sales_rows if table == "fact_sales" else 10
        con.execute(f"create schema if not exists {schema}_pending")
        con.execute(f"create or replace table {schema}_pending.{table} as select range as id from range({rows})")
    con.close()


def _baseline(state: Path) -> dict:
    return json.loads(json.loads(state.read_text())["variables"][BASELINE_VAR_NAME])


class TestRowCountGate:
    def test_clean_audit_saves_baseline_only_on_save(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, 100)

        assert gate.main(["audit", "--duckdb", str(db), "--state", str(state)]) == 0
        assert BASELINE_VAR_NAME not in json.loads(state.read_text())["variables"]

        assert gate.main(["save", "--state", str(state)]) == 0
        assert _baseline(state)["finance.fact_sales"] == 100

    def test_shrunk_pending_table_blocks_and_keeps_baseline(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        _pending_db(db, 100)
        gate.main(["audit", "--duckdb", str(db), "--state", str(state)])
        gate.main(["save", "--state", str(state)])

        _pending_db(db, 40)
        assert gate.main(["audit", "--duckdb", str(db), "--state", str(state)]) == 1
        gate.main(["save", "--state", str(state)])
        assert _baseline(state)["finance.fact_sales"] == 100

    def test_missing_pending_table_blocks(self, tmp_path):
        gate, db, state = _gate(), tmp_path / "db.duckdb", tmp_path / "state.json"
        duckdb.connect(str(db)).close()
        assert gate.main(["audit", "--duckdb", str(db), "--state", str(state)]) == 1


class TestStackWiring:
    def test_pending_vars_come_from_the_dag_plugin(self):
        assert "from orchestration.airflow.plugins.pos_restatement import pos_dbt_vars" in _STACK
        pending_uses = re.findall(r"'--vars', \(Get-PendingDbtVars\)", _STACK)
        assert len(pending_uses) == 3  # iceberg pending run + both pending audits

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
