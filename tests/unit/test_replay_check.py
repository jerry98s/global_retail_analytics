"""Replay fingerprints: ignore run metadata and row order, catch real changes."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


def _replay():
    spec = importlib.util.spec_from_file_location("replay_check", _REPO / "scripts" / "local" / "replay_check.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _table(con, rows: list[tuple]) -> None:
    con.execute("create or replace table silver.t (k varchar, v double, export_run_id varchar, computed_at timestamp)")
    con.executemany("insert into silver.t values (?, ?, ?, ?)", rows)


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("create schema silver")
    yield con
    con.close()


def test_run_metadata_and_row_order_do_not_change_the_fingerprint(con) -> None:
    replay = _replay()
    _table(con, [("a", 1.0, "run-1", "2026-11-08 01:00"), ("b", 2.0, "run-1", "2026-11-08 01:00")])
    first = replay.fingerprint_table(con, "silver", "t")
    _table(con, [("b", 2.0 + 1e-12, "run-2", "2026-11-08 02:00"), ("a", 1.0, "run-2", "2026-11-08 02:00")])
    second = replay.fingerprint_table(con, "silver", "t")
    assert first == second
    assert first["columns"] == ["k", "v"]


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([("a", 1.0, "r", None), ("b", 3.0, "r", None)], "same 2 rows, different content"),
        ([("a", 1.0, "r", None)], "rows 2 -> 1"),
        ([("a", 1.0, "r", None), ("a", 1.0, "r", None), ("b", 2.0, "r", None)], "rows 2 -> 3"),
    ],
)
def test_a_changed_value_or_row_is_a_mismatch(con, rows, expected) -> None:
    replay = _replay()
    _table(con, [("a", 1.0, "r", None), ("b", 2.0, "r", None)])
    first = {"silver.t": replay.fingerprint_table(con, "silver", "t")}
    _table(con, rows)
    second = {"silver.t": replay.fingerprint_table(con, "silver", "t")}
    assert replay.compare(first, second) == [f"silver.t: {expected}"]


def test_a_duplicated_row_pair_is_not_cancelled_out(con) -> None:
    replay = _replay()
    _table(con, [("a", 1.0, "r", None)])
    one = replay.fingerprint_table(con, "silver", "t")
    _table(con, [("a", 1.0, "r", None), ("a", 1.0, "r", None), ("a", 1.0, "r", None)])
    three = replay.fingerprint_table(con, "silver", "t")
    assert one["hash"] != three["hash"]


def test_a_missing_table_fails_instead_of_matching(con) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        _replay().fingerprint_table(con, "silver", "missing")


def test_snapshot_covers_spark_exports_models_and_every_pending_gold_table() -> None:
    replay = _replay()
    from orchestration.airflow.plugins.wap_publish import WAP_TABLES

    assert ("silver", "identity_resolution") in replay.SPARK_EXPORTS
    assert ("intermediate", "int_identity_changes") in replay.BUSINESS_MODELS
    source = (_REPO / "scripts" / "local" / "replay_check.py").read_text(encoding="utf-8")
    assert 'f"{schema}_pending", table) for schema, table in WAP_TABLES' in source
    assert len(WAP_TABLES) == 12


def test_replay_task_runs_spark_and_dbt_twice_and_never_publishes() -> None:
    stack = (_REPO / "scripts" / "local" / "run_local_stack.ps1").read_text(encoding="utf-8")
    block = re.search(r'"replay" \{(.*?)\n        "quality" \{', stack, re.S).group(1)
    assert block.index("-Task pos-parquet") < block.index("foreach ($pass in @('first', 'second'))")
    assert "-Task spark" in block and "-Task dbt" in block
    assert "replay_check.py compare" in block
    assert "verify_customer_360.py --pending" in block
    assert "Publish" not in block and "-Task quality" not in block
