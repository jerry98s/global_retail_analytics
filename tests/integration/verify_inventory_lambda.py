"""Canonical inventory (ADR-012) through the real dbt incremental build and WAP publish.

Runs in a pytest temporary directory with its own profile and its own
``local_retail.duckdb`` (the filename the views bind), never the stack
database. Each step writes one Spark export, clones live finance and control
to pending, builds them, publishes both in one call, then rebuilds serving:

1. One partition is incomplete, so the cutoff stays at 11:00. Gold has only
   hours before that. Serving adds the later Flink hours.
2. Coverage catches up through 15:00. Those hours become canonical and
   is_estimated stays false. Balances match the events.
3. An hourly Flink export lands during the published window. Serving adds
   that hour once. The fact and the control row do not change.
4. A late +10 receipt for 10:30 is in the next canonical export only. Hour 10
   and every later balance of that product/store move by +10 once.
5. The same export again changes nothing.
6. A run with no new stock movement still advances the published cutoff.
7. A dbt failure after Spark wrote Silver does not publish. Mixed run ids
   fail the source test and leave the live fact and control row as they were.

    pytest tests/integration/verify_inventory_lambda.py -m integration_duckdb
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
import yaml

from orchestration.airflow.plugins.wap_publish import clone_live_to_pending, publish_gold
from scripts.local.bootstrap_live_dim_product import bootstrap

_DBT = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")

pytestmark = [
    pytest.mark.integration_duckdb,
    pytest.mark.skipif(_DBT is None, reason="dbt not installed (uv sync --group dev)"),
]

_DBT_DIR = Path(__file__).resolve().parents[2] / "transformation" / "dbt_project"
_PARTITIONS = yaml.safe_load((_DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))[
    "vars"]["inventory_kafka_partitions"]
_PUBLISH = [("finance", "fact_inventory_snapshot"), ("control", "inventory_publication")]
_PENDING = json.dumps({"wap_phase": "pending"})
_INVENTORY_MODELS = (
    "int_inventory_cutoff",
    "int_inventory_hourly",
    "fact_inventory_snapshot",
    "inventory_publication",
)
_TESTS = (
    "inventory_canonical_run_is_one",
    "inventory_hourly_export_is_one",
    "inventory_publication_matches_canonical_run",
    "inventory_coverage_incomplete",
)
_DAY = "2026-11-04"
_DATE_KEY = 20261104

_SILVER_DDL = (
    """create schema if not exists silver""",
    """create schema if not exists control""",
    """create or replace table silver.inventory_events (
        event_id varchar, event_time timestamp, store_id varchar, product_id varchar,
        qty_delta integer, event_type varchar, scanner_id varchar, is_late boolean,
        schema_version varchar, event_date date, source_system varchar, kafka_topic varchar,
        kafka_partition integer, kafka_offset bigint, kafka_timestamp timestamp,
        ingested_at timestamp, export_run_id varchar)""",
    """create or replace table silver.inventory_hourly (
        snapshot_date_key integer, snapshot_hour integer, product_id varchar, store_id varchar,
        qty_delta_hour bigint, qty_received_hour bigint, is_estimated boolean, export_run_id varchar)""",
    """create or replace table silver.inventory_partition_coverage (
        inventory_run_id varchar, bronze_snapshot_id bigint, kafka_topic varchar,
        kafka_partition integer, required_start_offset bigint, required_end_offset bigint,
        captured_end_offset bigint, missing_offset_count bigint, coverage_status varchar,
        export_run_id varchar)""",
    """create or replace table silver.inventory_cutoff (
        inventory_run_id varchar, bronze_snapshot_id bigint,
        target_hour_exclusive integer, closed_hour_exclusive integer,
        coverage_status varchar, incomplete_partitions integer, export_run_id varchar)""",
)


def _dbt(tmp: Path, *args: str, ok: bool = True) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DBT_TARGET_PATH": str(tmp / "target"), "DBT_LOG_PATH": str(tmp / "logs")}
    result = subprocess.run(
        [_DBT, *args, "--profiles-dir", str(tmp), "--target", "local"],
        cwd=_DBT_DIR, env=env, text=True, capture_output=True, check=False,
    )
    if ok:
        assert result.returncode == 0, f"dbt {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"
    return result


def _write_silver(
    db: Path,
    run_id: str,
    pair_a: tuple[str, str],
    pair_b: tuple[str, str],
    *,
    closed_hour: int,
    status: str,
    incomplete_partition: int | None,
    late_receipt: bool,
    extra_run_id: str | None = None,
) -> None:
    """One Spark export: canonical events, one Flink export, coverage and the cutoff."""
    events = [
        ("E1", "09:10", pair_a, -5),
        ("E2", "11:20", pair_a, -2),
        ("E3", "13:05", pair_a, -1),
        ("E4", "10:15", pair_b, -4),
        ("E6", "14:10", pair_a, -3),
    ]
    if late_receipt:
        events.append(("E5", "10:30", pair_a, 10))
    flink_hours = [(9, pair_a, -5), (11, pair_a, -2), (13, pair_a, -1), (14, pair_a, -3), (10, pair_b, -4)]
    with duckdb.connect(str(db)) as con:
        for ddl in _SILVER_DDL:
            con.execute(ddl)
        con.execute("delete from silver.inventory_events")
        con.execute("delete from silver.inventory_hourly")
        con.execute("delete from silver.inventory_partition_coverage")
        con.execute("delete from silver.inventory_cutoff")
        for offset, (event_id, at, (product_id, store_id), qty) in enumerate(events):
            con.execute(
                "insert into silver.inventory_events (event_id, event_time, store_id, product_id, qty_delta, "
                "event_type, is_late, kafka_partition, kafka_offset, kafka_timestamp, export_run_id) "
                "values (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)",
                [event_id, f"{_DAY} {at}:00", store_id, product_id, qty,
                 "receipt" if qty > 0 else "sale_deduction", event_id == "E5", offset,
                 f"{_DAY} {at}:00", run_id],
            )
        if extra_run_id is not None:
            con.execute(
                "insert into silver.inventory_events (event_id, event_time, store_id, product_id, qty_delta, "
                "event_type, kafka_partition, kafka_offset, export_run_id) "
                "values ('E9', ?, ?, ?, 1, 'receipt', 0, 99, ?)",
                [f"{_DAY} 09:20:00", pair_a[1], pair_a[0], extra_run_id],
            )
        for hour, (product_id, store_id), qty in flink_hours:
            con.execute(
                "insert into silver.inventory_hourly values (?, ?, ?, ?, ?, 0, true, ?)",
                [_DATE_KEY, hour, product_id, store_id, qty, "FLINK"],
            )
        incomplete = 0
        for partition in range(_PARTITIONS):
            lagging = partition == incomplete_partition
            con.execute(
                "insert into silver.inventory_partition_coverage values "
                "(?, 1, 'inventory.events.v1', ?, 0, 100, ?, ?, ?, ?)",
                [run_id, partition, 99 if lagging else 100, 1 if lagging else 0,
                 "INCOMPLETE" if lagging else "COMPLETE", run_id],
            )
            incomplete += int(lagging)
        con.execute(
            "insert into silver.inventory_cutoff values (?, 1, ?, ?, ?, ?, ?)",
            [run_id, closed_hour, closed_hour, status, incomplete, run_id],
        )


def _publish_cycle(tmp: Path, db: Path) -> None:
    with duckdb.connect(str(db)) as con:
        clone_live_to_pending(con, _PUBLISH, dialect="duckdb")
    _dbt(tmp, "run", "--vars", _PENDING, "--select", *_INVENTORY_MODELS)
    _dbt(tmp, "test", "--vars", _PENDING, "--select", *_TESTS)
    with duckdb.connect(str(db)) as con:
        publish_gold(con, _PUBLISH, dialect="duckdb")
    _dbt(tmp, "run", "--select", "inventory_current_position")


def _snapshot(db: Path) -> tuple[dict, dict, tuple]:
    """Live fact rows, current positions, and the published cutoff."""
    with duckdb.connect(str(db), read_only=True) as con:
        duplicates = con.execute(
            "select count(*) - count(distinct (snapshot_date_key, snapshot_hour, product_key, store_key)) "
            "from finance.fact_inventory_snapshot"
        ).fetchone()[0]
        assert duplicates == 0
        fact = {
            (product_id, store_id, hour): (int(qty), bool(estimated))
            for product_id, store_id, hour, qty, estimated in con.execute(
                "select p.product_id, s.store_id, f.snapshot_hour, f.quantity_on_hand, f.is_estimated "
                "from finance.fact_inventory_snapshot f "
                "join marketing.dim_product p on f.product_key = p.product_key "
                "join finance.dim_store s on f.store_key = s.store_key"
            ).fetchall()
        }
        current = {
            (product_id, store_id): int(qty)
            for product_id, store_id, qty in con.execute(
                "select product_id, store_id, current_on_hand_qty from serving.inventory_current_position "
                "where provisional_qty_delta <> 0 or published_hour_key is not null"
            ).fetchall()
        }
        control = con.execute(
            "select inventory_run_id, closed_hour_exclusive, coverage_status "
            "from control.inventory_publication"
        ).fetchone()
    return fact, current, control


def _live_rows(db: Path) -> tuple[list, list]:
    with duckdb.connect(str(db), read_only=True) as con:
        fact = con.execute(
            "select snapshot_date_key, snapshot_hour, product_key, store_key, quantity_on_hand, is_estimated "
            "from finance.fact_inventory_snapshot order by 1, 2, 3, 4"
        ).fetchall()
        control = con.execute(
            "select inventory_run_id, closed_hour_exclusive, coverage_status, incomplete_partitions "
            "from control.inventory_publication"
        ).fetchall()
    return fact, control


def test_canonical_cutoff_and_late_event_through_gold(tmp_path: Path) -> None:
    assert "identity_graph" not in _INVENTORY_MODELS
    db = tmp_path / "local_retail.duckdb"
    (tmp_path / "profiles.yml").write_text(
        "global_retail_analytics:\n  target: local\n  outputs:\n    local:\n"
        f"      type: duckdb\n      path: '{db.as_posix()}'\n      threads: 4\n",
        encoding="utf-8",
    )
    if not (_DBT_DIR / "dbt_packages").is_dir():
        _dbt(tmp_path, "deps")
    _dbt(tmp_path, "seed")
    with duckdb.connect(str(db), read_only=True) as con:
        (a_product, a_store, a_open), (b_product, b_store, b_open) = con.execute(
            "select o.product_id, o.store_id, o.opening_qty from reference.inventory_opening_balance o "
            "join finance.dim_store s on o.store_id = s.store_id "
            "order by o.product_id, o.store_id limit 2"
        ).fetchall()
    pair_a, pair_b = (a_product, a_store), (b_product, b_store)
    a_open, b_open = int(a_open), int(b_open)

    # 1. A lagging partition holds the cutoff at 11:00. Gold is canonical only.
    _write_silver(
        db, "R1", pair_a, pair_b,
        closed_hour=2026110411, status="INCOMPLETE", incomplete_partition=7, late_receipt=False,
    )
    _dbt(tmp_path, "run", "--vars", _PENDING, "--select", "+dim_product")
    bootstrap(db)
    _publish_cycle(tmp_path, db)
    fact, current, control = _snapshot(db)
    assert control == ("R1", 2026110411, "INCOMPLETE")
    assert fact == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_b, 10): (b_open - 4, False),
    }
    assert current[pair_a] == a_open - 11
    assert current[pair_b] == b_open - 4

    # 2. Coverage catches up through 15:00. Later hours become canonical, not estimated.
    _write_silver(
        db, "R2", pair_a, pair_b,
        closed_hour=2026110415, status="COMPLETE", incomplete_partition=None, late_receipt=False,
    )
    _publish_cycle(tmp_path, db)
    fact, current, control = _snapshot(db)
    assert control == ("R2", 2026110415, "COMPLETE")
    assert fact == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_a, 11): (a_open - 7, False),
        (*pair_a, 13): (a_open - 8, False),
        (*pair_a, 14): (a_open - 11, False),
        (*pair_b, 10): (b_open - 4, False),
    }
    assert current[pair_a] == a_open - 11
    assert current[pair_b] == b_open - 4

    # 3. An hourly export during the published window is added once, and not to the fact.
    with duckdb.connect(str(db)) as con:
        con.execute(
            "insert into silver.inventory_hourly values (?, 15, ?, ?, -4, 0, true, 'HOURLY')",
            [_DATE_KEY, pair_a[0], pair_a[1]],
        )
        # One committed export: the previous Flink rows are this same export.
        con.execute("update silver.inventory_hourly set export_run_id = 'HOURLY'")
    held = _live_rows(db)
    _dbt(tmp_path, "run", "--select", "inventory_current_position")
    _dbt(tmp_path, "test", "--select", "inventory_hourly_export_is_one")
    fact, current, _ = _snapshot(db)
    assert _live_rows(db)[0] == held[0]
    assert current[pair_a] == a_open - 15
    assert fact[(*pair_a, 14)] == (a_open - 11, False)

    # 4. The late +10 receipt restates hour 10 and every later balance, once.
    _write_silver(
        db, "R3", pair_a, pair_b,
        closed_hour=2026110415, status="COMPLETE", incomplete_partition=None, late_receipt=True,
    )
    _publish_cycle(tmp_path, db)
    fact, current, control = _snapshot(db)
    assert control[1] == 2026110415
    assert fact == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_a, 10): (a_open + 5, False),
        (*pair_a, 11): (a_open + 3, False),
        (*pair_a, 13): (a_open + 2, False),
        (*pair_a, 14): (a_open - 1, False),
        (*pair_b, 10): (b_open - 4, False),
    }
    assert current[pair_a] == a_open - 1
    assert current[pair_b] == b_open - 4

    # 5. The same export again changes nothing.
    before = _snapshot(db)
    _write_silver(
        db, "R4", pair_a, pair_b,
        closed_hour=2026110415, status="COMPLETE", incomplete_partition=None, late_receipt=True,
    )
    _publish_cycle(tmp_path, db)
    assert _snapshot(db)[0] == before[0]
    assert _snapshot(db)[1] == before[1]

    # 6. No stock movement, and the cutoff still advances.
    _write_silver(
        db, "R5", pair_a, pair_b,
        closed_hour=2026110416, status="COMPLETE", incomplete_partition=None, late_receipt=True,
    )
    _publish_cycle(tmp_path, db)
    fact, current, control = _snapshot(db)
    assert fact == before[0]
    assert current[pair_a] == a_open - 1
    assert control == ("R5", 2026110416, "COMPLETE")

    # 7. Spark wrote Silver, then dbt fails. Live fact and control stay on R5.
    published = _live_rows(db)
    failed = _dbt(tmp_path, "run", "--vars", "{", "--select", "fact_inventory_snapshot", ok=False)
    assert failed.returncode != 0
    assert _live_rows(db) == published

    _write_silver(
        db, "R6", pair_a, pair_b,
        closed_hour=2026110417, status="COMPLETE", incomplete_partition=None, late_receipt=True,
        extra_run_id="OTHER",
    )
    mixed = _dbt(tmp_path, "test", "--select", "inventory_canonical_run_is_one", ok=False)
    assert mixed.returncode != 0
    assert _live_rows(db) == published
