"""Inventory lambda (ADR-012) through the real dbt incremental build and WAP publish.

Runs in a pytest temporary directory with its own profile and its own
``local_retail.duckdb`` (the filename the views bind), never the stack
database. Each step writes one Spark export of fixture Silver tables, clones
live ``finance.fact_inventory_snapshot`` to pending, builds it with
``wap_phase=pending``, publishes it with the shared WAP helper, then rebuilds
``serving.inventory_current_position``:

1. Bronze partition 7 lags at 11:40 while the others reach 14:30, so hours from
   11 stay on Flink (is_estimated) instead of being published as canonical.
2. Partition 7 catches up: those hours become canonical, balances unchanged.
3. A late +10 receipt for 10:30 reaches Bronze. Hour 10 and every later
   balance of that product/store move by +10 in the fact, and the current
   position moves by +10. The other pair and earlier hours do not change.
4. The same inputs again change nothing.

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
# Pinned to ingestion/kafka/topics.py by tests/unit/test_gold_wap.py; read here
# so the dbt CI job needs no Kafka client.
_PARTITIONS = yaml.safe_load((_DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))[
    "vars"]["inventory_kafka_partitions"]
_FACT = [("finance", "fact_inventory_snapshot")]
_PENDING = json.dumps({"wap_phase": "pending"})
_INVENTORY_MODELS = (
    "int_inventory_batch_coverage", "int_inventory_coverage_gaps",
    "int_inventory_hourly", "fact_inventory_snapshot",
)
_DAY = "2026-11-04"

_SILVER_DDL = (
    """create or replace table silver.inventory_events (
        event_id varchar, event_time timestamp, store_id varchar, product_id varchar,
        qty_delta integer, event_type varchar, scanner_id varchar, is_late boolean,
        schema_version varchar, event_date date, source_system varchar, kafka_topic varchar,
        kafka_partition integer, kafka_offset bigint, kafka_timestamp timestamp,
        ingested_at timestamp, export_run_id varchar)""",
    """create or replace table silver.inventory_hourly (
        snapshot_date_key integer, snapshot_hour integer, product_id varchar, store_id varchar,
        qty_delta_hour bigint, qty_received_hour bigint, is_estimated boolean, export_run_id varchar)""",
    """create or replace table silver.inventory_offset_gaps (
        source_system varchar, kafka_topic varchar, kafka_partition integer,
        last_offset_before bigint, first_offset_after bigint, missing_offsets bigint,
        gap_start_ts timestamp, gap_end_ts timestamp, export_run_id varchar)""",
    """create or replace table silver.inventory_partition_coverage (
        source_system varchar, kafka_topic varchar, kafka_partition integer,
        first_offset bigint, last_offset bigint,
        first_kafka_ts timestamp, last_kafka_ts timestamp, export_run_id varchar)""",
)


def _dbt(tmp: Path, *args: str) -> None:
    env = {**os.environ, "DBT_TARGET_PATH": str(tmp / "target"), "DBT_LOG_PATH": str(tmp / "logs")}
    result = subprocess.run(
        [_DBT, *args, "--profiles-dir", str(tmp), "--target", "local"],
        cwd=_DBT_DIR, env=env, text=True, capture_output=True, check=False,
    )
    assert result.returncode == 0, f"dbt {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"


def _write_silver(db: Path, run_id: str, pair_a: tuple[str, str], pair_b: tuple[str, str], *,
                  lagging_partition: int | None, late_receipt: bool) -> None:
    """One Spark export: canonical events, Flink hours, and Bronze partition coverage."""
    events = [
        ("E1", "09:10", pair_a, -5, "09:10"),
        ("E2", "11:20", pair_a, -2, "11:20"),
        ("E3", "13:05", pair_a, -1, "13:05"),
        ("E4", "10:15", pair_b, -4, "10:15"),
        ("E6", "14:10", pair_a, -3, "14:10"),
    ]
    if late_receipt:
        events.append(("E5", "10:30", pair_a, 10, "13:50"))
    # Flink dropped the late receipt: it arrived after its window closed.
    flink_hours = [(9, pair_a, -5), (11, pair_a, -2), (13, pair_a, -1), (14, pair_a, -3), (10, pair_b, -4)]
    with duckdb.connect(str(db)) as con:
        for ddl in _SILVER_DDL:
            con.execute(ddl)
        for offset, (event_id, at, (product_id, store_id), qty, kafka_at) in enumerate(events):
            con.execute(
                "insert into silver.inventory_events (event_id, event_time, store_id, product_id, qty_delta, "
                "event_type, is_late, kafka_partition, kafka_offset, kafka_timestamp, export_run_id) "
                "values (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)",
                [event_id, f"{_DAY} {at}:00", store_id, product_id, qty,
                 "receipt" if qty > 0 else "sale_deduction", event_id == "E5", offset,
                 f"{_DAY} {kafka_at}:00", run_id],
            )
        for hour, (product_id, store_id), qty in flink_hours:
            con.execute(
                "insert into silver.inventory_hourly values (?, ?, ?, ?, ?, 0, true, ?)",
                [int(_DAY.replace("-", "")), hour, product_id, store_id, qty, run_id],
            )
        for partition in range(_PARTITIONS):
            last = "11:40" if partition == lagging_partition else "14:30"
            con.execute(
                "insert into silver.inventory_partition_coverage values "
                "('retail-v1', 'inventory', ?, 0, 100, ?, ?, ?)",
                [partition, f"{_DAY} 08:05:00", f"{_DAY} {last}:00", run_id],
            )


def _publish_cycle(tmp: Path, db: Path) -> None:
    with duckdb.connect(str(db)) as con:
        clone_live_to_pending(con, _FACT, dialect="duckdb")
    _dbt(tmp, "run", "--vars", _PENDING, "--select", *_INVENTORY_MODELS)
    with duckdb.connect(str(db)) as con:
        publish_gold(con, _FACT, dialect="duckdb")
    _dbt(tmp, "run", "--select", "inventory_current_position")
    _dbt(tmp, "test", "--select", "inventory_hourly_uses_one_export_run")


def _snapshot(db: Path) -> tuple[dict, dict]:
    """Live fact rows and current positions keyed by product/store (and hour)."""
    with duckdb.connect(str(db), read_only=True) as con:
        duplicates = con.execute(
            "select count(*) - count(distinct (snapshot_date_key, snapshot_hour, product_key, store_key)) "
            "from finance.fact_inventory_snapshot"
        ).fetchone()[0]
        assert duplicates == 0
        fact = {
            (product_id, store_id, hour): (int(qty), estimated)
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
    return fact, current


def test_partition_coverage_and_late_event_through_gold(tmp_path: Path) -> None:
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

    # 1. Partition 7 lags at 11:40: hours from 11 are not fully captured in Bronze.
    _write_silver(db, "R1", pair_a, pair_b, lagging_partition=7, late_receipt=False)
    # The catalog publishes the products the inventory events mention.
    _dbt(tmp_path, "run", "--vars", _PENDING, "--select", "+dim_product")
    bootstrap(db)
    _publish_cycle(tmp_path, db)
    fact, _ = _snapshot(db)
    assert fact == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_a, 11): (a_open - 7, True),
        (*pair_a, 13): (a_open - 8, True),
        (*pair_a, 14): (a_open - 11, True),
        (*pair_b, 10): (b_open - 4, False),
    }

    # 2. Partition 7 catches up: hours 11 and 13 become canonical, balances unchanged.
    _write_silver(db, "R2", pair_a, pair_b, lagging_partition=None, late_receipt=False)
    _publish_cycle(tmp_path, db)
    before, current_before = _snapshot(db)
    assert before == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_a, 11): (a_open - 7, False),
        (*pair_a, 13): (a_open - 8, False),
        (*pair_a, 14): (a_open - 11, True),
        (*pair_b, 10): (b_open - 4, False),
    }
    assert current_before[pair_a] == a_open - 11

    # 3. The late +10 receipt for 10:30 restates hour 10 and every later balance.
    _write_silver(db, "R3", pair_a, pair_b, lagging_partition=None, late_receipt=True)
    _publish_cycle(tmp_path, db)
    after, current_after = _snapshot(db)
    assert after == {
        (*pair_a, 9): (a_open - 5, False),
        (*pair_a, 10): (a_open + 5, False),
        (*pair_a, 11): (a_open + 3, False),
        (*pair_a, 13): (a_open + 2, False),
        (*pair_a, 14): (a_open - 1, True),
        (*pair_b, 10): (b_open - 4, False),
    }
    assert current_after[pair_a] == current_before[pair_a] + 10
    assert current_after[pair_b] == current_before[pair_b] == b_open - 4

    # 4. A rerun over the same export changes nothing.
    _write_silver(db, "R4", pair_a, pair_b, lagging_partition=None, late_receipt=True)
    _publish_cycle(tmp_path, db)
    assert _snapshot(db) == (after, current_after)
