"""Late-event recovery demo for the inventory lambda (ADR-012). Local stack only.

One product/store with opening_qty 100 and no movements gets a -5 sale that
Flink aggregates, then a +10 receipt whose event_time falls in the sale's
already-closed Flink window. Flink drops the receipt as late; Bronze keeps it;
Spark's canonical replay restores it once the hour is covered, so the published
balance moves 100 -> 95 (provisional) -> 105 (canonical) and a rerun leaves 105.

    python scripts/local/demo_inventory_late_event.py pick
    python scripts/local/demo_inventory_late_event.py sale
    (background traffic: run_local_stack.ps1 -Task simulate, ~3 minutes)
    python scripts/local/demo_inventory_late_event.py late
    run_local_stack.ps1 -Task spark; -Task dbt; -Task quality
    python scripts/local/demo_inventory_late_event.py show
    (after the sale's hour has closed, with traffic past it: repeat the three
    stack tasks and `show`, then repeat them again for the rerun)

State (chosen pair, event ids, Kafka offsets) is kept in .local/demo/.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
from confluent_kafka import Producer

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from ingestion.kafka.msk_config import build_producer_config  # noqa: E402
from ingestion.kafka.producer_sim.inventory_producer import (  # noqa: E402
    InventoryEvent,
    message_key,
)
from ingestion.kafka.sim_entities import OPENING_STOCK  # noqa: E402
from ingestion.kafka.topics import TOPIC_INVENTORY  # noqa: E402

_STATE = _REPO / ".local" / "demo" / "inventory_late_event.json"
_DUCKDB = _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
OPENING_QTY = 100
# Local Flink: 1-minute windows plus a 60 s watermark. Waiting longer than
# both guarantees the receipt's window has fired before the receipt arrives.
MIN_SECONDS_BEFORE_LATE = 180


def _load_state() -> dict:
    if not _STATE.exists():
        raise SystemExit(f"No demo state at {_STATE}; run `pick` first.")
    return json.loads(_STATE.read_text(encoding="utf-8"))


def _save_state(state: dict) -> None:
    _STATE.parent.mkdir(parents=True, exist_ok=True)
    _STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(_DUCKDB), read_only=True)


def pick() -> None:
    """Choose a pair that starts at 100 and that no loaded export has touched."""
    candidates = sorted(pair for pair, qty in OPENING_STOCK.items() if qty == OPENING_QTY)
    con = _connect()
    try:
        touched = set(
            con.sql(
                "select product_id, store_id from silver.inventory_events "
                "union select product_id, store_id from silver.inventory_hourly"
            ).fetchall()
        )
    finally:
        con.close()
    for product_id, store_id in candidates:
        if (product_id, store_id) not in touched:
            _save_state({"product_id": product_id, "store_id": store_id, "events": {}})
            print(f"Demo pair: {product_id} / {store_id}, opening_qty {OPENING_QTY}")
            return
    raise SystemExit("Every pair with opening_qty 100 already has movements.")


def _send(event: InventoryEvent) -> dict:
    delivered: dict = {}

    def report(err, msg) -> None:
        if err:
            raise RuntimeError(f"Kafka delivery failed: {err}")
        delivered.update(partition=msg.partition(), offset=msg.offset())

    producer = Producer(build_producer_config("inventory-late-event-demo"))
    producer.produce(
        TOPIC_INVENTORY,
        key=message_key(event),
        value=json.dumps(asdict(event)).encode("utf-8"),
        callback=report,
    )
    if producer.flush(30) or not delivered:
        raise SystemExit("Kafka did not acknowledge the event within 30 s.")
    return {**asdict(event), **delivered}


def _event(state: dict, *, qty: int, event_type: str, event_time: datetime, late: bool) -> InventoryEvent:
    return InventoryEvent(
        event_id=str(uuid.uuid4()),
        event_time=event_time.isoformat(timespec="milliseconds"),
        store_id=state["store_id"],
        product_id=state["product_id"],
        qty_delta=qty,
        event_type=event_type,
        scanner_id="SCANNER-01",
        is_late=late,
    )


def sale() -> None:
    state = _load_state()
    if "sale" in state["events"]:
        raise SystemExit("The sale was already sent; start over with `pick`.")
    sent = _send(_event(state, qty=-5, event_type="sale_deduction",
                        event_time=datetime.now(timezone.utc), late=False))
    state["events"]["sale"] = sent
    _save_state(state)
    print(json.dumps(sent, indent=2))


def late() -> None:
    state = _load_state()
    sale_event = state["events"].get("sale")
    if sale_event is None or "late" in state["events"]:
        raise SystemExit("Send the sale first, and the late receipt only once.")
    sale_time = datetime.fromisoformat(sale_event["event_time"])
    waited = (datetime.now(timezone.utc) - sale_time).total_seconds()
    if waited < MIN_SECONDS_BEFORE_LATE:
        raise SystemExit(f"Only {waited:.0f}s since the sale; wait {MIN_SECONDS_BEFORE_LATE}s.")
    # Same Flink window as the sale, which has already fired.
    sent = _send(_event(state, qty=10, event_type="receipt",
                        event_time=sale_time + timedelta(seconds=1), late=True))
    state["events"]["late"] = sent
    _save_state(state)
    print(json.dumps(sent, indent=2))


def _print(con: duckdb.DuckDBPyConnection, title: str, sql: str, params: list) -> None:
    print(f"\n== {title}")
    rel = con.execute(sql, params)
    columns = [d[0] for d in rel.description]
    rows = rel.fetchall()
    print(" | ".join(columns))
    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))
    if not rows:
        print("(no rows)")


def show() -> None:
    state = _load_state()
    pair = [state["product_id"], state["store_id"]]
    print(f"Demo pair {pair[0]} / {pair[1]}; events sent:")
    for name, event in state["events"].items():
        print(f"  {name}: {event['qty_delta']:+d} at {event['event_time']} "
              f"(event_id {event['event_id']}, partition {event['partition']}, offset {event['offset']})")
    con = _connect()
    try:
        _print(con, "Flink speed layer: silver.inventory_hourly",
               "select snapshot_date_key, snapshot_hour, qty_delta_hour, qty_received_hour, export_run_id "
               "from silver.inventory_hourly where product_id = ? and store_id = ? order by 1, 2", pair)
        _print(con, "Spark canonical replay of Bronze: silver.inventory_events",
               "select event_id, event_time, qty_delta, event_type, kafka_partition, kafka_offset, "
               "kafka_timestamp, export_run_id from silver.inventory_events "
               "where product_id = ? and store_id = ? order by kafka_offset", pair)
        _print(con, "Bronze offset gaps (all partitions)",
               "select count(*) as gap_count from silver.inventory_offset_gaps", [])
        _print(con, "Spark coverage", "select * from intermediate.int_inventory_batch_coverage", [])
        _print(con, "dbt hour selection: intermediate.int_inventory_hourly",
               "select snapshot_date_key, snapshot_hour, qty_delta_hour, is_estimated, export_run_id "
               "from intermediate.int_inventory_hourly where product_id = ? and store_id = ? order by 1, 2",
               pair)
        _print(con, "Published Gold: finance.fact_inventory_snapshot",
               "select f.snapshot_date_key, f.snapshot_hour, f.quantity_on_hand, f.is_estimated "
               "from finance.fact_inventory_snapshot f "
               "join marketing.dim_product p on f.product_key = p.product_key "
               "join finance.dim_store s on f.store_key = s.store_key "
               "where p.product_id = ? and s.store_id = ? order by 1, 2", pair)
        _print(con, "Published Gold: summary.inventory_position_daily (movement days)",
               "select snapshot_date_key, ending_on_hand_qty, stockout_hour_count from "
               "summary.inventory_position_daily where product_id = ? and store_id = ? and had_movement "
               "order by 1", pair)
        _print(con, "Serving: serving.inventory_current_position",
               "select published_on_hand_qty, provisional_qty_delta, current_on_hand_qty, "
               "published_hour_key, as_of_hour_key, is_provisional "
               "from serving.inventory_current_position where product_id = ? and store_id = ?", pair)
        others = con.execute(
            "select count(*) from silver.inventory_events where product_id = ? and store_id = ? "
            "and event_id not in (select unnest(?))",
            [*pair, [e["event_id"] for e in state["events"].values()]],
        ).fetchone()[0]
        print(f"\nCanonical events on the pair other than the demo's: {others}")
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=["pick", "sale", "late", "show"])
    step = parser.parse_args().step
    {"pick": pick, "sale": sale, "late": late, "show": show}[step]()


if __name__ == "__main__":
    main()
