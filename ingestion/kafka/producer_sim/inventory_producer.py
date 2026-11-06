"""
Inventory stream producer simulator.
Emits realistic stock-change events at configurable throughput.
Simulates: normal events, late events, client retries (the original bytes
resent unchanged) and, rarely, conflicting copies (an old event_id with a
different payload, which Spark must flag rather than treat as a retry).
"""

import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, replace
from typing import Optional
from confluent_kafka import Producer
import structlog

from ingestion.kafka.msk_config import build_producer_config
from ingestion.kafka.sim_entities import PRODUCTS, STORES
from ingestion.kafka.topics import TOPIC_INVENTORY

log = structlog.get_logger()

KAFKA_CONFIG = build_producer_config("inventory-producer-sim")

TOPIC = TOPIC_INVENTORY

EVENT_TYPE_WEIGHTS = {"sale_deduction": 70, "receipt": 20, "adjustment": 5, "transfer": 5}
# Sign follows the movement: sales and outbound transfers remove stock,
# receipts add case quantities, cycle-count adjustments go either way.
QTY_BY_EVENT_TYPE = {
    "sale_deduction": (-1, -1, -1, -2, -3),
    "receipt": (5, 10),
    "adjustment": (-2, -1, 1, 2),
    "transfer": (-5, -3, -2),
}


@dataclass
class InventoryEvent:
    event_id:    str
    event_time:  str
    store_id:    str
    product_id:  str
    qty_delta:   int           # negative = sold/transferred, positive = received
    event_type:  str           # 'sale_deduction','receipt','adjustment','transfer'
    scanner_id:  str
    schema_version: str = "2.0.0"
    is_late:     bool = False  # metadata flag for simulation tracking

    @classmethod
    def generate(
        cls,
        late_seconds: int = 0,
        event_id: Optional[str] = None
    ) -> "InventoryEvent":
        event_time = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
        event_type = random.choices(
            list(EVENT_TYPE_WEIGHTS), weights=list(EVENT_TYPE_WEIGHTS.values())
        )[0]
        return cls(
            event_id   = event_id or str(uuid.uuid4()),
            event_time = event_time.isoformat(),
            store_id   = random.choice(STORES),
            product_id = random.choice(PRODUCTS),
            qty_delta  = random.choice(QTY_BY_EVENT_TYPE[event_type]),
            event_type = event_type,
            scanner_id = f"SCANNER-{random.randint(1, 10):02d}",
            schema_version = "2.0.0",
            is_late    = late_seconds > 60
        )


def message_key(event: InventoryEvent) -> bytes:
    return f"{event.store_id}:{event.product_id}".encode()


def conflicting_copy(original: InventoryEvent) -> InventoryEvent:
    """Same event_id, store and product, but a different quantity and later time."""
    later = datetime.fromisoformat(original.event_time) + timedelta(seconds=random.randint(1, 120))
    return replace(original, event_time=later.isoformat(), qty_delta=original.qty_delta + 1)


def next_message(
    r: float,
    recent: "deque[tuple[InventoryEvent, bytes]]",
    *,
    duplicate_pct: float,
    conflict_pct: float,
    late_event_pct: float,
) -> tuple[str, InventoryEvent, bytes]:
    """Pick one scenario for a draw ``r`` in [0, 1) and return (kind, event, payload)."""
    if r < duplicate_pct and recent:
        event, payload = random.choice(recent)
        return "retry", event, payload
    if r < duplicate_pct + conflict_pct and recent:
        event = conflicting_copy(random.choice(recent)[0])
        return "conflict", event, json.dumps(asdict(event)).encode("utf-8")
    if r < late_event_pct:
        event = InventoryEvent.generate(late_seconds=random.randint(61, 300))
        return "late", event, json.dumps(asdict(event)).encode("utf-8")
    event = InventoryEvent.generate()
    payload = json.dumps(asdict(event)).encode("utf-8")
    recent.append((event, payload))
    return "normal", event, payload


def delivery_report(err, msg):
    if err:
        log.error("delivery_failed", error=str(err))
    else:
        log.debug("delivered", topic=msg.topic(), partition=msg.partition(),
                  offset=msg.offset())


def run_producer(
    events_per_second: int = 100,
    duration_seconds: int = 300,
    late_event_pct: float = 0.02,    # 2% of events arrive 60–300s late
    duplicate_pct: float = 0.001,    # 0.1% client retries (identical bytes)
    conflict_pct: float = 0.0002,    # 0.02% conflicting copies of an old event_id
):
    producer = Producer(KAFKA_CONFIG)
    total_sent = 0
    start_time = time.time()

    log.info("producer_started", eps=events_per_second,
             duration=duration_seconds)

    recent: deque[tuple[InventoryEvent, bytes]] = deque(maxlen=1000)

    while time.time() - start_time < duration_seconds:
        batch_start = time.time()

        for _ in range(events_per_second):
            kind, event, payload = next_message(
                random.random(), recent,
                duplicate_pct=duplicate_pct,
                conflict_pct=conflict_pct,
                late_event_pct=late_event_pct,
            )
            if kind in ("retry", "conflict"):
                log.debug(f"emitting_{kind}", event_id=event.event_id)
            producer.produce(
                topic     = TOPIC,
                key       = message_key(event),
                value     = payload,
                callback  = delivery_report
            )
            total_sent += 1

        producer.poll(0)

        # Maintain target throughput
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

    producer.flush()
    log.info("producer_finished", total_sent=total_sent)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Inventory Kafka producer simulator")
    parser.add_argument("--eps", type=int, default=100, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Run duration in seconds")
    args = parser.parse_args()
    run_producer(events_per_second=args.eps, duration_seconds=args.duration)
