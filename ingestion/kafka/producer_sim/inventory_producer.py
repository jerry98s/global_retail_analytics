"""
Inventory stream producer simulator.
Emits realistic stock-change events at configurable throughput.
Simulates: normal events, late events, duplicate retries, poison messages.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from typing import Optional
from confluent_kafka import Producer
import structlog

from ingestion.kafka.msk_config import build_producer_config
from ingestion.kafka.sim_entities import PRODUCTS, STORES
from ingestion.kafka.topics import TOPIC_INVENTORY

log = structlog.get_logger()

KAFKA_CONFIG = build_producer_config("inventory-producer-sim")

TOPIC = TOPIC_INVENTORY


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
        force_duplicate: bool = False,
        event_id: Optional[str] = None
    ) -> "InventoryEvent":
        event_time = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
        return cls(
            event_id   = event_id or str(uuid.uuid4()),
            event_time = event_time.isoformat(),
            store_id   = random.choice(STORES),
            product_id = random.choice(PRODUCTS),
            qty_delta  = random.choice([-3, -2, -1, -1, -1, 5, 10, 20]),
            event_type = random.choices(
                ["sale_deduction", "receipt", "adjustment", "transfer"],
                weights=[70, 20, 5, 5]
            )[0],
            scanner_id = f"SCANNER-{random.randint(1, 10):02d}",
            schema_version = "2.0.0",
            is_late    = late_seconds > 60
        )


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
    duplicate_pct: float = 0.001,    # 0.1% duplicate retries
):
    producer = Producer(KAFKA_CONFIG)
    total_sent = 0
    start_time = time.time()

    log.info("producer_started", eps=events_per_second,
             duration=duration_seconds)

    recent_event_ids = []  # for duplicate simulation

    while time.time() - start_time < duration_seconds:
        batch_start = time.time()

        for _ in range(events_per_second):
            # Determine event scenario
            r = random.random()

            if r < duplicate_pct and recent_event_ids:
                # Simulate client retry — same event_id
                event = InventoryEvent.generate(
                    event_id=random.choice(recent_event_ids)
                )
                log.debug("emitting_duplicate", event_id=event.event_id)

            elif r < late_event_pct:
                # Simulate late-arriving event
                late_seconds = random.randint(61, 300)
                event = InventoryEvent.generate(late_seconds=late_seconds)

            else:
                # Normal event
                event = InventoryEvent.generate()
                recent_event_ids.append(event.event_id)
                if len(recent_event_ids) > 1000:
                    recent_event_ids.pop(0)

            payload = json.dumps(asdict(event)).encode("utf-8")
            producer.produce(
                topic     = TOPIC,
                key       = f"{event.store_id}:{event.product_id}".encode(),
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
