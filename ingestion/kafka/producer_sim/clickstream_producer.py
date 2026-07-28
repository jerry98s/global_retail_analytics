"""
Clickstream producer simulator.
Generates high-volume clickstream events for load and schema testing.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog
from confluent_kafka import Producer

from ingestion.kafka.msk_config import build_producer_config
from ingestion.kafka.sim_entities import LOYALTY_IDS, PRODUCTS
from ingestion.kafka.topics import TOPIC_CLICKSTREAM
from streaming.flink_jobs.event_types import (
    CLICKSTREAM_EVENT_TYPES,
    PLATFORMS,
)

log = structlog.get_logger()

KAFKA_CONFIG = build_producer_config(
    "clickstream-producer-sim",
    **{
        "batch.size": 500000,
        "linger.ms": 5,
        "queue.buffering.max.messages": 200000,
        "queue.buffering.max.kbytes": 2097152,
    },
)

TOPIC = TOPIC_CLICKSTREAM
EVENT_TYPES = list(CLICKSTREAM_EVENT_TYPES)


@dataclass
class ClickstreamEvent:
    """Canonical clickstream envelope."""

    event_id: str
    event_type: str
    event_time: str
    session_id: str
    client_id: str
    customer_id: Optional[str]
    platform: str
    app_version: str
    properties: Dict[str, Any]
    schema_version: str = "1.2.0"

    @classmethod
    def generate(cls) -> "ClickstreamEvent":
        event_type = random.choices(
            EVENT_TYPES, weights=[30, 20, 15, 10, 5, 5, 3, 5, 5, 2]
        )[0]
        cart_value = round(random.uniform(20.0, 500.0), 2)
        props: Dict[str, Any] = {
            "page": random.choice(["home", "search", "product", "cart", "checkout"]),
            "product_id": random.choice(PRODUCTS),
            "cart_value": cart_value,
        }
        if event_type == "checkout":
            props["order_id"] = f"ORDER-{uuid.uuid4().hex[:12].upper()}"

        if event_type == "login":
            props["marketing_consent"] = random.random() < 0.85
            props["analytics_consent"] = True

        # Use LOYAL-* ids so clickstream customer_id can link to POS loyalty_id.
        loyalty_pool = random.random() < 0.45
        customer_id = (
            random.choice(LOYALTY_IDS) if loyalty_pool else None
        )

        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            event_time=datetime.now(timezone.utc).isoformat(),
            session_id=f"SESS-{uuid.uuid4().hex[:24]}",
            client_id=f"CLIENT-{uuid.uuid4().hex[:16]}",
            customer_id=customer_id,
            platform=random.choice(PLATFORMS),
            app_version=f"{random.randint(1,4)}.{random.randint(0,9)}.{random.randint(0,9)}",
            properties=props,
        )


def delivery_report(err, msg) -> None:
    """Kafka delivery callback."""
    if err:
        log.error("delivery_failed", error=str(err))


def run_producer(events_per_second: int = 10000, duration_seconds: int = 60) -> None:
    """Emit clickstream events at configured throughput."""
    producer = Producer(KAFKA_CONFIG)
    started_at = time.time()
    emitted = 0

    log.info(
        "clickstream_producer_started",
        events_per_second=events_per_second,
        duration_seconds=duration_seconds,
    )

    while (time.time() - started_at) < duration_seconds:
        tick_started = time.time()
        for _ in range(events_per_second):
            event = ClickstreamEvent.generate()
            payload = json.dumps(asdict(event)).encode("utf-8")

            # Handle local producer backpressure by polling and retrying.
            while True:
                try:
                    producer.produce(
                        topic=TOPIC,
                        key=event.client_id.encode("utf-8"),
                        value=payload,
                        callback=delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(0.05)

            emitted += 1
            if emitted % 500 == 0:
                producer.poll(0)

        producer.poll(0)
        elapsed_tick = time.time() - tick_started
        if elapsed_tick < 1.0:
            time.sleep(1.0 - elapsed_tick)

    producer.flush()
    elapsed_total = round(time.time() - started_at, 2)
    log.info("clickstream_producer_finished", emitted=emitted, elapsed_seconds=elapsed_total)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clickstream Kafka producer simulator")
    parser.add_argument("--eps", type=int, default=10000, help="Events per second")
    parser.add_argument("--duration", type=int, default=30, help="Run duration in seconds")
    args = parser.parse_args()
    run_producer(events_per_second=args.eps, duration_seconds=args.duration)
