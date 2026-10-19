"""
POS daily snapshot producer simulator.
Emits transaction line items to Kafka to mimic daily batch export replay.
"""

from __future__ import annotations

import os
import json
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import structlog
from confluent_kafka import Producer

from ingestion.kafka.sim_entities import LOYALTY_IDS, PRODUCTS, STORES
from ingestion.kafka.topics import TOPIC_POS

log = structlog.get_logger()

KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "client.id": "pos-producer-sim",
}

TOPIC = TOPIC_POS


@dataclass
class PosTransactionLine:
    """Represents one POS line item record."""

    transaction_id: str
    line_item_number: int
    transaction_date: str
    store_id: str
    product_id: str
    loyalty_id: Optional[str]
    quantity_sold: int
    gross_revenue: float
    net_revenue: float
    gross_margin: float
    is_voided: bool

    @classmethod
    def generate(cls, transaction_id: str, line_item_number: int) -> "PosTransactionLine":
        quantity = random.randint(1, 4)
        unit_price = round(random.uniform(4.0, 120.0), 2)
        gross = round(quantity * unit_price, 2)
        discount = round(gross * random.uniform(0.0, 0.2), 2)
        net = round(gross - discount, 2)
        margin = round(net * random.uniform(0.1, 0.35), 2)
        return cls(
            transaction_id=transaction_id,
            line_item_number=line_item_number,
            transaction_date=datetime.now(timezone.utc).date().isoformat(),
            store_id=random.choice(STORES),
            product_id=random.choice(PRODUCTS),
            loyalty_id=random.choice(LOYALTY_IDS) if random.random() < 0.7 else None,
            quantity_sold=quantity,
            gross_revenue=gross,
            net_revenue=net,
            gross_margin=margin,
            is_voided=random.random() < 0.01,
        )


def delivery_report(err, msg) -> None:
    """Kafka delivery callback."""
    if err:
        log.error("delivery_failed", error=str(err))
    else:
        log.debug(
            "delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def run_producer(transaction_count: int = 5000, max_lines_per_txn: int = 5) -> None:
    """Emit a synthetic daily POS snapshot as individual line-item events."""
    producer = Producer(KAFKA_CONFIG)
    emitted = 0
    started_at = time.time()

    log.info(
        "pos_producer_started",
        transaction_count=transaction_count,
        max_lines_per_txn=max_lines_per_txn,
    )

    for _ in range(transaction_count):
        transaction_id = str(uuid.uuid4())
        line_count = random.randint(1, max_lines_per_txn)
        for line_item_number in range(1, line_count + 1):
            event = PosTransactionLine.generate(transaction_id, line_item_number)
            payload = json.dumps(asdict(event)).encode("utf-8")
            producer.produce(
                topic=TOPIC,
                key=transaction_id.encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            emitted += 1
        producer.poll(0)

    producer.flush()
    elapsed_seconds = round(time.time() - started_at, 2)
    log.info("pos_producer_finished", emitted=emitted, elapsed_seconds=elapsed_seconds)


if __name__ == "__main__":
    run_producer(transaction_count=1000, max_lines_per_txn=4)
