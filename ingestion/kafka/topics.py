"""
Kafka topic bootstrap utility + canonical topic-name constants.

Single source of truth for topic names and partition counts. Other modules
import the `TOPIC_*` constants instead of hardcoding strings — this is the
file to edit when a topic is added or renamed.

Used by:
  * `ingestion/kafka/producer_sim/*` — imports `TOPIC_*` for the produce() call
  * `transformation/redshift/seeds/` and Airflow DAGs — reference topic names
  * `tests/unit/test_topics.py` — imports `PARTITIONS_BY_TOPIC` to assert the
    bootstrap list matches the contract
  * This module's `create_topics()` — builds the `NewTopic` list from the
    constants and runs against the local Kafka cluster

Cloud MSK topics are provisioned by `infra/terraform/modules/kafka/main.tf`;
that module's `topic_names` local must match `tuple(PARTITIONS_BY_TOPIC)`
exactly — `tests/unit/test_topics.py` guards the Python side, and Terraform
`plan` will show drift on the cloud side.
"""

from __future__ import annotations

import os

from confluent_kafka.admin import AdminClient, NewTopic
import structlog

log = structlog.get_logger()

# Default 127.0.0.1 avoids Windows IPv6 localhost (::1) + Docker IPv4 publish issues.
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# --- Topic name constants (single source of truth) ------------------------- #
TOPIC_INVENTORY = "inventory.events.v1"
TOPIC_CLICKSTREAM = "clickstream.events.v1"
TOPIC_POS = "pos.transactions.v1"

TOPIC_DLQ_EVENTS = "dlq.events.v1"
TOPIC_DLQ_CLICKSTREAM_SCHEMA = "dlq.clickstream.schema_violations"
TOPIC_DLQ_CLICKSTREAM_BUSINESS = "dlq.clickstream.business_violations"
TOPIC_DLQ_INVENTORY_SCHEMA = "dlq.inventory.schema_violations"

# --- Partition counts (source of truth for tests + Terraform) -------------- #
# Keys must stay in sync with `infra/terraform/modules/kafka/main.tf:topic_names`.
PARTITIONS_BY_TOPIC: dict[str, int] = {
    TOPIC_INVENTORY: 12,
    TOPIC_CLICKSTREAM: 24,
    TOPIC_POS: 6,
    TOPIC_DLQ_EVENTS: 6,
    TOPIC_DLQ_CLICKSTREAM_SCHEMA: 6,
    TOPIC_DLQ_CLICKSTREAM_BUSINESS: 6,
    TOPIC_DLQ_INVENTORY_SCHEMA: 6,
}

# Local docker-compose runs with replication.factor=1; cloud MSK Serverless
# manages replication internally.
REPLICATION_FACTOR = 1

TOPICS: list[NewTopic] = [
    NewTopic(name, num_partitions=partitions, replication_factor=REPLICATION_FACTOR)
    for name, partitions in PARTITIONS_BY_TOPIC.items()
]


def create_topics() -> None:
    """Create all required topics if they do not exist."""
    admin = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "socket.connection.setup.timeout.ms": 20000,
            "request.timeout.ms": 30000,
        }
    )
    futures = admin.create_topics(TOPICS, request_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("topic_created", topic=topic)
        except Exception as exc:  # pragma: no cover - broker state dependent
            log.info("topic_exists_or_failed", topic=topic, error=str(exc))


if __name__ == "__main__":
    create_topics()
