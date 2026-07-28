"""Unit tests guarding the Kafka topic contract.

Topic names and partition counts are an interface other components depend on,
so changing them should be a deliberate, test-breaking act. The canonical
values live in ``ingestion.kafka.topics.PARTITIONS_BY_TOPIC`` — this module
just re-imports and asserts structural invariants.
"""

from __future__ import annotations

import pytest

from ingestion.kafka import topics

pytestmark = pytest.mark.unit


def test_exact_topic_set():
    """Test source of truth is topics.PARTITIONS_BY_TOPIC — adding a topic
    there is the only way to extend the contract."""
    assert set(topics.PARTITIONS_BY_TOPIC) == {
        "inventory.events.v1",
        "clickstream.events.v1",
        "pos.transactions.v1",
        "dlq.events.v1",
        "dlq.clickstream.schema_violations",
        "dlq.clickstream.business_violations",
        "dlq.inventory.schema_violations",
    }


def test_no_duplicate_topic_names():
    names = [t.topic for t in topics.TOPICS]
    assert len(names) == len(set(names))


def test_topics_list_matches_partitions_dict():
    """TOPICS list must be built 1:1 from PARTITIONS_BY_TOPIC."""
    assert {t.topic: t.num_partitions for t in topics.TOPICS} == topics.PARTITIONS_BY_TOPIC


@pytest.mark.parametrize("name,partitions", list(topics.PARTITIONS_BY_TOPIC.items()))
def test_partition_counts(name, partitions):
    by_name = {t.topic: t for t in topics.TOPICS}
    assert by_name[name].num_partitions == partitions


def test_all_single_replica_for_local():
    assert all(t.replication_factor == 1 for t in topics.TOPICS)


def test_named_constants_match_string_values():
    """The TOPIC_* constants must match the keys of PARTITIONS_BY_TOPIC —
    guards against typos when a new topic is added by name only."""
    assert topics.TOPIC_INVENTORY == "inventory.events.v1"
    assert topics.TOPIC_CLICKSTREAM == "clickstream.events.v1"
    assert topics.TOPIC_POS == "pos.transactions.v1"
    assert topics.TOPIC_DLQ_EVENTS == "dlq.events.v1"
    assert topics.TOPIC_DLQ_CLICKSTREAM_SCHEMA == "dlq.clickstream.schema_violations"
    assert topics.TOPIC_DLQ_CLICKSTREAM_BUSINESS == "dlq.clickstream.business_violations"
    assert topics.TOPIC_DLQ_INVENTORY_SCHEMA == "dlq.inventory.schema_violations"
