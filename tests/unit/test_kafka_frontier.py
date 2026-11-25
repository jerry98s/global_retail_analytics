"""The inventory coverage rule, without Kafka or Spark (ADR-012)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from spark.identity_resolution.kafka_frontier import (
    COMPLETE,
    INCOMPLETE,
    PartitionFrontier,
    PartitionStat,
    assess_coverage,
    frontier_document,
    hour_key,
    kafka_client_properties,
    lost_offset_count,
    parse_acknowledged_loss,
    parse_frontier,
    proven_end_offsets,
    target_hour_exclusive,
)
from streaming.flink_jobs._config import kafka_security_options

pytestmark = pytest.mark.unit

_TARGET = datetime(2026, 11, 21, 23, 0, 0)
_TOPIC = "inventory.events.v1"


def _frontier(partition: int, earliest: int, end: int) -> PartitionFrontier:
    return PartitionFrontier(partition, earliest, end)


def _stat(partition: int, present: int, captured: int | None, successor: datetime | None = None) -> PartitionStat:
    return PartitionStat(partition, present, captured, successor)


def _assess(**overrides):
    args = dict(
        expected_partitions=2,
        frontiers={0: _frontier(0, 0, 10), 1: _frontier(1, 0, 10)},
        stats={0: _stat(0, 10, 9), 1: _stat(1, 10, 9)},
        previous_end_offsets={},
        previous_closed_hour=None,
        target=_TARGET,
        lateness_seconds=3600,
        run_id="R1",
        bronze_snapshot_id=7,
        topic=_TOPIC,
    )
    args.update(overrides)
    return assess_coverage(**args)


def test_target_hour_is_the_latest_hour_old_enough_to_close() -> None:
    # 2026-11-22 00:15 minus 60s watermark minus 3600s lateness is 23:14 the day before.
    started = datetime(2026, 11, 22, 0, 15, tzinfo=timezone.utc)
    closed = target_hour_exclusive(started, watermark_seconds=60, lateness_seconds=3600)
    assert closed == datetime(2026, 11, 21, 23, 0, 0)
    assert hour_key(closed) == 2026112123


def test_complete_partitions_close_at_the_target() -> None:
    rows, summary = _assess()
    assert len(rows) == 2
    assert all(row["coverage_status"] == COMPLETE for row in rows)
    assert all(row["missing_offset_count"] == 0 for row in rows)
    assert summary["closed_hour_exclusive"] == 2026112123
    assert summary["coverage_status"] == COMPLETE
    assert summary["incomplete_partitions"] == 0


def test_empty_kafka_range_is_complete() -> None:
    rows, summary = _assess(
        frontiers={0: _frontier(0, 5, 5), 1: _frontier(1, 5, 5)},
        stats={},
    )
    assert rows[0]["required_start_offset"] == 5
    assert rows[0]["required_end_offset"] == 5
    assert rows[0]["missing_offset_count"] == 0
    assert summary["coverage_status"] == COMPLETE
    assert summary["closed_hour_exclusive"] == hour_key(_TARGET)


def test_offsets_kafka_deleted_before_bronze_captured_them_hold_the_cutoff() -> None:
    # Published through 50; Kafka now starts at 100; Bronze has only 100..119.
    # The successor timestamp must not advance the cutoff past the lost records.
    rows, summary = _assess(
        frontiers={0: _frontier(0, 100, 120), 1: _frontier(1, 0, 10)},
        stats={0: _stat(0, 20, 119, datetime(2026, 11, 21, 22, 30)), 1: _stat(1, 10, 9)},
        previous_end_offsets={0: 50},
        previous_closed_hour=2026112100,
    )
    assert rows[0]["required_start_offset"] == 50
    assert rows[0]["required_end_offset"] == 120
    assert rows[0]["missing_offset_count"] == 50
    assert rows[0]["coverage_status"] == INCOMPLETE
    assert summary["coverage_status"] == INCOMPLETE
    assert summary["closed_hour_exclusive"] == 2026112100
    assert lost_offset_count(_frontier(0, 100, 120), 50, 120, _stat(0, 20, 119)) == 50


def test_offsets_kafka_deleted_after_bronze_captured_them_are_complete() -> None:
    rows, summary = _assess(
        frontiers={0: _frontier(0, 100, 120), 1: _frontier(1, 0, 10)},
        stats={0: PartitionStat(0, 70, 119, None, present_before_retained=50), 1: _stat(1, 10, 9)},
        previous_end_offsets={0: 50},
    )
    assert rows[0]["required_start_offset"] == 50
    assert rows[0]["missing_offset_count"] == 0
    assert summary["coverage_status"] == COMPLETE


def test_acknowledged_loss_starts_at_kafkas_earliest_retained_offset() -> None:
    rows, summary = _assess(
        frontiers={0: _frontier(0, 100, 120), 1: _frontier(1, 0, 10)},
        stats={0: _stat(0, 20, 119), 1: _stat(1, 10, 9)},
        previous_end_offsets={0: 50},
        acknowledged_loss=frozenset({0}),
    )
    assert rows[0]["required_start_offset"] == 100
    assert rows[0]["missing_offset_count"] == 0
    assert summary["coverage_status"] == COMPLETE


def test_acknowledging_a_partition_without_a_loss_changes_nothing() -> None:
    rows, _ = _assess(previous_end_offsets={0: 0}, acknowledged_loss=frozenset({0}))
    assert rows[0]["required_start_offset"] == 0
    assert rows[0]["coverage_status"] == COMPLETE


def test_partition_kafka_lost_keeps_the_published_start() -> None:
    rows, _ = _assess(frontiers={0: _frontier(0, 0, 10)}, previous_end_offsets={1: 40})
    assert rows[1]["required_start_offset"] == 40
    assert proven_end_offsets(rows) == {0: 10, 1: 40}


def test_an_incomplete_partition_is_proven_only_through_its_start() -> None:
    rows, _ = _assess(
        stats={0: _stat(0, 10, 9), 1: _stat(1, 4, 3, None)},
        previous_end_offsets={1: 0},
    )
    assert proven_end_offsets(rows) == {0: 10, 1: 0}


def test_frontier_document_round_trips() -> None:
    rows, summary = _assess(stats={0: _stat(0, 10, 9), 1: _stat(1, 4, 3, None)}, previous_closed_hour=2026112110)
    document = frontier_document(summary, rows)
    assert document == {
        "inventory_run_id": "R1",
        "closed_hour_exclusive": 2026112110,
        "end_offsets": {"0": 10, "1": 0},
    }
    assert parse_frontier(json.dumps(document)) == ("R1", 2026112110, {0: 10, 1: 0})


def test_frontier_without_a_run_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="inventory_run_id"):
        parse_frontier(json.dumps({"inventory_run_id": "", "closed_hour_exclusive": None, "end_offsets": {}}))


def test_acknowledged_loss_parses_and_rejects_unknown_partitions() -> None:
    assert parse_acknowledged_loss("", 12) == frozenset()
    assert parse_acknowledged_loss(None, 12) == frozenset()
    assert parse_acknowledged_loss("3, 7", 12) == frozenset({3, 7})
    with pytest.raises(ValueError, match="outside 0..11"):
        parse_acknowledged_loss("12", 12)
    with pytest.raises(ValueError):
        parse_acknowledged_loss("x", 12)


def test_plaintext_kafka_adds_no_security_settings() -> None:
    assert kafka_client_properties("PLAINTEXT", "AWS_MSK_IAM") == {}


def test_msk_iam_settings_match_the_flink_jobs() -> None:
    props = kafka_client_properties("SASL_SSL", "AWS_MSK_IAM")
    assert props == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "AWS_MSK_IAM",
        "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    }
    flink = kafka_security_options({"kafka_security_protocol": "SASL_SSL", "kafka_sasl_mechanism": "AWS_MSK_IAM"})
    for key, value in props.items():
        assert f"'properties.{key}' = '{value}'" in flink


def test_unknown_kafka_security_protocol_is_rejected() -> None:
    with pytest.raises(ValueError, match="SSL"):
        kafka_client_properties("SSL", "AWS_MSK_IAM")


def test_internal_gap_pulls_the_cutoff_back_by_the_lateness_allowance() -> None:
    successor = datetime(2026, 11, 21, 22, 30)
    rows, summary = _assess(
        stats={0: _stat(0, 10, 9), 1: _stat(1, 8, 7, successor)},
    )
    assert rows[1]["coverage_status"] == INCOMPLETE
    assert rows[1]["missing_offset_count"] == 2
    # 22:30 minus 3600s is 21:30, floored to 21:00.
    assert summary["closed_hour_exclusive"] == 2026112121
    assert summary["coverage_status"] == INCOMPLETE
    assert summary["incomplete_partitions"] == 1


def test_lagging_partition_never_moves_the_cutoff_below_the_previous_publish() -> None:
    _, summary = _assess(
        stats={0: _stat(0, 10, 9), 1: _stat(1, 8, 7, datetime(2026, 11, 21, 22, 30))},
        previous_closed_hour=2026112122,
    )
    assert summary["closed_hour_exclusive"] == 2026112122
    assert summary["coverage_status"] == INCOMPLETE


def test_tail_gap_holds_the_previous_cutoff() -> None:
    rows, summary = _assess(
        stats={0: _stat(0, 10, 9), 1: _stat(1, 4, 3, None)},
        previous_closed_hour=2026112110,
    )
    assert rows[1]["missing_offset_count"] == 6
    assert summary["closed_hour_exclusive"] == 2026112110
    assert summary["coverage_status"] == INCOMPLETE


def test_partition_kafka_does_not_have_holds_the_previous_cutoff() -> None:
    rows, summary = _assess(
        frontiers={0: _frontier(0, 0, 10)},
        previous_closed_hour=2026112120,
    )
    missing = rows[1]
    assert missing["kafka_partition"] == 1
    assert missing["required_end_offset"] is None
    assert missing["coverage_status"] == INCOMPLETE
    assert summary["closed_hour_exclusive"] == 2026112120
    assert summary["incomplete_partitions"] == 1


def test_first_run_with_a_tail_gap_has_no_cutoff_yet() -> None:
    _, summary = _assess(stats={0: _stat(0, 10, 9), 1: _stat(1, 0, None, None)})
    assert summary["closed_hour_exclusive"] is None
    assert summary["coverage_status"] == INCOMPLETE
