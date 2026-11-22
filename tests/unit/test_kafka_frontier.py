"""The inventory coverage rule, without Kafka or Spark (ADR-012)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spark.identity_resolution.kafka_frontier import (
    COMPLETE,
    INCOMPLETE,
    PartitionFrontier,
    PartitionStat,
    assess_coverage,
    hour_key,
    target_hour_exclusive,
)

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


def test_retention_trimmed_start_is_the_later_of_kafka_and_the_previous_end() -> None:
    rows, summary = _assess(
        frontiers={0: _frontier(0, 100, 120), 1: _frontier(1, 0, 10)},
        stats={0: _stat(0, 20, 119), 1: _stat(1, 10, 9)},
        previous_end_offsets={0: 50},
    )
    assert rows[0]["required_start_offset"] == 100
    assert rows[0]["missing_offset_count"] == 0
    assert summary["coverage_status"] == COMPLETE


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
