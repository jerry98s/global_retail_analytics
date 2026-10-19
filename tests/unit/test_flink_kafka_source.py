"""Unit tests for Flink Kafka source connector reliability properties
(K-CONS from the Kafka checklist applied 2026-07-05).

Each Flink Kafka source table (the three event streams: bronze inventory,
bronze clickstream, silver inventory snapshot) must explicitly declare:

  - 'properties.enable.auto.commit' = 'false'
  - 'properties.isolation.level'    = 'read_committed'
  - 'properties.auto.offset.reset'  = 'earliest'

The Flink Kafka SQL connector defaults enable.auto.commit to false (Flink
commits offsets via the checkpoint committer under EXACTLY_ONCE), but we
make it explicit so a future contributor doesn't accidentally override it.
isolation.level=read_committed is defense-in-depth (our producers don't
use Kafka transactions today, but if a future producer does, we want to
skip un-committed messages). auto.offset.reset=earliest is the safety net
for the first startup when no committed offset exists.

We lint the source SQL rather than run a Flink job because:
  - Flink is a heavy dependency (PyFlink 1.17.1 + JARs) not available in CI.
  - The DDL is a Python f-string rendered into a Table API execute_sql call;
    the property keys appear verbatim in the source.

DLQ sinks are deliberately excluded — they're Kafka producers, not
consumers, so the consumer-side reliability props don't apply.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FLINK_JOBS = _REPO_ROOT / "streaming" / "flink_jobs"

# Files containing a Kafka *source* table (one per job). DLQ sinks are
# Kafka producers and don't need the consumer reliability props.
_KAFKA_SOURCE_FILES = [
    "inventory_bronze_job.py",
    "clickstream_bronze_job.py",
    "inventory_silver_job.py",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _kafka_source_blocks(source: str, job_label: str) -> list[str]:
    """Extract every `WITH ( 'connector' = 'kafka', ... )` block from a
    Flink job file. Returns the raw text of each block (the WITH clause
    contents, between the outer parens), so callers can grep for property
    keys.

    Uses a paren-balanced scan rather than `[^)]*` because the WITH block
    contains SQL comments with parentheses (e.g. `-- (Flink commits
    offsets via the checkpoint committer under EXACTLY_ONCE)`).
    """
    blocks: list[str] = []
    # Find each `WITH (` position and walk balanced parens to the matching
    # close. We scan the whole source — there's no risk of `WITH (`
    # appearing in a Python string literal outside the f-string DDLs.
    for m in re.finditer(r"\)\s*WITH\s*\(", source):
        open_pos = m.end() - 1  # position of the opening `(`
        depth = 0
        i = open_pos
        while i < len(source):
            c = source[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    blocks.append(source[open_pos + 1 : i])
                    break
            i += 1
    # Keep only blocks that declare the Kafka connector (filter out any
    # other WITH clause, e.g. an Iceberg connector).
    kafka_blocks = [b for b in blocks if re.search(r"'connector'\s*=\s*'kafka'", b, re.IGNORECASE)]
    assert kafka_blocks, (
        f"{job_label}: no Kafka connector WITH clauses found. Each Flink "
        f"job must declare at least one Kafka source table."
    )
    return kafka_blocks


def _is_dlq_block(block: str) -> bool:
    # Heuristic: DLQ sinks are Kafka *producers* — the Flink Kafka connector
    # treats a table as a sink when 'sink.partitioner' or 'value.format' is
    # set without scan.startup.mode. The simplest signal: the DLQ blocks
    # lack `scan.startup.mode`, which only applies to sources.
    return "scan.startup.mode" not in block


class TestFlinkKafkaSourceReliability:
    """K-CONS: explicit consumer-side reliability props on every Kafka
    source table in every Flink job."""

    @pytest.mark.parametrize("job_file", _KAFKA_SOURCE_FILES)
    def test_enable_auto_commit_false(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        source_blocks = [b for b in _kafka_source_blocks(src, job_file) if not _is_dlq_block(b)]
        assert source_blocks, f"{job_file}: no Kafka source block found"
        for block in source_blocks:
            assert re.search(
                r"'properties\.enable\.auto\.commit'\s*=\s*'false'",
                block,
                re.IGNORECASE,
            ), (
                f"{job_file}: Kafka source must explicitly set "
                f"'properties.enable.auto.commit' = 'false'. Flink commits "
                f"offsets via the checkpoint committer under EXACTLY_ONCE."
            )

    @pytest.mark.parametrize("job_file", _KAFKA_SOURCE_FILES)
    def test_isolation_level_read_committed(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        source_blocks = [b for b in _kafka_source_blocks(src, job_file) if not _is_dlq_block(b)]
        assert source_blocks, f"{job_file}: no Kafka source block found"
        for block in source_blocks:
            assert re.search(
                r"'properties\.isolation\.level'\s*=\s*'read_committed'",
                block,
                re.IGNORECASE,
            ), (
                f"{job_file}: Kafka source must set "
                f"'properties.isolation.level' = 'read_committed' — defense "
                f"in depth against future transactional producers."
            )

    @pytest.mark.parametrize("job_file", _KAFKA_SOURCE_FILES)
    def test_auto_offset_reset_earliest(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        source_blocks = [b for b in _kafka_source_blocks(src, job_file) if not _is_dlq_block(b)]
        assert source_blocks, f"{job_file}: no Kafka source block found"
        for block in source_blocks:
            assert re.search(
                r"'properties\.auto\.offset\.reset'\s*=\s*'earliest'",
                block,
                re.IGNORECASE,
            ), (
                f"{job_file}: Kafka source must set "
                f"'properties.auto.offset.reset' = 'earliest' as the safety "
                f"net for first startup / backfill replays."
            )

    @pytest.mark.parametrize("job_file", _KAFKA_SOURCE_FILES)
    def test_no_auto_commit_true_override(self, job_file: str) -> None:
        # A future contributor might try to "speed up" the consumer by
        # setting enable.auto.commit=true. That would break exactly-once
        # — Flink's checkpoint committer would race with Kafka's
        # auto-commit. Guard against that override.
        src = _read(_FLINK_JOBS / job_file)
        for block in _kafka_source_blocks(src, job_file):
            assert re.search(
                r"'properties\.enable\.auto\.commit'\s*=\s*'true'",
                block,
                re.IGNORECASE,
            ) is None, (
                f"{job_file}: Kafka source must NOT enable.auto.commit=true — "
                f"this races with Flink's checkpoint committer and breaks "
                f"exactly-once semantics."
            )
