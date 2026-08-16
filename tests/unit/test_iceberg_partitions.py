"""Unit tests for Iceberg CREATE TABLE partition specs (DL-A from the data
lake checklist applied 2026-07-05).

These tests guard the in-SQL `PARTITIONED BY` declarations on the three
Flink Iceberg sinks (bronze inventory_events, bronze clickstream_events,
silver inventory_hourly) plus the Hive-style partition key on the POS
Spectrum external table. Removing the partition spec silently regresses
Spectrum scan performance (full table scan instead of partition pruning),
so we lint the source SQL rather than rely on a runtime check.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FLINK_JOBS = _REPO_ROOT / "streaming" / "flink_jobs"
_SPECTRUM = _REPO_ROOT / "transformation" / "redshift" / "spectrum" / "bronze_external_tables.sql"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# Each Flink job file contains exactly one Iceberg CREATE TABLE (the
# Kafka/DLQ side tables live in default_catalog, not the Iceberg catalog),
# so we can scan the whole file for the Iceberg CREATE + its tail.
_ICEBERG_CREATE_RE = re.compile(
    # Match `CREATE TABLE IF NOT EXISTS {catalog}.{NS}.{TABLE} ( ... ) <tail>`
    # where the table identifier is the f-string placeholder form (e.g.
    # `{catalog}.{BRONZE_NAMESPACE}.{INVENTORY_EVENTS}`). Capture the tail
    # after the closing paren of the column list, up to the closing `"""`
    # of the Python f-string.
    r"CREATE TABLE IF NOT EXISTS\s+\{catalog\}\.\{[A-Z_]+\}\.\{[A-Z_]+\}\s*"
    r"\((?:[^()]|\([^()]*\))*\)"  # balanced column list (allows nested casts)
    r"(.*?)\"\"\"",  # tail: non-greedy up to the closing triple-quote
    re.DOTALL,
)


def _iceberg_tail(source: str, job_label: str) -> str:
    m = _ICEBERG_CREATE_RE.search(source)
    assert m is not None, (
        f"{job_label}: could not locate Iceberg CREATE TABLE block. The job "
        f"file should contain exactly one `CREATE TABLE IF NOT EXISTS "
        f"{{catalog}}.{{NS}}.{{TABLE}}` statement."
    )
    return m.group(1)


class TestIcebergPartitionSpecs:
    """All three Iceberg sinks must declare a daily partition spec."""

    def test_inventory_events_has_daily_partition(self) -> None:
        src = _read(_FLINK_JOBS / "inventory_bronze_job.py")
        tail = _iceberg_tail(src, "inventory_bronze_job.py")
        assert "PARTITIONED BY" in tail.upper(), (
            "bronze.inventory_events Iceberg DDL is missing PARTITIONED BY — "
            "data lake checklist DL-A regression."
        )
        assert re.search(r"PARTITIONED BY\s*\(\s*event_date\s*\)", tail, re.IGNORECASE), (
            "bronze.inventory_events should partition by event_date "
            "(derived CAST(event_time AS DATE) identity column — Flink SQL "
            "Hadoop catalog does not accept days(event_time) transform)."
        )

    def test_clickstream_events_has_daily_partition(self) -> None:
        src = _read(_FLINK_JOBS / "clickstream_bronze_job.py")
        tail = _iceberg_tail(src, "clickstream_bronze_job.py")
        assert "PARTITIONED BY" in tail.upper(), (
            "bronze.clickstream_events Iceberg DDL is missing PARTITIONED BY — "
            "data lake checklist DL-A regression."
        )
        assert re.search(r"PARTITIONED BY\s*\(\s*event_date\s*\)", tail, re.IGNORECASE), (
            "bronze.clickstream_events should partition by event_date "
            "(derived CAST(event_time AS DATE) identity column — Flink SQL "
            "Hadoop catalog does not accept days(event_time) transform)."
        )

    def test_inventory_hourly_has_identity_partition(self) -> None:
        src = _read(_FLINK_JOBS / "inventory_silver_job.py")
        tail = _iceberg_tail(src, "inventory_silver_job.py")
        assert "PARTITIONED BY" in tail.upper(), (
            "silver.inventory_hourly Iceberg DDL is missing PARTITIONED BY — "
            "data lake checklist DL-A regression."
        )
        assert re.search(r"PARTITIONED BY\s*\(\s*snapshot_date_key\s*\)", tail, re.IGNORECASE), (
            "silver.inventory_hourly should partition by snapshot_date_key "
            "(YYYYMMDD int) — identity partition matches the dbt filter column."
        )

    def test_no_high_cardinality_iceberg_partitions(self) -> None:
        """Guard against a future regression that partitions by event_id,
        store_id, product_id, or client_id — these would cause small-file
        explosion per the data lake checklist item 1.2."""
        forbidden = ["event_id", "client_id", "customer_id", "session_id", "scanner_id"]
        for job_file, table in [
            ("inventory_bronze_job.py", "inventory_events"),
            ("clickstream_bronze_job.py", "clickstream_events"),
            ("inventory_silver_job.py", "inventory_hourly"),
        ]:
            src = _read(_FLINK_JOBS / job_file)
            tail = _iceberg_tail(src, job_file)
            pb = re.search(r"PARTITIONED BY\s*\(([^)]*)\)", tail, re.IGNORECASE)
            assert pb is not None, f"{table}: missing PARTITIONED BY clause"
            spec = pb.group(1).lower()
            for col in forbidden:
                # Match the column name as a standalone token inside the spec,
                # not as a substring of `days(event_time)` etc.
                assert re.search(rf"\b{col}\b", spec) is None, (
                    f"{table}: PARTITIONED BY references high-cardinality column "
                    f"`{col}` — would cause small-file explosion (data lake "
                    f"checklist item 1.2)."
                )


class TestSpectrumPosPartitioning:
    """POS Spectrum external table must declare the dt partition key so
    Spectrum can prune to a single day when filtering on transaction_date."""

    def test_pos_external_table_has_dt_partition(self) -> None:
        src = _read(_SPECTRUM)
        # Locate the bronze.pos_transactions CREATE EXTERNAL TABLE block.
        m = re.search(
            r"CREATE EXTERNAL TABLE\s+bronze\.pos_transactions\s*\((?:[^()]|\([^()]*\))*\)\s*([^;]*);",
            src,
            re.DOTALL | re.IGNORECASE,
        )
        assert m is not None, "bronze.pos_transactions external table not found"
        tail = m.group(1)
        assert re.search(r"PARTITIONED BY\s*\(\s*dt\s+date\s*\)", tail, re.IGNORECASE), (
            "bronze.pos_transactions Spectrum DDL must declare "
            "`PARTITIONED BY (dt date)` so Hive-style dt=YYYY-MM-DD "
            "directories are pruned by Spectrum. Register each day with "
            "ALTER TABLE ... ADD IF NOT EXISTS PARTITION after the POS batch."
        )
