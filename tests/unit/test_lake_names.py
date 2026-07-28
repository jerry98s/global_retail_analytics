"""Tests for canonical lake object names."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "streaming" / "flink_jobs"))

from lake_names import (  # noqa: E402
    CLICKSTREAM_EVENTS,
    INVENTORY_EVENTS,
    INVENTORY_HOURLY,
    LOCAL_TABLE_PATHS,
    s3_parquet_location,
)


def test_bronze_table_names_match_spectrum_and_dbt() -> None:
    assert CLICKSTREAM_EVENTS == "clickstream_events"
    assert INVENTORY_EVENTS == "inventory_events"


def test_silver_hourly_table_name() -> None:
    assert INVENTORY_HOURLY == "inventory_hourly"


def test_local_paths_no_duplicate_namespace() -> None:
    assert "/bronze/bronze/" not in LOCAL_TABLE_PATHS["clickstream"]
    assert "/silver/silver/" not in LOCAL_TABLE_PATHS["inventory"]
    assert LOCAL_TABLE_PATHS["clickstream"].endswith("clickstream_events/data")
    assert LOCAL_TABLE_PATHS["inventory"].endswith("inventory_hourly/data")


def test_s3_parquet_location_layout() -> None:
    loc = s3_parquet_location("retail-platform-dev-bronze", "bronze", CLICKSTREAM_EVENTS)
    assert loc == "s3://retail-platform-dev-bronze/iceberg/bronze/clickstream_events/data/"
