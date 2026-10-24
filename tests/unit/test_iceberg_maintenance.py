"""Spark Iceberg maintenance job: targets, procedure SQL, argument checks."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spark.maintenance import iceberg_maintenance as job

pytestmark = pytest.mark.unit


def test_every_iceberg_table_is_expired_and_streaming_sinks_compacted():
    tables = {t.table: t for t in job.TARGETS}
    assert {name for name, t in tables.items() if t.compact} == {
        "bronze.inventory_records",
        "bronze.clickstream_records",
        "silver.inventory_hourly",
    }
    for name in (
        "silver.identity_resolution",
        "silver.identity_edges",
        "silver.clickstream_events",
        "silver.inventory_quarantine",
    ):
        assert name in tables and tables[name].catalog == job.SILVER_CATALOG
    assert all(t.catalog == job.BRONZE_CATALOG for t in job.TARGETS if t.layer == "bronze")


def test_procedure_sql_uses_named_arguments():
    target = job.TARGETS[0]
    rewrite = job.rewrite_sql(target, 5, 268435456)
    assert rewrite.startswith("CALL iceberg_bronze.system.rewrite_data_files(")
    assert "table => 'bronze.inventory_records'" in rewrite
    assert "'min-input-files', '5'" in rewrite

    expire = job.expire_sql(target, datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), 5)
    assert "older_than => TIMESTAMP '2026-01-02 03:04:05'" in expire
    assert "retain_last => 5" in expire


def test_rejects_non_positive_retention():
    with pytest.raises(SystemExit):
        job.parse_args(["--bronze-warehouse", "b", "--silver-warehouse", "s", "--retain-last", "0"])
