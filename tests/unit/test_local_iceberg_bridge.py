"""Contracts for local Iceberg → DuckDB → dbt fidelity path."""

from __future__ import annotations

import importlib
import tempfile
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


class TestLocalIcebergBridge:
    def test_compose_bind_mounts_host_iceberg(self) -> None:
        compose = (_REPO / "infra/docker/compose/docker-compose.yml").read_text(
            encoding="utf-8"
        )
        assert "../../../.local/iceberg:/tmp/iceberg" in compose
        assert "flink-iceberg:/tmp/iceberg" not in compose
        assert "INVENTORY_SILVER_WINDOW" in compose
        assert "INVENTORY_SILVER_WATERMARK_DELAY_SECONDS" in compose

    def test_dashboard_compose_uses_host_iceberg(self) -> None:
        compose = (
            _REPO / "infra/docker/compose/docker-compose.dashboard.yml"
        ).read_text(encoding="utf-8")
        assert "../../../.local/iceberg:/tmp/iceberg:ro" in compose

    def test_silver_job_accepts_window_env(self) -> None:
        src = (_REPO / "streaming/flink_jobs/inventory_silver_job.py").read_text(
            encoding="utf-8"
        )
        assert "INVENTORY_SILVER_WINDOW" in src
        assert "_silver_window_interval" in src
        assert "{window_interval}" in src
        assert "INTERVAL '1' HOUR" in src
        assert "TO_TIMESTAMP(REPLACE(SUBSTRING(event_time, 1, 23)" in src
        assert "WATERMARK FOR event_ts" in src

    def test_load_script_maps_expected_tables(self) -> None:
        src = (_REPO / "scripts/local/load_iceberg_to_duckdb.py").read_text(
            encoding="utf-8"
        )
        for name in (
            "clickstream_events",
            "inventory_events",
            "pos_transactions",
            "inventory_hourly",
            "identity_resolution",
        ):
            assert name in src
        assert "consumer_current/identity_resolution/**/*.parquet" in src
        assert "silver/identity_resolution/data/**/*.parquet" not in src

    def test_run_local_stack_has_spark_identity_task(self) -> None:
        # ADR-010: local GraphFrames runs in the Spark Docker image against
        # .local/iceberg; the dbt task falls back to the seed fixture when no
        # Spark output exists.
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        assert '"spark"' in ps1
        assert "Invoke-SparkIdentityLocal" in ps1
        assert "--profile spark" in ps1
        assert "spark-identity" in ps1
        assert "'identity_resolution'" in ps1  # fixture fallback seed select
        assert "Get-ChildItem -Path $identityParquetDir -Filter '*.parquet'" in ps1
        assert "Invoke-WapBootstrapLiveDimProduct" in ps1
        assert "bootstrap_live_dim_product.py" in ps1
        # Cold DuckDB: dim_product parents (staging views, int_product_catalog)
        # do not exist yet, so the bootstrap must build them too.
        assert "'+dim_product'" in ps1

    def test_compose_spark_identity_profile(self) -> None:
        compose = (_REPO / "infra/docker/compose/docker-compose.yml").read_text(
            encoding="utf-8"
        )
        assert "spark-identity:" in compose
        assert 'profiles: ["spark"]' in compose
        assert "../../../spark:/opt/spark-identity" in compose
        dockerfile = (_REPO / "infra/docker/spark/Dockerfile").read_text(
            encoding="utf-8"
        )
        versions = (_REPO / "infra/docker/spark/versions.env").read_text(
            encoding="utf-8"
        )
        flink_iceberg = next(
            line.split("=", 1)[1].strip()
            for line in (_REPO / "infra/docker/flink/versions.env")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.startswith("ICEBERG_VERSION=")
        )
        assert f"ICEBERG_VERSION={flink_iceberg}" in versions
        assert f"ICEBERG_VERSION={flink_iceberg}" in dockerfile
        assert "apache/spark:3.4.1-python3" in dockerfile
        assert "graphframes" in dockerfile

    def test_run_local_stack_supports_iceberg_dbt_source(self) -> None:
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        assert 'ValidateSet("iceberg", "seeds")' in ps1
        assert "load_iceberg_to_duckdb.py" in ps1
        assert "generate_pos_parquet" in ps1
        assert "'dim_date', 'dim_store'" in ps1 or "dim_date dim_store" in ps1
        all_block = ps1[ps1.find('"all" {') :]
        assert all_block.find("-Task flink") < all_block.find("-Task simulate")
        assert all_block.find("-Task pos-parquet") < all_block.find("-Task spark")
        assert all_block.find("-Task spark") < all_block.find("Invoke-DbtIceberg")

    def test_pos_write_local_parquet_layout(self) -> None:
        mod = importlib.import_module("ingestion.batch.generate_pos_parquet")
        rows = mod.generate_rows(date(2026, 7, 13), 2, 2, seed_override=1)
        table = mod.rows_to_table(rows)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "pos_transactions"
            path = Path(mod.write_local_parquet(str(out), "2026-07-13", table))
            assert path.name == "part-00000.parquet"
            assert path.parent.name == "dt=2026-07-13"
            assert pq.read_table(path).num_rows == table.num_rows

    def test_dim_seeds_cover_sim_entities(self) -> None:
        stores = (
            _REPO / "transformation/dbt_project/seeds/finance/dim_store.csv"
        ).read_text(encoding="utf-8")
        assert "STORE-001" in stores and "STORE-020" in stores
        dates = (
            _REPO / "transformation/dbt_project/seeds/finance/dim_date.csv"
        ).read_text(encoding="utf-8")
        assert "20260101" in dates and "20261231" in dates
