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
        ):
            assert name in src

    def test_run_local_stack_supports_iceberg_dbt_source(self) -> None:
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        assert 'ValidateSet("iceberg", "seeds")' in ps1
        assert "load_iceberg_to_duckdb.py" in ps1
        assert "generate_pos_parquet" in ps1
        assert "'dim_date', 'dim_store'" in ps1 or "dim_date dim_store" in ps1
        all_block = ps1[ps1.find('"all"') :]
        assert all_block.find("-Task flink") < all_block.find("-Task simulate")

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
