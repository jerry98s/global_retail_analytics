"""Contracts for local Iceberg → DuckDB → dbt fidelity path."""

from __future__ import annotations

import importlib
import importlib.util
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

    def test_flink_submission_ships_raw_capture_helper(self) -> None:
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        files = ps1.split('$PyFiles = @(', 1)[1].split(") -join ','", 1)[0]
        assert "'/opt/streaming/flink_jobs/raw_bronze.py'" in files

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
        assert "/data/**" not in src.replace(
            '"bronze/pos_transactions/data/**/*.parquet"', ""
        ), "only POS Bronze may be globbed; exports are read through manifests"

    def test_run_local_stack_has_spark_identity_task(self) -> None:
        # ADR-010: local GraphFrames runs in the Spark Docker image against
        # .local/iceberg; fidelity mode requires real Spark exports.
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        assert '"spark"' in ps1
        assert "Invoke-SparkIdentityLocal" in ps1
        assert "--profile spark" in ps1
        assert "spark-identity" in ps1
        assert "seeding fixture identity_resolution" not in ps1
        assert "Fixture inputs are only allowed" in ps1
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


def _bridge():
    spec = importlib.util.spec_from_file_location(
        "load_iceberg_to_duckdb", _REPO / "scripts/local/load_iceberg_to_duckdb.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _publish(iceberg: Path, name: str, run: str, values: list[int]) -> None:
    """Mimic consumer_exports.publish: a run dir, then the manifest Spark writes
    with the Spark container's file:/tmp/iceberg URLs."""
    import json

    import pyarrow as pa

    run_dir = iceberg / "consumer_current" / name / f"run={run}"
    run_dir.mkdir(parents=True)
    pq.write_table(pa.table({"v": values}), run_dir / "part-00000.parquet")
    url = f"file:/tmp/iceberg/consumer_current/{name}/run={run}/part-00000.parquet"
    (iceberg / "consumer_current" / name / "manifest").write_text(
        json.dumps({"entries": [{"url": url, "mandatory": True}]}), encoding="utf-8"
    )


class TestManifestExports:
    def test_loads_only_the_run_the_manifest_lists(self, tmp_path: Path) -> None:
        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        for name in bridge._EXPORTS:
            _publish(iceberg, name, "20261104T010000Z", [1, 2, 3])
            _publish(iceberg, name, "20261104T020000Z", [7])
        stale = iceberg / "silver" / "inventory_hourly" / "data"
        stale.mkdir(parents=True)
        import pyarrow as pa

        pq.write_table(pa.table({"v": [99, 99]}), stale / "superseded.parquet")

        counts = bridge.load_all(iceberg, tmp_path / "t.duckdb", require_pos=False)

        for name, schema in bridge._EXPORTS.items():
            assert counts[f"{schema}.{name}"] == 1, name
        assert counts["bronze.pos_transactions"] == 0

    def test_missing_manifest_fails_in_fidelity_mode(self, tmp_path: Path) -> None:
        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        _publish(iceberg, "clickstream_events", "20261104T010000Z", [1])
        with pytest.raises(FileNotFoundError, match="inventory_events"):
            bridge.load_all(iceberg, tmp_path / "t.duckdb", require_pos=False)

    def test_manifest_pointing_at_a_deleted_run_fails(self, tmp_path: Path) -> None:
        import shutil

        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        _publish(iceberg, "identity_edges", "20261104T010000Z", [1])
        shutil.rmtree(iceberg / "consumer_current" / "identity_edges" / "run=20261104T010000Z")
        with pytest.raises(FileNotFoundError, match="lists missing files"):
            bridge._manifest_files(iceberg, "identity_edges")
