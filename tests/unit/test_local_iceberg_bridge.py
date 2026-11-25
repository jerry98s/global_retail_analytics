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
        assert "event_ts AS {event_ts_sql()}" in src
        assert "WATERMARK FOR event_ts" in src
        # Append-only plan (verify_flink_inventory_dedup.py): dedup per window.
        assert "PARTITION BY window_start, window_end, event_id" in src

    def test_load_script_maps_expected_tables(self) -> None:
        src = (_REPO / "scripts/local/load_iceberg_to_duckdb.py").read_text(
            encoding="utf-8"
        )
        for name in (
            "clickstream_events",
            "inventory_events",
            "pos_transactions",
            "pos_returns",
            "inventory_hourly",
            "identity_resolution",
        ):
            assert name in src
        bronze_globs = ('"bronze/pos_transactions/data/**/*.parquet"', '"bronze/pos_returns/data/**/*.parquet"')
        for glob in bronze_globs:
            src = src.replace(glob, "")
        assert "/data/**" not in src, "only POS Bronze may be globbed; exports are read through manifests"

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
        # A parquet file under the Iceberg table is not a snapshot.
        assert counts["silver.inventory_hourly"] == 0

    def test_hourly_load_reads_the_committed_snapshot_only(self, tmp_path: Path) -> None:
        import json

        import pyarrow as pa

        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        data = iceberg / "silver" / "inventory_hourly" / "data"
        meta = iceberg / "silver" / "inventory_hourly" / "metadata"
        data.mkdir(parents=True)
        meta.mkdir()
        pq.write_table(pa.table({"v": [7]}), data / "part.parquet")
        pq.write_table(pa.table({"v": [99]}), data / "superseded.parquet")
        # Paths are the container warehouse. The loader maps them onto iceberg.
        live = b"file:///tmp/iceberg/silver/inventory_hourly/data/part.parquet"
        stale = b"file:///tmp/iceberg/silver/inventory_hourly/data/superseded.parquet"
        (meta / "snap-new.avro").write_bytes(live)
        (meta / "snap-old.avro").write_bytes(stale)
        (meta / "list-new.avro").write_bytes(
            b"file:///tmp/iceberg/silver/inventory_hourly/metadata/snap-new.avro"
        )
        (meta / "list-old.avro").write_bytes(
            b"file:///tmp/iceberg/silver/inventory_hourly/metadata/snap-old.avro"
        )
        current = {
            "current-snapshot-id": 2,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "manifest-list": "file:///tmp/iceberg/silver/inventory_hourly/metadata/list-old.avro",
                },
                {
                    "snapshot-id": 2,
                    "manifest-list": "file:///tmp/iceberg/silver/inventory_hourly/metadata/list-new.avro",
                },
            ],
        }
        (meta / "v1.metadata.json").write_text(
            json.dumps({"current-snapshot-id": 1, "snapshots": current["snapshots"][:1]}),
            encoding="utf-8",
        )
        (meta / "v2.metadata.json").write_text(json.dumps(current), encoding="utf-8")
        (meta / "version-hint.text").write_text("2", encoding="utf-8")

        counts = bridge.load_all(iceberg, tmp_path / "t.duckdb", require_streams=False, require_pos=False)
        assert counts["silver.inventory_hourly"] == 1
        import duckdb

        value = duckdb.connect(str(tmp_path / "t.duckdb"), read_only=True).execute(
            "select v from silver.inventory_hourly"
        ).fetchall()
        assert value == [(7,)]
        assert counts["bronze.pos_transactions"] == 0
        assert counts["bronze.pos_returns"] == 0

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


def _bronze_day(iceberg: Path, table: str, day: str, rows: int = 1) -> None:
    import pyarrow as pa

    part = iceberg / "bronze" / table / "data" / f"dt={day}"
    part.mkdir(parents=True)
    pq.write_table(pa.table({"v": list(range(rows))}), part / "part-00000.parquet")


class TestReturnsDelivery:
    def test_a_pos_day_without_a_returns_file_fails(self, tmp_path: Path) -> None:
        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        for day in ("2026-11-20", "2026-11-21"):
            _bronze_day(iceberg, "pos_transactions", day)
        _bronze_day(iceberg, "pos_returns", "2026-11-20")
        assert bridge.missing_returns_dates(iceberg) == [date(2026, 11, 21)]
        with pytest.raises(FileNotFoundError, match="2026-11-21"):
            bridge.load_all(iceberg, tmp_path / "t.duckdb", require_streams=False)
        assert not (tmp_path / "t.duckdb").exists()

    def test_a_zero_row_returns_file_is_a_delivered_empty_day(self, tmp_path: Path) -> None:
        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        _bronze_day(iceberg, "pos_transactions", "2026-11-21")
        _bronze_day(iceberg, "pos_returns", "2026-11-21", rows=0)
        assert bridge.missing_returns_dates(iceberg) == []

    def test_every_restated_day_needs_a_returns_file(self, tmp_path: Path) -> None:
        bridge = _bridge()
        iceberg = tmp_path / "iceberg"
        _bronze_day(iceberg, "pos_transactions", "2026-11-20")
        _bronze_day(iceberg, "pos_returns", "2026-11-20")
        restated = bridge._date_range(date(2026, 11, 20), date(2026, 11, 22))
        assert bridge.missing_returns_dates(iceberg, restated) == [
            date(2026, 11, 21), date(2026, 11, 22),
        ]

    def test_stack_passes_the_restatement_range_to_the_loader(self) -> None:
        ps1 = (_REPO / "scripts/local/run_local_stack.ps1").read_text(encoding="utf-8")
        loader = ps1.split("function Invoke-LoadDuckdb", 1)[1].split("\n}", 1)[0]
        assert "'--restate-from', $RestateFrom, '--restate-to', $RestateTo" in loader
