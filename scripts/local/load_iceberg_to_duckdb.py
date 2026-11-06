"""Load local Flink Iceberg Parquet (and optional POS batch) into DuckDB for dbt.

After Flink + simulate + POS + Spark, canonical stream, inventory_hourly and
identity exports land under ``.local/iceberg/consumer_current/<name>/``. Each
Spark run writes a new ``run=<id>/`` directory and then replaces the
``manifest`` beside it (``spark/identity_resolution/consumer_exports.py``).
This script reads exactly the files that manifest lists, the same contract as
the Spectrum tables on ``main``, and materializes them into
``local_retail.duckdb`` schemas that match dbt ``source()`` names:

  silver.clickstream_events
  silver.inventory_events
  silver.inventory_hourly     (Spark export of Flink's committed snapshot)
  silver.inventory_offset_gaps (missing Kafka offsets in inventory Bronze)
  silver.identity_resolution  (Spark GraphFrames, ADR-010/011 — required in
                               fidelity mode)
  silver.identity_edges       (same Spark run; audit copy of graph edges)
  bronze.pos_transactions     (from generate_pos_parquet --output-dir)

Reference dims (``finance.dim_date``, ``finance.dim_store``) stay as dbt seeds.

Usage::

    python scripts/local/load_iceberg_to_duckdb.py
    python scripts/local/load_iceberg_to_duckdb.py --iceberg-dir .local/iceberg \\
        --duckdb transformation/dbt_project/local_retail.duckdb
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import structlog

log = structlog.get_logger()

_REPO = Path(__file__).resolve().parents[2]

_CONSUMER_ROOT = "consumer_current"
_MANIFEST = "manifest"

# Spark consumer exports (logical name -> schema), read through their manifest.
# Never glob an Iceberg table's data/ dir or an export's run dirs: both can hold
# files from superseded runs or snapshots.
_EXPORTS: dict[str, str] = {
    "clickstream_events": "silver",
    "inventory_events": "silver",
    "inventory_hourly": "silver",
    "inventory_offset_gaps": "silver",
    "identity_resolution": "silver",
    "identity_edges": "silver",
}

# POS Bronze is plain Hive-partitioned Parquet (one file per dt=), not Iceberg.
_POS_GLOB = "bronze/pos_transactions/data/**/*.parquet"


def _require_duckdb():
    try:
        import duckdb  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "duckdb is required. Install with: pip install 'duckdb>=1.0,<2.0'"
        ) from exc
    return __import__("duckdb")


def _glob_parquet(iceberg_dir: Path, pattern: str) -> list[Path]:
    return sorted(p for p in iceberg_dir.glob(pattern) if p.is_file())


def _manifest_files(iceberg_dir: Path, name: str) -> list[Path]:
    """Host paths of the files the export's manifest lists; [] when unpublished.

    Manifest URLs carry the Spark container's path (``file:/tmp/iceberg/...``),
    so each is re-rooted under ``iceberg_dir`` by its path below the export.
    """
    export_dir = iceberg_dir / _CONSUMER_ROOT / name
    manifest = export_dir / _MANIFEST
    if not manifest.is_file():
        return []
    marker = f"/{_CONSUMER_ROOT}/{name}/"
    files: list[Path] = []
    for entry in json.loads(manifest.read_text(encoding="utf-8"))["entries"]:
        url = entry["url"].replace("\\", "/")
        if marker not in url:
            raise ValueError(f"{manifest}: entry outside {marker}: {url}")
        files.append(export_dir / url.split(marker, 1)[1])
    missing = [str(f) for f in files if not f.is_file()]
    if missing:
        raise FileNotFoundError(f"{manifest} lists missing files: {missing}")
    return files


def _load_table(
    con,
    schema: str,
    table: str,
    files: list[Path],
    *,
    required: bool,
) -> int:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    if not files:
        if required:
            raise FileNotFoundError(
                f"No Parquet for {schema}.{table}. Run Flink + simulate + POS + Spark "
                f"first; fidelity mode does not substitute seed fixtures."
            )
        # Drop stale leftovers (e.g. seed-mode rows) so empty Iceberg stays empty.
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        log.warning("table_empty_skipped", schema=schema, table=table)
        return 0

    # DuckDB list_value of paths — works on Windows with forward slashes.
    paths_sql = ", ".join("'" + str(p).replace("\\", "/") + "'" for p in files)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {schema}.{table} AS
        SELECT * FROM read_parquet([{paths_sql}], union_by_name=true)
        """
    )
    n = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    log.info("table_loaded", schema=schema, table=table, rows=n, files=len(files))
    return int(n)


def load_all(
    iceberg_dir: Path,
    duckdb_path: Path,
    *,
    require_streams: bool = True,
    require_pos: bool = True,
    require_silver: bool = False,
) -> dict[str, int]:
    duckdb = _require_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    counts: dict[str, int] = {}
    try:
        for name, schema in _EXPORTS.items():
            required = require_silver if name == "inventory_hourly" else require_streams
            counts[f"{schema}.{name}"] = _load_table(
                con, schema, name, _manifest_files(iceberg_dir, name), required=required
            )
        counts["bronze.pos_transactions"] = _load_table(
            con,
            "bronze",
            "pos_transactions",
            _glob_parquet(iceberg_dir, _POS_GLOB),
            required=require_pos,
        )
    finally:
        con.close()
    return counts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--iceberg-dir",
        type=Path,
        default=_REPO / ".local" / "iceberg",
        help="Host path to Iceberg warehouse (bind-mounted into Flink).",
    )
    p.add_argument(
        "--duckdb",
        type=Path,
        default=_REPO / "transformation" / "dbt_project" / "local_retail.duckdb",
        help="DuckDB file used by dbt --target local.",
    )
    p.add_argument(
        "--allow-empty-streams",
        action="store_true",
        help="Do not fail when canonical event / identity Parquet is missing.",
    )
    p.add_argument(
        "--allow-empty-pos",
        action="store_true",
        help="Do not fail when POS Parquet is missing.",
    )
    p.add_argument(
        "--require-silver",
        action="store_true",
        help="Fail when Spark has not published a silver.inventory_hourly export yet.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    iceberg_dir = args.iceberg_dir.resolve()
    if not iceberg_dir.is_dir():
        print(
            f"ERROR: Iceberg dir not found: {iceberg_dir}\n"
            "Start the stack with bind-mounted .local/iceberg, then flink+simulate.",
            file=sys.stderr,
        )
        return 1
    try:
        counts = load_all(
            iceberg_dir,
            args.duckdb.resolve(),
            require_streams=not args.allow_empty_streams,
            require_pos=not args.allow_empty_pos,
            require_silver=args.require_silver,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print("Loaded into DuckDB:")
    for k, v in counts.items():
        print(f"  {k}: {v} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
