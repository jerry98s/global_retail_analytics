"""Load local Flink Iceberg Parquet (and optional POS batch) into DuckDB for dbt.

After ``run_local_stack.ps1 -Task flink`` + ``simulate``, Iceberg data lands under
``.local/iceberg/{bronze|silver}/.../data/``. This script materializes those
files into ``local_retail.duckdb`` schemas that match dbt ``source()`` names:

  bronze.clickstream_events
  bronze.inventory_events
  bronze.pos_transactions   (from generate_pos_parquet --output-dir)
  silver.inventory_hourly
  silver.identity_resolution  (from the Spark GraphFrames job, ADR-010 —
                               optional; fixture mode seeds it instead)

Reference dims (``finance.dim_date``, ``finance.dim_store``) stay as dbt seeds.

Usage::

    python scripts/local/load_iceberg_to_duckdb.py
    python scripts/local/load_iceberg_to_duckdb.py --iceberg-dir .local/iceberg \\
        --duckdb transformation/dbt_project/local_retail.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import structlog

log = structlog.get_logger()

_REPO = Path(__file__).resolve().parents[2]

# Relative to --iceberg-dir (Iceberg data/ layout from Flink / local POS / Spark).
_TABLES: dict[str, tuple[str, str]] = {
    # logical name -> (schema, relative glob under iceberg dir)
    "clickstream_events": ("bronze", "bronze/clickstream_events/data/**/*.parquet"),
    "inventory_events": ("bronze", "bronze/inventory_events/data/**/*.parquet"),
    "pos_transactions": ("bronze", "bronze/pos_transactions/data/**/*.parquet"),
    "inventory_hourly": ("silver", "silver/inventory_hourly/data/**/*.parquet"),
    "identity_resolution": ("silver", "silver/identity_resolution/data/**/*.parquet"),
}


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
                f"No Parquet for {schema}.{table}. Run Flink + simulate "
                f"(or generate_pos_parquet --output-dir) first."
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
        for name, (schema, pattern) in _TABLES.items():
            files = _glob_parquet(iceberg_dir, pattern)
            required = False
            if name in ("clickstream_events", "inventory_events"):
                required = require_streams
            elif name == "pos_transactions":
                required = require_pos
            elif name == "inventory_hourly":
                required = require_silver
            counts[f"{schema}.{name}"] = _load_table(
                con, schema, name, files, required=required
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
        help="Do not fail when clickstream/inventory bronze Parquet is missing.",
    )
    p.add_argument(
        "--allow-empty-pos",
        action="store_true",
        help="Do not fail when POS Parquet is missing.",
    )
    p.add_argument(
        "--require-silver",
        action="store_true",
        help="Fail when silver.inventory_hourly has no Parquet yet.",
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
