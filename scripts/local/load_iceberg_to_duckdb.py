"""Load local Flink Iceberg Parquet (and optional POS batch) into DuckDB for dbt.

After Flink + simulate + POS + Spark, canonical stream and identity exports
land under ``.local/iceberg/consumer_current/<name>/``. Each Spark run writes
a new ``run=<id>/`` directory and then replaces the
``manifest`` beside it (``spark/identity_resolution/consumer_exports.py``).
This script reads exactly the files that manifest lists, the same contract as
the Spectrum tables on ``main``, and materializes them into
``local_retail.duckdb`` schemas that match dbt ``source()`` names:

  silver.clickstream_events
  silver.inventory_events
  silver.inventory_hourly     (Flink's committed Iceberg snapshot, not an export)
  silver.inventory_offset_gaps (missing Kafka offsets in inventory Bronze)
  silver.inventory_partition_coverage (Kafka frontier versus Bronze, one
                               row per expected partition)
  silver.inventory_cutoff     (one row: the daily closed hour)
  silver.identity_resolution  (Spark GraphFrames, ADR-010/011 — required in
                               fidelity mode)
  silver.identity_edges       (same Spark run; verified loyalty links that merged)
  silver.loyalty_link_claims  (same Spark run; every login naming a loyalty card)
  silver.device_session_associations (same Spark run; signed-in intervals per
                               device, attribution only)
  bronze.pos_transactions     (from generate_pos_parquet --output-dir)
  bronze.pos_returns          (from generate_pos_returns_parquet; required with POS.
                               Every POS day and every restated day needs its
                               own returns file: a day without returns is a
                               zero-row file, so a missing file is an error)

Reference dims (``finance.dim_date``, ``finance.dim_store``) stay as dbt seeds.

Usage::

    python scripts/local/load_iceberg_to_duckdb.py
    python scripts/local/load_iceberg_to_duckdb.py --iceberg-dir .local/iceberg \\
        --duckdb transformation/dbt_project/local_retail.duckdb
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from urllib.request import url2pathname

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
    "inventory_offset_gaps": "silver",
    "inventory_partition_coverage": "silver",
    "inventory_cutoff": "silver",
    "identity_resolution": "silver",
    "identity_edges": "silver",
    "loyalty_link_claims": "silver",
    "device_session_associations": "silver",
}

# POS Bronze is plain Hive-partitioned Parquet (one file per dt=), not Iceberg.
_POS_GLOB = "bronze/pos_transactions/data/**/*.parquet"
_POS_RETURNS_GLOB = "bronze/pos_returns/data/**/*.parquet"


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


def _partition_dates(iceberg_dir: Path, pattern: str) -> set[date]:
    """dt= dates holding at least one Parquet file under the glob."""
    dates: set[date] = set()
    for path in _glob_parquet(iceberg_dir, pattern):
        name = path.parent.name
        if name.startswith("dt="):
            dates.add(date.fromisoformat(name.removeprefix("dt=")))
    return dates


def missing_returns_dates(iceberg_dir: Path, restated: Iterable[date] = ()) -> list[date]:
    """POS and restated dates without a delivered returns file, sorted."""
    required = _partition_dates(iceberg_dir, _POS_GLOB) | set(restated)
    return sorted(required - _partition_dates(iceberg_dir, _POS_RETURNS_GLOB))


def _date_range(start: date, end: date) -> list[date]:
    if start > end:
        raise ValueError(f"--restate-from {start} is after --restate-to {end}")
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


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


def _latest_metadata(table_dir: Path) -> Path | None:
    """Metadata file of the current snapshot, or None when the table is absent.

    ``version-hint.text`` names the current file. The highest ``vN`` file is
    the fallback. Nothing under ``data/`` is consulted.
    """
    meta = table_dir / "metadata"
    if not meta.is_dir():
        return None
    hint = meta / "version-hint.text"
    if hint.is_file():
        version = hint.read_text(encoding="utf-8").strip()
        current = meta / f"v{version}.metadata.json"
        if current.is_file():
            return current
    files = sorted(meta.glob("v*.metadata.json"))
    return files[-1] if files else None


_CONTAINER_WAREHOUSE = ("file:///tmp/iceberg", "file:/tmp/iceberg")


def _host_uri(uri: str, iceberg_dir: Path) -> str:
    """Map the Flink container warehouse onto the host directory.

    Metadata written inside the compose container names files under
    ``file:///tmp/iceberg``. That directory is the bind-mounted ``iceberg_dir``.
    """
    host = iceberg_dir.resolve().as_uri().rstrip("/")
    for prefix in _CONTAINER_WAREHOUSE:
        if uri.startswith(prefix):
            return host + uri[len(prefix):]
    return uri


def _host_path(uri: str, iceberg_dir: Path) -> Path:
    rewritten = _host_uri(uri, iceberg_dir)
    parsed = urlparse(rewritten)
    if parsed.scheme == "file":
        return Path(url2pathname(parsed.path))
    return Path(rewritten)


# Iceberg stores file locations as UTF-8 strings inside Avro manifests. Reading
# those strings keeps the loader off pyiceberg's file IO, which rejects Windows
# file URIs. Flink's hourly sink is append-only, so a parquet path in the
# current snapshot's manifests is a live file.
_URI_IN_MANIFEST = re.compile(rb"file:/(?:[\x20-\x7e])+?\.(?:parquet|avro)")


def _uris_ending(data: bytes, suffix: str) -> list[str]:
    needle = suffix.encode("ascii")
    found: list[str] = []
    for match in _URI_IN_MANIFEST.findall(data):
        if not match.endswith(needle):
            continue
        uri = match.decode("ascii")
        if uri not in found:
            found.append(uri)
    return found


def _snapshot_files(metadata: Path, iceberg_dir: Path) -> list[Path]:
    """Parquet files the current snapshot's manifest list names."""
    doc = json.loads(metadata.read_text(encoding="utf-8"))
    current = doc.get("current-snapshot-id")
    manifest_list = next(
        (
            snap.get("manifest-list")
            for snap in doc.get("snapshots") or []
            if snap.get("snapshot-id") == current
        ),
        None,
    )
    if not manifest_list:
        return []
    manifest_list_path = _host_path(str(manifest_list), iceberg_dir)
    if not manifest_list_path.is_file():
        raise FileNotFoundError(
            f"silver.inventory_hourly manifest list is missing: {manifest_list_path}"
        )
    files: list[Path] = []
    seen: set[str] = set()
    for manifest_uri in _uris_ending(manifest_list_path.read_bytes(), "avro"):
        manifest_path = _host_path(manifest_uri, iceberg_dir)
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"silver.inventory_hourly manifest is missing: {manifest_path}"
            )
        for data_uri in _uris_ending(manifest_path.read_bytes(), "parquet"):
            if data_uri in seen:
                continue
            seen.add(data_uri)
            files.append(_host_path(data_uri, iceberg_dir))
    return files


def _load_hourly(con, iceberg_dir: Path, *, required: bool) -> int:
    metadata = _latest_metadata(iceberg_dir / "silver" / "inventory_hourly")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    if metadata is None:
        if required:
            raise FileNotFoundError(
                "No silver.inventory_hourly Iceberg table. Run the Flink "
                "inventory snapshot job before loading."
            )
        con.execute("DROP TABLE IF EXISTS silver.inventory_hourly")
        log.warning("table_empty_skipped", schema="silver", table="inventory_hourly")
        return 0
    files = _snapshot_files(metadata, iceberg_dir)
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "silver.inventory_hourly snapshot lists missing files: " + ", ".join(missing)
        )
    if not files:
        con.execute(
            """
            CREATE OR REPLACE TABLE silver.inventory_hourly (
                snapshot_date_key INTEGER,
                snapshot_hour INTEGER,
                product_id VARCHAR,
                store_id VARCHAR,
                qty_delta_hour BIGINT,
                qty_received_hour BIGINT,
                is_estimated BOOLEAN
            )
            """
        )
        log.info("table_loaded", schema="silver", table="inventory_hourly", rows=0, files=0)
        return 0
    paths_sql = ", ".join("'" + str(path).replace("\\", "/") + "'" for path in files)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver.inventory_hourly AS
        SELECT * FROM read_parquet([{paths_sql}], union_by_name=true)
        """
    )
    n = con.execute("SELECT COUNT(*) FROM silver.inventory_hourly").fetchone()[0]
    log.info("table_loaded", schema="silver", table="inventory_hourly", rows=n, files=len(files))
    return int(n)


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
    restated_dates: Iterable[date] = (),
) -> dict[str, int]:
    if require_pos:
        missing = missing_returns_dates(iceberg_dir, restated_dates)
        if missing:
            raise FileNotFoundError(
                "No bronze.pos_returns file for "
                + ", ".join(d.isoformat() for d in missing)
                + ". A day without returns is a zero-row file; generate it with "
                "python -m ingestion.batch.generate_pos_returns_parquet --date <day> "
                "--pos-dir <iceberg>/bronze/pos_transactions --output-dir <iceberg>/bronze/pos_returns"
            )
    duckdb = _require_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    counts: dict[str, int] = {}
    try:
        counts["silver.inventory_hourly"] = _load_hourly(
            con, iceberg_dir, required=require_silver
        )
        for name, schema in _EXPORTS.items():
            counts[f"{schema}.{name}"] = _load_table(
                con, schema, name, _manifest_files(iceberg_dir, name), required=require_streams
            )
        counts["bronze.pos_transactions"] = _load_table(
            con,
            "bronze",
            "pos_transactions",
            _glob_parquet(iceberg_dir, _POS_GLOB),
            required=require_pos,
        )
        counts["bronze.pos_returns"] = _load_table(
            con,
            "bronze",
            "pos_returns",
            _glob_parquet(iceberg_dir, _POS_RETURNS_GLOB),
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
        help="Fail when Flink has not created silver.inventory_hourly yet.",
    )
    p.add_argument(
        "--restate-from",
        type=date.fromisoformat,
        help="First POS restatement day (YYYY-MM-DD); every restated day needs a returns file.",
    )
    p.add_argument(
        "--restate-to",
        type=date.fromisoformat,
        help="Last POS restatement day (YYYY-MM-DD), set together with --restate-from.",
    )
    args = p.parse_args()
    if (args.restate_from is None) != (args.restate_to is None):
        p.error("--restate-from and --restate-to must be set together")
    return args


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
        restated = (
            _date_range(args.restate_from, args.restate_to) if args.restate_from else []
        )
        counts = load_all(
            iceberg_dir,
            args.duckdb.resolve(),
            require_streams=not args.allow_empty_streams,
            require_pos=not args.allow_empty_pos,
            require_silver=args.require_silver,
            restated_dates=restated,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print("Loaded into DuckDB:")
    for k, v in counts.items():
        print(f"  {k}: {v} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
