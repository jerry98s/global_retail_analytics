"""Write one distinct loyalty-id file per POS date.

The hourly identity job reads this extract instead of the POS line-item
files. Each requested date is its own partition, so a missing day leaves the
other days in place and a restated day replaces only itself.

Layouts, matching the POS bronze partitions::

  <pos-dir>/data/dt=<YYYY-MM-DD>/*.parquet
  <output-dir>/data/dt=<YYYY-MM-DD>/part-00000.parquet
  s3://<bucket>/<prefix>/data/dt=<YYYY-MM-DD>/part-00000.parquet

Blank and whitespace-only loyalty ids are dropped. A requested date with no
POS Parquet file fails; an empty file is a day with no loyalty ids and is
written as an empty extract.
"""

from __future__ import annotations

import argparse
import io
import re
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_dates(text: str) -> list[str]:
    """Comma-separated YYYY-MM-DD dates, in order, with duplicates removed."""
    seen: list[str] = []
    for raw in text.split(","):
        day = raw.strip()
        if not day:
            continue
        if not _ISO_DATE.match(day):
            raise ValueError(f"date must be YYYY-MM-DD, got {day!r}")
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise ValueError(f"date is not a real date: {day!r}") from exc
        if day not in seen:
            seen.append(day)
    if not seen:
        raise ValueError("at least one YYYY-MM-DD date is required")
    return seen


def distinct_loyalty_table(table: pa.Table) -> pa.Table:
    """Trimmed, non-blank ``loyalty_id`` values, sorted, one column."""
    if "loyalty_id" not in table.column_names:
        raise ValueError("POS partition has no loyalty_id column")
    found: set[str] = set()
    for value in table.column("loyalty_id").to_pylist():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            found.add(text)
    return pa.table({"loyalty_id": pa.array(sorted(found), type=pa.string())})


def _read_local_partition(part_dir: Path) -> pa.Table:
    files = sorted(path for path in part_dir.glob("*.parquet") if path.is_file())
    if not files:
        raise FileNotFoundError(f"POS partition missing: {part_dir}")
    tables = [pq.read_table(path, columns=["loyalty_id"]) for path in files]
    return pa.concat_tables(tables)


def _write_local(output_dir: Path, txn_date: str, table: pa.Table) -> str:
    part_dir = output_dir / "data" / f"dt={txn_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    for old in part_dir.glob("*.parquet"):
        old.unlink()
    path = part_dir / "part-00000.parquet"
    pq.write_table(table, path, compression="snappy")
    return str(path)


def extract_local(pos_dir: str | Path, dates: list[str], output_dir: str | Path) -> list[str]:
    """Extract ``dates`` from a local POS tree. Returns the files written."""
    source = Path(pos_dir)
    target = Path(output_dir)
    written: list[str] = []
    for txn_date in dates:
        table = distinct_loyalty_table(_read_local_partition(source / "data" / f"dt={txn_date}"))
        written.append(_write_local(target, txn_date, table))
    return written


def parse_s3_uri(uri: str) -> tuple[str, str]:
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected s3://bucket/prefix URI, got: {uri}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return parsed.netloc, prefix


def _s3_parquet_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet")
        )
    return sorted(keys)


def _read_s3_partition(client, bucket: str, prefix: str, txn_date: str) -> pa.Table:
    part = f"{prefix}data/dt={txn_date}/"
    keys = _s3_parquet_keys(client, bucket, part)
    if not keys:
        raise FileNotFoundError(f"POS partition missing: s3://{bucket}/{part}")
    tables = []
    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body), columns=["loyalty_id"]))
    return pa.concat_tables(tables)


def _write_s3(client, bucket: str, prefix: str, txn_date: str, table: pa.Table) -> str:
    part = f"{prefix}data/dt={txn_date}/"
    key = f"{part}part-00000.parquet"
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    for old in _s3_parquet_keys(client, bucket, part):
        if old != key:
            client.delete_object(Bucket=bucket, Key=old)
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return f"s3://{bucket}/{key}"


def extract_s3(pos_s3: str, output_s3: str, dates: list[str], client=None) -> list[str]:
    """Extract ``dates`` from POS objects in S3. Returns the object URIs written."""
    if client is None:
        import boto3

        client = boto3.client("s3")
    source_bucket, source_prefix = parse_s3_uri(pos_s3)
    target_bucket, target_prefix = parse_s3_uri(output_s3)
    written: list[str] = []
    for txn_date in dates:
        table = distinct_loyalty_table(
            _read_s3_partition(client, source_bucket, source_prefix, txn_date)
        )
        written.append(_write_s3(client, target_bucket, target_prefix, txn_date, table))
    return written


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write distinct POS loyalty ids, one Parquet file per date."
    )
    parser.add_argument("--dates", required=True, help="Comma-separated YYYY-MM-DD dates.")
    parser.add_argument("--pos-dir", help="Local POS bronze directory (parent of data/dt=).")
    parser.add_argument("--output-dir", help="Local directory for the loyalty extract.")
    parser.add_argument("--pos-s3", help="S3 prefix of the POS bronze partitions.")
    parser.add_argument("--output-s3", help="S3 prefix for the loyalty extract.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        dates = parse_dates(args.dates)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if bool(args.pos_dir) == bool(args.pos_s3):
        raise SystemExit("Pass exactly one of --pos-dir or --pos-s3.")
    if args.pos_dir:
        if not args.output_dir:
            raise SystemExit("--pos-dir writes with --output-dir.")
        written = extract_local(args.pos_dir, dates, args.output_dir)
    else:
        if not args.output_s3:
            raise SystemExit("--pos-s3 writes with --output-s3.")
        written = extract_s3(args.pos_s3, args.output_s3, dates)
    for path in written:
        print(path)


if __name__ == "__main__":
    main()
