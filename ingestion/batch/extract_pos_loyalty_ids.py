"""Commit one distinct loyalty-id file per POS date.

Files for a run land under ``run=<id>/`` first. The ``manifest`` is replaced
only after every requested date has been written, and it keeps previously
committed dates that this run did not touch. A failure deletes the new run
and leaves the previous manifest in place, so a reader never sees a mix of
old and new dates.

Layouts::

  <pos-dir>/data/dt=<YYYY-MM-DD>/*.parquet
  <output-dir>/run=<id>/dt=<YYYY-MM-DD>/part-00000.parquet
  <output-dir>/manifest

Blank and whitespace-only loyalty ids are dropped. A requested date with no
POS Parquet file fails. An empty POS file is a committed day with no loyalty
ids.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spark.identity_resolution.pos_loyalty import merge_manifest, parse_manifest

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MANIFEST_NAME = "manifest"


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


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


def _read_local_manifest(root: Path) -> dict | None:
    path = root / MANIFEST_NAME
    if not path.is_file():
        return None
    return parse_manifest(json.loads(path.read_text(encoding="utf-8")))


def _commit_local_manifest(root: Path, run_dir: Path, payload: dict) -> None:
    temporary = run_dir / "manifest.tmp"
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(temporary, root / MANIFEST_NAME)


def extract_local(pos_dir: str | Path, dates: list[str], output_dir: str | Path) -> dict:
    """Extract ``dates`` and commit the manifest. Returns the committed manifest."""
    source = Path(pos_dir)
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    run_id = new_run_id()
    run_dir = root / f"run={run_id}"
    created = False
    try:
        run_dir.mkdir()
        created = True
        updates: list[tuple[str, str]] = []
        for txn_date in dates:
            table = distinct_loyalty_table(_read_local_partition(source / "data" / f"dt={txn_date}"))
            part_dir = run_dir / f"dt={txn_date}"
            part_dir.mkdir()
            pq.write_table(table, part_dir / "part-00000.parquet", compression="snappy")
            updates.append((txn_date, f"run={run_id}/dt={txn_date}/part-00000.parquet"))
        payload = merge_manifest(_read_local_manifest(root), updates, run_id)
        _commit_local_manifest(root, run_dir, payload)
    except Exception:
        if created:
            shutil.rmtree(run_dir, ignore_errors=True)
        raise
    return payload


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


def _missing_object(exc: Exception) -> bool:
    if isinstance(exc, KeyError):
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}
    return False


def _read_s3_manifest(client, bucket: str, key: str) -> dict | None:
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        if _missing_object(exc):
            return None
        raise
    return parse_manifest(json.loads(body))


def _put_parquet(client, bucket: str, key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def extract_s3(pos_s3: str, output_s3: str, dates: list[str], client=None) -> dict:
    """Extract ``dates`` and commit the manifest. Returns the committed manifest.

    New objects are removed if a later date fails, and the previous manifest
    is left untouched.
    """
    if client is None:
        import boto3

        client = boto3.client("s3")
    source_bucket, source_prefix = parse_s3_uri(pos_s3)
    target_bucket, target_prefix = parse_s3_uri(output_s3)
    run_id = new_run_id()
    uploaded: list[str] = []
    try:
        updates: list[tuple[str, str]] = []
        for txn_date in dates:
            table = distinct_loyalty_table(
                _read_s3_partition(client, source_bucket, source_prefix, txn_date)
            )
            relative = f"run={run_id}/dt={txn_date}/part-00000.parquet"
            key = f"{target_prefix}{relative}"
            _put_parquet(client, target_bucket, key, table)
            uploaded.append(key)
            updates.append((txn_date, relative))
        payload = merge_manifest(
            _read_s3_manifest(client, target_bucket, f"{target_prefix}{MANIFEST_NAME}"),
            updates,
            run_id,
        )
        client.put_object(
            Bucket=target_bucket,
            Key=f"{target_prefix}{MANIFEST_NAME}",
            Body=json.dumps(payload).encode("utf-8"),
        )
    except Exception:
        for key in uploaded:
            client.delete_object(Bucket=target_bucket, Key=key)
        raise
    return payload


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
        committed = extract_local(args.pos_dir, dates, args.output_dir)
    else:
        if not args.output_s3:
            raise SystemExit("--pos-s3 writes with --output-s3.")
        committed = extract_s3(args.pos_s3, args.output_s3, dates)
    print(
        f"committed loyalty extract {committed['run_id']} "
        f"dates={','.join(committed['dates'])}"
    )


if __name__ == "__main__":
    main()
