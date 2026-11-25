"""After the finance publish, record which inventory run is live (ADR-012).

The daily Spark run writes ``<consumer root>/inventory_frontier/run=<id>.json``
with its cutoff and the Kafka offset each partition is proven through. Once
the WAP publish has committed ``control.inventory_publication``, this copies
that run's file to ``published.json``, where the next Spark run starts. A run
whose audit failed is never promoted, so it is never a starting point.

The names match ``spark/identity_resolution/kafka_frontier.py``. Kept free of
Airflow imports so the rule is unit-testable offline.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

FRONTIER_DIR = "inventory_frontier"
PUBLISHED_FRONTIER = "published.json"
PUBLICATION_SQL = "SELECT inventory_run_id FROM control.inventory_publication"

Reader = Callable[[str], "bytes | None"]
Writer = Callable[[str, bytes], None]


def published_inventory_run(rows: list[tuple[Any, ...]]) -> str | None:
    """The run id in live ``control.inventory_publication``; None before the first publish."""
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(f"control.inventory_publication must have one row, found {len(rows)}")
    run_id = rows[0][0]
    return None if run_id is None else str(run_id)


def promote_frontier(run_id: str | None, read: Reader, write: Writer) -> str:
    """Copy the published run's frontier to ``published.json``. Rewriting the same run is a no-op."""
    if run_id is None:
        return "no published inventory run"
    candidate = f"{FRONTIER_DIR}/run={run_id}.json"
    body = read(candidate)
    if body is None:
        raise FileNotFoundError(f"Published inventory run {run_id} has no frontier file {candidate}")
    if json.loads(body).get("inventory_run_id") != run_id:
        raise ValueError(f"{candidate} does not describe run {run_id}")
    write(f"{FRONTIER_DIR}/{PUBLISHED_FRONTIER}", body)
    return f"published inventory frontier is run {run_id}"


def local_store(root: Path) -> tuple[Reader, Writer]:
    """Files under the local consumer export root. Writes replace the file in one rename."""
    def read(key: str) -> bytes | None:
        path = root / key
        return path.read_bytes() if path.exists() else None

    def write(key: str, body: bytes) -> None:
        path = root / key
        staging = path.with_name(path.name + ".tmp")
        staging.write_bytes(body)
        os.replace(staging, path)

    return read, write


def s3_store(root_uri: str, client: Any) -> tuple[Reader, Writer]:
    """Objects under an ``s3://`` (or ``s3a://``) consumer export root."""
    parsed = urlparse(root_uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Not an S3 export root: {root_uri}")
    bucket = parsed.netloc
    prefix = parsed.path.strip("/")

    def key_for(key: str) -> str:
        return f"{prefix}/{key}" if prefix else key

    def read(key: str) -> bytes | None:
        try:
            return client.get_object(Bucket=bucket, Key=key_for(key))["Body"].read()
        except client.exceptions.NoSuchKey:
            return None

    def write(key: str, body: bytes) -> None:
        client.put_object(Bucket=bucket, Key=key_for(key), Body=body, ContentType="application/json")

    return read, write


def promote_published_frontier_task(**_: Any) -> str:
    """Airflow callable after ``wap_publish_gold``: live run id from Redshift, files in S3."""
    import boto3
    from airflow.models import Variable

    from wap_publish import _connect_redshift, _query

    conn = _connect_redshift()
    try:
        rows = _query(conn, PUBLICATION_SQL, (), "redshift")
    finally:
        conn.close()
    root = Variable.get("silver_iceberg_warehouse").rstrip("/") + "/consumer_current"
    read, write = s3_store(root, boto3.client("s3"))
    return promote_frontier(published_inventory_run(rows), read, write)
