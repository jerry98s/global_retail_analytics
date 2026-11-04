"""Snapshot-safe plain-Parquet exports for readers outside the Iceberg catalog.

Spectrum cannot read Iceberg metadata, and globbing a table's ``data/`` dir
returns files from superseded or uncommitted snapshots. Each export therefore
writes a fresh run directory, then replaces one manifest object listing exactly
that run's files::

    <root>/<name>/run=<utc timestamp>/part-*.parquet
    <root>/<name>/manifest            (Redshift Spectrum manifest JSON)

Spectrum external tables use ``LOCATION '<root>/<name>/manifest'``. A single
object write is atomic, so a query sees the previous run or the new one, never
a partial or empty directory. The previous run directory is kept for queries
still scanning it; older runs are deleted.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

MANIFEST = "manifest"
KEEP_RUNS = 2


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _hadoop_path(spark: SparkSession, uri: str):
    return spark._jvm.org.apache.hadoop.fs.Path(uri)


def _spectrum_url(uri: str) -> str:
    return "s3://" + uri[len("s3a://"):] if uri.startswith("s3a://") else uri


def publish(frame: DataFrame, root: str, name: str, run_id: str) -> str:
    """Write ``frame`` as a new run of export ``name``; returns the run dir."""
    spark = frame.sparkSession
    base_uri = f"{root.rstrip('/')}/{name}"
    run_uri = f"{base_uri}/run={run_id}"
    frame.write.mode("errorifexists").parquet(run_uri)

    base = _hadoop_path(spark, base_uri)
    fs = base.getFileSystem(spark._jsc.hadoopConfiguration())
    entries = [
        {
            "url": _spectrum_url(status.getPath().toString()),
            "mandatory": True,
            "meta": {"content_length": int(status.getLen())},
        }
        for status in fs.listStatus(_hadoop_path(spark, run_uri))
        if status.getPath().getName().endswith(".parquet")
    ]
    out = fs.create(_hadoop_path(spark, f"{base_uri}/{MANIFEST}"), True)
    try:
        out.write(bytearray(json.dumps({"entries": entries}).encode("utf-8")))
    finally:
        out.close()

    runs = sorted(
        status.getPath().getName()
        for status in fs.listStatus(base)
        if status.isDirectory() and status.getPath().getName().startswith("run=")
    )
    for old in runs[:-KEEP_RUNS]:
        fs.delete(_hadoop_path(spark, f"{base_uri}/{old}"), True)
    return run_uri


def current_files(spark: SparkSession, root: str, name: str) -> list[str]:
    """Files the manifest currently points at (for readers and tests)."""
    uri = f"{root.rstrip('/')}/{name}/{MANIFEST}"
    text = "\n".join(spark.sparkContext.textFile(uri).collect())
    return [entry["url"] for entry in json.loads(text)["entries"]]


def read_current(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(*current_files(spark, root, name))
