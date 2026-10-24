"""Iceberg table maintenance: compaction and snapshot expiration (Spark).

Submitted daily as an EMR step by ``lakehouse_daily_iceberg_maintenance``.
Iceberg ships its maintenance procedures (``rewrite_data_files``,
``expire_snapshots``) for Spark only; Flink 1.17 SQL has no ``CALL``.

Flink tables (append-only streaming sinks) are compacted, then expired.
Spark-replaced tables (``createOrReplace`` every marketing run) are only
expired: each run already writes compact files, but every run adds a snapshot.

Expiration keeps snapshots newer than the layer's retention window and always
keeps the last ``--retain-last`` snapshots, so a bad run can still roll back.
Re-running on the same state is a no-op. A missing table is skipped (it has
not been created yet), any other failure fails the step.

    spark-submit --master yarn --deploy-mode cluster \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
      iceberg_maintenance.py \
      --bronze-warehouse s3://<bucket>-bronze/iceberg \
      --silver-warehouse s3://<bucket>-silver/iceberg
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

BRONZE_CATALOG = "iceberg_bronze"
SILVER_CATALOG = "iceberg_silver"


class Target(NamedTuple):
    catalog: str
    table: str
    layer: str
    compact: bool


TARGETS = (
    Target(BRONZE_CATALOG, "bronze.inventory_records", "bronze", True),
    Target(BRONZE_CATALOG, "bronze.clickstream_records", "bronze", True),
    Target(SILVER_CATALOG, "silver.inventory_hourly", "silver", True),
    Target(SILVER_CATALOG, "silver.identity_resolution", "silver", False),
    Target(SILVER_CATALOG, "silver.identity_edges", "silver", False),
    *(
        Target(SILVER_CATALOG, f"silver.{stream}_{suffix}", "silver", False)
        for stream in ("clickstream", "inventory")
        for suffix in ("events", "quarantine", "duplicates")
    ),
)


def rewrite_sql(target: Target, min_input_files: int, target_file_size_bytes: int) -> str:
    return (
        f"CALL {target.catalog}.system.rewrite_data_files("
        f"table => '{target.table}', "
        f"options => map('min-input-files', '{min_input_files}', "
        f"'target-file-size-bytes', '{target_file_size_bytes}'))"
    )


def expire_sql(target: Target, older_than: datetime, retain_last: int) -> str:
    cutoff = older_than.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"CALL {target.catalog}.system.expire_snapshots("
        f"table => '{target.table}', "
        f"older_than => TIMESTAMP '{cutoff}', "
        f"retain_last => {retain_last})"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bronze-warehouse", required=True)
    parser.add_argument("--silver-warehouse", required=True)
    parser.add_argument("--bronze-retention-days", type=int, default=7)
    parser.add_argument("--silver-retention-days", type=int, default=30)
    parser.add_argument("--retain-last", type=int, default=5)
    parser.add_argument("--min-input-files", type=int, default=5)
    parser.add_argument("--target-file-size-bytes", type=int, default=268435456)
    args = parser.parse_args(argv)
    for name in ("bronze_retention_days", "silver_retention_days", "retain_last", "min_input_files"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be >= 1")
    return args


def main(argv: list[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    spark = (
        SparkSession.builder.appName("iceberg-maintenance")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}.warehouse", args.bronze_warehouse)
        .config(f"spark.sql.catalog.{SILVER_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SILVER_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SILVER_CATALOG}.warehouse", args.silver_warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    now = datetime.now(timezone.utc)
    retention = {"bronze": args.bronze_retention_days, "silver": args.silver_retention_days}
    try:
        for target in TARGETS:
            name = f"{target.catalog}.{target.table}"
            if not spark.catalog.tableExists(name):
                print(f"[iceberg-maintenance] skip {name}: table does not exist", flush=True)
                continue
            statements = []
            if target.compact:
                statements.append(rewrite_sql(target, args.min_input_files, args.target_file_size_bytes))
            older_than = now - timedelta(days=retention[target.layer])
            statements.append(expire_sql(target, older_than, args.retain_last))
            for sql in statements:
                print(f"[iceberg-maintenance] {sql}", flush=True)
                for row in spark.sql(sql).collect():
                    print(f"[iceberg-maintenance] result {row.asDict()}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
