"""Write 1x, 5x or 10x Bronze into another Iceberg warehouse.

Reads the local Bronze catalog and the POS Parquet, and writes a disjoint
copy under the destination warehouse. The source warehouse is only read.
One copy keeps the original ids. More than one copy tags every replica, so
the copies are different people and the identity job cannot deduplicate them
back into the original.
"""

from __future__ import annotations

import argparse
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

from scale_ids import PARTITION_STRIDE, remap_clickstream, remap_inventory, tag_identifier


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replicate Bronze for the identity benchmark")
    parser.add_argument("--source-warehouse", required=True)
    parser.add_argument("--dest-warehouse", required=True)
    parser.add_argument("--copies", type=int, required=True)
    parser.add_argument("--pos-source", required=True)
    parser.add_argument("--pos-dest", required=True)
    return parser.parse_args()


def _session(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder.appName("identity-benchmark-replicate")
        .master("local[*]")
        .config("spark.sql.catalog.iceberg_src", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_src.type", "hadoop")
        .config("spark.sql.catalog.iceberg_src.warehouse", args.source_warehouse)
        .config("spark.sql.catalog.iceberg_dst", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_dst.type", "hadoop")
        .config("spark.sql.catalog.iceberg_dst.warehouse", args.dest_warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def _tag_column(column: str, replica: str) -> F.Column:
    value = F.col(column)
    trimmed = F.trim(value)
    blank = value.isNull() | (F.length(trimmed) == 0)
    return F.when(blank, value).otherwise(F.concat(value, F.lit("#r"), F.col(replica).cast("string")))


def _scale_records(spark: SparkSession, stream: str, copies: int) -> DataFrame:
    source = spark.table(f"iceberg_src.bronze.{stream}_records")
    if copies == 1:
        return source
    replicas = spark.range(copies).withColumnRenamed("id", "replica")
    scaled = source.crossJoin(replicas)
    remap = F.udf(remap_clickstream if stream == "clickstream" else remap_inventory, BinaryType())
    return (
        scaled.withColumn("raw_payload", remap("raw_payload", "replica"))
        .withColumn("kafka_partition", F.col("kafka_partition") + F.col("replica") * F.lit(PARTITION_STRIDE))
        .drop("replica")
    )


def _scale_pos(spark: SparkSession, path: str, copies: int) -> DataFrame:
    pos = spark.read.parquet(path)
    if copies == 1:
        return pos
    replicas = spark.range(copies).withColumnRenamed("id", "replica")
    scaled = pos.crossJoin(replicas)
    tagged = scaled.withColumn("loyalty_id", _tag_column("loyalty_id", "replica"))
    if "transaction_id" in pos.columns:
        tagged = tagged.withColumn("transaction_id", _tag_column("transaction_id", "replica"))
    return tagged.drop("replica")


def main() -> None:
    args = _parse_args()
    if args.copies < 1:
        raise SystemExit("--copies must be >= 1")
    if args.source_warehouse.rstrip("/") == args.dest_warehouse.rstrip("/"):
        raise SystemExit("destination warehouse must be a different directory from the source")
    spark = _session(args)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_dst.bronze")
    counts = {"copies": args.copies}
    for stream in ("clickstream", "inventory"):
        frame = _scale_records(spark, stream, args.copies)
        frame.writeTo(f"iceberg_dst.bronze.{stream}_records").using("iceberg").createOrReplace()
        counts[f"{stream}_rows"] = spark.table(f"iceberg_dst.bronze.{stream}_records").count()
    pos = _scale_pos(spark, args.pos_source, args.copies)
    pos.write.mode("overwrite").parquet(args.pos_dest)
    counts["pos_rows"] = spark.read.parquet(args.pos_dest).count()
    # Tag format shared with the payload rewriter. A mismatch would unlink POS.
    if args.copies > 1 and tag_identifier("LOYAL-1", 0) != "LOYAL-1#r0":
        raise SystemExit("POS tag format drifted from scale_ids.tag_identifier")
    print("BENCHMARK_INPUT " + json.dumps(counts, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
