"""Spark + GraphFrames identity-resolution job (ADR-010).

Replaces the dbt bounded N-hop connected-components models
(``int_identity_edges`` / ``int_identity_components``) with a true graph
computation. Reads bronze clickstream (Iceberg) and POS loyalty IDs
(Parquet), builds the identity graph, runs GraphFrames connected
components, and writes:

- ``silver.identity_resolution`` — one row per identifier with
  customer_key / confidence / method (dbt reads this as a source;
  ``int_identity_resolution`` is a thin view over it).
- ``silver.identity_edges`` — audit copy of the graph edges.

The business rules (edge types, public-device threshold, rep priority,
confidence/method mapping, customer_key formula) live in
``graph_logic.py`` and are shared with the dbt seed fixture generator —
this job mirrors them in DataFrame/GraphFrames operations.

Cloud (EMR 6.15, Spark 3.4) — submitted by the marketing hourly DAG or
``deploy_platform.ps1 -Action spark``:

    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,graphframes:graphframes:0.8.3-spark3.4-s_2.12 \
      identity_resolution_job.py \
      --bronze-warehouse s3://<bucket>-bronze/iceberg \
      --silver-warehouse s3://<bucket>-silver/iceberg \
      --pos-parquet-path s3://<bucket>-bronze/iceberg/bronze/pos_transactions/ \
      --checkpoint-dir s3://<bucket>-checkpoints/graphframes/

Local (laptop, local-testing-version stack):

    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,graphframes:graphframes:0.8.3-spark3.4-s_2.12 \
      identity_resolution_job.py --local \
      --bronze-warehouse .local/iceberg \
      --silver-warehouse .local/iceberg \
      --pos-parquet-path .local/pos_parquet/
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from graph_logic import (
    CONFIDENCE_ANCHOR,
    CONFIDENCE_CUSTOMER_STANDALONE,
    CONFIDENCE_DEVICE_ONLY,
    CONFIDENCE_LOYALTY_MATCH,
    CONFIDENCE_PUBLIC_DEVICE,
    CONFIDENCE_SESSION_LINKED,
    DEFAULT_PUBLIC_DEVICE_THRESHOLD,
    EDGE_LOYALTY_VALUE_MATCH,
    EDGE_SESSION_LINK,
    METHOD_COMPONENT_ANCHOR,
    METHOD_CUSTOMER_STANDALONE,
    METHOD_DEVICE_ONLY,
    METHOD_LOYALTY_MATCH,
    METHOD_LOYALTY_MEMBER,
    METHOD_PUBLIC_DEVICE_EXCLUDED,
    METHOD_SESSION_LINKED,
    PREFIX_CLIENT,
    PREFIX_CUSTOMER,
    PREFIX_LOYALTY,
)

BRONZE_CATALOG = "iceberg_bronze"
SILVER_CATALOG = "iceberg_silver"
OUTPUT_TABLE = f"{SILVER_CATALOG}.silver.identity_resolution"
EDGES_TABLE = f"{SILVER_CATALOG}.silver.identity_edges"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GraphFrames identity resolution")
    parser.add_argument("--bronze-warehouse", required=True,
                        help="Iceberg Hadoop catalog warehouse holding bronze.clickstream_events")
    parser.add_argument("--silver-warehouse", required=True,
                        help="Iceberg Hadoop catalog warehouse for silver.identity_* output")
    parser.add_argument("--pos-parquet-path", required=True,
                        help="POS batch Parquet path (plain Parquet, not Iceberg)")
    parser.add_argument("--checkpoint-dir", default="/tmp/graphframes-checkpoints",
                        help="GraphFrames checkpoint dir (S3 on EMR, local path otherwise)")
    parser.add_argument("--public-device-threshold", type=int,
                        default=DEFAULT_PUBLIC_DEVICE_THRESHOLD)
    parser.add_argument("--master", default=None,
                        help="Spark master override (default: yarn on EMR, local[*] with --local)")
    parser.add_argument("--local", action="store_true",
                        help="Laptop mode: local[*] master, file:// warehouses")
    return parser.parse_args()


def _build_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("identity-resolution-graphframes")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{BRONZE_CATALOG}.warehouse", args.bronze_warehouse)
        .config(f"spark.sql.catalog.{SILVER_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SILVER_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SILVER_CATALOG}.warehouse", args.silver_warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )
    if args.local:
        builder = builder.master(args.master or "local[*]")
    elif args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    return spark


def build_edges(clickstream: DataFrame, pos: DataFrame, threshold: int) -> tuple[DataFrame, DataFrame]:
    """Return (edges, public_devices) DataFrames mirroring graph_logic.build_edges."""
    pairs = (
        clickstream
        .filter(F.col("client_id").isNotNull() & F.col("customer_id").isNotNull())
        .select(
            F.col("client_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("event_time"),
        )
    )

    public_devices = (
        pairs.groupBy("client_id")
        .agg(F.countDistinct("customer_id").alias("distinct_customer_count"))
        .filter(F.col("distinct_customer_count") >= F.lit(threshold))
        .select(F.concat(F.lit(PREFIX_CLIENT), F.col("client_id")).alias("public_node"))
    )

    session_edges = (
        pairs.groupBy("client_id", "customer_id")
        .agg(F.max("event_time").alias("last_observed_at"))
        .select(
            F.concat(F.lit(PREFIX_CLIENT), F.col("client_id")).alias("src"),
            F.concat(F.lit(PREFIX_CUSTOMER), F.col("customer_id")).alias("dst"),
            F.lit(EDGE_SESSION_LINK).alias("edge_type"),
            F.col("last_observed_at"),
        )
        .join(public_devices, F.col("src") == F.col("public_node"), "left_anti")
    )

    customer_last_seen = pairs.groupBy("customer_id").agg(F.max("event_time").alias("last_seen_at"))
    loyalty_ids = (
        pos.filter(F.col("loyalty_id").isNotNull())
        .select(F.col("loyalty_id").cast("string"))
        .distinct()
    )
    loyalty_edges = (
        loyalty_ids.join(customer_last_seen, loyalty_ids["loyalty_id"] == customer_last_seen["customer_id"])
        .select(
            F.concat(F.lit(PREFIX_LOYALTY), F.col("loyalty_id")).alias("src"),
            F.concat(F.lit(PREFIX_CUSTOMER), F.col("customer_id")).alias("dst"),
            F.lit(EDGE_LOYALTY_VALUE_MATCH).alias("edge_type"),
            F.col("last_seen_at").alias("last_observed_at"),
        )
    )

    return session_edges.unionByName(loyalty_edges), public_devices


def assign_resolution(
    spark: SparkSession,
    vertices: DataFrame,
    edges: DataFrame,
    public_devices: DataFrame,
) -> DataFrame:
    """GraphFrames connected components + confidence/method assignment.

    Mirrors graph_logic.assign_resolution: component representative is the
    min node by priority (loyalty < customer < client), customer_key is the
    deterministic md5 formula shared with dbt's generate_customer_key.
    """
    from graphframes import GraphFrame

    graph = GraphFrame(vertices.select(F.col("node").alias("id")), edges.select("src", "dst"))
    components = graph.connectedComponents().withColumnRenamed("id", "node")

    sort_keyed = components.withColumn(
        "sort_key",
        F.when(F.col("node").startswith(PREFIX_LOYALTY), F.concat(F.lit("1:"), F.col("node")))
        .when(F.col("node").startswith(PREFIX_CUSTOMER), F.concat(F.lit("2:"), F.col("node")))
        .when(F.col("node").startswith(PREFIX_CLIENT), F.concat(F.lit("3:"), F.col("node")))
        .otherwise(F.concat(F.lit("9:"), F.col("node"))),
    )
    reps = sort_keyed.groupBy("component").agg(F.min("sort_key").alias("rep_sort_key"))
    resolved = (
        sort_keyed.join(reps, "component")
        .withColumn("component_rep_node", F.expr("substring(rep_sort_key, 3)"))
        .withColumn(
            "component_rep_type",
            F.when(F.col("rep_sort_key").startswith("1:"), F.lit("loyalty_id"))
            .when(F.col("rep_sort_key").startswith("2:"), F.lit("customer_id"))
            .otherwise(F.lit("client_id")),
        )
        .withColumn(
            "identifier_type",
            F.when(F.col("node").startswith(PREFIX_LOYALTY), F.lit("loyalty_id"))
            .when(F.col("node").startswith(PREFIX_CUSTOMER), F.lit("customer_id"))
            .otherwise(F.lit("client_id")),
        )
        .withColumn("identifier_value", F.expr("substring(node, instr(node, ':') + 1)"))
    )

    # The public-device list is tiny (thresholded), so a broadcast join is safe.
    resolved = resolved.join(
        F.broadcast(public_devices),
        resolved["node"] == public_devices["public_node"],
        "left",
    ).withColumn("is_public_device", F.col("public_node").isNotNull()).drop("public_node")

    rep_is_strong = F.col("component_rep_type").isin("loyalty_id", "customer_id")
    return (
        resolved
        .withColumn(
            "customer_key",
            F.when(
                F.col("is_public_device"),
                F.abs(F.pmod(F.conv(F.md5(F.concat(F.lit(PREFIX_CLIENT), F.col("identifier_value"))).substr(1, 8), 16, 10).cast("bigint"), F.lit(100_000_000))) + 1,
            ).otherwise(
                F.abs(F.pmod(F.conv(F.md5(F.col("component_rep_node")).substr(1, 8), 16, 10).cast("bigint"), F.lit(100_000_000))) + 1
            ),
        )
        .withColumn(
            "confidence_score",
            F.when(F.col("is_public_device"), F.lit(CONFIDENCE_PUBLIC_DEVICE))
            .when(F.col("identifier_type") == "loyalty_id", F.lit(CONFIDENCE_ANCHOR))
            .when(
                (F.col("identifier_type") == "customer_id") & (F.col("component_rep_type") == "loyalty_id"),
                F.lit(CONFIDENCE_LOYALTY_MATCH),
            )
            .when(F.col("identifier_type") == "customer_id", F.lit(CONFIDENCE_CUSTOMER_STANDALONE))
            .when((F.col("identifier_type") == "client_id") & rep_is_strong, F.lit(CONFIDENCE_SESSION_LINKED))
            .otherwise(F.lit(CONFIDENCE_DEVICE_ONLY))
            .cast("decimal(5,4)"),
        )
        .withColumn(
            "resolution_method",
            F.when(F.col("is_public_device"), F.lit(METHOD_PUBLIC_DEVICE_EXCLUDED))
            .when(
                (F.col("identifier_type") == "loyalty_id") & (F.col("node") == F.col("component_rep_node")),
                F.lit(METHOD_COMPONENT_ANCHOR),
            )
            .when(F.col("identifier_type") == "loyalty_id", F.lit(METHOD_LOYALTY_MEMBER))
            .when(
                (F.col("identifier_type") == "customer_id") & (F.col("component_rep_type") == "loyalty_id"),
                F.lit(METHOD_LOYALTY_MATCH),
            )
            .when(F.col("identifier_type") == "customer_id", F.lit(METHOD_CUSTOMER_STANDALONE))
            .when((F.col("identifier_type") == "client_id") & rep_is_strong, F.lit(METHOD_SESSION_LINKED))
            .otherwise(F.lit(METHOD_DEVICE_ONLY)),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "identifier_type",
            "identifier_value",
            "customer_key",
            "confidence_score",
            "resolution_method",
            "is_public_device",
            "component_rep_node",
            "component_rep_type",
            "computed_at",
        )
    )


def main() -> None:
    args = _parse_args()
    spark = _build_session(args)

    clickstream = spark.table(f"{BRONZE_CATALOG}.bronze.clickstream_events")
    pos = spark.read.parquet(args.pos_parquet_path)

    edges, public_devices = build_edges(clickstream, pos, args.public_device_threshold)

    loyalty_nodes = (
        pos.filter(F.col("loyalty_id").isNotNull())
        .select(F.concat(F.lit(PREFIX_LOYALTY), F.col("loyalty_id").cast("string")).alias("node"))
        .distinct()
    )
    customer_nodes = (
        clickstream.filter(F.col("customer_id").isNotNull())
        .select(F.concat(F.lit(PREFIX_CUSTOMER), F.col("customer_id").cast("string")).alias("node"))
        .distinct()
    )
    client_nodes = (
        clickstream.filter(F.col("client_id").isNotNull())
        .select(F.concat(F.lit(PREFIX_CLIENT), F.col("client_id").cast("string")).alias("node"))
        .distinct()
    )
    vertices = loyalty_nodes.unionByName(customer_nodes).unionByName(client_nodes)

    resolution = assign_resolution(spark, vertices, edges, public_devices)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_CATALOG}.silver")
    resolution.writeTo(OUTPUT_TABLE).using("iceberg").createOrReplace()
    edges.writeTo(EDGES_TABLE).using("iceberg").createOrReplace()

    spark.stop()


if __name__ == "__main__":
    main()
