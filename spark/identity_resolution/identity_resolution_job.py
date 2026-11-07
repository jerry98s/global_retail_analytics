"""Spark + GraphFrames identity-resolution job (ADR-003, ADR-010).

Promotes raw Bronze into canonical Silver, then reads canonical clickstream
(Iceberg) and POS loyalty IDs (Parquet), builds the identity graph, runs
GraphFrames connected components, and writes:

- ``silver.identity_resolution`` — one row per customer_id / loyalty_id with
  customer_key / confidence / method (dbt reads this as a source;
  ``int_identity_resolution`` is a thin view over it).
- ``silver.identity_edges`` — the verified loyalty links that merged.
- ``silver.loyalty_link_claims`` — every login that named a loyalty card,
  verified or not (audit).
- ``silver.device_session_associations`` — signed-in intervals per device and
  session instance (attribution only; never graph edges).

The business rules live in ``graph_logic.py`` and are shared with the dbt
seed fixture generator — this job mirrors them in DataFrame/GraphFrames
operations.

Cloud (EMR, on `main`) — submitted by the marketing hourly DAG:

    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,graphframes:graphframes:0.8.3-spark3.4-s_2.12 \
      identity_resolution_job.py \
      --bronze-warehouse s3://<bucket>-bronze/iceberg \
      --silver-warehouse s3://<bucket>-silver/iceberg \
      --pos-parquet-path s3://<bucket>-bronze/iceberg/bronze/pos_transactions/ \
      --checkpoint-dir s3://<bucket>-checkpoints/graphframes/

Local (laptop, local-testing-version stack):

    .\\scripts\\local\\run_local_stack.ps1 -Task spark

    (Docker image retail-analytics/spark-identity:3.4.1; warehouse
     file:///tmp/iceberg via the compose bind-mount.)
"""

from __future__ import annotations

import argparse
from urllib.parse import urlparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from .canonical_events import promote_events
    from .consumer_exports import new_run_id, publish
except ImportError:
    from canonical_events import promote_events
    from consumer_exports import new_run_id, publish

try:  # package import in tests; script import under spark-submit
    from .graph_logic import (
        CONFIDENCE_ANCHOR,
        CONFIDENCE_CUSTOMER_STANDALONE,
        CONFIDENCE_LOYALTY_MATCH,
        EDGE_VERIFIED_LOYALTY_LINK,
        END_LAST_EVENT,
        END_LOGOUT,
        EVIDENCE_SIGNED_IN_SESSION,
        EVIDENCE_VERIFIED_LOGIN,
        METHOD_COMPONENT_ANCHOR,
        METHOD_CUSTOMER_STANDALONE,
        METHOD_LOYALTY_MATCH,
        METHOD_LOYALTY_MEMBER,
        PREFIX_CUSTOMER,
        PREFIX_LOYALTY,
    )
except ImportError:  # pragma: no cover - spark-submit script mode
    from graph_logic import (
        CONFIDENCE_ANCHOR,
        CONFIDENCE_CUSTOMER_STANDALONE,
        CONFIDENCE_LOYALTY_MATCH,
        EDGE_VERIFIED_LOYALTY_LINK,
        END_LAST_EVENT,
        END_LOGOUT,
        EVIDENCE_SIGNED_IN_SESSION,
        EVIDENCE_VERIFIED_LOGIN,
        METHOD_COMPONENT_ANCHOR,
        METHOD_CUSTOMER_STANDALONE,
        METHOD_LOYALTY_MATCH,
        METHOD_LOYALTY_MEMBER,
        PREFIX_CUSTOMER,
        PREFIX_LOYALTY,
    )

BRONZE_CATALOG = "iceberg_bronze"
SILVER_CATALOG = "iceberg_silver"
OUTPUT_TABLE = f"{SILVER_CATALOG}.silver.identity_resolution"
EDGES_TABLE = f"{SILVER_CATALOG}.silver.identity_edges"
CLAIMS_TABLE = f"{SILVER_CATALOG}.silver.loyalty_link_claims"
ASSOCIATIONS_TABLE = f"{SILVER_CATALOG}.silver.device_session_associations"
INVENTORY_HOURLY_TABLE = f"{SILVER_CATALOG}.silver.inventory_hourly"
INVENTORY_HOURLY_SCHEMA = (
    "snapshot_date_key int, snapshot_hour int, product_id string, store_id string, "
    "qty_delta_hour bigint, qty_received_hour bigint, is_estimated boolean"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GraphFrames identity resolution")
    parser.add_argument('--canonical-only', action='store_true',
                        help='Replay Bronze into canonical Silver and audit tables; skip identity/POS')
    parser.add_argument("--bronze-warehouse", required=True,
                        help="Iceberg Hadoop catalog warehouse holding bronze.*_records")
    parser.add_argument("--silver-warehouse", required=True,
                        help="Iceberg Hadoop catalog warehouse for silver.identity_* output")
    parser.add_argument("--pos-parquet-path",
                        help="POS batch Parquet path (plain Parquet, not Iceberg)")
    parser.add_argument("--checkpoint-dir", default="/tmp/graphframes-checkpoints",
                        help="GraphFrames checkpoint dir (S3 on EMR, local path otherwise)")
    parser.add_argument(
        "--consumer-parquet-root",
        default=None,
        help=(
            "Snapshot-safe plain-Parquet export root used by Spectrum/DuckDB. "
            "Defaults to <silver-warehouse>/consumer_current."
        ),
    )
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
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    return spark


def _clean(value: Column) -> Column:
    """Spark expression matching graph_logic.normalize_identifier."""
    trimmed = F.trim(value.cast("string"))
    return F.when(F.length(trimmed) > 0, trimmed)


def _signed_in_v13(clickstream: DataFrame) -> DataFrame:
    """Signed-in v1.3 events: client, session instance and account all present."""
    return (
        clickstream.select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("event_time"),
            F.col("properties"),
            _clean(F.col("client_id")).alias("client_id"),
            _clean(F.col("session_instance_id")).alias("session_instance_id"),
            _clean(F.col("customer_id")).alias("customer_id"),
        )
        .filter(
            F.col("client_id").isNotNull()
            & F.col("session_instance_id").isNotNull()
            & F.col("customer_id").isNotNull()
        )
    )


def loyalty_claims(clickstream: DataFrame) -> DataFrame:
    """Mirror of graph_logic.loyalty_claims."""
    return (
        _signed_in_v13(clickstream)
        .filter(F.col("event_type") == "login")
        .withColumn("loyalty_id", _clean(F.get_json_object("properties", "$.loyalty_id")))
        .filter(F.col("loyalty_id").isNotNull())
        .select(
            "event_id",
            "event_time",
            "client_id",
            "session_instance_id",
            "customer_id",
            "loyalty_id",
            # from_json, not get_json_object: the string "true" is not verification.
            F.coalesce(
                F.from_json("properties", "loyalty_link_verified boolean").getField("loyalty_link_verified"),
                F.lit(False),
            ).alias("is_verified"),
        )
    )


def build_edges(claims: DataFrame) -> DataFrame:
    """Mirror of graph_logic.build_edges: one edge per verified (loyalty, account)."""
    return (
        claims.filter(F.col("is_verified"))
        .groupBy("loyalty_id", "customer_id")
        .agg(
            F.min("event_time").alias("first_verified_at"),
            F.max("event_time").alias("last_verified_at"),
            F.min(F.struct("event_time", "event_id")).getField("event_id").alias("evidence_event_id"),
        )
        .select(
            F.concat(F.lit(PREFIX_LOYALTY), F.col("loyalty_id")).alias("src"),
            F.concat(F.lit(PREFIX_CUSTOMER), F.col("customer_id")).alias("dst"),
            F.lit(EDGE_VERIFIED_LOYALTY_LINK).alias("edge_type"),
            "first_verified_at",
            "last_verified_at",
            "evidence_event_id",
        )
    )


def build_vertices(clickstream: DataFrame, pos: DataFrame, edges: DataFrame) -> DataFrame:
    """POS loyalty IDs, verified-link loyalty IDs and clickstream accounts."""
    pos_loyalty = (
        pos.select(_clean(F.col("loyalty_id")).alias("loyalty_id"))
        .filter(F.col("loyalty_id").isNotNull())
        .select(F.concat(F.lit(PREFIX_LOYALTY), F.col("loyalty_id")).alias("node"))
    )
    linked_loyalty = edges.select(F.col("src").alias("node"))
    customers = (
        clickstream.select(_clean(F.col("customer_id")).alias("customer_id"))
        .filter(F.col("customer_id").isNotNull())
        .select(F.concat(F.lit(PREFIX_CUSTOMER), F.col("customer_id")).alias("node"))
    )
    return pos_loyalty.unionByName(linked_loyalty).unionByName(customers).distinct()


def device_session_associations(clickstream: DataFrame) -> DataFrame:
    """Mirror of graph_logic.device_session_associations."""
    signed_in = _signed_in_v13(clickstream)
    keys = ["client_id", "session_instance_id", "customer_id"]
    intervals = signed_in.groupBy(*keys).agg(
        F.min("event_time").alias("valid_from"),
        F.max("event_time").alias("last_event_at"),
        F.max(F.when(F.col("event_type") == "logout", F.col("event_time"))).alias("logout_at"),
        F.max((F.col("event_type") == "login").cast("int")).alias("has_login"),
        F.count(F.lit(1)).alias("event_count"),
    )
    accounts = signed_in.groupBy("client_id", "session_instance_id").agg(
        F.countDistinct("customer_id").alias("accounts_in_instance")
    )
    return (
        intervals.join(accounts, ["client_id", "session_instance_id"])
        .select(
            *keys,
            "valid_from",
            F.coalesce("logout_at", "last_event_at").alias("valid_to"),
            F.when(F.col("logout_at").isNotNull(), F.lit(END_LOGOUT))
            .otherwise(F.lit(END_LAST_EVENT)).alias("end_reason"),
            F.when(F.col("has_login") == 1, F.lit(EVIDENCE_VERIFIED_LOGIN))
            .otherwise(F.lit(EVIDENCE_SIGNED_IN_SESSION)).alias("evidence"),
            "event_count",
            (F.col("accounts_in_instance") > 1).alias("is_ambiguous"),
        )
    )


def _consumer_root(args: argparse.Namespace) -> str:
    root = args.consumer_parquet_root or (
        args.silver_warehouse.rstrip("/") + "/consumer_current"
    )
    parsed = urlparse(root)
    if parsed.scheme and parsed.scheme not in {"file", "s3", "s3a"}:
        raise ValueError(f"Unsupported consumer Parquet URI scheme: {parsed.scheme}")
    return root.rstrip("/")


def write_consumer_exports(
    resolution: DataFrame,
    edges: DataFrame,
    claims: DataFrame,
    associations: DataFrame,
    root: str,
    run_id: str | None = None,
) -> None:
    """Publish the identity outputs as snapshot-safe exports (consumer_exports)."""
    run_id = run_id or new_run_id()
    publish(resolution, root, "identity_resolution", run_id)
    publish(edges, root, "identity_edges", run_id)
    publish(claims, root, "loyalty_link_claims", run_id)
    publish(associations, root, "device_session_associations", run_id)


def export_inventory_hourly(spark: SparkSession, root: str, run_id: str | None = None) -> None:
    """Export Flink's committed silver.inventory_hourly snapshot for Spectrum.

    Reading through the Iceberg catalog returns only committed rows; Spectrum
    globbing the table's data/ dir would also return uncommitted or replaced
    files. An empty export is written until the Flink job creates the table.
    """
    empty = spark.createDataFrame([], INVENTORY_HOURLY_SCHEMA)
    if spark.catalog.tableExists(INVENTORY_HOURLY_TABLE):
        # Spectrum maps Parquet columns by position: pin the DDL order.
        frame = spark.table(INVENTORY_HOURLY_TABLE).select(*empty.columns)
    else:
        frame = empty
    publish(frame, root, "inventory_hourly", run_id or new_run_id())


def _customer_key(node: Column) -> Column:
    """Spark form of graph_logic.customer_key."""
    digest = F.conv(F.md5(node).substr(1, 8), 16, 10).cast("bigint")
    return F.abs(F.pmod(digest, F.lit(100_000_000))) + 1


def assign_resolution(spark: SparkSession, vertices: DataFrame, edges: DataFrame) -> DataFrame:
    """GraphFrames connected components + confidence/method assignment.

    Mirrors graph_logic.assign_resolution: component representative is the
    min node by priority (loyalty < customer), customer_key is the
    deterministic md5 formula shared with dbt's generate_customer_key.
    """
    from graphframes import GraphFrame

    graph = GraphFrame(vertices.select(F.col("node").alias("id")), edges.select("src", "dst"))
    components = graph.connectedComponents().withColumnRenamed("id", "node")

    sort_keyed = components.withColumn(
        "sort_key",
        F.when(F.col("node").startswith(PREFIX_LOYALTY), F.concat(F.lit("1:"), F.col("node")))
        .when(F.col("node").startswith(PREFIX_CUSTOMER), F.concat(F.lit("2:"), F.col("node")))
        .otherwise(F.concat(F.lit("9:"), F.col("node"))),
    )
    reps = sort_keyed.groupBy("component").agg(F.min("sort_key").alias("rep_sort_key"))
    resolved = (
        sort_keyed.join(reps, "component")
        .withColumn("component_rep_node", F.expr("substring(rep_sort_key, 3)"))
        .withColumn(
            "component_rep_type",
            F.when(F.col("rep_sort_key").startswith("1:"), F.lit("loyalty_id"))
            .otherwise(F.lit("customer_id")),
        )
        .withColumn(
            "identifier_type",
            F.when(F.col("node").startswith(PREFIX_LOYALTY), F.lit("loyalty_id"))
            .otherwise(F.lit("customer_id")),
        )
        .withColumn("identifier_value", F.expr("substring(node, instr(node, ':') + 1)"))
    )

    is_loyalty = F.col("identifier_type") == "loyalty_id"
    rep_is_loyalty = F.col("component_rep_type") == "loyalty_id"
    return (
        resolved
        .withColumn("customer_key", _customer_key(F.col("component_rep_node")))
        .withColumn(
            "confidence_score",
            F.when(is_loyalty, F.lit(CONFIDENCE_ANCHOR))
            .when(rep_is_loyalty, F.lit(CONFIDENCE_LOYALTY_MATCH))
            .otherwise(F.lit(CONFIDENCE_CUSTOMER_STANDALONE))
            .cast("decimal(5,4)"),
        )
        .withColumn(
            "resolution_method",
            F.when(is_loyalty & (F.col("node") == F.col("component_rep_node")), F.lit(METHOD_COMPONENT_ANCHOR))
            .when(is_loyalty, F.lit(METHOD_LOYALTY_MEMBER))
            .when(rep_is_loyalty, F.lit(METHOD_LOYALTY_MATCH))
            .otherwise(F.lit(METHOD_CUSTOMER_STANDALONE)),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "identifier_type",
            "identifier_value",
            "customer_key",
            "confidence_score",
            "resolution_method",
            "component_rep_node",
            "component_rep_type",
            "computed_at",
        )
    )


def main() -> None:
    args = _parse_args()
    spark = _build_session(args)

    if not args.canonical_only and not args.pos_parquet_path:
        raise ValueError('--pos-parquet-path is required unless --canonical-only is set')
    # One run id for every export, so dbt can check it combined one run.
    run_id = new_run_id()
    events = promote_events(spark, BRONZE_CATALOG, SILVER_CATALOG, _consumer_root(args), run_id)
    export_inventory_hourly(spark, _consumer_root(args), run_id)
    if args.canonical_only:
        spark.stop()
        return
    clickstream = events['clickstream']
    pos = spark.read.parquet(args.pos_parquet_path)

    claims = loyalty_claims(clickstream)
    edges = build_edges(claims)
    vertices = build_vertices(clickstream, pos, edges)
    resolution = assign_resolution(spark, vertices, edges)
    associations = device_session_associations(clickstream)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_CATALOG}.silver")
    resolution.writeTo(OUTPUT_TABLE).using("iceberg").createOrReplace()
    edges.writeTo(EDGES_TABLE).using("iceberg").createOrReplace()
    claims.writeTo(CLAIMS_TABLE).using("iceberg").createOrReplace()
    associations.writeTo(ASSOCIATIONS_TABLE).using("iceberg").createOrReplace()
    write_consumer_exports(resolution, edges, claims, associations, _consumer_root(args), run_id)

    spark.stop()


if __name__ == "__main__":
    main()
