"""Canonical lake object names (Iceberg, Spectrum, S3, dbt bronze sources).

Authoritative spec: docs/data-model/naming-conventions.md

Topic names live in `ingestion/kafka/topics.py` (single source of truth for
Kafka topic constants). Flink jobs read topic names from `flink_conf.yaml` at
runtime, so this module does not redeclare them.
"""

from __future__ import annotations

# Iceberg namespaces (Flink CREATE DATABASE)
BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"

# Bronze — table name = Spectrum name = dbt source name
CLICKSTREAM_EVENTS = "clickstream_events"
INVENTORY_EVENTS = "inventory_events"
POS_TRANSACTIONS = "pos_transactions"

# Silver — grain in the table name
INVENTORY_HOURLY = "inventory_hourly"

# Local Docker warehouse root (must match ICEBERG_*_WAREHOUSE in docker-compose.yml)
LOCAL_WAREHOUSE_ROOT = "/tmp/iceberg"

# Relative Parquet data dirs under LOCAL_WAREHOUSE_ROOT (namespace/table/data)
LOCAL_TABLE_PATHS = {
    "clickstream": f"{LOCAL_WAREHOUSE_ROOT}/{BRONZE_NAMESPACE}/{CLICKSTREAM_EVENTS}/data",
    "inventory": f"{LOCAL_WAREHOUSE_ROOT}/{SILVER_NAMESPACE}/{INVENTORY_HOURLY}/data",
}


def s3_parquet_location(bucket: str, namespace: str, table: str) -> str:
    """Spectrum LOCATION for bronze/silver Parquet under the standard layout."""
    return f"s3://{bucket}/iceberg/{namespace}/{table}/data/"


def relative_parquet_dir(namespace: str, table: str) -> str:
    """Path fragment under LOCAL_ICEBERG_DIR for dashboard local mode."""
    return f"{namespace}/{table}/data"
