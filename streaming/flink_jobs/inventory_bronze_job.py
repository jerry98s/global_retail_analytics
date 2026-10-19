"""
Inventory Bronze Flink job.
Pipeline: Kafka -> schema validate -> dedup -> Iceberg Bronze (raw events).
Invalid rows are routed to a Kafka DLQ topic for investigation — mirrors the
clickstream bronze DLQ pattern.

Feeds dbt staging (bronze.inventory_events). The inventory_hourly job
continues to write aggregated Silver snapshots for operational use.
"""

from __future__ import annotations

from typing import Any, Dict

from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from _config import (  # noqa: E402
    apply_state_config,
    kafka_security_options,
    load_simple_yaml,
    resolve_config_path,
    resolve_parallelism,
    watermark_interval_sql,
)
from event_types import (  # noqa: E402
    INVENTORY_EVENT_TYPES,
)
from lake_names import (  # noqa: E402
    BRONZE_NAMESPACE,
    INVENTORY_EVENTS,
)

VALID_EVENT_TYPES = INVENTORY_EVENT_TYPES


def _apply_checkpoint_config(
    env: StreamExecutionEnvironment, checkpoint_cfg: Dict[str, Any]
) -> None:
    env.enable_checkpointing(int(checkpoint_cfg["checkpoint_interval_ms"]))
    cp = env.get_checkpoint_config()
    cp.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    cp.set_min_pause_between_checkpoints(
        int(checkpoint_cfg["min_pause_between_checkpoints_ms"])
    )
    cp.set_checkpoint_timeout(int(checkpoint_cfg["checkpoint_timeout_ms"]))
    cp.set_max_concurrent_checkpoints(int(checkpoint_cfg["max_concurrent_checkpoints"]))
    cp.set_tolerable_checkpoint_failure_number(
        int(checkpoint_cfg["tolerable_checkpoint_failures"])
    )
    cp.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )


def run() -> None:
    checkpoint_cfg = load_simple_yaml(resolve_config_path("checkpoints.yaml"))
    state_cfg = load_simple_yaml(resolve_config_path("state.yaml"))
    flink_cfg = load_simple_yaml(resolve_config_path("flink_conf.yaml"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(resolve_parallelism(flink_cfg, "inventory_parallelism"))
    _apply_checkpoint_config(env, checkpoint_cfg)
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=EnvironmentSettings.in_streaming_mode(),
    )
    # F-STATE: rocksdb state backend + incremental checkpoints + 7d TTL
    # + 1min source idle timeout. See streaming/config/state.yaml.
    apply_state_config(env, t_env, state_cfg)

    bronze_wm = watermark_interval_sql(
        flink_cfg, "inventory_bronze_watermark_delay_seconds", 30
    )

    catalog = str(flink_cfg["iceberg_catalog_name"])
    t_env.execute_sql(
        f"""
        CREATE CATALOG {catalog} WITH (
          'type' = 'iceberg',
          'catalog-type' = '{flink_cfg["iceberg_catalog_type"]}',
          'warehouse' = '{flink_cfg["iceberg_warehouse_bronze"]}'
        )
        """
    )
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{BRONZE_NAMESPACE}")
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{BRONZE_NAMESPACE}.{INVENTORY_EVENTS} (
          event_id STRING,
          event_time TIMESTAMP(3),
          store_id STRING,
          product_id STRING,
          qty_delta INT,
          event_type STRING,
          scanner_id STRING,
          is_late BOOLEAN,
          schema_version STRING,
          event_date DATE
        )
        -- Daily partitioning via derived event_date (CAST(event_time AS DATE)).
        -- Flink SQL / Iceberg Hadoop catalog does not accept the days()
        -- transform in PARTITIONED BY here, so we materialize event_date as
        -- an identity partition column instead. Same pruning effect for
        -- Spectrum WHERE event_date = ... (or CAST(event_time AS DATE)).
        -- See docs/runbooks/iceberg-maintenance.md (DL-A).
        PARTITIONED BY (event_date)
        """
    )

    security_opts = kafka_security_options(flink_cfg)
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.inventory_events_kafka (
          event_id STRING,
          event_time STRING,
          event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(event_time, 1, 23), 'T', ' ')),
          store_id STRING,
          product_id STRING,
          qty_delta INT,
          event_type STRING,
          scanner_id STRING,
          is_late BOOLEAN,
          schema_version STRING,
          -- Watermark asymmetry (P3.2 from docs/runbooks/dw-checklist-audit.md):
          -- Bronze default 30s of lateness; silver inventory_silver_job
          -- default 60s. Tunable via inventory_bronze_watermark_delay_seconds.
          -- Producers can emit up to ~300s late under reconnect/retry.
          -- Events later than the watermark are dropped by design.
          WATERMARK FOR event_ts AS event_ts - {bronze_wm}
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{flink_cfg["inventory_topic"]}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_inventory_bronze"]}',
          'scan.startup.mode' = 'latest-offset',
          -- K-CONS from the Kafka checklist: Flink Kafka connector defaults
          -- enable.auto.commit to false (Flink commits offsets via the
          -- checkpoint committer under EXACTLY_ONCE), but we make it
          -- explicit so a future contributor doesn't accidentally override
          -- it. isolation.level=read_committed skips un-committed
          -- transactional messages from other producers (defense in depth
          -- even though our producers don't use Kafka transactions today).
          -- auto.offset.reset=earliest is the safety net for the first
          -- startup when no committed offset exists AND scan.startup.mode
          -- is overridden to 'earliest-offset' for backfills.
          'properties.enable.auto.commit' = 'false',
          'properties.isolation.level' = 'read_committed',
          'properties.auto.offset.reset' = 'earliest',
          -- F-SRC from the Flink production checklist: dynamic partition
          -- discovery every 5 minutes. Without this, newly-added Kafka
          -- partitions (e.g. when ops bumps inventory.events.v1 from 12
          -- to 24 partitions) require a Flink job restart. With this,
          -- the Kafka source picks up the new partitions at the next
          -- 5-minute boundary.
          'properties.partition.discovery.interval.ms' = '300000',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
          {security_opts}
        )
        """
    )

    dlq_topic = flink_cfg.get(
        "inventory_dlq_topic", "dlq.inventory.schema_violations"
    )
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.inventory_schema_dlq (
          event_id STRING,
          event_time STRING,
          store_id STRING,
          product_id STRING,
          qty_delta INT,
          event_type STRING,
          scanner_id STRING,
          is_late BOOLEAN,
          schema_version STRING,
          error_reason STRING,
          rejected_at TIMESTAMP(3)
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{dlq_topic}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_inventory_bronze"]}-dlq',
          'format' = 'json'
          {security_opts}
        )
        """
    )

    event_type_list = ",".join([f"'{item}'" for item in VALID_EVENT_TYPES])
    valid_predicate = f"""
        event_id IS NOT NULL
        AND event_ts IS NOT NULL
        AND store_id IS NOT NULL
        AND product_id IS NOT NULL
        AND qty_delta IS NOT NULL
        AND event_type IN ({event_type_list})
        AND scanner_id IS NOT NULL
        AND REGEXP_EXTRACT(schema_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NOT NULL
    """

    bronze_sql = f"""
    INSERT INTO {catalog}.{BRONZE_NAMESPACE}.{INVENTORY_EVENTS}
    WITH validated AS (
      SELECT
        event_id,
        event_ts AS event_time,
        store_id,
        product_id,
        qty_delta,
        event_type,
        scanner_id,
        COALESCE(is_late, FALSE) AS is_late,
        schema_version,
        CAST(event_ts AS DATE) AS event_date
      FROM default_catalog.default_database.inventory_events_kafka
      WHERE {valid_predicate}
    ),
    deduped AS (
      SELECT event_id, event_time, store_id, product_id, qty_delta,
             event_type, scanner_id, is_late, schema_version, event_date
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) AS rn
        FROM validated
      )
      WHERE rn = 1
    )
    SELECT * FROM deduped
    """

    dlq_sql = f"""
    INSERT INTO default_catalog.default_database.inventory_schema_dlq
    SELECT
      event_id,
      event_time,
      store_id,
      product_id,
      qty_delta,
      event_type,
      scanner_id,
      COALESCE(is_late, FALSE) AS is_late,
      schema_version,
      CASE
        WHEN event_id IS NULL THEN 'missing_event_id'
        WHEN event_ts IS NULL THEN 'invalid_event_time'
        WHEN store_id IS NULL THEN 'missing_store_id'
        WHEN product_id IS NULL THEN 'missing_product_id'
        WHEN qty_delta IS NULL THEN 'missing_qty_delta'
        WHEN event_type NOT IN ({event_type_list}) THEN 'invalid_event_type'
        WHEN scanner_id IS NULL THEN 'missing_scanner_id'
        WHEN REGEXP_EXTRACT(schema_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NULL
          THEN 'invalid_schema_version'
        ELSE 'schema_violation'
      END AS error_reason,
      CURRENT_TIMESTAMP AS rejected_at
    FROM default_catalog.default_database.inventory_events_kafka
    WHERE NOT ({valid_predicate})
    """

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(bronze_sql)
    stmt_set.add_insert_sql(dlq_sql)
    result = stmt_set.execute()
    job_client = result.get_job_client()
    if job_client is not None:
        print(f"Started job: {job_client.get_job_id()}")


if __name__ == "__main__":
    run()
