"""
Inventory silver Flink job.
Pipeline: Kafka -> dedup -> watermark -> hourly window aggregation -> Iceberg Silver.
Writes silver.inventory_hourly (kappa upstream for finance.fact_inventory_snapshot).
"""

from __future__ import annotations

import os
import re
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
from lake_names import (  # noqa: E402
    SILVER_NAMESPACE,
    INVENTORY_HOURLY,
)

# Flink SQL INTERVAL literal, e.g. INTERVAL '1' HOUR (prod) or INTERVAL '1' MINUTE (local).
_WINDOW_INTERVAL_RE = re.compile(
    r"^INTERVAL\s+'\d+'\s+(SECOND|MINUTE|HOUR|DAY)$",
    re.IGNORECASE,
)


def _silver_window_interval() -> str:
    """Tumble window size for inventory silver.

    Override with env ``INVENTORY_SILVER_WINDOW`` for local short sims
    (default compose uses ``INTERVAL '1' MINUTE`` so windows close without
    waiting for the wall-clock hour). Cloud/EMR leave unset → 1 hour.
    """
    raw = os.environ.get("INVENTORY_SILVER_WINDOW", "INTERVAL '1' HOUR").strip()
    if not _WINDOW_INTERVAL_RE.match(raw):
        raise ValueError(
            "INVENTORY_SILVER_WINDOW must look like INTERVAL '1' HOUR "
            f"(got {raw!r})"
        )
    return raw


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
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env, environment_settings=env_settings
    )
    # F-STATE: rocksdb state backend + incremental checkpoints + 7d TTL
    # (mandatory for this job's dedup self-join — without TTL the dedup
    # state grows by ~1 row per event_id forever) + 1min source idle
    # timeout. See streaming/config/state.yaml.
    apply_state_config(env, t_env, state_cfg)

    silver_wm = watermark_interval_sql(
        flink_cfg, "inventory_silver_watermark_delay_seconds", 60
    )
    window_interval = _silver_window_interval()

    catalog = str(flink_cfg["iceberg_catalog_name"])
    t_env.execute_sql(
        f"""
        CREATE CATALOG {catalog} WITH (
          'type' = 'iceberg',
          'catalog-type' = '{flink_cfg["iceberg_catalog_type"]}',
          'warehouse' = '{flink_cfg["iceberg_warehouse_silver"]}'
        )
        """
    )
    # Iceberg catalog only manages Iceberg tables (no watermarks, no Kafka
    # connector). We register the managed sink there, and create the Kafka
    # source in the in-memory `default_catalog`.
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{SILVER_NAMESPACE}")
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{SILVER_NAMESPACE}.{INVENTORY_HOURLY} (
          snapshot_date_key INT,
          snapshot_hour INT,
          product_id STRING,
          store_id STRING,
          qty_delta_hour BIGINT,
          qty_received_hour BIGINT,
          is_estimated BOOLEAN
        )
        -- Identity partition on snapshot_date_key (YYYYMMDD int). Silver is
        -- the upstream for finance.fact_inventory_snapshot's kappa path; the
        -- daily dbt incremental filters on snapshot_date_key, so partition
        -- pruning on this column collapses a 7-day backfill to 7 partitions
        -- out of ~365/year. See docs/runbooks/iceberg-maintenance.md (DL-A
        -- from the data lake checklist).
        PARTITIONED BY (snapshot_date_key)
        """
    )

    security_opts = kafka_security_options(flink_cfg)
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.inventory_events (
          event_id STRING,
          event_time STRING,
          -- Same ISO-8601 parse as inventory_bronze_job: JSON TIMESTAMP(3)
          -- does not reliably bind producer `...T...+00:00` strings, which
          -- leaves null watermarks and prevents TUMBLE windows from closing.
          event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(event_time, 1, 23), 'T', ' ')),
          store_id STRING,
          product_id STRING,
          qty_delta INT,
          event_type STRING,
          scanner_id STRING,
          is_late BOOLEAN,
          -- Watermark asymmetry (P3.2): silver default 60s (wider than bronze).
          -- Tunable via inventory_silver_watermark_delay_seconds.
          WATERMARK FOR event_ts AS event_ts - {silver_wm}
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{flink_cfg["inventory_topic"]}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_inventory"]}',
          'scan.startup.mode' = 'latest-offset',
          -- K-CONS from the Kafka checklist: explicit enable.auto.commit=
          -- false + isolation.level=read_committed + auto.offset.reset=
          -- earliest. See inventory_bronze_job.py for the full rationale.
          'properties.enable.auto.commit' = 'false',
          'properties.isolation.level' = 'read_committed',
          'properties.auto.offset.reset' = 'earliest',
          -- F-SRC from the Flink production checklist: dynamic partition
          -- discovery every 5 minutes (see inventory_bronze_job.py for
          -- the full rationale).
          'properties.partition.discovery.interval.ms' = '300000',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
          {security_opts}
        )
        """
    )

    job_sql = f"""
    INSERT INTO {catalog}.{SILVER_NAMESPACE}.{INVENTORY_HOURLY}
    WITH deduped AS (
      SELECT event_id, event_time, store_id, product_id, qty_delta
      FROM (
        SELECT
          event_id,
          event_ts AS event_time,
          store_id,
          product_id,
          qty_delta,
          ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_ts DESC
          ) AS rn
        FROM default_catalog.default_database.inventory_events
        WHERE event_ts IS NOT NULL
      )
      WHERE rn = 1
    )
    SELECT
      CAST(DATE_FORMAT(window_start, 'yyyyMMdd') AS INT) AS snapshot_date_key,
      CAST(EXTRACT(HOUR FROM window_start) AS INT) AS snapshot_hour,
      product_id,
      store_id,
      CAST(SUM(qty_delta) AS BIGINT) AS qty_delta_hour,
      CAST(SUM(CASE WHEN qty_delta > 0 THEN qty_delta ELSE 0 END) AS BIGINT) AS qty_received_hour,
      FALSE AS is_estimated
    FROM TABLE(
      TUMBLE(
        TABLE deduped,
        DESCRIPTOR(event_time),
        {window_interval}
      )
    )
    GROUP BY window_start, product_id, store_id
    """

    result = t_env.execute_sql(job_sql)
    job_client = result.get_job_client()
    if job_client is not None:
        print(f"Started job: {job_client.get_job_id()}")


if __name__ == "__main__":
    run()
