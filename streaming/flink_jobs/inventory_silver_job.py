"""
Inventory silver Flink job.
Pipeline: Kafka -> dedup -> watermark -> hourly window aggregation -> Iceberg Silver.
Writes silver.inventory_hourly (speed layer for finance.fact_inventory_snapshot, ADR-012).
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
    # F-STATE: rocksdb state backend + incremental checkpoints + 7d TTL +
    # 1min source idle timeout. Window dedup/aggregation free their state
    # when the window closes; the TTL is a backstop. See streaming/config/state.yaml.
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
        -- the speed layer for finance.fact_inventory_snapshot (ADR-012);
        -- partition pruning on this column collapses a 7-day backfill to 7
        -- partitions out of ~365/year.
        PARTITIONED BY (snapshot_date_key)
        """
    )

    security_opts = kafka_security_options(flink_cfg)
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.inventory_events (
          event_id STRING,
          event_time STRING,
          -- JSON TIMESTAMP(3) does not reliably bind producer
          -- `...T...+00:00` strings; parse the string instead.
          event_ts AS {event_ts_sql()},
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
          'scan.startup.mode' = 'group-offsets',
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

    job_sql = f"INSERT INTO {catalog}.{SILVER_NAMESPACE}.{INVENTORY_HOURLY}\n" + hourly_select_sql(
        source="default_catalog.default_database.inventory_events",
        window_interval=window_interval,
    )

    result = t_env.execute_sql(job_sql)
    job_client = result.get_job_client()
    if job_client is not None:
        print(f"Started job: {job_client.get_job_id()}")


def event_ts_sql(column: str = "event_time") -> str:
    """UTC rowtime from an ISO-8601 string, never null.

    Accepts any (or no) fractional seconds and a trailing Z or +/-HH:MM
    offset, matching Spark's to_timestamp. A null rowtime fails the watermark
    operator and restarts the job, so an unparseable value becomes 1970: its
    window closed long ago and Flink drops it, while Bronze keeps the raw record.

    CASE, not COALESCE: on Flink 1.17.1 a COALESCE in a watermarked computed
    column fails code generation on the TaskManager (FLINK-28693).
    """
    def offset(group: int) -> str:
        return f"REGEXP_EXTRACT({column}, '([+-])([0-9][0-9]):([0-9][0-9])$', {group})"

    offset_minutes = (
        f"(CASE WHEN {offset(1)} IN ('+', '-')"
        f" THEN (CASE WHEN {offset(1)} = '-' THEN -1 ELSE 1 END)"
        f" * (CAST({offset(2)} AS INT) * 60 + CAST({offset(3)} AS INT))"
        f" ELSE 0 END)"
    )
    local = f"TO_TIMESTAMP(REPLACE(SUBSTRING({column}, 1, 19), 'T', ' '))"
    return (
        f"(CASE WHEN {local} IS NULL THEN TIMESTAMP '1970-01-01 00:00:00'"
        f" ELSE TIMESTAMPADD(MINUTE, -{offset_minutes}, {local}) END)"
    )


def hourly_select_sql(*, source: str, window_interval: str) -> str:
    """Event-time tumble, window dedup, window aggregate: silver.inventory_hourly rows.

    Every operator is append-only, so each closed window writes one row per
    (product, store) to the append Iceberg sink. A retry carries the original
    event_time, so its copies share a window; the first one is kept. A copy of
    an event_id with a different event_time is a conflict, not a retry: Spark
    flags it from Bronze, and its hour is provisional until then.
    """
    return f"""
    WITH deduped AS (
      SELECT *
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end, event_id
            ORDER BY event_ts ASC
          ) AS rn
        FROM TABLE(TUMBLE(TABLE {source}, DESCRIPTOR(event_ts), {window_interval}))
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
    FROM deduped
    GROUP BY window_start, window_end, product_id, store_id
    """


if __name__ == "__main__":
    run()
