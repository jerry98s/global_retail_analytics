"""
Clickstream Bronze Flink job.
Pipeline: Kafka -> schema validate -> business validate -> dedup -> Iceberg Bronze;
invalid schema rows -> schema DLQ; valid schema but bad checkout -> business DLQ.
"""

from __future__ import annotations

from typing import Any, Dict

from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from _config import (  # noqa: E402  -- packaged flat via Flink `-pyfs`
    apply_state_config,
    kafka_security_options,
    load_simple_yaml,
    resolve_config_path,
    resolve_parallelism,
)
from event_types import (  # noqa: E402
    CLICKSTREAM_EVENT_TYPES,
    PLATFORMS,
)
from lake_names import (  # noqa: E402
    BRONZE_NAMESPACE,
    CLICKSTREAM_EVENTS,
)


VALID_EVENT_TYPES = CLICKSTREAM_EVENT_TYPES


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
    env.set_parallelism(resolve_parallelism(flink_cfg, "clickstream_parallelism"))
    _apply_checkpoint_config(env, checkpoint_cfg)
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env, environment_settings=env_settings
    )
    # F-STATE: rocksdb state backend + incremental checkpoints + 7d TTL
    # + 1min source idle timeout. See streaming/config/state.yaml.
    apply_state_config(env, t_env, state_cfg)

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
    # Iceberg catalog only manages Iceberg tables (no watermarks, no Kafka
    # connector). We register the managed sink there, and create the Kafka
    # source in the in-memory `default_catalog`.
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{BRONZE_NAMESPACE}")
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{BRONZE_NAMESPACE}.{CLICKSTREAM_EVENTS} (
          event_id STRING,
          event_type STRING,
          event_time TIMESTAMP(3),
          session_id STRING,
          client_id STRING,
          customer_id STRING,
          platform STRING,
          app_version STRING,
          properties STRING,
          schema_version STRING,
          event_date DATE
        )
        -- Daily partitioning via derived event_date (CAST(event_time AS DATE)).
        -- Flink SQL / Iceberg Hadoop catalog does not accept the days()
        -- transform in PARTITIONED BY here, so we materialize event_date as
        -- an identity partition column instead. Same pruning effect for
        -- Spectrum WHERE event_date = ... . See
        -- docs/runbooks/iceberg-maintenance.md (DL-A).
        PARTITIONED BY (event_date)
        """
    )

    security_opts = kafka_security_options(flink_cfg)
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.clickstream_events (
          event_id STRING,
          event_type STRING,
          event_time STRING,
          event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(event_time, 1, 23), 'T', ' ')),
          session_id STRING,
          client_id STRING,
          customer_id STRING,
          platform STRING,
          app_version STRING,
          properties STRING,
          schema_version STRING,
          WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{flink_cfg["clickstream_topic"]}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_clickstream"]}',
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
          -- Known limitation: malformed JSON is dropped by the Kafka JSON
          -- format decoder before Flink SQL sees the row, so it cannot be
          -- routed to DLQ from this StatementSet. Fixing that requires a
          -- DataStream raw-bytes source. See docs/runbooks/dlq-investigation.md.
          'json.ignore-parse-errors' = 'true'
          {security_opts}
        )
        """
    )

    event_type_list = ",".join([f"'{item}'" for item in VALID_EVENT_TYPES])
    platform_list = ",".join([f"'{p}'" for p in PLATFORMS])
    valid_predicate = f"""
        event_id IS NOT NULL
        AND event_type IN ({event_type_list})
        AND event_ts IS NOT NULL
        AND session_id IS NOT NULL
        AND client_id IS NOT NULL
        AND platform IN ({platform_list})
        AND REGEXP_EXTRACT(app_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NOT NULL
        AND REGEXP_EXTRACT(schema_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NOT NULL
    """
    # Business rules for checkout (ingestion/schemas/checkout_properties.json):
    # non-checkout events always pass; checkout requires order_id pattern +
    # non-negative cart_value. Schema-valid but business-invalid rows go to
    # dlq.clickstream.business_violations (not schema DLQ).
    business_predicate = """
        event_type <> 'checkout'
        OR (
          JSON_VALUE(properties, '$.order_id') IS NOT NULL
          AND REGEXP_EXTRACT(
                JSON_VALUE(properties, '$.order_id'),
                '^ORDER-[A-Z0-9]{12}$',
                0
              ) IS NOT NULL
          AND CAST(JSON_VALUE(properties, '$.cart_value') AS DOUBLE) IS NOT NULL
          AND CAST(JSON_VALUE(properties, '$.cart_value') AS DOUBLE) >= 0
        )
    """

    schema_dlq_topic = flink_cfg.get(
        "clickstream_dlq_topic", "dlq.clickstream.schema_violations"
    )
    business_dlq_topic = flink_cfg.get(
        "clickstream_business_dlq_topic", "dlq.clickstream.business_violations"
    )
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.clickstream_schema_dlq (
          event_id STRING,
          event_type STRING,
          event_time STRING,
          session_id STRING,
          client_id STRING,
          customer_id STRING,
          platform STRING,
          app_version STRING,
          properties STRING,
          schema_version STRING,
          error_reason STRING,
          rejected_at TIMESTAMP(3)
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{schema_dlq_topic}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_clickstream"]}-dlq',
          'format' = 'json'
          {security_opts}
        )
        """
    )
    t_env.execute_sql(
        f"""
        CREATE TABLE default_catalog.default_database.clickstream_business_dlq (
          event_id STRING,
          event_type STRING,
          event_time STRING,
          session_id STRING,
          client_id STRING,
          customer_id STRING,
          platform STRING,
          app_version STRING,
          properties STRING,
          schema_version STRING,
          error_reason STRING,
          rejected_at TIMESTAMP(3)
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{business_dlq_topic}',
          'properties.bootstrap.servers' = '{flink_cfg["kafka_bootstrap_servers"]}',
          'properties.group.id' = '{flink_cfg["consumer_group_clickstream"]}-biz-dlq',
          'format' = 'json'
          {security_opts}
        )
        """
    )

    bronze_sql = f"""
    INSERT INTO {catalog}.{BRONZE_NAMESPACE}.{CLICKSTREAM_EVENTS}
    WITH validated AS (
      SELECT
        event_id,
        event_type,
        event_ts AS event_time,
        session_id,
        client_id,
        customer_id,
        platform,
        app_version,
        properties,
        schema_version,
        CAST(event_ts AS DATE) AS event_date
      FROM default_catalog.default_database.clickstream_events
      WHERE ({valid_predicate})
        AND ({business_predicate})
    ),
    deduped AS (
      SELECT
        event_id,
        event_type,
        event_time,
        session_id,
        client_id,
        customer_id,
        platform,
        app_version,
        properties,
        schema_version,
        event_date
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time DESC
          ) AS rn
        FROM validated
      )
      WHERE rn = 1
    )
    SELECT * FROM deduped
    """

    schema_dlq_sql = f"""
    INSERT INTO default_catalog.default_database.clickstream_schema_dlq
    SELECT
      event_id,
      event_type,
      event_time,
      session_id,
      client_id,
      customer_id,
      platform,
      app_version,
      properties,
      schema_version,
      CASE
        WHEN event_id IS NULL THEN 'missing_event_id'
        WHEN event_type NOT IN ({event_type_list}) THEN 'invalid_event_type'
        WHEN event_ts IS NULL THEN 'invalid_event_time'
        WHEN session_id IS NULL THEN 'missing_session_id'
        WHEN client_id IS NULL THEN 'missing_client_id'
        WHEN platform NOT IN ({platform_list}) THEN 'invalid_platform'
        WHEN REGEXP_EXTRACT(app_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NULL THEN 'invalid_app_version'
        WHEN REGEXP_EXTRACT(schema_version, '^[0-9]+\\.[0-9]+\\.[0-9]+$', 0) IS NULL THEN 'invalid_schema_version'
        ELSE 'schema_violation'
      END AS error_reason,
      CURRENT_TIMESTAMP AS rejected_at
    FROM default_catalog.default_database.clickstream_events
    WHERE NOT ({valid_predicate})
    """

    business_dlq_sql = f"""
    INSERT INTO default_catalog.default_database.clickstream_business_dlq
    SELECT
      event_id,
      event_type,
      event_time,
      session_id,
      client_id,
      customer_id,
      platform,
      app_version,
      properties,
      schema_version,
      CASE
        WHEN JSON_VALUE(properties, '$.order_id') IS NULL THEN 'checkout_missing_order_id'
        WHEN REGEXP_EXTRACT(
               JSON_VALUE(properties, '$.order_id'),
               '^ORDER-[A-Z0-9]{{12}}$',
               0
             ) IS NULL THEN 'checkout_invalid_order_id'
        WHEN CAST(JSON_VALUE(properties, '$.cart_value') AS DOUBLE) IS NULL
          THEN 'checkout_missing_cart_value'
        WHEN CAST(JSON_VALUE(properties, '$.cart_value') AS DOUBLE) < 0
          THEN 'checkout_negative_cart_value'
        ELSE 'business_violation'
      END AS error_reason,
      CURRENT_TIMESTAMP AS rejected_at
    FROM default_catalog.default_database.clickstream_events
    WHERE ({valid_predicate})
      AND NOT ({business_predicate})
    """

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(bronze_sql)
    stmt_set.add_insert_sql(schema_dlq_sql)
    stmt_set.add_insert_sql(business_dlq_sql)
    result = stmt_set.execute()
    job_client = result.get_job_client()
    if job_client is not None:
        print(f"Started job: {job_client.get_job_id()}")


if __name__ == "__main__":
    run()
