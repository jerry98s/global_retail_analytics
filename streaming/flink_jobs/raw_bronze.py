"""Lossless Kafka -> Iceberg ledger. No payload parsing or business state."""
from __future__ import annotations

try:
    from ._config import kafka_security_options
except ImportError:
    from _config import kafka_security_options


def literal(value: object) -> str:
    return str(value).replace("'", "''")


def capture_sql(cfg: dict, stream: str, catalog: str) -> tuple[str, str, str]:
    if stream not in {'clickstream', 'inventory'}:
        raise ValueError(f'Unknown stream: {stream}')
    group_key = 'consumer_group_clickstream' if stream == 'clickstream' else 'consumer_group_inventory_bronze'
    source = f'default_catalog.default_database.{stream}_records_kafka'
    target = f'{catalog}.bronze.{stream}_records'
    source_system = literal(cfg.get('bronze_source_system', 'retail-v1'))
    source_sql = f"""
        CREATE TABLE {source} (
          raw_key BYTES,
          raw_payload BYTES,
          kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
          kafka_partition INT METADATA FROM 'partition' VIRTUAL,
          kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
          kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{literal(cfg[stream + '_topic'])}',
          'properties.bootstrap.servers' = '{literal(cfg['kafka_bootstrap_servers'])}',
          'properties.group.id' = '{literal(cfg[group_key])}-raw-v1',
          'scan.startup.mode' = 'group-offsets',
          'properties.enable.auto.commit' = 'false',
          'properties.isolation.level' = 'read_committed',
          'properties.auto.offset.reset' = 'earliest',
          'properties.partition.discovery.interval.ms' = '300000',
          'key.format' = 'raw',
          'key.fields' = 'raw_key',
          'value.format' = 'raw',
          'value.fields-include' = 'EXCEPT_KEY'
          {kafka_security_options(cfg)}
        )
    """
    sink_sql = f"""
        CREATE TABLE IF NOT EXISTS {target} (
          raw_key BYTES,
          raw_payload BYTES,
          source_system STRING,
          kafka_topic STRING,
          kafka_partition INT,
          kafka_offset BIGINT,
          kafka_timestamp TIMESTAMP(3),
          ingested_at TIMESTAMP(3),
          ingestion_date DATE
        ) PARTITIONED BY (ingestion_date)
    """
    insert_sql = f"""
        INSERT INTO {target}
        SELECT raw_key, raw_payload, '{source_system}', kafka_topic,
               kafka_partition, kafka_offset, CAST(kafka_timestamp AS TIMESTAMP(3)),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), CURRENT_DATE
        FROM {source}
    """
    return source_sql, sink_sql, insert_sql


def run_capture(stream: str) -> None:
    # Imports are deferred so SQL contract checks need no Flink installation.
    from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
    from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    try:
        from ._config import apply_state_config, load_simple_yaml, resolve_config_path, resolve_parallelism
    except ImportError:
        from _config import apply_state_config, load_simple_yaml, resolve_config_path, resolve_parallelism

    cfg = load_simple_yaml(resolve_config_path('flink_conf.yaml'))
    checkpoints = load_simple_yaml(resolve_config_path('checkpoints.yaml'))
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(resolve_parallelism(cfg, stream + '_parallelism'))
    env.enable_checkpointing(int(checkpoints['checkpoint_interval_ms']))
    cp = env.get_checkpoint_config()
    cp.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    cp.set_min_pause_between_checkpoints(int(checkpoints['min_pause_between_checkpoints_ms']))
    cp.set_checkpoint_timeout(int(checkpoints['checkpoint_timeout_ms']))
    cp.set_max_concurrent_checkpoints(int(checkpoints['max_concurrent_checkpoints']))
    cp.set_tolerable_checkpoint_failure_number(int(checkpoints['tolerable_checkpoint_failures']))
    cp.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    table_env = StreamTableEnvironment.create(env, EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set_local_timezone('UTC')
    apply_state_config(env, table_env, load_simple_yaml(resolve_config_path('state.yaml')))
    catalog = str(cfg['iceberg_catalog_name'])
    table_env.execute_sql(f"""CREATE CATALOG {catalog} WITH (
        'type' = 'iceberg', 'catalog-type' = '{literal(cfg['iceberg_catalog_type'])}',
        'warehouse' = '{literal(cfg['iceberg_warehouse_bronze'])}')""")
    table_env.execute_sql(f'CREATE DATABASE IF NOT EXISTS {catalog}.bronze')
    source, sink, insert = capture_sql(cfg, stream, catalog)
    table_env.execute_sql(source)
    table_env.execute_sql(sink)
    result = table_env.execute_sql(insert)
    if result.get_job_client() is not None:
        print(f'Started job: {result.get_job_client().get_job_id()}')
