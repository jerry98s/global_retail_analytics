"""Rebuild canonical Silver events from a committed Bronze snapshot.

Full rebuild deliberately trades scan cost for deterministic, cross-batch
deduplication. No watermark/TTL excludes historical or late records here.
"""
from __future__ import annotations

import json
import time

from pyspark.sql import DataFrame, Window, functions as F, types as T

try:
    from .consumer_exports import new_run_id, publish
    from .event_contract import parse_event
except ImportError:
    from consumer_exports import new_run_id, publish
    from event_contract import parse_event

TRANSPORT = ['source_system', 'kafka_topic', 'kafka_partition', 'kafka_offset']
CLICK_FIELDS = ['event_id', 'event_type', 'event_time', 'session_id', 'session_instance_id', 'client_id',
                'customer_id', 'platform', 'app_version', 'properties', 'schema_version', 'event_date']
INV_FIELDS = ['event_id', 'event_time', 'store_id', 'product_id', 'qty_delta',
              'event_type', 'scanner_id', 'is_late', 'schema_version', 'event_date']


def canonicalize(raw: DataFrame, stream: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return canonical, quarantine and duplicate audit frames; disjoint counts."""
    if stream not in {'clickstream', 'inventory'}:
        raise ValueError(f'Unknown stream: {stream}')
    fields = CLICK_FIELDS if stream == 'clickstream' else INV_FIELDS
    types = {'qty_delta': T.IntegerType(), 'is_late': T.BooleanType()}
    schema = T.StructType([T.StructField(name, types.get(name, T.StringType()))
                           for name in fields + ['error_reason']])
    decode = F.udf(lambda payload: json.dumps(parse_event(payload, stream)), T.StringType())
    parsed = raw.withColumn('_event', F.from_json(decode('raw_payload'), schema))
    parsed = parsed.select('*', '_event.*').drop('_event')
    parsed = (parsed.withColumn('event_time', F.to_timestamp('event_time'))
              .withColumn('event_date', F.to_date('event_date')))
    invalid = parsed.filter(F.col('error_reason').isNotNull())
    valid = parsed.filter(F.col('error_reason').isNull())
    # Transport copies are counted separately from different Kafka records
    # carrying the same business event. Keep ordering stable across replays.
    transport_order = Window.partitionBy(*TRANSPORT).orderBy(
        F.col('ingested_at').asc_nulls_last(), F.sha2('raw_payload', 256).asc())
    transport_ranked = valid.withColumn('_transport_rank', F.row_number().over(transport_order))
    transport_copies = transport_ranked.filter('_transport_rank > 1')
    unique_records = transport_ranked.filter('_transport_rank = 1').drop('_transport_rank')
    # The earliest copy of an event_id wins, as in Flink's window dedup. A
    # later copy with the same payload is a retry; one with a different
    # payload is a conflict, kept for audit and never counted.
    event_order = Window.partitionBy('event_id').orderBy(
        F.col('event_time').asc(), F.col('kafka_timestamp').asc_nulls_last(),
        *[F.col(name).asc() for name in TRANSPORT])
    payload_hash = F.sha2(F.to_json(F.struct(*fields)), 256)
    ranked = unique_records.withColumn('_event_rank', F.row_number().over(event_order))
    winner = F.struct(*[F.col(name).alias(name) for name in TRANSPORT])
    ranked = (ranked.withColumn('canonical_transport', F.first(winner).over(event_order))
              .withColumn('_payload_matches', payload_hash == F.first(payload_hash).over(event_order)))
    business_copies = ranked.filter('_event_rank > 1').drop('_event_rank')
    business_copies = business_copies.withColumn(
        'duplicate_kind',
        F.when(F.col('_payload_matches'), F.lit('business_duplicate')).otherwise(F.lit('payload_conflict')),
    ).drop('_payload_matches')
    ranked = ranked.drop('_payload_matches')
    winners = ranked.filter('_event_rank = 1').select('event_id', 'canonical_transport')
    transport_copies = (transport_copies.drop('_transport_rank')
                        .join(winners, 'event_id', 'inner')
                        .withColumn('duplicate_kind', F.lit('transport_replay')))
    duplicates = business_copies.unionByName(transport_copies)
    canonical = ranked.filter('_event_rank = 1').select(
        *fields, *TRANSPORT, 'kafka_timestamp', 'ingested_at')
    return canonical, invalid, duplicates


PARTITION = ['source_system', 'kafka_topic', 'kafka_partition']


def offset_gaps(raw: DataFrame) -> DataFrame:
    """Missing Kafka offsets per partition in a Bronze snapshot.

    Bronze keeps every delivered record (valid, invalid and duplicate), and the
    producers are not transactional, so offsets between a partition's first
    and last record are contiguous. A gap means Bronze lost records that Flink
    may still have counted; its Kafka-time range bounds the affected hours.
    """
    offsets = raw.select(*PARTITION, 'kafka_offset', 'kafka_timestamp').dropDuplicates(
        [*PARTITION, 'kafka_offset'])
    order = Window.partitionBy(*PARTITION).orderBy('kafka_offset')
    return (offsets
            .withColumn('_prev_offset', F.lag('kafka_offset').over(order))
            .withColumn('_prev_ts', F.lag('kafka_timestamp').over(order))
            .filter(F.col('kafka_offset') - F.col('_prev_offset') > 1)
            .select(*PARTITION,
                    F.col('_prev_offset').alias('last_offset_before'),
                    F.col('kafka_offset').alias('first_offset_after'),
                    (F.col('kafka_offset') - F.col('_prev_offset') - 1).alias('missing_offsets'),
                    F.col('_prev_ts').alias('gap_start_ts'),
                    F.col('kafka_timestamp').alias('gap_end_ts')))


def partition_coverage(raw: DataFrame) -> DataFrame:
    """First and last captured Kafka offset and time per partition in a Bronze snapshot.

    The last captured offset is the partition's high-water mark in this
    snapshot. dbt closes an hour only once every expected partition has
    records past it, so a lagging or never-captured partition keeps the hour
    on Flink. A first offset above 0 means earlier records never reached Bronze.
    """
    return (raw.groupBy(*PARTITION)
            .agg(F.min('kafka_offset').alias('first_offset'),
                 F.max('kafka_offset').alias('last_offset'),
                 F.min('kafka_timestamp').alias('first_kafka_ts'),
                 F.max('kafka_timestamp').alias('last_kafka_ts')))


def write_exports(canonical, invalid, duplicates, root: str, stream: str, run_id: str) -> None:
    for suffix, frame in [('events', canonical), ('quarantine', invalid), ('duplicates', duplicates)]:
        publish(frame, root, f'{stream}_{suffix}', run_id)


def log_stage(stage: str, started: float, **fields) -> None:
    """One line the local benchmark parses. Harmless in a normal run."""
    payload = {"stage": stage, "seconds": round(time.perf_counter() - started, 1), **fields}
    print("BENCHMARK_STAGE " + json.dumps(payload, sort_keys=True), flush=True)


def snapshot_bytes(spark, table: str) -> int:
    """Bytes in the table's current snapshot, which is the one a full rebuild scans."""
    row = spark.sql(
        f"SELECT coalesce(cast(sum(file_size_in_bytes) AS bigint), 0) AS bytes FROM {table}.files"
    ).first()
    return int(row.bytes)


def promote_events(spark, bronze_catalog: str, silver_catalog: str, root: str,
                   run_id: str | None = None,
                   streams: tuple[str, ...] = ('clickstream', 'inventory'),
                   snapshots: dict | None = None) -> dict[str, DataFrame]:
    """Pin each source snapshot, persist outputs, then return canonical sources.

    ``streams`` limits the rebuild: hourly identity skips inventory, and the
    daily inventory run skips clickstream. Iceberg reads are snapshot-aware,
    and each plain-Parquet export switches atomically through its manifest
    (consumer_exports). Run one writer at a time. Inventory publishes
    ``inventory_offset_gaps``; partition coverage is written by the job from
    the Kafka frontier (kafka_frontier), not here. ``snapshots`` receives each
    stream's pinned Bronze snapshot id.
    """
    outputs = {}
    run_id = run_id or new_run_id()
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {silver_catalog}.silver')
    for stream in streams:
        started = time.perf_counter()
        source = f'{bronze_catalog}.bronze.{stream}_records'
        # Resolve a committed snapshot before starting the several output actions.
        # Fail closed for missing raw tables rather than using legacy filtered data.
        head = spark.sql(f'SELECT snapshot_id FROM {source}.history ORDER BY made_current_at DESC LIMIT 1').first()
        if head is None:
            raw = spark.table(source).limit(0)
        else:
            raw = spark.read.option('snapshot-id', str(head.snapshot_id)).table(source)
        if snapshots is not None:
            snapshots[stream] = None if head is None else int(head.snapshot_id)
        canonical, invalid, duplicates = canonicalize(raw, stream)
        frames = [frame.cache() for frame in (canonical, invalid, duplicates)]
        try:
            counts = [frame.count() for frame in frames]
            input_rows = raw.count()
            if input_rows != sum(counts):
                raise RuntimeError(f'{stream} reconciliation failed: {input_rows} != {counts}')
            for suffix, frame in zip(('events', 'quarantine', 'duplicates'), frames):
                frame.writeTo(f'{silver_catalog}.silver.{stream}_{suffix}').using('iceberg').createOrReplace()
            if stream == 'inventory':
                gaps = offset_gaps(raw)
                gaps.writeTo(f'{silver_catalog}.silver.inventory_offset_gaps').using('iceberg').createOrReplace()
                publish(gaps, root, 'inventory_offset_gaps', run_id)
            write_exports(*frames, root, stream, run_id)
            scanned = snapshot_bytes(spark, source) if head is not None else 0
            print(json.dumps(dict(stream=stream, bronze_snapshot_id=head.snapshot_id if head else None,
                                  input_rows=input_rows, input_bytes=scanned, canonical_rows=counts[0],
                                  invalid_rows=counts[1], duplicate_rows=counts[2],
                                  payload_conflict_rows=frames[2].filter(
                                      F.col('duplicate_kind') == 'payload_conflict').count()),
                             sort_keys=True))
            log_stage(stream + '_promote', started, input_rows=input_rows, input_bytes=scanned)
        finally:
            for frame in frames:
                frame.unpersist()
        outputs[stream] = spark.table(f'{silver_catalog}.silver.{stream}_events')
    return outputs
