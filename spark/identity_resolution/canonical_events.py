"""Rebuild canonical Silver events from a committed Bronze snapshot.

Full rebuild deliberately trades scan cost for deterministic, cross-batch
deduplication. No watermark/TTL excludes historical or late records here.
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame, Window, functions as F, types as T

try:
    from .consumer_exports import new_run_id, publish
    from .event_contract import parse_event
except ImportError:
    from consumer_exports import new_run_id, publish
    from event_contract import parse_event

TRANSPORT = ['source_system', 'kafka_topic', 'kafka_partition', 'kafka_offset']
CLICK_FIELDS = ['event_id', 'event_type', 'event_time', 'session_id', 'client_id',
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
    event_order = Window.partitionBy('event_id').orderBy(
        F.col('event_time').desc(), F.col('kafka_timestamp').desc_nulls_last(),
        *[F.col(name).desc() for name in TRANSPORT])
    ranked = unique_records.withColumn('_event_rank', F.row_number().over(event_order))
    winner = F.struct(*[F.col(name).alias(name) for name in TRANSPORT])
    ranked = ranked.withColumn('canonical_transport', F.first(winner).over(event_order))
    business_copies = ranked.filter('_event_rank > 1').drop('_event_rank')
    business_copies = business_copies.withColumn('duplicate_kind', F.lit('business_duplicate'))
    winners = ranked.filter('_event_rank = 1').select('event_id', 'canonical_transport')
    transport_copies = (transport_copies.drop('_transport_rank')
                        .join(winners, 'event_id', 'inner')
                        .withColumn('duplicate_kind', F.lit('transport_replay')))
    duplicates = business_copies.unionByName(transport_copies)
    canonical = ranked.filter('_event_rank = 1').select(
        *fields, *TRANSPORT, 'kafka_timestamp', 'ingested_at')
    return canonical, invalid, duplicates


def write_exports(canonical, invalid, duplicates, root: str, stream: str, run_id: str) -> None:
    for suffix, frame in [('events', canonical), ('quarantine', invalid), ('duplicates', duplicates)]:
        publish(frame, root, f'{stream}_{suffix}', run_id)


def promote_events(spark, bronze_catalog: str, silver_catalog: str, root: str) -> dict[str, DataFrame]:
    """Pin each source snapshot, persist outputs, then return canonical sources.

    Iceberg reads are snapshot-aware, and each plain-Parquet export switches
    atomically through its manifest (consumer_exports). Run one writer at a time.
    """
    outputs = {}
    run_id = new_run_id()
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {silver_catalog}.silver')
    for stream in ('clickstream', 'inventory'):
        source = f'{bronze_catalog}.bronze.{stream}_records'
        # Resolve a committed snapshot before starting the several output actions.
        # Fail closed for missing raw tables rather than using legacy filtered data.
        head = spark.sql(f'SELECT snapshot_id FROM {source}.history ORDER BY made_current_at DESC LIMIT 1').first()
        if head is None:
            raw = spark.table(source).limit(0)
        else:
            raw = spark.read.option('snapshot-id', str(head.snapshot_id)).table(source)
        canonical, invalid, duplicates = canonicalize(raw, stream)
        frames = [frame.cache() for frame in (canonical, invalid, duplicates)]
        try:
            counts = [frame.count() for frame in frames]
            input_rows = raw.count()
            if input_rows != sum(counts):
                raise RuntimeError(f'{stream} reconciliation failed: {input_rows} != {counts}')
            for suffix, frame in zip(('events', 'quarantine', 'duplicates'), frames):
                frame.writeTo(f'{silver_catalog}.silver.{stream}_{suffix}').using('iceberg').createOrReplace()
            write_exports(*frames, root, stream, run_id)
            print(json.dumps(dict(stream=stream, bronze_snapshot_id=head.snapshot_id if head else None,
                                  input_rows=input_rows, canonical_rows=counts[0],
                                  invalid_rows=counts[1], duplicate_rows=counts[2]), sort_keys=True))
        finally:
            for frame in frames:
                frame.unpersist()
        outputs[stream] = spark.table(f'{silver_catalog}.silver.{stream}_events')
    return outputs
