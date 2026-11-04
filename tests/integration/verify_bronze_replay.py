"""Run with spark-submit; no Kafka, AWS, or GraphFrames needed."""
import json
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from spark.identity_resolution.canonical_events import canonicalize, promote_events, write_exports
from spark.identity_resolution.consumer_exports import new_run_id, read_current


def main():
    spark = (SparkSession.builder.master('local[2]').appName('bronze-replay-test')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    schema = ('raw_payload binary, raw_key binary, source_system string, kafka_topic string, '
              'kafka_partition int, kafka_offset long, kafka_timestamp timestamp, '
              'ingested_at timestamp, ingestion_date date')
    now = datetime(2026, 9, 17)

    def record(offset, value):
        return (value, b'key', 'test-v1', 'clicks', 0, offset, now, now, now.date())

    def event(event_id, time='2026-09-17T00:00:00Z'):
        return json.dumps(dict(event_id=event_id, event_time=time, event_type='page_view',
            session_id='s', client_id='c', platform='web', app_version='1.0.0',
            schema_version='1.0.0', properties={})).encode()

    rows = [record(1, event('E1')), record(1, event('E1')), record(2, event('E1')),
            record(3, b'{broken'), record(4, b'\xff'), record(5, None),
            record(6, event('late', '2001-01-01T00:00:00Z'))]
    try:
        raw = spark.createDataFrame(rows, schema)
        canonical, invalid, duplicates = canonicalize(raw, 'clickstream')
        assert raw.count() == 7
        assert canonical.count() == 2
        assert invalid.count() == 3
        assert duplicates.count() == 2
        assert {r.error_reason for r in invalid.collect()} == {'invalid_json', 'invalid_utf8', 'tombstone'}
        assert canonical.filter("event_id = 'E1'").first().kafka_offset == 2
        assert {r.duplicate_kind for r in duplicates.collect()} == {'transport_replay', 'business_duplicate'}
        assert {r.canonical_transport.kafka_offset for r in duplicates.collect()} == {2}
        # Inventory uses the same transport/business separation, without a TTL.
        inv = json.dumps(dict(event_id='I1', event_time='2001-01-01T00:00:00Z',
            store_id='s', product_id='p', qty_delta=-3, event_type='sale_deduction',
            scanner_id='d', schema_version='1.0.0')).encode()
        inv_raw = spark.createDataFrame([record(10, inv), record(11, inv), record(12, b'{}')], schema)
        inv_events, inv_bad, inv_dup = canonicalize(inv_raw, 'inventory')
        assert (inv_events.count(), inv_bad.count(), inv_dup.count()) == (1, 1, 1)
        assert inv_events.first().qty_delta == -3
        # Repartitioning and replaying the same snapshot cannot change winners.
        again, _, _ = canonicalize(raw.repartition(3), 'clickstream')
        assert {(r.event_id, r.kafka_offset) for r in again.collect()} == {('E1', 2), ('late', 6)}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve().as_uri()
            write_exports(canonical, invalid, duplicates, root, 'clickstream', new_run_id())
            # A changed business policy/input must replace previous output.
            second, bad2, dup2 = canonicalize(raw.filter('kafka_offset = 6'), 'clickstream')
            write_exports(second, bad2, dup2, root, 'clickstream', new_run_id())
            assert read_current(spark, root, 'clickstream_events').count() == 1
            assert read_current(spark, root, 'clickstream_quarantine').count() == 0
            assert read_current(spark, root, 'clickstream_duplicates').count() == 0
        # Original byte payloads, including undecodable values, survived unchanged.
        assert [(r.kafka_offset, bytes(r.raw_payload) if r.raw_payload is not None else None)
                for r in raw.orderBy('kafka_offset').collect()] == [(r[5], r[0]) for r in rows]
        print('PASS: Bronze replay, reconciliation, deterministic dedup and replacement exports')
        if '--iceberg' in sys.argv:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve().as_uri()
                spark.conf.set('spark.sql.catalog.replay', 'org.apache.iceberg.spark.SparkCatalog')
                spark.conf.set('spark.sql.catalog.replay.type', 'hadoop')
                spark.conf.set('spark.sql.catalog.replay.warehouse', root + '/warehouse')
                spark.sql('CREATE NAMESPACE replay.bronze')
                raw.writeTo('replay.bronze.clickstream_records').using('iceberg').create()
                inv_raw.writeTo('replay.bronze.inventory_records').using('iceberg').create()
                before = spark.sql('SELECT snapshot_id FROM replay.bronze.clickstream_records.snapshots').collect()
                for _ in range(2):
                    outputs = promote_events(spark, 'replay', 'replay', root + '/consumer_current')
                    assert outputs['clickstream'].count() == 2
                    assert outputs['inventory'].count() == 1
                    assert read_current(spark, root + '/consumer_current', 'clickstream_events').count() == 2
                assert spark.table('replay.bronze.clickstream_records').count() == 7
                assert spark.sql('SELECT snapshot_id FROM replay.bronze.clickstream_records.snapshots').collect() == before
                print('PASS: pinned Iceberg snapshots, repeat promotion, no Bronze mutations')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
