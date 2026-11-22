"""Per-partition Kafka completeness for one daily inventory canonicalization.

The daily Spark run asks Kafka where each partition stood at the cutoff, then
checks that the pinned Bronze snapshot holds every offset up to that point.
``assess_coverage`` is pure so the rule is tested without Kafka or Spark.
A partition Bronze has never captured still gets a row.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta

COMPLETE = "COMPLETE"
INCOMPLETE = "INCOMPLETE"


def hour_key(moment: datetime) -> int:
    """YYYYMMDDHH in UTC. Naive datetimes are already UTC."""
    if moment.tzinfo is not None:
        from datetime import timezone
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment.year * 1_000_000 + moment.month * 10_000 + moment.day * 100 + moment.hour


def target_hour_exclusive(run_started: datetime, watermark_seconds: int, lateness_seconds: int) -> datetime:
    """Exclusive end of the latest hour whose close is old enough to trust.

    An hour ending at ``T`` is eligible once ``T + watermark + lateness`` is at
    or before ``run_started``. The result is that ``T``, floored to the hour,
    naive UTC.
    """
    from datetime import timezone
    if run_started.tzinfo is not None:
        run_started = run_started.astimezone(timezone.utc).replace(tzinfo=None)
    limit = run_started - timedelta(seconds=watermark_seconds + lateness_seconds)
    return limit.replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class PartitionFrontier:
    """Kafka's view of one partition at the cutoff. None means Kafka has no partition."""
    partition: int
    earliest_retained: int | None
    offset_at_cutoff: int | None


@dataclass(frozen=True)
class PartitionStat:
    """Bronze offsets inside the required range. successor is the next present record after the first gap."""
    partition: int
    present_count: int
    captured_end_offset: int | None
    earliest_successor_ts: datetime | None


def _bounds(frontier: PartitionFrontier | None, previous_end: int | None) -> tuple[int, int] | None:
    if frontier is None or frontier.earliest_retained is None or frontier.offset_at_cutoff is None:
        return None
    start = frontier.earliest_retained
    if previous_end is not None:
        start = max(start, previous_end)
    return start, max(frontier.offset_at_cutoff, start)


def assess_coverage(
    *,
    expected_partitions: int,
    frontiers: dict[int, PartitionFrontier],
    stats: dict[int, PartitionStat],
    previous_end_offsets: dict[int, int],
    previous_closed_hour: int | None,
    target: datetime,
    lateness_seconds: int,
    run_id: str,
    bronze_snapshot_id: int | None,
    topic: str,
) -> tuple[list[dict], dict]:
    """One coverage row per expected partition, plus the one-row cutoff summary.

    Every partition must be complete for the cutoff to reach ``target``. A gap
    whose next Bronze record has a timestamp pulls the cutoff back to that
    time minus the lateness allowance. A gap with no later Bronze record
    (Bronze is behind Kafka's frontier) holds the previously published cutoff.
    The cutoff never moves backwards.
    """
    rows: list[dict] = []
    limits: list[datetime] = []
    hold_previous = False
    for partition in range(expected_partitions):
        bounds = _bounds(frontiers.get(partition), previous_end_offsets.get(partition))
        stat = stats.get(partition, PartitionStat(partition, 0, None, None))
        if bounds is None:
            missing: int | None = None
            start = end = captured = None
            status = INCOMPLETE
            hold_previous = True
        else:
            start, end = bounds
            missing = (end - start) - stat.present_count
            if missing < 0:
                missing = 0
            captured = stat.captured_end_offset
            status = COMPLETE if missing == 0 else INCOMPLETE
            if missing:
                if stat.earliest_successor_ts is None:
                    hold_previous = True
                else:
                    limits.append(stat.earliest_successor_ts)
        rows.append({
            "inventory_run_id": run_id,
            "bronze_snapshot_id": bronze_snapshot_id,
            "kafka_topic": topic,
            "kafka_partition": partition,
            "required_start_offset": start,
            "required_end_offset": end,
            "captured_end_offset": captured,
            "missing_offset_count": missing,
            "coverage_status": status,
        })

    target_key = hour_key(target)
    incomplete = sum(1 for row in rows if row["coverage_status"] == INCOMPLETE)
    if incomplete == 0:
        closed: int | None = target_key
        overall = COMPLETE
    else:
        overall = INCOMPLETE
        if hold_previous or not limits:
            closed = previous_closed_hour
        else:
            earliest = min(limits) - timedelta(seconds=lateness_seconds)
            closed = hour_key(earliest.replace(minute=0, second=0, microsecond=0))
            closed = min(closed, target_key)
            if previous_closed_hour is not None and closed < previous_closed_hour:
                closed = previous_closed_hour
    summary = {
        "inventory_run_id": run_id,
        "bronze_snapshot_id": bronze_snapshot_id,
        "target_hour_exclusive": target_key,
        "closed_hour_exclusive": closed,
        "coverage_status": overall,
        "incomplete_partitions": incomplete,
    }
    return rows, summary


def _millis(moment: datetime) -> int:
    from datetime import timezone
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return int(calendar.timegm(moment.timetuple()) * 1000)


def read_frontiers(spark, brokers: str, topic: str, cutoff: datetime, partitions: int) -> dict[int, PartitionFrontier]:
    """Kafka beginning offset and the offset at ``cutoff`` for each expected partition.

    Uses the Kafka client on the Spark classpath (``spark-sql-kafka``), so the
    same code runs in the local image and on EMR. An offset with no timestamp
    at or after the cutoff means the whole retained log is already before it.
    """
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", f"inventory-coverage-{_millis(cutoff)}")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    try:
        topic_partition = jvm.org.apache.kafka.common.TopicPartition
        assigned = jvm.java.util.ArrayList()
        handles = []
        for partition in range(partitions):
            handle = topic_partition(topic, partition)
            handles.append(handle)
            assigned.add(handle)
        consumer.assign(assigned)
        beginnings = consumer.beginningOffsets(assigned)
        ends = consumer.endOffsets(assigned)
        stamps = jvm.java.util.HashMap()
        millis = _millis(cutoff)
        for handle in handles:
            stamps.put(handle, jvm.java.lang.Long.valueOf(millis))
        found = consumer.offsetsForTimes(stamps)
        frontiers = {}
        for handle in handles:
            begin = beginnings.get(handle)
            end = ends.get(handle)
            at = found.get(handle)
            if at is not None:
                offset_at = int(at.offset())
            elif end is not None:
                offset_at = int(end)
            else:
                offset_at = None
            frontiers[int(handle.partition())] = PartitionFrontier(
                int(handle.partition()),
                None if begin is None else int(begin),
                offset_at,
            )
        return frontiers
    finally:
        consumer.close()


def bronze_partition_stats(raw, bounds: dict[int, tuple[int, int]]) -> dict[int, PartitionStat]:
    """Present offsets per partition inside ``[start, end)``, collected as one row each."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    if not bounds:
        return {}
    spark = raw.sparkSession
    bound_rows = [(partition, start, end) for partition, (start, end) in bounds.items()]
    bounds_df = spark.createDataFrame(bound_rows, "kafka_partition int, start_offset long, end_offset long")
    scoped = (
        raw.select("kafka_partition", "kafka_offset", "kafka_timestamp")
        .dropDuplicates(["kafka_partition", "kafka_offset"])
        .join(bounds_df, "kafka_partition", "inner")
        .where((F.col("kafka_offset") >= F.col("start_offset")) & (F.col("kafka_offset") < F.col("end_offset")))
    )
    ranked = scoped.withColumn(
        "rn", F.row_number().over(Window.partitionBy("kafka_partition").orderBy("kafka_offset"))
    ).withColumn("expected", F.col("start_offset") + F.col("rn") - 1)
    first_gap = (
        ranked.where(F.col("kafka_offset") != F.col("expected"))
        .groupBy("kafka_partition")
        .agg(F.min("rn").alias("rn"))
    )
    successors = (
        ranked.join(first_gap, ["kafka_partition", "rn"])
        .select("kafka_partition", F.col("kafka_timestamp").alias("successor_ts"))
    )
    totals = scoped.groupBy("kafka_partition").agg(
        F.count("*").alias("present_count"),
        F.max("kafka_offset").alias("captured_end"),
    )
    joined = (
        bounds_df.join(totals, "kafka_partition", "left")
        .join(successors, "kafka_partition", "left")
    )
    stats = {}
    for row in joined.collect():
        stats[int(row.kafka_partition)] = PartitionStat(
            int(row.kafka_partition),
            int(row.present_count or 0),
            None if row.captured_end is None else int(row.captured_end),
            row.successor_ts,
        )
    return stats


def write_inventory_coverage(
    spark,
    *,
    raw,
    snapshot_id: int | None,
    brokers: str,
    topic: str,
    expected_partitions: int,
    watermark_seconds: int,
    lateness_seconds: int,
    run_started: datetime,
    run_id: str,
    silver_catalog: str,
    root: str,
    previous_closed_hour: int | None,
    previous_end_offsets: dict[int, int],
) -> dict:
    """Write the coverage rows and the one-row cutoff, to Iceberg and the export root."""
    from pyspark.sql import types as T

    try:
        from .consumer_exports import publish
    except ImportError:
        from consumer_exports import publish

    target = target_hour_exclusive(run_started, watermark_seconds, lateness_seconds)
    frontiers = read_frontiers(spark, brokers, topic, target, expected_partitions)
    bounds = {}
    for partition, frontier in frontiers.items():
        bound = _bounds(frontier, previous_end_offsets.get(partition))
        if bound is not None:
            bounds[partition] = bound
    stats = bronze_partition_stats(raw, bounds)
    rows, summary = assess_coverage(
        expected_partitions=expected_partitions,
        frontiers=frontiers,
        stats=stats,
        previous_end_offsets=previous_end_offsets,
        previous_closed_hour=previous_closed_hour,
        target=target,
        lateness_seconds=lateness_seconds,
        run_id=run_id,
        bronze_snapshot_id=snapshot_id,
        topic=topic,
    )
    coverage_schema = T.StructType([
        T.StructField("inventory_run_id", T.StringType()),
        T.StructField("bronze_snapshot_id", T.LongType()),
        T.StructField("kafka_topic", T.StringType()),
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("required_start_offset", T.LongType()),
        T.StructField("required_end_offset", T.LongType()),
        T.StructField("captured_end_offset", T.LongType()),
        T.StructField("missing_offset_count", T.LongType()),
        T.StructField("coverage_status", T.StringType()),
    ])
    cutoff_schema = T.StructType([
        T.StructField("inventory_run_id", T.StringType()),
        T.StructField("bronze_snapshot_id", T.LongType()),
        T.StructField("target_hour_exclusive", T.IntegerType()),
        T.StructField("closed_hour_exclusive", T.IntegerType()),
        T.StructField("coverage_status", T.StringType()),
        T.StructField("incomplete_partitions", T.IntegerType()),
    ])
    coverage = spark.createDataFrame(rows, coverage_schema)
    cutoff = spark.createDataFrame([summary], cutoff_schema)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver_catalog}.silver")
    coverage.writeTo(f"{silver_catalog}.silver.inventory_partition_coverage").using("iceberg").createOrReplace()
    cutoff.writeTo(f"{silver_catalog}.silver.inventory_cutoff").using("iceberg").createOrReplace()
    publish(coverage, root, "inventory_partition_coverage", run_id)
    publish(cutoff, root, "inventory_cutoff", run_id)
    return summary
