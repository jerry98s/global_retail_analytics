# ADR-012: Inventory Flow — Spark Owns Covered Hours (Lambda)

**Status:** Accepted
**Date:** 2026-10-21
**Author:** Data platform team
**Supersedes:** the Gold-input decision in [ADR-007](ADR-007-inventory-kappa.md)

---

## Context

ADR-007 made Flink's `silver.inventory_hourly` the only input of
`finance.fact_inventory_snapshot`. Since then the Bronze ledger keeps every
inventory Kafka record, and Spark rebuilds deduplicated
`silver.inventory_events` from it on every identity run. That gave the flow a
second, complete view of the same movements, used only by the reconciliation
check.

The Flink number has limits the Spark number does not:

1. **Late scans are lost.** A record that reaches Kafka after its window plus
   the 60 s watermark is dropped from `inventory_hourly` for good.
2. **Deduplication is per window.** Flink drops copies of an `event_id`
   within one event-time window (a retry carries the original `event_time`).
   A copy with a different `event_time` is a conflict that only Spark, with
   all of Bronze, can see.
3. **Recovery depends on Kafka.** Rebuilding means replaying the topic, which
   is limited by retention. A restart restores the newest retained
   checkpoint, or else resumes from the committed consumer-group offsets.

Gold is rebuilt daily by `warehouse_daily_batch_pipeline`, so Flink's
low latency does not reach Gold readers for hours that are already closed.

## Decision

Gold inventory is Spark-canonical through a Kafka-proven cutoff. Flink stays
the speed layer in serving. The warehouse DAG runs Spark `--mode inventory`
before dbt. The marketing DAG and `deploy_platform.ps1 -Action spark` run
`--mode hourly` (clickstream, the Flink export, and identity). Airflow rejects
`--mode all`. Both Spark steps use the `emr_spark` pool (size 1):
`airflow pools set emr_spark 1 "one Spark step at a time"`.

- The target cutoff is the latest whole hour whose end plus the 60-second
  watermark plus 3600 seconds of lateness is already in the past.
  `closed_hour_exclusive` is the first hour Gold does not include.
- `kafka_frontier.py` asks Kafka, per partition, for the earliest retained
  offset and the offset at the cutoff. Brokers are the Airflow Variable
  `msk_bootstrap_brokers`. The topic is `inventory.events.v1` with 12
  partitions. `silver.inventory_partition_coverage` has one row per expected
  partition. `silver.inventory_cutoff` is the summary row. Incomplete coverage
  holds the cutoff at the last proven hour and warns. It does not block the
  POS publish. Spectrum DDL for both exports is in
  `transformation/redshift/spectrum/silver_external_tables.sql`.
- An event that reaches Kafka after the cutoff is applied once by the next
  daily run, because the fact recomputes full history.
- `int_inventory_hourly` sums canonical events for hours strictly before the
  cutoff. Flink is not an input. `is_estimated` stays on the fact and is
  always false, because dropping a live column aborts WAP.
- `control.inventory_publication` is one row, in schema `control` (Redshift
  DDL `12_inventory_publication.sql`), published in the same transaction as
  `fact_inventory_snapshot`. The first publish after rollout rewrites
  Flink-only hours already in live as zero movement. Their balances change
  once.
- `serving.inventory_current_position` is the published balance plus Flink
  hours at or after the live cutoff. It never reads pending coverage.
- Flink deduplicates with a window Top-1 (`ROW_NUMBER() ... ORDER BY event_ts
  ASC` per window and `event_id`, keep-first) feeding the window aggregate, so
  the job is append-only end to end and each window is written once. Spark
  also keeps the earliest copy of an `event_id`, and labels a later copy with
  a different payload `payload_conflict` rather than a retry.
- `serving.inventory_current_position` is the speed-layer read: the latest
  published balance per product and store plus Flink hours after that
  pair's own newest fact hour. Hours Spark covers are never added from Flink,
  since Spark is canonical there. It is a view built after the warehouse publish, so it follows
  every `inventory_hourly` export without waiting for the daily rebuild.
- `fact_inventory_snapshot` reads `int_inventory_hourly`. Each run recomputes
  every running balance and upserts only rows that are new or whose balance
  or `is_estimated` changed. A rewritten hour keeps its stored `product_key`.
  A stored hour that lost all its movement is re-emitted with delta 0 and the
  carried balance.
- `inventory_daily_product_store` rolls up every day and upserts only days
  that changed.
- The reconciliation test compares covered hours only. A mismatch there
  measures how far the provisional Flink number was off; Gold already uses
  Spark's number for that hour.

## Consequences

- **Late scans and old duplicates are corrected in Gold** on the next daily
  run, once. Gold has no provisional hours. Provisional stock is only in
  `serving.inventory_current_position`.
- **Past hours change.** A Spark correction moves that hour and every later
  balance for the same product and store. The fact and its daily summary are
  built for this, so no `--full-refresh` is needed after a correction.
- **Each run scans the full fact and summary** to find changed rows. This is
  the price of correcting history; revisit if the fact grows past what a
  daily full comparison can handle.
- **Incremental and full-refresh results can differ by zero-movement rows.**
  A full refresh omits hours with no movement; an incremental run keeps a
  previously loaded hour with its carried balance. Balances agree.
- **Two hourly definitions must stay aligned.** `int_inventory_hourly` and
  Flink's SQL must bucket by the same event-time hour. The reconciliation
  check catches drift.
- **Incomplete coverage holds the cutoff.** POS finance still publishes. Open
  hours stay on Flink in serving only. The first publish after rollout
  restates Flink-only hours already in live: they are re-emitted with zero
  movement, and the balance changes once.
- **Current stock is about an hour old.** The serving view is as fresh as the
  Flink window (1 hour on cloud) plus the hourly `inventory_hourly` export.
- **One stockout measure.** Stockout duration lives only in
  `summary.inventory_position_daily`, which counts every hour the carried
  balance is at or below zero. `inventory_daily_product_store` no longer has
  `stockout_hour_count`; an existing live table needs
  `ALTER TABLE summary.inventory_daily_product_store DROP COLUMN stockout_hour_count`
  before the column stops appearing (WAP publish requires matching columns,
  and the incremental keeps it as NULL until then).

## Alternatives considered

### Keep ADR-007 (Flink only)

Rejected: late scans stay lost and the only repair path is a Kafka replay
that the job cannot start without a code change.

### Spark only

Rejected for now: the newest hours would be missing from Gold until Spark
next runs, and Flink would only feed the reconciliation check.

### Full rebuild of the fact on every run

Rejected: correct, but it restates every stored row to the current
`product_key` and makes the daily summary drift from the fact.

## Related

- [ADR-002 — Batch vs. stream per pipeline](ADR-002-batch-vs-stream.md)
- [ADR-007 — Inventory kappa conversion](ADR-007-inventory-kappa.md)
- [ADR-009 — Write-Audit-Publish](ADR-009-write-audit-publish.md)
- `spark/identity_resolution/kafka_frontier.py`
- `transformation/dbt_project/models/intermediate/int_inventory_cutoff.sql`
- `transformation/redshift/ddl/12_inventory_publication.sql`
- `transformation/dbt_project/models/intermediate/int_inventory_hourly.sql`
- `transformation/dbt_project/models/marts/finance/fact_inventory_snapshot.sql`
- `transformation/dbt_project/models/marts/serving/inventory_current_position.sql`
- `scripts/local/demo_inventory_late_event.py` on `local-testing-version` (late-event recovery demo)
