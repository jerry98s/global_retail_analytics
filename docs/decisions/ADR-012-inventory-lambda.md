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

Split each hour by who has complete data for it:

- `int_inventory_batch_coverage` defines the hours Spark fully covers: from
  the hour of the first record Bronze captured, up to (not including) the hour
  that was still open when the snapshot was taken. An hour counts as closed
  once Kafka has received records past its end plus the Flink watermark
  delay.
- Spark records every break in Kafka offsets per partition of the pinned
  Bronze snapshot in `silver.inventory_offset_gaps` (producers are not
  transactional, so offsets are contiguous). `int_inventory_coverage_gaps`
  widens each gap by the maximum event lateness
  (`inventory_max_event_lateness_seconds`, default 3600) and those hours stay
  on Flink even inside the coverage range. A warn-severity test reports them.
- `int_inventory_hourly` sums Spark's canonical events for covered hours
  (`is_estimated = false`) and takes Flink's `inventory_hourly` for every
  other hour (`is_estimated = true`). The ranges do not overlap. It is a
  table carrying `export_run_id`, and a blocking test fails the audit when
  its inputs come from more than one Spark run.
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

- **Late scans and old duplicates are corrected in Gold** once Spark covers
  the hour. Readers can tell provisional hours by `is_estimated`.
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
- **Locally, hours rarely become covered.** A short laptop run sits inside one
  clock hour, so almost every hour stays on Flink. Covered hours appear when
  a run spans an hour boundary.
- **Bronze gaps fall back to Flink.** A break in Bronze offsets keeps the
  affected hours on Flink instead of publishing short Spark numbers. A gap
  that Bronze never refills stays provisional until someone replays or
  accepts it.
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
- `transformation/dbt_project/models/intermediate/int_inventory_batch_coverage.sql`
- `transformation/dbt_project/models/intermediate/int_inventory_hourly.sql`
- `transformation/dbt_project/models/marts/finance/fact_inventory_snapshot.sql`
- `transformation/dbt_project/models/marts/serving/inventory_current_position.sql`
- `scripts/local/demo_inventory_late_event.py` on `local-testing-version` (late-event recovery demo)
