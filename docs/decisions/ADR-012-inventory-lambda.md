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
2. **Deduplication is bounded.** The dedup state expires after 7 days.
3. **Recovery depends on Kafka.** Rebuilding means replaying the topic, which
   is limited by retention, and `inventory_silver_job` starts from
   `latest-offset`.

Gold is rebuilt daily by `warehouse_daily_batch_pipeline`, so Flink's
low latency does not reach Gold readers for hours that are already closed.

## Decision

Split each hour by who has complete data for it:

- `int_inventory_batch_coverage` defines the hours Spark fully covers: from
  the hour of the first record Bronze captured, up to (not including) the hour
  that was still open when the snapshot was taken. An hour counts as closed
  once Kafka has received records past its end plus the Flink watermark
  delay.
- `int_inventory_hourly` sums Spark's canonical events for covered hours
  (`is_estimated = false`) and takes Flink's `inventory_hourly` for every
  other hour (`is_estimated = true`). The ranges do not overlap.
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
- **Spark gaps become Gold gaps.** If Bronze capture stops mid-range, Spark's
  numbers for that range are short and Gold uses them. The reconciliation
  check flags these hours as Flink-only quantity.

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
