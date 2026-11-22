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

Gold inventory is Spark-canonical through a cutoff. Flink stays the speed
layer in serving, and the hourly Customer 360 job. The daily warehouse DAG
runs inventory Spark before dbt. The marketing DAG runs hourly Spark
(clickstream, the Flink `inventory_hourly` export, and identity). Airflow
rejects `--mode all`; that mode is the local stack only. Both Spark steps
use the `emr_spark` pool (one slot): `airflow pools set emr_spark 1 "one Spark step at a time"`.
A manual local `all` run writes both exports and must not overlap either
scheduled mode. There is no lock for that case.

- The target cutoff is the latest whole hour `T` such that
  `T + watermark + inventory_max_event_lateness_seconds` is at or before the
  run start. Defaults are 60 seconds and 3600 seconds. The local Flink window
  uses a 5 second watermark; that setting is the speed layer only and does
  not age this cutoff. `closed_hour_exclusive` is the first hour Gold does
  not include (YYYYMMDDHH).
- `spark/identity_resolution/kafka_frontier.py` asks Kafka, per partition,
  for the earliest retained offset and the offset at the cutoff
  (`offsetsForTimes`, beginning and end offsets, via `spark-sql-kafka`).
  Cloud brokers are the Airflow Variable `msk_bootstrap_brokers`. Locally the
  job uses the compose listener `kafka:29092`. The topic and partition count
  match `TOPIC_INVENTORY` / `PARTITIONS_BY_TOPIC` (`inventory.events.v1`, 12).
- `silver.inventory_partition_coverage` has one row per expected partition,
  including a partition Bronze has never captured. The required range is
  `[max(Kafka earliest, previous required end), offset at the cutoff)`.
  `missing_offset_count` is that span minus the distinct Bronze offsets
  inside it. `COMPLETE` means the count is zero. `silver.inventory_cutoff`
  is the one summary row: when every partition is complete,
  `closed_hour_exclusive` equals the target. Otherwise it is the hour of the
  earliest gap's next Bronze timestamp, minus the lateness allowance, and
  never earlier than the previous published cutoff. A tail gap (Bronze
  behind Kafka, no later Bronze timestamp) or a partition Kafka does not
  have holds the previous cutoff. Status is `INCOMPLETE`. An empty Kafka
  range (`begin == end`) is complete, so a quiet hour can still advance.
- An event whose event time is before the cutoff but which reaches Kafka
  later is a correction for the next daily run. The fact recomputes balances
  from full canonical history, so the correction lands once. An event Flink
  drops after its watermark can be missing from hourly serving until that
  publish. That is accepted.
- `int_inventory_hourly` sums `silver.inventory_events` for hours strictly
  before `closed_hour_exclusive`. Flink is not an input. `is_estimated` stays
  on the fact because dropping a live column aborts WAP, and it is always
  false. `silver.inventory_offset_gaps` stays Spark evidence and does not
  feed Gold or serving.
- `control.inventory_publication` is one row (run id, cutoff, coverage
  status, incomplete partition count, `published_at`). It lives in `control`,
  not in a Kimball schema, and it is on the finance WAP list so it commits
  in the same transaction as `fact_inventory_snapshot`. Incomplete coverage
  holds the cutoff and warns (`inventory_coverage_incomplete`). It does not
  block the POS publish. The first run after this rollout rewrites Flink-only
  hours already in live as zero movement; their balances change once.
- Blocking tests read source run ids before any aggregation. Canonical
  events, coverage and the cutoff share one `inventory_run_id`, and coverage
  has exactly `inventory_kafka_partitions` rows. The Flink export is one
  `export_run_id`. The pending publication run id is that canonical run, and
  the new cutoff is never below the live cutoff.
- `serving.inventory_current_position` is the latest published fact balance
  plus Flink `silver.inventory_hourly` hours at or after the live published
  cutoff. It never reads pending Spark coverage. Before the first control
  row, every Flink hour is provisional on top of `inventory_opening_balance`.
- `fact_inventory_snapshot` still recomputes every running balance and
  upserts only rows that are new or whose balance or `is_estimated` changed.
  A rewritten hour keeps its stored `product_key`. A stored hour that lost
  its movement is re-emitted with delta 0. The hourly export and the daily
  canonical run use different run ids; serving reads one committed Flink
  export, so an hourly export during the daily build is not added twice.
- `int_inventory_reconciliation_hourly` still compares Flink with Spark for
  hours before the cutoff. A mismatch there is how far the provisional
  number was off. Gold already uses Spark for that hour.
- Flink still deduplicates with a window Top-1. Spark keeps the earliest
  copy of an `event_id` and labels a later copy with a different payload
  `payload_conflict`.

## Consequences

- **Late scans and old duplicates are corrected in Gold** on the next daily
  run, once. Gold has no provisional hours. `is_estimated` remains and is
  always false. Provisional stock is only in `serving.inventory_current_position`.
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
- **A lake from before this cutoff export fails the DuckDB bridge** until
  Spark runs again in `all` or `inventory` mode. The loader requires
  `silver.inventory_cutoff`.
- **Incomplete coverage holds the cutoff.** POS finance still publishes. The
  warn test fires. A lagging or never-captured partition does not publish a
  short Spark balance for the open hours; those hours stay on Flink in
  serving only.
- **The first publish after rollout restates Flink-only hours already in
  live.** They are re-emitted with zero movement, and the balance changes
  once to the canonical history.
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
- `transformation/dbt_project/models/intermediate/int_inventory_hourly.sql`
- `transformation/dbt_project/models/marts/control/inventory_publication.sql`
- `transformation/dbt_project/models/marts/finance/fact_inventory_snapshot.sql`
- `transformation/dbt_project/models/marts/serving/inventory_current_position.sql`
- `scripts/local/demo_inventory_late_event.py` (late-event recovery demo)
- `tests/integration/verify_inventory_lambda.py` (partition coverage and a
  late event through the dbt incremental build and WAP publish)
