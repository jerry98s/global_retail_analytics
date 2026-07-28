# ADR-007: Inventory Flow — Kappa Conversion

**Status:** Accepted
**Date:** 2026-07-05
**Author:** Data platform team

---

## Context

Before this ADR, the inventory flow exhibited a **split-brain lambda** shape:

- `inventory_bronze_job` (Flink) wrote `bronze.inventory_events` (raw events).
- `inventory_silver_job` (Flink) wrote `silver.inventory_hourly` (hourly
  aggregated deltas, deduped).
- `fact_inventory_snapshot.sql` (dbt) **re-aggregated from bronze** — it
  re-deduped, re-windowed into hourly buckets, and re-derived the running
  balance, ignoring `silver.inventory_hourly` entirely.

Two independent paths consumed the same Kafka topic and produced
semantically-equivalent hourly aggregations through different code. This caused:

1. **Duplicated windowing logic** — Flink hourly tumbling window and the dbt
   `group by date, hour` had to stay in lockstep. A change to one required a
   matching change to the other.
2. **Semantic drift risk** — silver columns had to be renamed from
   `quantity_on_hand` / `quantity_available` (which were actually hourly deltas
   in silver, not running balances) to `qty_delta_hour` / `qty_received_hour`
   precisely because the gold mart didn't read silver and had independently
   invented the misleading names.
3. **Wasted Redshift compute** — every dbt run re-derived hourly buckets from
   raw events that Flink had already derived minutes earlier.
4. **Brittle incremental logic** — the mart's `where ... >= current_timestamp - 2h`
   filter would skip rows after a runner restart that took longer than 2 hours,
   and could not recover from a partial insert.

This is not classic lambda (a separate batch path for historical reprocessing);
the silver job runs continuously and writes hourly. It is closer to a kappa
pipeline where the second path simply ignored the first one's output.

## Decision

Convert the inventory flow to a clean **kappa** architecture:

- `silver.inventory_hourly` (Flink `inventory_silver_job` output) is the
  authoritative stream-output layer.
- `fact_inventory_snapshot.sql` reads from `source('silver', 'inventory_hourly')`
  and adds only:
  - the running-balance window (`sum(qty_delta_hour) over partition by
    product_id, store_id order by snapshot_date_key, snapshot_hour`), and
  - surrogate-key joins to `dim_product` and `dim_store`.
- Bronze `inventory_events` is kept as the audit/replay layer and is still
  loaded into Redshift Spectrum, but no Gold model reads from it.

The incremental lookback is now crash-resilient:

```sql
where (snapshot_date_key * 100 + snapshot_hour) > (
    select coalesce(max(snapshot_date_key * 100 + snapshot_hour), 0)
    from {{ this }}
)
```

A single integer sorts the same way `(date_key, hour)` does, so a partial
insert cannot cause the next run to skip hours between `max(date_key)` and
`max(hour)` on a previous date. `delete+insert` on the composite `unique_key`
makes the partial rows idempotent on retry.

## Consequences

- **One owner of hourly aggregation.** `inventory_silver_job` is the single
  place that decides dedup, watermark, and window semantics. The dbt mart
  inherits them by reading silver.
- **Silver is now in Redshift.** `silver.inventory_hourly` is registered as a
  Spectrum external table (`transformation/redshift/spectrum/silver_external_tables.sql`).
  This was already true for the column-rename fix; this ADR makes the
  **Gold reads silver** contract explicit.
- **Local DuckDB sim does not build this mart.** The local sim seeds only
  `bronze.clickstream_events` and `bronze.pos_transactions`, so
  `fact_inventory_snapshot` cannot be built under `--target local` without
  adding a silver seed. CI uses `dbt run --select +identity_graph` so the mart
  is never built in CI; Redshift builds it as part of the full dbt run.
- **Late silver events** (an hour's `qty_delta_hour` changing after the row was
  emitted) would require recomputing balances for all subsequent hours of that
  `(product_id, store_id)` partition. The current scan-all-silver-then-filter
  strategy handles this correctly: the window recomputes from full history, and
  `delete+insert` overwrites any hour whose balance changed. Cost is bounded by
  silver table size, which is hourly grain and small.
- **`stg_inventory_events` stays.** It is consumed by `int_product_catalog.sql`
  to detect inventory-only SKUs (products that appear in inventory streams but
  not in POS). It is not bypassed — only the inventory *mart* stopped reading
  bronze directly.

## Alternatives considered

### Keep lambda (re-aggregate from bronze in the mart)

Rejected because of the duplicated windowing logic and the semantic drift it
already caused (the `quantity_on_hand` / `qty_delta_hour` rename). The cost of
keeping two aggregations in sync exceeds the cost of one extra Spectrum read
of silver per dbt run.

### True incremental — store previous balance, only process new silver rows

Rejected for now. More efficient (no full-history scan) but cannot recover
from a late silver correction without a special-case replay path. The
scan-all-then-filter approach is simpler and the silver table is small enough
that the cost is negligible. Revisit if silver grows beyond ~10M rows/month.

### Add a silver seed for the local DuckDB sim

Deferred. The local sim is scoped to the identity graph chain. Adding a
silver inventory seed (plus dim_date / dim_product / dim_store seeds for the
surrogate-key joins) is doable but out of scope for this ADR. The mart is
documented as Redshift-only until local coverage is needed.

## Related

- [ADR-002 — Batch vs. stream per pipeline](ADR-002-batch-vs-stream.md):
  per-pipeline latency decisions.
- [ADR-006 — Flink vs. Spark](ADR-006-flink-vs-spark.md): Flink is the sole
  stream processor; silver is its output.
- `transformation/dbt_project/models/marts/finance/fact_inventory_snapshot.sql`
  — the rewritten mart.
- `transformation/redshift/spectrum/silver_external_tables.sql` — the Spectrum
  external table that exposes silver to Redshift.
