# Runbook: Backfill Verification (P2.2)

## Purpose

Before declaring any Gold mart "production-ready" (or before re-launching one
after a schema change, bug fix, or pipeline incident), backfill 7 days of
history and cross-verify the rebuilt rows against upstream source reports.
This catches:

- Silent row loss (incremental lookback too narrow, missed partitions).
- Surrogate-key drift (dim_product SCD2 reset, identity_graph re-keyed).
- Aggregation regressions (kappa conversion, fact_customer_session windowing).
- Currency / timezone / fiscal-year edge cases at week boundaries.

This runbook is the single source of truth for backfill procedure across all
10 audited Gold models. Per-model sections below specify the exact dbt
invocation, the verification query, and the tolerance.

## Prerequisites

- Cloud Redshift credentials in env: `RS_HOST`, `RS_USER`, `RS_PASSWORD`,
  `RS_DATABASE`. (Local DuckDB sim does not have the Silver/DIM sources
  required for finance marts — backfill is a cloud-only procedure.)
- Bronze (Spectrum) and Silver (Spectrum) external schemas are registered and
  refreshed (see `docs/REDSHIFT.md`).
- Flink jobs have been running (or replayed) for the backfill window so
  Bronze/Silver partitions exist for the full 7-day range.
- dbt project at `transformation/dbt_project/` with `profiles.yml` configured
  for `--target prod`.

## Standard backfill invocation

The dbt project does not yet define explicit `backfill_start` / `backfill_end`
vars (a future enhancement tracked separately). Until then, backfill is done
via `--full-refresh` against the model + its upstream:

```bash
cd transformation/dbt_project

# Pick the model from the per-model section below.
dbt run --select +<model_name> --target prod --full-refresh \
        --vars '{"run_date": "<YYYY-MM-DD>"}'
```

`--full-refresh` ignores the incremental lookback and recomputes the whole
table from upstream sources. This is safe for our incremental models because:

- `fact_sales`, `fact_inventory_snapshot`, `fact_customer_session`,
  `dim_customer`, `dim_product` all use `delete+insert` + `unique_key` —
  a full refresh produces a clean replacement, no duplicates.
- `int_identity_resolution`, `int_session_reconstruction`, `int_rfm_scoring`
  are incremental intermediates that are also rebuilt by `+<model>` selection.

For models that read from Silver (`fact_inventory_snapshot`), confirm the
Flink `inventory_silver_job` has covered the backfill window — see the
"Silver coverage check" query in the per-model section.

## Per-model verification

### fact_sales (finance)

**Backfill command:**

```bash
dbt run --select +fact_sales --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query — row count by day vs Bronze source:**

```sql
-- Gold mart rows by day
select date_key, count(*) as gold_rows
from finance.fact_sales
where date_key between 20260622 and 20260628
group by 1
order by 1;

-- Bronze source rows by day (Spectrum)
select
  cast(to_char(transaction_date, 'YYYYMMDD') as integer) as date_key,
  count(*) as bronze_rows
from bronze.pos_transactions
where transaction_date between '2026-06-22' and '2026-06-28'
  and not is_voided  -- fact_sales filters voided lines
group by 1
order by 1;
```

**Pass criteria:** Gold rows per day within 0.1% of Bronze rows per day
(small delta from late-arriving voids and SCD2 join fan-out is acceptable).
A 1%+ delta indicates lost rows or duplicate product_key joins.

### fact_inventory_snapshot (finance, kappa path)

**Silver coverage check (Flink inventory_silver_job output):**

```sql
select snapshot_date_key, count(*) as silver_rows
from silver.inventory_hourly
where snapshot_date_key between 20260622 and 20260628
group by 1
order by 1;
```

If any day is missing, replay the Flink inventory_silver_job for that
range before backfilling the mart. See
`docs/runbooks/late-event-remediation.md` for the replay procedure.

**Backfill command:**

```bash
dbt run --select +fact_inventory_snapshot --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query — running balance monotonicity:**

```sql
-- Per (product_key, store_key), quantity_on_hand should never jump
-- backward by more than one hourly delta (allows for late corrections).
select
  product_key,
  store_key,
  snapshot_date_key,
  snapshot_hour,
  quantity_on_hand,
  quantity_on_hand - lag(quantity_on_hand) over (
      partition by product_key, store_key
      order by snapshot_date_key, snapshot_hour
  ) as hourly_delta
from finance.fact_inventory_snapshot
where snapshot_date_key between 20260622 and 20260628
order by product_key, store_key, snapshot_date_key, snapshot_hour
limit 100;
```

**Pass criteria:** `hourly_delta` matches `silver.qty_delta_hour` (within
rounding for `numeric(38,0)` cast). No NULL `quantity_on_hand`. No
`quantity_on_hand < 0` unless caused by an over-ship in Bronze (then
`quantity_available = 0` per the `greatest(qoh, 0)` floor).

### fact_customer_session (marketing)

**Backfill command:**

```bash
dbt run --select +fact_customer_session --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query — session reconstruction sanity:**

```sql
-- Session count by day vs Bronze clickstream events
select
  session_date_key,
  count(distinct session_id) as sessions,
  count(*) as session_rows
from marketing.fact_customer_session
where session_date_key between 20260622 and 20260628
group by 1
order by 1;

-- Expected: sessions per day should be in the same order of magnitude as
-- distinct client_ids active in Bronze clickstream for that day.
select
  cast(to_char(event_time, 'YYYYMMDD') as integer) as date_key,
  count(distinct session_id) as bronze_sessions
from bronze.clickstream_events
where event_time between '2026-06-22' and '2026-06-28'
group by 1
order by 1;
```

**Pass criteria:** Gold sessions per day within 5% of Bronze distinct
session_ids (the 30-minute inactivity cutoff in `int_session_reconstruction`
may merge or split a small fraction of sessions).

### dim_customer (marketing, Type 1 + RFM/consent enrichment)

**Backfill command:**

```bash
dbt run --select +dim_customer --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query:**

```sql
-- Row count vs identity_graph active mappings
select
  (select count(*) from marketing.dim_customer) as dim_customer_rows,
  (select count(distinct customer_key)
   from marketing.identity_graph where is_active) as identity_graph_customers;

-- RFM segment distribution should be non-degenerate
select rfm_segment, count(*) from marketing.dim_customer group by 1 order by 2 desc;
-- Expected: 4 segments (champions/loyal/at_risk/cant_loose) with > 0 rows each.
```

**Pass criteria:** `dim_customer_rows == identity_graph_customers` (every
active identity graph customer has a dim_customer row). All 4 RFM segments
populated. No NULL `marketing_consent` (the consent gate is hard-required).

### dim_product (marketing, SCD2)

**Backfill command:**

```bash
dbt run --select int_product_catalog dim_product --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query — SCD2 integrity:**

```sql
-- One current record per natural key
select product_id, count(*) filter (where is_current) as current_count
from marketing.dim_product
group by 1
having count(*) filter (where is_current) <> 1;

-- No overlapping effective ranges
with overlaps as (
  select p1.product_id
  from marketing.dim_product p1
  join marketing.dim_product p2
    on p1.product_id = p2.product_id
   and p1.effective_from < p2.effective_to
   and p2.effective_from < p1.effective_to
   and p1.effective_from <> p2.effective_from
)
select distinct product_id from overlaps;
```

**Pass criteria:** Both queries return zero rows. Use the
`no_scd2_overlaps` and `one_current_per_natural_key` dbt tests as a
quicker equivalent (`dbt test --select dim_product`).

### identity_graph (marketing)

**Backfill command:**

```bash
dbt run --select +identity_graph --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

**Verification query:**

```sql
-- No identifier mapped to multiple customer_keys
select identifier_type, identifier_value, count(distinct customer_key) as keys
from marketing.identity_graph
where is_active
group by 1, 2
having count(distinct customer_key) > 1;

-- Public devices excluded
select count(*) from marketing.identity_graph
where is_active and is_public_device = true;
```

**Pass criteria:** Both queries return zero rows. Public devices are
filtered before `identity_graph` is built — see ADR-005 (graph-based
identity resolution).

### dim_date / dim_store (finance, seed-loaded)

These are loaded by `scripts/cloud/bootstrap_redshift.ps1`, not by dbt. To
backfill, re-run the bootstrap script. Verification:

```sql
-- dim_date should cover the backfill window
select min(date_key), max(date_key), count(*)
from finance.dim_date
where date_key between 20260101 and 20261231;
-- Expected: 365 rows for 2026, all dates present.

-- dim_store should be stable
select count(*) from finance.dim_store where is_active;
-- Expected: 20 (STORE-001..020).
```

### Intermediate models (int_identity_resolution, int_session_reconstruction, int_rfm_scoring)

Verified transitively by their downstream marts above. If a mart
verification fails, isolate the failing intermediate by running its
own dbt tests:

```bash
dbt test --select int_identity_resolution int_session_reconstruction int_rfm_scoring \
         --target prod
```

## Cross-verification against external reports

For each backfilled mart, compare Gold aggregates to the upstream
operational report (POS system / inventory management / web analytics)
for the same 7-day window. Tolerance: 1% on revenue, 0.5% on units,
exact match on transaction count.

| Mart | External report source | Comparison metric |
|------|------------------------|-------------------|
| `fact_sales` | POS daily sales report | `sum(net_revenue)`, `count(distinct transaction_id)` |
| `fact_inventory_snapshot` | Inventory management daily snapshot | `quantity_on_hand` at end-of-day per (product, store) |
| `fact_customer_session` | Web analytics daily sessions | `count(distinct session_id)` per day |
| `dim_customer` | CRM customer export | `count(*) where marketing_consent = true` |

## Common pitfalls

- **Silver partitions missing:** backfill of `fact_inventory_snapshot` will
  silently produce fewer rows if Silver Flink job hasn't covered the
  window. Always run the Silver coverage check first.
- **Bronze not refreshed:** Spectrum external tables read S3 at query
  time. If Iceberg metadata hasn't been refreshed, the backfill sees stale
  Bronze. Run `MSCK REPAIR TABLE bronze.pos_transactions;` (or the
  Iceberg equivalent) before backfilling.
- **Full-refresh on dim_product during business hours:** SCD2 full refresh
  can take 10+ minutes on large catalogs. Run during the maintenance
  window (00:00-01:00 UTC) to avoid blocking downstream reads.
- **Forgetting `--vars '{"run_date": "..."}'`:** some models depend on
  `run_date` for filtering. Omitting it falls back to `current_date`,
  which can skew incremental windows.

## Post-backfill

1. Run `dbt test --target prod` to execute all data tests on the backfilled
   models.
2. Trigger the GE `gold_layer_daily` checkpoint:
   `great_expectations checkpoint run gold_layer_daily`.
3. Confirm the row-count reconciliation task in `warehouse_daily_batch_pipeline`
   does not warn against the new baseline (a single-day +500% delta is
   expected after a 7-day backfill — update the baseline manually via
   the MWAA UI if needed, or wait for the next clean run to auto-seed).
4. Update the audit log entry for the model in
   `docs/runbooks/dw-checklist-audit.md` (item 9, "Backfill verify").

## References

- `docs/runbooks/dw-checklist-audit.md` — audit gap list (P2.2 = this runbook)
- `docs/runbooks/upstream-incident-response.md` — fallback when upstream
  is missing or spiking (P2.3)
- `docs/runbooks/late-event-remediation.md` — replay Flink for late arrivals
- `transformation/redshift/spectrum/` — Bronze + Silver external table DDL
- `scripts/cloud/bootstrap_redshift.ps1` — `dim_date` / `dim_store` loader
