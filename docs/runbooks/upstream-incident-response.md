# Runbook: Upstream Incident Response (P2.3)

Covers two related upstream-incident scenarios across all Gold marts:

1. **Upstream missing** — Bronze/Silver partitions for a date range are
   absent, zero, or partially loaded. The mart runs but produces no rows
   (or partial rows) for that range.
2. **Volume spike** — A 10x+ day-over-day jump in upstream volume that
   can break incremental lookbacks, exhaust Redshift memory, or fan out
   SCD2 joins beyond the WLM timeout.

The row-count reconciliation task in `warehouse_daily_batch_pipeline` (P2.5) emits
the trigger warning for both scenarios — see the
[detection](#detection-via-row-count-reconciliation-p25) section below.

## Triggers

- Airflow `row_count_reconciliation` task logs a WARNING: "Row-count
  delta for `<schema>.<table>`: X -> Y (`±N%` > 20% threshold)".
- Dashboard tiles show empty results for a date range that should have
  data.
- GE checkpoint `gold_layer_daily` fails with a `expect_row_count_to_be_between`
  violation.
- Manual alert from a downstream consumer (BI team, finance, marketing).

## Detection via row-count reconciliation (P2.5)

The `row_count_reconciliation` Airflow task writes a WARNING log line
per mart that exceeds the 20% day-over-day threshold:

```
WARNING - Row-count delta for finance.fact_sales: 100000 -> 30 (-99.97% > 20% threshold)
WARNING - Row-count delta for finance.fact_inventory_snapshot: 720 -> 8400 (+1066% > 20% threshold)
```

Negative deltas (drop) point to **upstream missing**; positive deltas
(spike) point to **volume spike**. The Airflow Variable
`gold_row_counts_baseline` is preserved across warning runs, so the
next run still compares against the pre-incident counts — this is by
design (see `orchestration/airflow/plugins/row_count_reconciliation.py`).

To inspect the last reconciliation result without re-running:

```sql
-- Recent task instance logs in Airflow UI: Admin -> Task Instances
-- Filter: dag_id=warehouse_daily_batch_pipeline, task_id=row_count_reconciliation
-- Look for the "WARN" lines in the log.
```

## Step 1: Classify the incident

| Signal | Likely cause | Section |
|--------|--------------|---------|
| All Gold marts dropped simultaneously | Bronze/Silver external table metadata stale, or Redshift unreachable | [Upstream missing — systemic](#upstream-missing--systemic) |
| Single mart dropped, others fine | Upstream partition for that source missing or filtered out | [Upstream missing — per-model](#upstream-missing--per-model) |
| Single mart spiked 10x+ | Backfill ran, producer replayed, or upstream data contract changed | [Volume spike — per-model](#volume-spike--per-model) |
| All marts spiked 2-3x | Seasonal event (Black Friday, marketing campaign) — likely NOT an incident | [Volume spike — seasonal](#volume-spike--seasonal) |

## Upstream missing — systemic

Affects all Gold marts. Usually means Bronze/Silver is unreachable or
metadata is stale.

### Investigation

```sql
-- Bronze reachability (Spectrum)
select count(*) from bronze.pos_transactions where transaction_date = current_date - 1;
select count(*) from bronze.clickstream_events
where event_time >= dateadd('day', -1, current_date);
select count(*) from bronze.inventory_events
where event_time >= dateadd('day', -1, current_date);

-- Silver reachability
select count(*) from silver.inventory_hourly
where snapshot_date_key = cast(to_char(current_date - 1, 'YYYYMMDD') as integer);
```

If all return 0:

1. Check EMR cluster is running: `aws emr describe-cluster --cluster-id <emr_cluster_id>`.
2. Check Flink jobs are running:
   `aws emr list-steps --cluster-id <emr_cluster_id> --step-states RUNNING PENDING`.
3. Check Kafka topics have producers:
   `aws kafka list-clusters` → broker SASL IAM string →
   `kafka-consumer-groups --bootstrap-server <brokers> --list`.
4. Refresh Iceberg metadata in Spectrum:
   ```sql
   -- Redshift query editor
   MSCK REPAIR TABLE bronze.pos_transactions;
   MSCK REPAIR TABLE bronze.clickstream_events;
   MSCK REPAIR TABLE bronze.inventory_events;
   MSCK REPAIR TABLE silver.inventory_hourly;
   ```

### Recovery

1. Resolve the upstream root cause (start producers, restart Flink jobs,
   restore S3 access).
2. Replay Flink bronze jobs for the missing window using
   `scan.startup.mode = 'earliest-offset'` and a constrained
   consumer-group reset. See `docs/runbooks/late-event-remediation.md`
   for the consumer-group reset procedure.
3. Wait for Bronze + Silver partitions to land (monitor via the
   reachability queries above).
4. Trigger `warehouse_daily_batch_pipeline` manually in the Airflow UI.
5. Confirm `row_count_reconciliation` no longer warns — the baseline
   will auto-update on the first clean run.

## Upstream missing — per-model

A single mart drops while others stay flat. The Bronze/Silver source
for that mart is missing partitions or filtered out by a join.

### Per-model diagnosis

#### fact_sales

```sql
-- Bronze POS rows by day
select cast(to_char(transaction_date, 'YYYYMMDD') as integer) as date_key, count(*)
from bronze.pos_transactions
where transaction_date >= dateadd('day', -7, current_date)
group by 1 order by 1;
```

If a day shows 0 rows, the POS Parquet batch for that day failed to
land. Re-run `generate_pos_parquet.py` for the missing date — see
`warehouse_daily_batch_pipeline` task `generate_pos_parquet`, or run manually:

```bash
python ingestion/batch/generate_pos_parquet.py --date 2026-06-28 \
       --output-s3 s3://<bronze-bucket>/iceberg/bronze/pos_transactions/
```

Then refresh Spectrum metadata and re-run the dbt mart:

```bash
dbt run --select +fact_sales --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

#### fact_inventory_snapshot (kappa)

Silver is the upstream — see the per-model section in
`docs/runbooks/backfill-verification.md` for the Silver coverage query.
If Silver is missing hours, replay the Flink `inventory_silver_job`
for the affected window.

#### fact_customer_session

Bronze clickstream is the upstream. If Bronze has rows but the mart
shows 0 sessions, the 30-minute inactivity cutoff in
`int_session_reconstruction` may be too aggressive for the missing day,
or the incremental lookback dropped the window. Backfill with
`--full-refresh`:

```bash
dbt run --select +fact_customer_session --target prod --full-refresh \
        --vars '{"run_date": "2026-06-28"}'
```

#### dim_customer / identity_graph

These rebuild from Bronze POS + clickstream. If they dropped, the
identity chain intermediate (`int_identity_edges`,
`int_identity_components`, `int_identity_resolution`) likely failed to
re-process historical edges after an `--full-refresh` race. Re-run
with explicit selection:

```bash
dbt run --select int_identity_edges int_identity_public_devices \
              int_identity_components int_identity_resolution \
              identity_graph dim_customer \
         --target prod --full-refresh \
         --vars '{"run_date": "2026-06-28"}'
```

#### dim_product (SCD2)

If `dim_product` drops rows, the SCD2 merge likely reset to the seed
catalog and missed a delta. Re-run `int_product_catalog` then
`dim_product`:

```bash
dbt run --select int_product_catalog dim_product --target prod --full-refresh
```

If `is_current=true` rows are missing per natural key, the SCD2 macro
has a bug — escalate to the data platform owner.

#### dim_date / dim_store

These are seed-loaded by `scripts/cloud/bootstrap_redshift.ps1`. If they're
empty or stale, re-run the bootstrap script (cloud only):

```powershell
.\scripts\cloud\bootstrap_redshift.ps1 -Env dev
```

## Volume spike — per-model

A 10x+ day-over-day jump. Common causes: a backfill ran (legitimate),
a producer replayed historical data (often legitimate but unexpected),
or an upstream data contract changed (often a bug).

### Investigation

```sql
-- Bronze volume by day for the spiked mart's source
select cast(to_char(event_time, 'YYYYMMDD') as integer) as date_key, count(*)
from bronze.clickstream_events  -- or pos_transactions / inventory_events
where event_time >= dateadd('day', -14, current_date)
group by 1 order by 1;
```

If a single day shows 10x the baseline, classify:

- **Backfill ran:** the day's volume is correct, the spike is expected.
  Verify the new row count matches the source report, then let
  `row_count_reconciliation` update the baseline on the next clean run.
- **Producer replayed:** the day's volume is double-counted (real data
  + replayed historical data). The unique_key on the mart should
  de-duplicate via `delete+insert`, but verify:
  ```sql
  select transaction_id, line_item_number, count(*)
  from finance.fact_sales
  where date_key = 20260628
  group by 1, 2
  having count(*) > 1;
  ```
  If duplicates exist, the `unique_key` is wrong or
  `incremental_strategy` is not `delete+insert`. Re-run with
  `--full-refresh`.
- **Contract changed:** the upstream producer started emitting a new
  field that fans out the join. Example: a new `product_id` variant
  joining `dim_product` on `product_id` produces multiple SCD2 rows
  per source row. Inspect the join fan-out:
  ```sql
  select b.transaction_id, b.line_item_number, count(p.product_key)
  from bronze.pos_transactions b
  left join marketing.dim_product p
    on b.product_id = p.product_id and p.is_current
  where b.transaction_date = '2026-06-28'
  group by 1, 2
  having count(p.product_key) > 1;
  ```

### Per-model recovery

| Mart | Spike cause | Recovery |
|------|-------------|----------|
| `fact_sales` | POS batch ran twice | Verify `unique_key=['transaction_id','line_item_number']` dedupes; `--full-refresh` if not |
| `fact_inventory_snapshot` | Silver Flink job replayed | Verify `unique_key=['snapshot_date_key','snapshot_hour','product_key','store_key']` dedupes; running balance recomputes correctly via the window |
| `fact_customer_session` | Clickstream producer replayed sessions | Verify `unique_key='session_id'` dedupes; `--full-refresh` if `int_session_reconstruction` produced split sessions |
| `dim_customer` / `identity_graph` | Identity graph re-keyed (new identifier merged existing clusters) | **Expected behavior** — the new identity graph is correct. Update baseline manually via MWAA UI to avoid persistent warnings. |
| `dim_product` | SCD2 history expanded (new `effective_from` rows for past dates) | **Expected behavior** — verify via `no_scd2_overlaps` test. |
| `dim_date` / `dim_store` | Should never spike (static seeds) | Investigate as a script bug if it does. |

## Volume spike — seasonal

A 2-3x day-over-day jump across multiple marts (e.g., Black Friday,
marketing campaign launch) is expected. To avoid persistent
reconciliation warnings during known events:

1. **Pre-event:** Manually set `row_count_delta_threshold` to a higher
   value (e.g., `0.50`) via the MWAA UI for the event duration.
2. **Post-event:** Reset to `0.20` and let the baseline auto-update on
   the first clean run after the event volume normalizes.

Alternatively, let the warnings fire — they will appear in the Airflow
UI but will not break the pipeline (the task does not fail the DAG on
warning, by design).

## Recovery — final step

After any of the per-model recovery procedures above:

1. Trigger `warehouse_daily_batch_pipeline` in the Airflow UI (or wait for the
   next scheduled run).
2. Verify `row_count_reconciliation` no longer warns for the affected
   mart. If it does, the recovery did not fully resolve the issue —
   re-investigate.
3. Manually update the `gold_row_counts_baseline` Airflow Variable if
   the new row count is correct but persistent warnings are noisy
   (e.g., after a planned backfill). Set it to the current counts
   (JSON map) and the next run will compare against the new baseline.
4. Open an incident summary if the incident affected downstream
   consumers (dashboard, BI reports, marketing audiences). Include:
   - Detection time and signal (reconciliation warning / user report).
   - Root cause classification (upstream missing / spike / seasonal).
   - Affected marts and date range.
   - Recovery actions taken.
   - Prevention action (e.g., add a GE `expect_row_count_to_be_between`
     with a tighter bound for the affected mart — see P2.8).

## References

- `docs/runbooks/backfill-verification.md` — planned backfill procedure
  (the legitimate version of a "spike")
- `docs/runbooks/late-event-remediation.md` — Flink replay for late
  arrivals (a common cause of "upstream missing")
- `docs/runbooks/dlq-investigation.md` — when the cause is bad upstream
  data, not missing data
- `orchestration/airflow/plugins/row_count_reconciliation.py` — the
  detection callable (P2.5)
- `docs/runbooks/dw-checklist-audit.md` — audit gap list (P2.3 = this
  runbook)
