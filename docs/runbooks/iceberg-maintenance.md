# Runbook: Iceberg maintenance + data-lake health

Closes the data-lake checklist items DL-A through DL-M applied 2026-07-05.
This runbook is the single source of truth for layout, maintenance,
monitoring, and query-tuning on the Iceberg + Redshift Spectrum stack.

## Scope

Three Iceberg tables + one Spectrum external Parquet table:

| Table | Writer | Partition spec | Snapshot retention |
|---|---|---|---|
| `bronze.inventory_events` | Flink `inventory_bronze_job` | `event_date` (identity) | 7 days |
| `bronze.clickstream_events` | Flink `clickstream_bronze_job` | `event_date` (identity) | 7 days |
| `silver.inventory_hourly` | Flink `inventory_silver_job` | `snapshot_date_key` (identity) | 30 days |
| `bronze.pos_transactions` | `generate_pos_parquet.py` (Parquet) | Hive `dt date` (Spectrum) | n/a — no Iceberg snapshots |

## Layout (DL-A)

- **Time-based, low-cardinality partitions only.** All Iceberg tables
  partition by date (either `event_date` identity column or
  `snapshot_date_key` identity). High-cardinality columns
  (`event_id`, `client_id`, `customer_id`, `session_id`, `scanner_id`)
  are **never** used as partition keys — they would cause small-file
  explosion per the data lake checklist item 1.2. A unit test
  (`tests/unit/test_iceberg_partitions.py::TestIcebergPartitionSpecs::test_no_high_cardinality_iceberg_partitions`)
  guards against future regressions.
- **Expected partition count:** ~365/year per table (daily granularity).
  Well within the 100–10,000 band recommended by the checklist.
- **Expected partition size:** 1–10 GB at production volume (10k
  clickstream events/sec peak). Local Docker volumes are far smaller —
  the partition spec is the same, the per-partition size is not.
- **Z-Ordering / clustering:** Not applied today. Flink Iceberg DDL
  does not expose sort-order directly; setting it requires an Iceberg
  Java API call or `ALTER TABLE`. Defer until a query pattern emerges
  that benefits (e.g. frequent `WHERE store_id = ... AND product_id = ...`
  filters on Silver).

## Maintenance (DL-D)

The `lakehouse_daily_iceberg_maintenance` Airflow DAG (`orchestration/airflow/dags/lakehouse_daily_iceberg_maintenance.py`)
runs daily at 03:00 UTC — off-peak, after `warehouse_daily_batch_pipeline` finishes
(~02:30 UTC) and before the next `marketing_hourly_customer_360_pipeline` run. It
submits a one-shot Flink batch job (`streaming/flink_jobs/iceberg_maintenance.py`)
to the existing EMR cluster that runs two Iceberg procedures per table:

1. **`rewrite_data_files`** (compaction) — merges small files into
   target-size files. Threshold: `min-input-files=5` (only compact
   partitions with ≥5 small files, to avoid no-op compaction cost).
   Target file size: 256 MB (within the 128–256 MB band recommended
   by the data lake checklist item 2.2).
2. **`expire_snapshots`** — physically deletes orphan data + metadata
   files for snapshots older than the retention window. Bronze: 7-day
   retention (high-volume, replay needs are short). Silver: 30-day
   retention (lower volume, longer replay value for the kappa path).
   At least 5 recent snapshots are always retained regardless of the
   cutoff, so a botched run can still roll back via Iceberg time-travel.

### Tuning via Airflow Variables / env

The maintenance job reads these env vars (set as Airflow Variables and
pass through the EMR step's env block, or override locally for testing):

| Env var | Default | Tune when |
|---|---|---|
| `ICEBERG_MIN_INPUT_FILES` | `5` | Small-file accumulation faster than daily compaction can keep up |
| `ICEBERG_TARGET_FILE_SIZE_BYTES` | `268435456` (256 MB) | Spectrum scan perf improves with larger files but compaction cost rises |
| `ICEBERG_SNAPSHOTS_RETAIN_LAST` | `5` | Need more time-travel rollback points |
| `BRONZE_SNAPSHOT_RETENTION_DAYS` | `7` | Compliance / replay window changes |
| `SILVER_SNAPSHOT_RETENTION_DAYS` | `30` | Compliance / replay window changes |

### Idempotency

`rewrite_data_files` is a no-op when there are no small files to compact.
`expire_snapshots` is a no-op when no snapshots are older than the cutoff.
Re-running the DAG for the same Iceberg state is safe.

### Manual trigger

```bash
# Cloud: trigger the DAG via Airflow UI, or via CLI on MWAA:
aws mwaa invoke-cli-command --name <env-mwaa-env> --cli-command "dags trigger lakehouse_daily_iceberg_maintenance"

# Or submit the Flink batch job directly to EMR (bypassing Airflow):
aws emr add-steps --cluster-id <emr_cluster_id> --steps file://step.json
# step.json builds the same `flink run` command the DAG emits — see
# orchestration/airflow/dags/lakehouse_daily_iceberg_maintenance.py.
```

### Monitoring thresholds (DL-M)

The ADR-001 consequence "alert if avg file size < 64MB" is implemented
via the data-skipping + file-size checks below. Tiered alert thresholds
per the checklist:

| Metric | Threshold | Action |
|---|---|---|
| Avg Iceberg data file size | < 64 MB | Increase `lakehouse_daily_iceberg_maintenance` frequency to twice-daily (06:00 + 18:00 UTC) |
| Iceberg data files per partition | > 10,000 | Revisit partition spec (consider monthly) or increase compaction frequency |
| Iceberg snapshot count per table | > 500 | Shorten retention window or increase `expire_snapshots` frequency |
| Iceberg manifest-list size | > 10 GB | Run `rewrite_manifests` procedure (not yet wired; add to DAG if triggered) |
| Redshift Spectrum scan bytes | > 1 TB / query | Check predicate pushdown (see below) — partition pruning may be failing |

These are not yet wired as CloudWatch alarms (Terraform module is
deferred per ADR-006 revisit triggers). The row-count reconciliation
task in `warehouse_daily_batch_pipeline` (P2.5) acts as a coarse proxy: a
sudden drop in Gold mart rows often traces back to a Spectrum scan
timeout caused by small-file proliferation.

## Concurrency (DL-F)

Iceberg uses Optimistic Concurrency Control (OCC). Two writers trying
to commit to the same partition simultaneously will conflict; one will
throw `CommitFailedException` and need to retry.

**Current platform state:** no OCC conflicts possible today. Each
Iceberg table has exactly one writer:
- `bronze.inventory_events` ← `inventory_bronze_job` (Flink)
- `bronze.clickstream_events` ← `clickstream_bronze_job` (Flink)
- `silver.inventory_hourly` ← `inventory_silver_job` (Flink)
- `bronze.pos_transactions` ← `generate_pos_parquet.py` (Airflow daily batch)

The `lakehouse_daily_iceberg_maintenance` job runs at 03:00 UTC, outside the Flink
streaming commit cycle (every 10s) — compaction + expiration are
atomic per partition and conflict-resolved by Iceberg internally.

**Future OCC risk:** if a second writer is ever added to any of these
tables (e.g. a backfill job running parallel to the streaming writer),
stagger the writes or aggregate via a single UNION writer per the
checklist item 3.1.

## Schema evolution (DL-I, DL-J)

### New columns

dbt incremental models declare `on_schema_change`:

| Setting | Used by | Behavior |
|---|---|---|
| `append_new_columns` | 8 of 9 incremental models (facts, intermediates, dim_product, identity_graph) | New columns from the SELECT list are added to the target as NULLABLE. Existing columns are untouched. Matches checklist item 4.2 ("avoid renaming or dropping columns directly"). |
| `sync_all_columns` | `dim_customer` (Type 1 dimension) | Adds new columns AND removes columns that disappear from the SELECT. Used when a removed column is intentional (e.g. a deprecated RFM field). Carries the item 4.2 risk — document any SELECT-list removal in the model header comment. |

A unit test (`tests/unit/test_dbt_on_schema_change.py`) enforces that
every `materialized='incremental'` model declares one of these two
safe values. The dbt default `ignore` silently drops new columns and
is the most dangerous option.

### Column rename / drop

Direct column renames or drops in the Iceberg schema require a data
contract version bump (major for breaking, minor for optional additions)
per `docs/data-contracts/README.md`. The JSON Schema in
`ingestion/schemas/` is the runtime enforcement point; Flink bronze
jobs route rows that fail schema validation to the DLQ.

## Query tuning (DL-K, DL-L)

### Redshift ANALYZE

The `warehouse_daily_batch_pipeline` DAG runs `ANALYZE finance; ANALYZE marketing;
ANALYZE serving;` after `dbt_marts` and before `dbt_tests`. This keeps
the Redshift cost-based optimizer's table statistics fresh so it
chooses the right join order for:
- dbt test queries (run immediately after ANALYZE)
- GE checkpoint queries (run after dbt tests)
- BI consumer queries that hit Gold shortly after the daily batch

Without ANALYZE, the CBO can pick a broadcast join on two massive Gold
tables and OOM the query (checklist item 5.1).

### Predicate pushdown verification

Spectrum pushes S3 filters to the Iceberg scan layer. To verify
predicate pushdown is working for a specific query, inspect the
Spectrum scan summary:

```sql
-- Recent Spectrum queries
SELECT query, userid, xid, query_scanned_bytes, query_returned_rows,
       query_execution_time
FROM sys_query_history
WHERE query_id IN (
  SELECT query_id
  FROM sys_scan_s3_history
  WHERE query_scanned_bytes > 1e9  -- >1TB scanned = pushdown likely failing
)
ORDER BY start_time DESC
LIMIT 20;

-- Or for a specific query (after it runs):
SELECT *
FROM svl_s3query_summary
WHERE query = <pg_query_id>
ORDER BY segment, step;
```

If `query_scanned_bytes` is much larger than the partition size you
expect to be reading, predicate pushdown is failing — likely causes:

1. **Partition spec missing** — fixed by DL-A; verify with
   `SHOW CREATE TABLE bronze.inventory_events;` (look for
   `PARTITIONED BY`).
2. **Filter on non-partition column** — e.g. filtering on `store_id`
   without an `event_time` filter. Add a partition-column predicate
   or rely on Iceberg column stats for data skipping.
3. **Spectrum external table not refreshed** — for POS, run
   `MSCK REPAIR TABLE bronze.pos_transactions;` after each daily batch
   to register new `dt=YYYY-MM-DD` directories.

## References

- ADR-001: `docs/decisions/ADR-001-table-format.md` — Iceberg choice
  + maintenance consequences.
- ADR-006: `docs/decisions/ADR-006-flink-vs-spark.md` — Spark deferred
  until maintenance triggers; the `lakehouse_daily_iceberg_maintenance` DAG uses Flink
  batch mode instead.
- ADR-007: `docs/decisions/ADR-007-inventory-kappa.md` — kappa
  conversion that made silver the upstream for
  `fact_inventory_snapshot`.
- Data lake checklist (source): applied 2026-07-05; closure recorded
  in `docs/runbooks/dw-checklist-audit.md` Part 6.
- DAG: `orchestration/airflow/dags/lakehouse_daily_iceberg_maintenance.py`
- Maintenance job: `streaming/flink_jobs/iceberg_maintenance.py`
- Regression tests: `tests/unit/test_iceberg_partitions.py`,
  `tests/unit/test_dbt_on_schema_change.py`
