# ADR-008 Amendment: Hourly Freshness Measurement and Observability

**Status:** Accepted  
**Date:** 2026-10-26  
**Author:** Data platform team  
**Amends:** [ADR-008](ADR-008-metadata-database.md)

---

## Context

The original ADR-008 metadata collector measured table freshness by querying `MAX(timestamp_column)` and treating date keys (`YYYYMMDD` integers) as "loaded through end of that day" (midnight of the following day). This worked correctly for daily-grain tables with 24-hour SLAs but produced false "fresh" results for hourly tables.

**Bug discovered:**
- `silver.inventory_hourly` has `snapshot_date_key` (e.g., `20261024`) and `snapshot_hour` (e.g., `7`)
- 2-hour SLA requires detecting staleness when data stops at 7 AM but is measured at 4 PM same day
- Old logic: `20261024` → `2026-10-24 23:59:59` → lag = 4 hours → **OK** (wrong)
- Correct: `20261024` + hour `7` → `2026-10-24 08:00:00` → lag = 8 hours → **BREACH**

**Additional gaps identified:**
1. Catalog did not track critical Spark outputs (`silver.identity_resolution`, `silver.identity_edges`)
2. Metadata collection failures (returning `sla_status = 'unknown'`) were not alertable separately from breaches
3. Long-running streaming jobs (Flink) tracked submission success but not ongoing health

## Decision

### 1. Hourly Freshness Measurement (P0)

**Change `_as_loaded_through()` in `scripts/common/metadata_observer.py`:**
- Accept optional `hour_value` parameter
- For date keys with hour: return `date + timedelta(hours=hour_value + 1)`
- For date keys without hour: preserve existing "end of day" behavior (backward compatible)

**Update `collect_table_freshness()`:**
- Infer hour column name from timestamp column: `snapshot_date_key` → `snapshot_hour`
- Query both `MAX(date_key)` and `MAX(hour)` for rows matching the max date
- Pass hour to `_as_loaded_through()` when available
- Fall back to date-only for tables without hour column (e.g., daily summary tables)

**Test coverage:**
- New test: `test_hourly_inventory_freshness_with_stale_data_breaches_sla`
- Verifies 7 AM data measured at 4 PM (9-hour gap) breaches 120-minute SLA
- Existing test: `test_freshness_reads_date_keys_as_end_of_day` unchanged (daily table compatibility)

### 2. Complete Metadata Catalog (P1)

**Add to `metadata/catalog/layer_catalog.yml`:**
- `silver.identity_resolution` (Iceberg, 120-minute SLA)
- `silver.identity_edges` (Iceberg, 120-minute SLA)

**Test coverage:**
- New test: `test_layer_catalog_includes_critical_spark_outputs`
- Fails if identity objects are missing

**Rationale:** ADR-010 made identity resolution the authoritative Spark batch job. Its outputs are critical for `dim_customer` and `fact_customer_session`; freshness must be tracked.

### 3. Observable Metadata Failures (P1)

**Add to `metadata/catalog/metric_catalog.yml`:**
- `table_freshness_unknown_count` — count of `sla_status = 'unknown'` (metadata probe failures)
- `metadata_collection_failure_count` — count of pipeline runs with metadata errors

**Test coverage:**
- New test: `test_freshness_unknown_status_is_observable`
- Creates one good table and one missing table
- Verifies unknown status can be queried as a separate metric

**Rationale:** The collector is fail-open by design (ADR-008), so metadata outages never flip a successful business pipeline to failed. However, repeatedly returning "unknown" freshness means observability is degraded. Alerting separately on unknown status lets on-call distinguish:
- **Breach:** data is stale (producer/pipeline issue)
- **Unknown:** cannot measure (metadata collector/database issue)

### 4. Streaming Job Health Monitoring (P1)

**New runbook:** `docs/runbooks/streaming-job-monitoring.md`

Covers monitoring long-running Flink jobs beyond Airflow submission:
- **Table freshness SLA** — already implemented, hourly checks in `quality_hourly_ge_checkpoint`
- **Consumer lag** — query MSK CloudWatch metrics (`SumOffsetLag`, `MaxOffsetLag`)
- **Checkpoint health** — query Flink UI/REST API for checkpoint age and success rate
- **DLQ volume** — future enhancement (side-output Iceberg tables)
- **Job restart detection** — compare YARN app IDs across submissions

**Design principle:** "Implement the monitoring hooks/metrics your stack already supports; don't invent a new platform."

**Rationale:** Airflow records Flink job **submission** success, not ongoing health. A job can fail hours later. Rather than building a parallel monitoring system, the runbook documents how to query existing Flink UI, Flink REST API, MSK CloudWatch, and EMR YARN metrics. Table freshness SLA already provides the business-facing signal; the runbook adds operational depth for on-call investigation.

## Alternatives Considered

### A. Add checkpoint age to `meta` tables

**Rejected.** Flink REST API and CloudWatch already expose checkpoint metrics. Duplicating them into Redshift would require:
- Lambda poller hitting Flink REST API every 5 minutes
- Custom CloudWatch metric ingestion
- New `meta.flink_checkpoint_health` table

Cost: significant. Benefit: marginal over querying CloudWatch/Flink UI directly.

**Decision:** Document the query paths; defer table-based checkpoint tracking unless on-call requests it after real incidents.

### B. Separate `last_materialization_time` column

**Considered.** Track when dbt/Spark last **wrote** the table, distinct from the max event timestamp inside it.

**Rejected for this amendment.** Tables already record `updated_at` via dbt metadata. For freshness SLA, the **event timestamp** is the signal we care about (when did the latest business event occur), not the wall-clock time dbt ran. If a batch runs at 2 AM processing yesterday's 11 PM data, we want to measure lag against 11 PM, not 2 AM.

**Future consideration:** If incremental models show "up-to-date" event timestamps but haven't actually run in days (e.g., skipped due to upstream failure), this could cause false "fresh" signals. Revisit if operational metrics show this gap.

### C. Invent new DLQ Iceberg tables

**Deferred (P2).** Flink jobs log quarantine events; they're visible in EMR logs and CloudWatch Logs Insights. Adding `bronze.clickstream_dlq` / `bronze.inventory_dlq` Iceberg tables is a clean enhancement but not urgent — schema validation is stable and DLQ volume is near zero.

**Decision:** Document the log-search workaround; implement DLQ tables when (a) quarantine volume justifies it or (b) a schema migration project needs structured DLQ audit.

## Consequences

### Positive

- **Hourly SLA breaches now detected correctly.** No more false "fresh" for stale hourly data.
- **Identity objects tracked.** Spark job failures visible in freshness dashboard.
- **Unknown status alertable.** Metadata outages distinguished from data staleness.
- **Streaming health documented.** On-call has a playbook for Flink issues.
- **Zero new infrastructure.** All monitoring uses existing CloudWatch, Flink UI, MSK APIs.
- **Backward compatible.** Daily tables unchanged; only hourly tables with `*_hour` columns affected.

### Negative / Accepted Trade-offs

- **Hour column inference is convention-based.** If a table has `snapshot_date_key` but names its hour column something other than `snapshot_hour`, the inference fails and it falls back to date-only. Mitigation: catalog validation test (`test_layer_catalog_grain_matches_model_unique_key`) would catch grain mismatches.
- **No proactive checkpoint monitoring.** Still requires querying Flink UI or CloudWatch; not automated into alerts. Acceptable — table freshness SLA already provides the business signal; checkpoint details are diagnostic depth.
- **DLQ volume not tracked in tables.** Deferred to P2. Current log-search workaround is manual but sufficient for near-zero quarantine rate.

## Rollout

1. ✅ Unit tests for hourly freshness, identity catalog, unknown status
2. ✅ Update `metadata_observer.py` (backward-compatible change)
3. ✅ Update `layer_catalog.yml` (add identity objects)
4. ✅ Update `metric_catalog.yml` (add unknown/failure metrics)
5. ✅ Add `streaming-job-monitoring.md` runbook
6. Run CI to validate (dbt compile, unit tests, semantic layer checks)
7. Deploy to cloud:
   - Metadata YAML catalogs bundled with MWAA plugins
   - No DDL changes (existing `meta.table_freshness.sla_status` already supports `'unknown'`)
   - Freshness checks run hourly in `quality_hourly_ge_checkpoint` (no DAG changes)
8. Verify in dev:
   - Stop a Flink job → wait 3 hours → confirm `silver.inventory_hourly` shows `sla_status = 'breach'`
   - Query `SELECT SUM(CASE WHEN sla_status = 'unknown' THEN 1 ELSE 0 END) FROM meta.table_freshness WHERE measured_at > NOW() - INTERVAL '1 day'` → baseline unknown count
9. Update alerting (if not already covered):
   - CloudWatch Alarm on `table_freshness_breach_count > 0` (already exists)
   - CloudWatch Alarm on `table_freshness_unknown_count > 5` per day (new)
   - MSK consumer lag alarms per runbook thresholds (manual setup, not in Terraform yet)

---

**Review checklist:**
- [x] P0 (hourly freshness) implemented and tested
- [x] P1 (catalog completeness) implemented and tested
- [x] P1 (metadata failures observable) implemented and tested  
- [x] P1 (streaming monitoring) documented in runbook
- [x] P2 (metric governance) already satisfied by ADR-013 (no changes needed)
- [x] Backward compatible (daily tables unchanged)
- [x] No new infrastructure (uses existing systems)
