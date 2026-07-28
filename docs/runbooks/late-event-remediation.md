# Runbook: Late Event Remediation

## Trigger

- Inventory or clickstream late-arrival rate exceeds expected SLA.
- Downstream aggregates show discontinuities for recent windows.

## Detection Queries

```sql
select
  date_trunc('hour', event_time) as event_hour,
  avg(datediff('second', event_time, ingest_time)) as avg_lag_seconds,
  max(datediff('second', event_time, ingest_time)) as max_lag_seconds
from bronze.clickstream_events
where ingest_time >= dateadd('hour', -6, current_timestamp)
group by 1
order by 1 desc;
```

## Remediation Steps

1. Confirm producer clock skew and NTP health.
2. Increase Flink watermark lateness tolerance if required.
3. Re-run impacted dbt incremental windows with a wider backfill window.
4. Rebuild affected hourly snapshots for impacted keys only.
5. Validate no duplicate `event_id` after backfill.

## Exit Criteria

- Late-arrival percentage returns to baseline.
- Dashboard and warehouse aggregates reconcile for impacted intervals.
