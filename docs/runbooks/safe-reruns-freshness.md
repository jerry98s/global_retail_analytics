# Safe Reruns with Metadata Freshness Checks

## Problem

The metadata freshness check (`scripts/common/metadata_observer.py` →
`collect_table_freshness()`) measures table freshness against the current time by
default. During replay/backfill operations, this causes **false SLA breaches**
because replayed data is legitimately stale relative to "now" but was fresh at its
logical execution time.

### Example

- An hourly table has data through hour 7 (7-8 AM on Oct 24, 2026)
- SLA: 120 minutes
- **Live run at 4 PM same day**: lag = 480 minutes → **breach** (correct)
- **Replay of 9 AM run in Nov 2026**: lag = ~30 days → **breach** (false positive)

The replayed data was fresh at the logical date (9 AM = 60 min lag), but measuring
against "now" in November incorrectly flags it as stale.

## Solution

Use the `measured_at` parameter to measure freshness from the replay's logical
execution time instead of the current time.

## When to Use

### Use `measured_at` for:
- Manual DAG reruns / clears (Airflow UI "Clear" → "Clear task" on past runs)
- Backfill operations filling historical gaps
- Testing/validation with historical data
- Any scenario where you're processing data from a past logical date

### Do NOT use `measured_at` for:
- Normal scheduled/live runs (default behavior is correct)
- Monitoring current pipeline health
- Ad-hoc data refreshes for the current date

## Implementation

### Airflow DAG Reruns (Future Implementation)

When the safe reruns feature is implemented, `metadata_airflow.collect_freshness_task()`
will automatically use the DAG run's `logical_date` for reruns:

```python
def collect_freshness_task_safe(**context: Any) -> int:
    """Safe rerun version: uses logical_date for measured_at."""
    dag_run = context.get("dag_run")
    logical_date = getattr(dag_run, "logical_date", None)
    
    # For live runs, logical_date is recent and behaves like now()
    # For reruns, logical_date is the original scheduled time
    written = observer.collect_table_freshness(
        writer,
        analytics,
        execution_id=execution_id,
        measured_at=logical_date,  # Use logical date instead of default now()
    )
```

### Local Testing/Replay

For local development or manual testing with historical data:

```python
from datetime import datetime
from scripts.common import metadata_observer as mo

# Replay scenario: validating 2026-10-24 data in November 2026
mo.collect_table_freshness(
    writer,
    analytics_connection,
    execution_id="replay_20261024",
    measured_at=datetime(2026, 10, 24, 23, 59, 59),  # End of the replay day
)
```

### CLI (Future Enhancement)

A future CLI flag will support explicit `measured_at` for scripted reruns:

```bash
python scripts/common/metadata_observer.py collect-freshness \
  --backend redshift \
  --execution-id replay_20261024 \
  --measured-at "2026-10-24T23:59:59"
```

## Expected Behavior

### Hourly Tables

**Table**: `silver.inventory_hourly` (SLA: 120 minutes)  
**Data**: Hour 7 (7-8 AM) on Oct 24, 2026

| Measured At | Lag | Status | Reason |
|-------------|-----|--------|--------|
| Oct 24, 9 AM | 60 min | ok | Within SLA |
| Oct 24, 10 AM | 120 min | ok | At SLA edge |
| Oct 24, 4 PM | 480 min | breach | Exceeds SLA |

### Daily Tables

**Table**: `summary.sales_daily` (SLA: 1440 minutes = 1 day)  
**Data**: Date key 20261020 (loaded through end of Oct 20)

| Measured At | Lag | Status | Reason |
|-------------|-----|--------|--------|
| Oct 21, noon | 12 hours | ok | Within SLA |
| Oct 22, noon | 36 hours | warn | 1-2x SLA |
| Oct 24, noon | 84 hours | breach | > 2x SLA |

## Failure Messages

When freshness SLA is breached, `collect_table_freshness()` writes status='breach'
to `meta.table_freshness` and `freshness_breaches()` returns the affected tables.
The Airflow task raises:

```
RuntimeError: Freshness SLA breached: silver.inventory_hourly (lag 480 min)
```

For multiple breaches:

```
RuntimeError: Freshness SLA breached: silver.inventory_hourly (lag 480 min); summary.sales_daily (lag 5040 min)
```

## Testing

See `tests/unit/test_metadata_observer.py` →
`test_freshness_check_documents_expected_failure_and_replay()` for test scenarios
covering:
- Valid data (within SLA) → status=ok
- Stale data (> SLA) → status=breach + RuntimeError
- Replay with `measured_at` → avoids false breach

## Related

- **Implementation**: `scripts/common/metadata_observer.py`
- **Airflow**: `orchestration/airflow/plugins/metadata_airflow.py` →
  `collect_freshness_task()`
- **DAG**: `quality_hourly_ge_checkpoint` (runs freshness checks hourly)
- **Catalog**: `metadata/catalog/layer_catalog.yml` (SLA definitions)
- **ADR**: PR #4 – Hourly table freshness measurement fix
