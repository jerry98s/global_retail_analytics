# Local Data Query Runbook

Use this runbook after the local Kafka/Flink stack has produced data into the
Docker-backed Iceberg warehouse.

> **Interactive equivalent.** `notebooks/01_data_walkthrough.ipynb` walks
> through all tables Bronze → Serving (including these Bronze/Silver queries)
> as a Jupyter notebook. This page is the paste-able terminal version for
> operators who don't want to open Jupyter.

## Local fidelity note (Flink Iceberg → dbt DuckDB)

Local default path reuses Flink Iceberg Parquet for dbt:

| Source | Upstream | Local dbt input |
|---|---|---|
| `bronze.clickstream_events` | Flink `clickstream_bronze_job` | `.local/iceberg/...` Parquet |
| `bronze.inventory_events` | Flink `inventory_bronze_job` | `.local/iceberg/...` Parquet |
| `silver.inventory_hourly` | Flink `inventory_silver_job` (1‑min window locally) | `.local/iceberg/...` Parquet |
| `bronze.pos_transactions` | `generate_pos_parquet --output-dir` (no local Flink POS job) | `.local/iceberg/bronze/pos_transactions/` |
| `finance.dim_date` / `dim_store` | No stream | **dbt seeds only** |

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink          # start BEFORE simulate
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task dbt            # -DbtSource iceberg (default)
```

Fixture CSV bronze/silver seeds remain for CI identity scenarios:

```powershell
.\scripts\local\run_local_stack.ps1 -Task dbt -DbtSource seeds
```

## Prerequisites

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink
.\scripts\local\run_local_stack.ps1 -Task simulate -ClickstreamEventsPerSecond 500 -ClickstreamDurationSeconds 30
```

The queries below run Python inside the `flink-taskmanager` container, so your
host machine does not need `pyarrow`, `pandas`, or `duckdb` installed. The
compose file path is `infra/docker/compose/docker-compose.yml`.

For brevity in the snippets, the `docker compose` invocation is shown as:

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "<python>"
```

## Overview

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
for name, path in {
    'clickstream_events': Path('/tmp/iceberg/bronze/clickstream_events/data'),
    'inventory_hourly': Path('/tmp/iceberg/silver/inventory_hourly/data'),
}.items():
    files = sorted(path.glob('*.parquet'))
    print(f'{name}: {len(files)} parquet files')
    if files:
        t = pq.read_table([str(f) for f in files])
        print(f'  rows: {t.num_rows}')
        print(f'  columns: {\", \".join(t.column_names)}')
"
```

For a summary-only view (counts but no sample rows), drop the `head()` calls in
the cells below.

## Clickstream Queries

Count rows and summarize event types:

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
click = pq.read_table([str(f) for f in Path('/tmp/iceberg/bronze/clickstream_events/data').glob('*.parquet')]).to_pandas()
print('rows:', len(click))
print(click.groupby('event_type').size().sort_values(ascending=False).to_string())
"
```

Summarize events by platform:

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
click = pq.read_table([str(f) for f in Path('/tmp/iceberg/bronze/clickstream_events/data').glob('*.parquet')]).to_pandas()
print('rows:', len(click))
print(click.groupby('platform').size().sort_values(ascending=False).to_string())
"
```

Show checkout-related events (limit 20):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
click = pq.read_table([str(f) for f in Path('/tmp/iceberg/bronze/clickstream_events/data').glob('*.parquet')]).to_pandas()
checkouts = click[click['event_type'].isin(['checkout_start', 'checkout'])]
print('checkout rows:', len(checkouts))
print(checkouts[['event_time', 'event_type', 'session_id', 'customer_id', 'platform', 'properties']].head(20).to_string(index=False))
"
```

Show sample clickstream rows (limit 10):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
click = pq.read_table([str(f) for f in Path('/tmp/iceberg/bronze/clickstream_events/data').glob('*.parquet')]).to_pandas()
print(click.head(10).to_string(index=False))
"
```

## Inventory Queries

Show inventory totals (hourly deltas, NOT running balances):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
inv = pq.read_table([str(f) for f in Path('/tmp/iceberg/silver/inventory_hourly/data').glob('*.parquet')]).to_pandas()
print('rows:', len(inv))
print(inv[['qty_delta_hour', 'qty_received_hour']].sum().to_string())
"
```

Show highest inventory snapshots by hourly delta (limit 20):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
inv = pq.read_table([str(f) for f in Path('/tmp/iceberg/silver/inventory_hourly/data').glob('*.parquet')]).to_pandas()
print(inv.sort_values('qty_delta_hour', ascending=False).head(20).to_string(index=False))
"
```

Show negative on-hand hourly deltas (limit 20):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
inv = pq.read_table([str(f) for f in Path('/tmp/iceberg/silver/inventory_hourly/data').glob('*.parquet')]).to_pandas()
negative = inv[inv['qty_delta_hour'] < 0].sort_values('qty_delta_hour')
print(negative.head(20).to_string(index=False))
"
```

Show sample inventory rows (limit 10):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "
from pathlib import Path
import pyarrow.parquet as pq
inv = pq.read_table([str(f) for f in Path('/tmp/iceberg/silver/inventory_hourly/data').glob('*.parquet')]).to_pandas()
print(inv.head(10).to_string(index=False))
"
```

## Direct File Checks

List all local Parquet files:

```powershell
docker compose -f infra/docker/compose/docker-compose.yml exec flink-taskmanager bash -lc "find /tmp/iceberg -name '*.parquet' -type f | sort"
```

Clickstream data path:

```text
/tmp/iceberg/bronze/clickstream_events/data
```

Inventory hourly (silver) data path:

```text
/tmp/iceberg/silver/inventory_hourly/data
```

## Summary layer (DuckDB after `dbt`)

After `-Task dbt`, query reusable daily rollups in `local_retail.duckdb`:

```powershell
.\.venv\Scripts\python.exe -c @"
import duckdb
con = duckdb.connect('transformation/dbt_project/local_retail.duckdb', read_only=True)
for t in [
    'summary.sales_daily_store',
    'summary.inventory_daily_product_store',
    'summary.sessions_daily_platform',
]:
    n = con.execute(f'select count(*) from {t}').fetchone()[0]
    print(f'{t}: {n} rows')
print(con.execute('select * from summary.sales_daily_store limit 5').fetchdf())
"@
```

## Metadata database (local)

Operational history is written to
`transformation/dbt_project/local_metadata.duckdb` (gitignored) by
`scripts/common/metadata_observer.py` during `-Task dbt`, `-Task quality`,
and `-Task all`. Cloud equivalent: Redshift database `metadata`, schema `meta`
(see [ADR-008](../decisions/ADR-008-metadata-database.md)).

```powershell
.\.venv\Scripts\python.exe -c @"
import duckdb
con = duckdb.connect('transformation/dbt_project/local_metadata.duckdb', read_only=True)
print(con.execute('''
  select execution_id, pipeline_name, status, started_at, ended_at, duration_seconds
  from meta.pipeline_run
  order by started_at desc
  limit 5
''').fetchdf())
print(con.execute('''
  select schema_name, table_name, row_count, sla_status, measured_at
  from meta.table_freshness
  order by measured_at desc
  limit 20
''').fetchdf())
print(con.execute('''
  select check_system, status, count(*) as n
  from meta.dq_check_result
  group by 1, 2
  order by 1, 2
''').fetchdf())
"@
```

Bootstrap cloud metadata DDL (two Query Editor connections):

```powershell
.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -MetadataOnly
```

## Notes

- `.crc` files are Hadoop checksum sidecars. The actual data files end in
  `.parquet`.
- The inventory job uses a 1-hour tumbling window in `main` (production
  default). The `local-testing-version` branch shortens it to one minute and
  switches Kafka to `earliest-offset` for faster local iteration — tweak
  there, not in `main`.
- If a query returns no files, run the Flink jobs and producers again — wait
  at least one window-and-checkpoint cycle for files to appear.
- For an interactive end-to-end walkthrough of every table (Bronze → Serving,
  including these Bronze/Silver queries), open
  `notebooks/01_data_walkthrough.ipynb` in Jupyter / VSCode.
- Platform layer vocabulary:
  [docs/data-model/platform-layers.md](../data-model/platform-layers.md).
