# Scripts

One wrapper runs the laptop pipeline. Use a sibling script only when you need
that step on its own.

## `scripts/local/`

| Script | Purpose |
|--------|---------|
| **`run_local_stack.ps1`** | **Wrapper** — `up` / `topics` / `flink` / `simulate` / `pos-parquet` / `spark` / `load-duckdb` / `dbt` / `quality` / `all`. Default `-DbtSource iceberg`. `-DbtSource seeds` uses CSV fixtures. Uses repo **`.venv`**. |
| `load_iceberg_to_duckdb.py` | Materializes Flink, POS, and Spark Parquet into `local_retail.duckdb`. |
| `run_ge_local.py` | Runs the Great Expectations `silver_events_local` checkpoint (Silver event contract) against DuckDB. |
| `bootstrap_live_dim_product.py` | Seeds the live `dim_product` clone the catalog model reads. |
| `row_count_gate.py` | The warehouse DAG's row-count publish gate against DuckDB. `quality` / `all` run `audit` before publish and `save` (baseline) after. State: `.local/row_count_gate.json`. |

## `scripts/common/`

| Script | Purpose |
|--------|---------|
| `metadata_observer.py` | Operational metadata collector. Writes `local_metadata.duckdb`, never Gold. Fail-open. |

```powershell
uv sync --group dev
.\scripts\local\run_local_stack.ps1 -Task flink
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task pos-parquet
.\scripts\local\run_local_stack.ps1 -Task spark
.\scripts\local\run_local_stack.ps1 -Task dbt
.\scripts\local\run_local_stack.ps1 -Task quality
.\scripts\local\run_local_stack.ps1 -Task all
```

Restating corrected POS days (same contract as the warehouse DAG's
`restate_from` / `restate_to`): fix the Parquet under
`.local/iceberg/bronze/pos_transactions`, then pass the inclusive range to both
steps. The POS generator is skipped and only those days are rebuilt, audited
and published.

```powershell
.\scripts\local\run_local_stack.ps1 -Task dbt -RestateFrom 2026-10-10 -RestateTo 2026-10-11
.\scripts\local\run_local_stack.ps1 -Task quality -RestateFrom 2026-10-10 -RestateTo 2026-10-11
```

Every day in the range must have POS lines, or the audit
`pos_restatement_days_have_source_rows` blocks the publish: a missing file is
not an empty day. When the corrected file legitimately has no lines for a day,
list it in both steps (`restate_empty_dates` on the DAG). That day is cleared
from `fact_sales` and `sales_daily_store` in pending; a listed day that still
has lines fails the audit.

```powershell
.\scripts\local\run_local_stack.ps1 -Task dbt -RestateFrom 2026-10-10 -RestateTo 2026-10-11 -RestateEmptyDates 2026-10-11
.\scripts\local\run_local_stack.ps1 -Task quality -RestateFrom 2026-10-10 -RestateTo 2026-10-11 -RestateEmptyDates 2026-10-11
```

See `infra/docker/README.md` for the compose files.
