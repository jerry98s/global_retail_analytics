# Scripts

One wrapper runs the laptop pipeline. Use a sibling script only when you need
that step on its own.

## `scripts/local/`

| Script | Purpose |
|--------|---------|
| **`run_local_stack.ps1`** | **Wrapper** — `up` / `topics` / `flink` / `flink-stop` / `simulate` / `pos-parquet` / `spark` / `load-duckdb` / `dbt` / `quality` / `replay` / `all`. Default `-DbtSource iceberg`. `-DbtSource seeds` uses CSV fixtures. Uses repo **`.venv`**. |
| `load_iceberg_to_duckdb.py` | Materializes Flink, POS, and Spark Parquet into `local_retail.duckdb`. |
| `run_ge_local.py` | Runs the Great Expectations `silver_events_local` checkpoint (Silver event contract) against DuckDB. |
| `bootstrap_live_dim_product.py` | Seeds the live `dim_product` clone the catalog model reads. |
| `row_count_gate.py` | The warehouse DAG's row-count publish gate against DuckDB. `quality` / `all` run `audit` before publish and `save` (baseline) after. State: `.local/row_count_gate.json`. |
| `replay_check.py` | Compares fingerprints after `-Task replay`. |
| `benchmark_identity.py` | 1×/5×/10× Spark identity timing on disjoint Bronze copies under `.local/benchmark`. |
| `demo_identity_change.py` | Carries one live identity change through Kafka, Flink, Spark and dbt. |
| `demo_wap_isolation.py` | Publishes finance twice while marketing is blocked, on a copy. |
| `demo_inventory_late_event.py` | Late inventory event recovery through Flink and Spark. |

## `scripts/common/`

| Script | Purpose |
|--------|---------|
| `metadata_observer.py` | Operational metadata collector. Writes `local_metadata.duckdb`, never Gold. Fail-open. |
| `run_ge_checkpoint.py` | Runs a GE checkpoint against Redshift (`RS_SQLALCHEMY_URL`). |
| `generate_semantic_queries.py` | Generates the dashboard SQL from the dbt semantic layer; CI `--check`. |

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
