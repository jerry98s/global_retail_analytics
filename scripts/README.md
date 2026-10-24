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

See `infra/docker/README.md` for the compose files.
