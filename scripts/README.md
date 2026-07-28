# Scripts

Entry points split by lifecycle: cloud ops vs. local-dev helpers. Each side has
one **wrapper** with a `-Task` switch that triggers the sibling scripts in the
right order; run a sibling directly only when you need its full flag set.

## `scripts/cloud/` — AWS platform ops (PowerShell)

| Script | Purpose |
|--------|---------|
| **`run_cloud_stack.ps1`** | **Wrapper** — one `-Task` for the whole cloud lifecycle: `apply` / `bootstrap` / `deploy` / `producers` / `verify` / `status` / `all`. Default `-Env dev`; `-Task all` runs apply -> bootstrap -> deploy -> producers -> verify (auto-approves terraform). |
| `run_terraform.ps1` | Terraform — bootstrap + platform apply/plan/destroy/**output** |
| `bootstrap_redshift.ps1` | Generate combined Redshift DDL + seeds + Spectrum SQL (+ optional metadata DB) |
| `deploy_platform.ps1` | Post-apply — sync MWAA, submit Flink, Airflow vars, status, **verify** (smoke checks) |
| `run_msk_producers.ps1` | Publish clickstream/inventory to MSK via EMR step (MSK IAM) |

## `scripts/local/` — local-dev helpers

| Script | Purpose |
|--------|---------|
| **`run_local_stack.ps1`** | **Wrapper** — local Docker stack: `up` / `topics` / `simulate` / `flink` / `flink-stop` / `pos-parquet` / `load-duckdb` / `dbt` / `quality` / `all`. Default `-DbtSource iceberg` reuses Flink Parquet for dbt; `-DbtSource seeds` uses CSV fixtures. Uses repo **`.venv`** (create with `uv sync --group dev`). |
| `load_iceberg_to_duckdb.py` | Sub-script invoked by `run_local_stack -Task load-duckdb` (and `-Task dbt` in iceberg mode). Materializes Flink Iceberg Parquet + POS Parquet into `local_retail.duckdb` as `bronze.*` / `silver.*` tables matching dbt `source()` names. |
| `run_ge_local.py` | Sub-script invoked by `run_local_stack -Task quality`. Runs the GE `gold_layer_local` checkpoint against DuckDB (Pandas-batch port of the cloud Redshift checkpoint) + records the run via `metadata_observer`. |

## `scripts/common/` — shared Python (imported, not run directly)

| Script | Purpose |
|--------|---------|
| `metadata_observer.py` | Operational metadata collector (local DuckDB + cloud Redshift). Writes to a separate metadata DB — never into Gold schemas. Fail-open. Invoked by `run_local_stack -Task quality`/`dbt` and by the Airflow plugin `orchestration/airflow/plugins/metadata_airflow.py`. CLI: `init-local` / `start-run` / `finish-run` / `parse-dbt` / `collect-freshness` / `seed-catalog`. |

### Python environment (uv / `.venv`)

Do **not** `pip install` dbt into the system Python. Use the project venv:

```powershell
uv sync --group dev          # creates/updates .venv from pyproject.toml
.\.venv\Scripts\Activate.ps1 # optional; run_local_stack.ps1 uses .venv automatically
```

> **Scope.** `scripts/` is **do-work only** (apply, deploy, sync, submit, generate, bootstrap, start/stop). Anything that verifies, peeks, or analyzes lives elsewhere:
> - **`notebooks/`** — `.ipynb`-only analysis notebooks: `01_data_walkthrough` (end-to-end Bronze->Serving table walkthrough), `02_cost_model`. See `notebooks/README.md`.
> - **`tests/integration/`** — CI test gates like `verify_local_identity.py` (dbt identity-graph scenarios).

## Cloud (typical flow)

Preferred — one wrapper:

```powershell
.\scripts\cloud\run_cloud_stack.ps1 -Task all                 # full fresh deploy (dev)
.\scripts\cloud\run_cloud_stack.ps1 -Task apply -Env prod -AutoApprove
.\scripts\cloud\run_cloud_stack.ps1 -Task deploy              # after code changes
.\scripts\cloud\run_cloud_stack.ps1 -Task producers -Stream both -DurationSeconds 120
.\scripts\cloud\run_cloud_stack.ps1 -Task verify              # post-deploy smoke checks
```

Equivalent manual sequence (when you need per-script flags):

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack bootstrap -Action apply
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply

.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -IncludeSilver -IncludeMetadata
# Run target/redshift_bootstrap_dev.sql in Redshift Query Editor v2

.\scripts\cloud\deploy_platform.ps1 -Env dev
.\scripts\cloud\run_msk_producers.ps1 -Env dev -Stream both -DurationSeconds 120
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action verify

# Full cloud E2E checklist lives in docs/REDSHIFT.md (deploy + dbt + Airflow)
# and docs/ENVIRONMENTS.md (env-vs-per-env matrix, promotion path).
```

## Local (typical flow)

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink      # before simulate (latest-offset)
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task dbt        # Iceberg -> DuckDB + dim seeds
.\scripts\local\run_local_stack.ps1 -Task quality    # dbt test + GE gold_layer_local + pytest
.\scripts\local\run_local_stack.ps1 -Task all        # up -> topics -> flink -> simulate -> dbt -> quality
```

See `infra/docker/README.md` for the local Docker layout and ad-hoc
`docker compose -f infra/docker/compose/docker-compose.yml ...` commands.

## `deploy_platform.ps1` actions

| Action | When |
|--------|------|
| `all` (default) | After code changes — MWAA sync + Flink |
| `flink` | Streaming jobs only |
| `mwaa-sync` | DAGs/dbt/GE only |
| `airflow-vars` | Print variables for MWAA UI |
| `status` | EMR + S3 quick check |
| `verify` | Post-deploy smoke checks (EMR state, S3 bronze, Redshift row-counts, dashboard HTTP) |
