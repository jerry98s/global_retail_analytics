# Agent Guide - Global Retail Analytics Platform

Instructions for AI agents and automation working in this repository.

## Guidance Hierarchy

Do not duplicate long instructions across files.

| File | Purpose |
|---|---|
| `.cursor/rules/*.mdc` | Cursor's active scoped agent rules |
| `.cursor/README.md` | Explains the Cursor rule layout |
| `AGENTS.md` | Cross-agent and human-readable operating guide |
| `.cursorrules` | Legacy compatibility pointer only |
| `ARCHITECTURE.md` | Current platform architecture authority |

Use `ARCHITECTURE.md`, `docs/ENVIRONMENTS.md`, `docs/data-model/`, and
`docs/decisions/ADR-*.md` for design decisions. Use `docs/runbooks/` for
operational procedures.

## What This Repo Is

A production-style retail data platform with three sources (POS batch,
inventory stream, clickstream stream). Cloud path: Kafka/MSK → Flink → Iceberg
→ Redshift/dbt → Airflow/MWAA, plus GE, Terraform, and Streamlit. Local path
simulates the lake with Docker Kafka/Flink and DuckDB.

## Branches

| Branch | Use for |
|---|---|
| **`main`** | Fully cloud-deployed platform (dev/prod AWS). |
| **`local-testing-version`** | Local Docker/Flink/DuckDB testing and demos. |

Do **not** merge local-only compose bind-mounts, short Flink windows, or
DuckDB-only wiring into `main` as a blanket merge. Cherry-pick shared cloud
logic when the user asks.

## Common Tasks

### Local Stack (Windows) — `local-testing-version`

```powershell
git checkout local-testing-version
uv sync --group dev
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task dbt
.\scripts\local\run_local_stack.ps1 -Task quality
```

- Default `-DbtSource iceberg`: Flink Parquet under `.local/iceberg` + local POS
  Parquet → DuckDB; seed only `dim_date` / `dim_store`.
- `-DbtSource seeds`: curated CSV fixtures (identity CI /
  `verify_local_identity.py`).
- Start **Flink before simulate** (`latest-offset`). Task `all` does this.
- Kafka host: `127.0.0.1:9092`. Flink UI: `http://localhost:8082`.
- Prefer `.\.venv\Scripts\python.exe` / `dbt.exe` (stack script uses `.venv`).
- `-Task quality`: `dbt test` + GE `gold_layer_local` (DuckDB) + `pytest tests/unit`.
- Identity graph (ADR-010): `-Task spark` runs the GraphFrames job under local
  PySpark (needs Spark 3.4 + Java on PATH) and writes
  `.local/iceberg/silver/identity_resolution`. Without it, `-Task dbt` seeds the
  generated fixture so the chain still builds.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/ -q
```

### Cloud Platform — `main` (and cloud work on that branch)

```powershell
# Wrapper (default -Env dev; -Task all = apply -> bootstrap -> deploy -> producers -> verify)
.\scripts\cloud\run_cloud_stack.ps1 -Task all
.\scripts\cloud\run_cloud_stack.ps1 -Task apply -Env prod -AutoApprove
.\scripts\cloud\run_cloud_stack.ps1 -Task deploy
.\scripts\cloud\run_cloud_stack.ps1 -Task verify

# Or run siblings directly when you need per-script flags:
.\scripts\cloud\run_terraform.ps1 -Stack bootstrap -Action apply
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply
.\scripts\cloud\deploy_platform.ps1 -Env dev
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars
```

Never run raw Terraform from a stack directory.

For a guided lowest-cost cloud end-to-end run (MWAA/dashboard off, laptop-driven
dbt/GE, evidence capture, destroy checklist), follow
`docs/runbooks/dev-cloud-test-run.md`. EMR sizing is parameterized via
`emr_master_instance_type` / `emr_core_instance_type` / `emr_core_instance_count`
/ `emr_core_bid_price` in tfvars (dev.tfvars.example shows the minimal shape).

## Current Airflow DAGs

- `warehouse_daily_batch_pipeline` — POS + **`marts.finance`**
- `marketing_hourly_customer_360_pipeline` — Spark identity step (EMR) + **`marts.marketing`** / C360
- `streaming_manual_flink_jobs`
- `catalog_bihourly_product_scd2_refresh`
- `quality_hourly_ge_checkpoint`
- `lakehouse_daily_iceberg_maintenance`

## Spark (identity resolution, ADR-010)

- `spark/identity_resolution/identity_resolution_job.py` (PySpark +
  GraphFrames) builds edges + connected components and writes
  `silver.identity_resolution` / `silver.identity_edges` (Iceberg).
- `spark/identity_resolution/graph_logic.py` is the rules source of truth;
  `generate_fixture.py` regenerates the dbt seed fixture (CI `--check`).
- dbt `int_identity_resolution` is a thin view over
  `source('silver', 'identity_resolution')`; the old SQL edge/component
  models are retired. WAP unchanged — Gold marts stay dbt-built.
- Submit: marketing DAG (hourly) or `deploy_platform.ps1 -Action spark`.

## Metadata + Summary

- Analytics schema `summary.*` (dbt `marts.summary`) holds daily rollups from
  one Gold fact each — see `docs/data-model/platform-layers.md`.
- Operational DB `metadata.meta.*` (local: `local_metadata.duckdb`) is written
  by `scripts/common/metadata_observer.py` (fail-open). Not a Glue replacement.
- Cloud bootstrap: `.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -MetadataOnly`
  (two Query Editor scripts; switch database between create and schema DDL).
- Implement shared contracts on `local-testing-version` first; cherry-pick
  cloud-safe summary/metadata/Airflow wiring to `main` without local compose
  bind-mounts.

## Do Not

- Commit secrets (`.env`, `profiles.yml`, real `*.tfvars`, credentials JSON).
- Commit generated caches (`__pycache__/`, dbt `target/`, `.terraform/`, `.local/iceberg` data).
- Replace Iceberg with Delta/Hudi or raw Parquet-only lake tables.
- Add SCD Type 2 to dimensions other than `dim_product`.
- Hardcode warehouse, Kafka, Redshift, or EMR endpoints.
- Reintroduce lean/redshift-dev stacks or CSV POS batch paths.
- Treat `local-testing-version` local Docker settings as cloud defaults on `main`.
- Mix operational metadata tables into Kimball Gold schemas.
- Build Gold marts directly into live `finance`/`marketing`/`summary` — Gold uses
  Write-Audit-Publish (ADR-009): clone live → `*_pending`, audit, then publish
  via `orchestration/airflow/plugins/wap_publish.py`. Incrementals anchor on
  `{{ this }}` (pending is a live clone). Cross-DAG Gold reads use
  `{{ wap_live_ref('model') }}`. Each Gold table has exactly one owning DAG.
- Add `finance.dim_date` / `finance.dim_store` to the WAP publish list (stable
  seed reference dims, not dbt-built marts).

## Key References

- Design: `ARCHITECTURE.md`, `docs/decisions/ADR-*.md`, `docs/data-model/`
- Environments: `docs/ENVIRONMENTS.md`, `docs/REDSHIFT.md`
- Local Iceberg queries: `docs/runbooks/local-data-queries.md`
- Cloud test run (lowest cost): `docs/runbooks/dev-cloud-test-run.md`
- Data contracts: `ingestion/schemas/`
- Operations: `docs/runbooks/`
- Cloud: `scripts/cloud/run_cloud_stack.ps1` (wrapper), `scripts/cloud/run_terraform.ps1`, `scripts/cloud/deploy_platform.ps1`, `scripts/cloud/bootstrap_redshift.ps1`, `scripts/cloud/run_msk_producers.ps1`
- Local (`local-testing-version` only): `scripts/local/run_local_stack.ps1` (wrapper), `scripts/local/load_iceberg_to_duckdb.py`, `scripts/local/run_ge_local.py`; shared: `scripts/common/metadata_observer.py`
- Cursor: `.cursor/README.md`, `.cursor/rules/*.mdc`
