# Agent Guide - Global Retail Analytics Platform

Instructions for AI agents and automation working in this repository.

## Guidance Hierarchy

This file is the single cross-agent guide: branches, commands, and do-nots live
here only. `.cursor/rules/*.mdc` hold path-scoped facts (dbt, Flink, Spark,
Airflow, quality, Python) and must not repeat this file. `ARCHITECTURE.md` is the
architecture authority.

Use `ARCHITECTURE.md`, `docs/ENVIRONMENTS.md`, `docs/data-model/`, and
`docs/decisions/ADR-*.md` for design decisions. Use `docs/runbooks/` for
operational procedures.

## What This Repo Is

A production-style retail data platform with three sources (POS batch,
inventory stream, clickstream stream). Cloud path: Kafka/MSK → Flink → Iceberg
→ Redshift/dbt → Airflow/MWAA, plus Spark GraphFrames identity resolution
(ADR-010), GE, Terraform, and Streamlit. Local path simulates the lake with
Docker Kafka/Flink/Spark and DuckDB.

## Branches

| Branch | Use for |
|---|---|
| **`main`** | Fully cloud-deployed platform (dev/prod AWS). |
| **`local-testing-version`** | Local Docker/Flink/DuckDB testing and demos. |
| **`showcase`** | Trimmed laptop run of the critical flow. Do not copy its deletions onto `main`. |

Do **not** merge local-only compose bind-mounts, short Flink windows, or
DuckDB-only wiring into `main` as a blanket merge. Cherry-pick shared cloud
logic when the user asks.

## Common Tasks

### Agent verification and review

- For pipeline correctness reviews, use `$review-data-pipeline` from
  `.agents/skills/review-data-pipeline/SKILL.md`; reviews do not authorize fixes.
- For verifying pipeline changes, use `$verify-data-pipeline` from
  `.agents/skills/verify-data-pipeline/SKILL.md`. Select checks for the changed
  layer and branch; do not substitute fixture success for live-path evidence.
- Before claiming success, report the checkout/commit, commands and working
  directories, exit codes, relevant test counts, and failed/skipped checks with
  reasons. Distinguish unit, integration, and deployed-cloud evidence.
- Preserve existing data and unrelated changes. Test commands can mutate data:
  the dbt idempotency test deletes `local_retail.duckdb` in its checkout; run it
  only in a disposable checkout with an isolated profile and database.
- Workflow examples and check selection: `docs/runbooks/codex-workflow.md`.
  These instructions are guidance, not deployment authorization or a security
  boundary. Hooks are not enabled by this setup.

### Local Stack (Windows) — `local-testing-version`

```powershell
git checkout local-testing-version
uv sync --group dev
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task pos-parquet
.\scripts\local\run_local_stack.ps1 -Task spark
.\scripts\local\run_local_stack.ps1 -Task dbt
.\scripts\local\run_local_stack.ps1 -Task quality
```

- Default `-DbtSource iceberg`: Flink Parquet under `.local/iceberg` + local POS
  Parquet → DuckDB; seed only `dim_date` / `dim_store` / `product_master` /
  `inventory_opening_balance` (catalog attributes for `dim_product` and opening
  shelf counts for `fact_inventory_snapshot`, generated from
  `ingestion/kafka/sim_entities.py`; CI runs
  `python -m ingestion.batch.generate_product_master --check`).
- `-DbtSource seeds`: curated CSV fixtures (identity CI /
  `verify_local_identity.py`).
- Start **Flink before simulate** (`latest-offset`). Task `all` does this.
- Kafka host: `127.0.0.1:9092`. Flink UI: `http://localhost:8082`.
- Prefer `.\.venv\Scripts\python.exe` / `dbt.exe` (stack script uses `.venv`).
- `-Task quality`: `dbt test` (the Gold WAP gate) + GE `silver_events_local` (Silver event contract) + `pytest tests/unit`.
- Identity graph (ADR-010): `-Task spark` runs the GraphFrames job in the
  local Spark 3.4.1 Docker image (`infra/docker/spark`) and writes
  `.local/iceberg/silver/identity_resolution`. `-Task all` (iceberg source)
  includes this after POS Parquet. Spark first promotes raw Bronze to canonical
  Silver (ADR-011). Fidelity mode requires these exports; use `-DbtSource seeds`
  explicitly for fixtures. Spark promotion of raw Bronze lives in
  `spark/identity_resolution/canonical_events.py`.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/ -q
```

### Cloud deploy

AWS Terraform, Redshift, and the cloud scripts are not in this branch. They
live on `main`.

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
  `silver.identity_resolution` / `silver.identity_edges` (Iceberg), plus
  local DuckDB bridge (never glob Iceberg `data/` dirs — superseded snapshot
  files can linger). Blank identifiers are normalized away before graph build.
- `spark/identity_resolution/graph_logic.py` is the rules source of truth;
  `generate_fixture.py` regenerates the dbt seed fixture (CI `--check`).
  CI also runs the real DataFrame/GraphFrames path via
  `tests/integration/verify_spark_identity.py`.
- dbt `int_identity_resolution` is a thin view over
  `source('silver', 'identity_resolution')`; the old SQL edge/component
  models are retired. WAP unchanged — Gold marts stay dbt-built.
- Submit: `.\scripts\local\run_local_stack.ps1 -Task spark`, or the marketing DAG on `main`.

## Metadata + Summary

- Analytics schema `summary.*` (dbt `marts.summary`) holds daily rollups from
  one Gold fact each — see `docs/data-model/platform-layers.md`.
- Operational DB `metadata.meta.*` (local: `local_metadata.duckdb`) is written
  by `scripts/common/metadata_observer.py` (fail-open). Not a Glue replacement.

## Model Rules

- Kimball facts and dims with surrogate integer keys. No fact-to-fact joins
  (drill across conformed dims instead).
- Gate PII with `marketing_consent`.
- Event contract changes bump `schema_version` (`ingestion/schemas/`).
- POS lands as Parquet in Bronze; there is no POS Kafka topic.

## Do Not

- Commit secrets (`.env`, `transformation/dbt_project/profiles.yml`, real `*.tfvars`, `*_credentials.json`), placeholders, or TODOs.
- Commit runtime output (`__pycache__/`, dbt `target/`, `.local/**`, `quality/great_expectations/uncommitted/`).
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
- Environments: `docs/ENVIRONMENTS.md`
- Local Iceberg queries: `docs/runbooks/local-data-queries.md`
- Data contracts: `ingestion/schemas/`
- Operations: `docs/runbooks/`
- Local run: `scripts/local/run_local_stack.ps1`, `scripts/local/load_iceberg_to_duckdb.py`, `scripts/local/run_ge_local.py`; shared: `scripts/common/metadata_observer.py`
- Cursor: `.cursor/rules/*.mdc` (scoped), `.cursor/skills/local-stack-loop/`
