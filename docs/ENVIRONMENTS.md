# Environments

Three flavours — one deployment path for cloud, one branch for local:

| Flavour | Where | What runs | Use for |
|---|---|---|---|
| **local** | your laptop (`local-testing-version` branch) | Docker Compose: Kafka + Flink + Iceberg on local disk | Inner loop, unit tests, dashboard local mode |
| **dev** | AWS | MSK + EMR + Redshift + S3 + optional MWAA (platform stack) | Full pipeline integration, pre-prod validation |
| **prod** | AWS | Same platform stack, prod-sized tfvars | Production workload |

> **Single source of truth:** cloud **dev** and **prod** share the same Terraform
> code (`infra/terraform/`), the same Flink jobs, dbt project, and Airflow DAGs.
> Only `envs/<env>.tfvars` and state keys differ. Local dev is isolated on the
> `local-testing-version` branch with Docker-only settings (short silver
> windows, `.local/iceberg` bind-mount, Flink before simulate for
> `latest-offset`) — never merged to `main` except via cherry-pick of
> specific shared fixes.

**Local dbt vs local Flink:** Default local dbt (`-DbtSource iceberg`) loads
Flink Parquet from `.local/iceberg` plus local POS Parquet into DuckDB; only
`dim_date` / `dim_store` stay as seeds. Use `-DbtSource seeds` for curated
CSV fixtures (identity CI). Python tooling uses **`.venv`** (`uv sync --group dev`).

---

## Repo layout

```
infra/terraform/
├── bootstrap/              # one-time: state bucket, locks, budget
├── envs/                   # per-env backend HCL + tfvars (dev, prod, …)
│   ├── dev.backend.hcl.example    # copy -> dev.backend.hcl (gitignored)
│   ├── dev.tfvars.example         # copy -> dev.tfvars      (gitignored)
│   ├── prod.backend.hcl.example
│   └── prod.tfvars.example
├── main.tf                 # platform stack: MSK + EMR + Redshift + S3 + MWAA + dashboard
├── backend.tf
└── modules/                # s3, kafka, emr, redshift, mwaa, streamlit
```

State keys: `platform/dev/terraform.tfstate`, `platform/prod/terraform.tfstate`.

---

## How configuration is selected

Top-level entry point: `scripts/cloud/run_cloud_stack.ps1` (wraps the four cloud scripts behind one `-Task` switch). For infra-only operations, the direct entry point is `scripts/cloud/run_terraform.ps1`.

```powershell
.\scripts\cloud\run_cloud_stack.ps1 -Task <apply|bootstrap|deploy|producers|spark|verify|status|all> -Env <dev|prod>
.\scripts\cloud\run_terraform.ps1 -Stack <bootstrap|platform> -Env <dev|prod> -Action <init|plan|apply|destroy|output>
```

Internally:

1. `cd infra/terraform/` (or `bootstrap/`)
2. `terraform init -reconfigure -backend-config=envs/<env>.backend.hcl`
3. `terraform <action> -var-file=envs/<env>.tfvars`

Both `envs/<env>.backend.hcl` and `envs/<env>.tfvars` are gitignored (they hold
your state bucket name, account ID, VPC/subnet IDs, and Redshift password). On a
fresh clone, copy the `*.example` templates first — see
[`infra/terraform/envs/README.md`](../infra/terraform/envs/README.md).

---

## Day-to-day commands

**Cloud deploy commands live in [`docs/REDSHIFT.md`](./REDSHIFT.md)** — they
are kept there because every cloud deploy ends in Redshift + dbt + Airflow
configuration, and duplicating them here has historically caused the two
pages to drift. This page covers only the parts of the env story that
Redshift.md doesn't:

- Local (laptop, Docker) quick-start
- What's shared vs. per-env
- End-to-end data flow
- Promotion path
- Safety rules
- When to use which flavour

For the cloud apply / deploy / Spectrum / dbt sequence, see
[`docs/REDSHIFT.md`](./REDSHIFT.md).

### Local (laptop — not Terraform)

```powershell
git checkout local-testing-version
uv sync --group dev
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink      # before simulate (latest-offset)
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task pos-parquet
.\scripts\local\run_local_stack.ps1 -Task spark      # GraphFrames identity (ADR-010); Docker profile spark
.\scripts\local\run_local_stack.ps1 -Task dbt        # -DbtSource iceberg (default)
.\scripts\local\run_local_stack.ps1 -Task quality    # dbt test + GE gold_layer_local + pytest
docker compose -f infra/docker/compose/docker-compose.yml -f infra/docker/compose/docker-compose.dashboard.yml up -d dashboard
```

Local `-Task quality` runs Great Expectations against DuckDB via
`scripts/local/run_ge_local.py` (queries from `gold_layer_local.yml`, same
suites as cloud `gold_layer_daily`). Batches are loaded with DuckDB→Pandas
because GE 0.18's SqlAlchemy+duckdb-engine path is unreliable. Cloud GE remains
MWAA `quality_hourly_ge_checkpoint` / warehouse daily `ge_gold_checkpoint`.

For querying the local Iceberg warehouse: see
[`docs/runbooks/local-data-queries.md`](./runbooks/local-data-queries.md).

---

## What is shared vs. per-env

| Concern | Shared | Per-env |
|---|---|---|
| Terraform state bucket | yes (bootstrap) | state **key** differs |
| Platform stack code | yes | — |
| Backend HCL + tfvars | — | `envs/<env>.*` |
| Resource names | — | `${project}-${env}-…` prefix |
| S3 buckets | — | `retail-platform-dev-bronze`, `…-prod-bronze`, etc. |
| MSK / EMR / Redshift / MWAA | — | one set per env |

---

## End-to-end data flow (cloud)

```
MSK topics
  ├─ inventory.events.v1  → Flink inventory_bronze  → s3://…-bronze/iceberg/bronze/inventory_events/
  ├─ (same topic)         → Flink inventory_hourly   → s3://…-silver/iceberg/silver/inventory_hourly/
  └─ clickstream.events.v1 → Flink clickstream_bronze → s3://…-bronze/iceberg/bronze/clickstream_events/

Airflow warehouse_daily_batch_pipeline (00:15 UTC)
  └─ generate_pos_parquet → s3://…-bronze/iceberg/bronze/pos_transactions/data/dt=…/

Redshift Spectrum (transformation/redshift/spectrum/bronze_external_tables.sql)
  └─ bronze.{inventory_events, clickstream_events, pos_transactions}

dbt (staging → intermediate → marts) → finance.* / marketing.* / summary.*

GE checkpoint gold_layer_daily → validates fact_sales + dim_product + dim_customer + customer_360_view + fact_inventory_snapshot + inventory_bronze

metadata DB (same Redshift workgroup; local: local_metadata.duckdb)
  └─ meta.{layer_catalog, metric_catalog, pipeline_run, table_freshness, dq_check_result}
```

**Deploy order:** `tf apply` → `deploy_platform.ps1 -Env dev` → `bronze_external_tables.sql` →
`bootstrap_redshift.ps1 -MetadataOnly` → `deploy_platform.ps1 -Action airflow-vars`
(+ password, `redshift_metadata_database`) → wait for or trigger `warehouse_daily_batch_pipeline`.

---

## Promotion path

```
local-testing-version  →  cherry-pick fixes to main  →  platform dev  →  platform prod
     (Docker)                  (code only)              (same tf + scripts)   (tfvars)
```

One platform stack only — Redshift, MWAA, and EMR all live in `infra/terraform/`.

---

## Safety rules

1. State key is set at init time via backend HCL — dev tfvars cannot touch prod state.
2. `run_terraform.ps1` requires both backend HCL and tfvars to exist for the chosen env.
3. Default tags include `Environment` for cost allocation.
4. Secrets only in git-ignored `*.tfvars` or `TF_VAR_*` env vars.

---

## When to use which

| Task | Where |
|---|---|
| Edit Flink job logic | Local branch first, then `deploy_platform.ps1 -Env dev` |
| Test MSK IAM auth | Platform dev |
| Test dbt marts | Platform dev Redshift + Spectrum |
| Test Airflow batch path | Platform dev MWAA (`enable_mwaa = true`) |
| Portfolio demo (no AWS cost) | Local branch + dashboard |
| Production cutover | Platform prod |
