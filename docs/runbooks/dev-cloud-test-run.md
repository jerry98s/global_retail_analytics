# Dev Cloud Test Run (Minimal Cost)

One-day, lowest-cost end-to-end run of the cloud platform on the `dev` config:
**S3 + MSK Serverless + EMR Flink + Redshift Serverless only** — no MWAA, no
App Runner dashboard. All three flows run Bronze → Silver → Gold with quality
gates; dbt and Great Expectations run from your laptop against Redshift.

Expected cost: a few dollars if destroyed the same day. The 100 RPU-hour
Redshift cap and the $50 AWS Budget (bootstrap stack) are the safety nets.

> **Rule zero:** the run is not finished until step 12 (destroy) is done.
> MSK Serverless and EMR bill continuously while they exist.

## Prerequisites

| Requirement | Check |
|---|---|
| AWS CLI configured with admin-ish permissions | `aws sts get-caller-identity` |
| Terraform >= 1.6 | `terraform version` |
| Project venv | `uv sync --group dev` |
| Cloud CLI venv | `uv venv .venv-cloud --python 3.11` then `uv pip install --python .venv-cloud\Scripts\python.exe dbt-redshift==1.8.1 redshift-connector==2.1.4 great-expectations==0.18.21` |
| On `main` branch, clean tree | `git status` |
| EMR default roles exist in the account | `aws iam get-role --role-name EMR_DefaultRole` and `EMR_EC2_DefaultRole` (create via `aws emr create-default-roles` if missing) |

## Step 0 — Config files (once)

Copy templates and fill in real values (all four files are gitignored):

```powershell
cd infra\terraform\bootstrap
copy terraform.tfvars.example terraform.tfvars     # state_bucket_name, budget_alert_email
cd ..\envs
copy dev.backend.hcl.example dev.backend.hcl       # bucket = state_bucket_name above
# dev.tfvars already exists with minimal sizing — fill in the REPLACE_ME values:
cd ..\..
```

In `infra/terraform/envs/dev.tfvars` replace:

| Value | How to find it |
|---|---|
| `vpc_id`, `private_subnet_ids` (>= 2 AZs) | `aws ec2 describe-vpcs` / `aws ec2 describe-subnets --filters Name=vpc-id,Values=<vpc>` |
| `msk_security_group_ids` | `aws ec2 create-security-group` in that VPC (no inbound rules needed beyond EMR same-VPC traffic) |
| `ACCOUNT_ID` in the two EMR role ARNs | `aws sts get-caller-identity --query Account --output text` |
| `redshift_subnet_ids` (>= 3 AZs) | same subnet list, pick 3 |
| `redshift_allowed_cidrs` | `curl -s https://checkip.amazonaws.com` then `"<ip>/32"` — never `0.0.0.0/0` |
| `redshift_admin_password` | strong secret; do not commit |

## Step 1 — Bootstrap (once per account)

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack bootstrap -Action apply
```

Creates the Terraform state bucket, lock table, and the $50 monthly budget
with email alerts. Negligible cost; leave it in place permanently.

## Step 2 — Platform apply (~20–30 min)

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply
```

Creates S3 buckets, MSK Serverless, the EMR Flink cluster (m5.large master +
1 m5.large spot core), Redshift Serverless (8 RPU, auto-pause, 100 RPU-hour
monthly cap), Glue catalog, and Secrets Manager entries.

Save the outputs — every later step reads them:

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action output
```

## Step 3 — Submit Flink jobs

```powershell
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action flink
```

Submits `clickstream_bronze`, `inventory_bronze`, and `inventory_hourly`
(silver) as EMR steps. Check they are running:

```powershell
aws emr list-steps --cluster-id <emr_cluster_id> --step-states RUNNING PENDING
```

## Step 4 — Produce stream events (small)

```powershell
.\scripts\cloud\run_msk_producers.ps1 -Env dev -Stream both -DurationSeconds 60
```

Defaults of 500 eps clickstream / 50 eps inventory for 60 s land ~33k events —
enough to prove the path. Give Flink a few minutes to checkpoint into Iceberg.

## Step 5 — POS batch to S3 bronze

```powershell
$env:POS_BRONZE_S3_PATH = "<pos_bronze_s3_path from terraform output>"
.\.venv\Scripts\python.exe ingestion/batch/generate_pos_parquet.py --transaction-count 1000
```

## Step 6 — Spark GraphFrames identity

POS must exist before this step. The command submits the real GraphFrames job
and blocks until the EMR step reaches `COMPLETED`; a failed or timed-out step
returns a non-zero exit instead of allowing stale identity data downstream.

```powershell
.\scripts\cloud\run_cloud_stack.ps1 -Task spark -Env dev
```

The job writes Iceberg `silver.identity_resolution` / `identity_edges` plus
replace-only `consumer_current/*` Parquet exports. Spectrum reads the exports,
never a raw Iceberg `data/` directory containing superseded snapshots.

## Step 7 — Redshift DDL + Spectrum

```powershell
.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -IncludeSilver -IncludeMetadata
```

This **generates** SQL under `target/`; it does not execute it. Run the scripts
in Redshift Query Editor v2, in order:

1. `target/redshift_bootstrap_dev.sql` — on database `dev` (Gold DDL + Spectrum
   external tables; placeholders already substituted from terraform outputs)
2. `target/redshift_metadata_create_dev.sql` — on database `dev`
3. `target/redshift_metadata_schema_dev.sql` — after switching to database `metadata`

Back on database `dev`, register the POS partition for the date generated in
step 5 (Spectrum does not discover Hive partitions automatically):

```sql
ALTER TABLE bronze.pos_transactions
ADD IF NOT EXISTS PARTITION (dt='YYYY-MM-DD')
LOCATION 's3://<bronze-bucket>/iceberg/bronze/pos_transactions/data/dt=YYYY-MM-DD/';
```

## Step 8 — dbt + Write-Audit-Publish from the laptop

```powershell
$env:RS_HOST     = "<redshift_endpoint>"   # host only, no port
$env:RS_PORT     = "5439"
$env:RS_USER     = "rs_admin"
$env:RS_PASSWORD = "<redshift_admin_password>"
$env:RS_DATABASE = "dev"

$Dbt = (Resolve-Path .venv-cloud\Scripts\dbt.exe)
Push-Location transformation\dbt_project
if (-not (Test-Path profiles.yml)) { Copy-Item profiles.yml.example profiles.yml }
& $Dbt deps
& $Dbt seed --target dev --select dim_date dim_store
& $Dbt run --target dev --select staging intermediate
Pop-Location
```

Do not run every mart directly into live schemas. The commands below reuse the
same ownership lists and clone/publish implementation as the Airflow DAGs.
Each set is cloned, built under `wap_phase=pending`, tested, audited by Great
Expectations, and only then published. On a first run the DDL-created empty live
tables provide the clone shape.

```powershell
$Wap = "orchestration/airflow/plugins/wap_publish.py"
$Ge = "scripts/common/run_ge_checkpoint.py"
$GeRoot = "quality/great_expectations"
$env:RS_SQLALCHEMY_URL = "redshift+psycopg2://rs_admin:<password>@<redshift_endpoint>:5439/dev"

# Catalog owner: marketing.dim_product only.
.\.venv-cloud\Scripts\python.exe $Wap clone catalog
Push-Location transformation\dbt_project
& $Dbt run --target dev --select dim_product --vars '{"wap_phase":"pending"}'
& $Dbt test --target dev --select dim_product --vars '{"wap_phase":"pending"}'
Pop-Location
.\.venv-cloud\Scripts\python.exe $Ge --ge-root $GeRoot --checkpoint gold_layer_daily --pending-tables marketing.dim_product
.\.venv-cloud\Scripts\python.exe $Wap publish catalog

# Finance owner plus its summary rollups.
.\.venv-cloud\Scripts\python.exe $Wap clone finance
Push-Location transformation\dbt_project
& $Dbt run --target dev --select marts.finance sales_daily_store inventory_daily_product_store --vars '{"wap_phase":"pending"}'
& $Dbt test --target dev --select staging intermediate marts.finance sales_daily_store inventory_daily_product_store --vars '{"wap_phase":"pending"}'
Pop-Location
.\.venv-cloud\Scripts\python.exe $Ge --ge-root $GeRoot --checkpoint gold_layer_daily --pending-tables finance.fact_sales,finance.fact_inventory_snapshot,summary.sales_daily_store,summary.inventory_daily_product_store
.\.venv-cloud\Scripts\python.exe $Wap publish finance

# Marketing owner plus sessions summary; dim_product remains catalog-owned.
.\.venv-cloud\Scripts\python.exe $Wap clone marketing
Push-Location transformation\dbt_project
& $Dbt run --target dev --select stg_clickstream_events stg_pos_transactions intermediate marts.marketing sessions_daily_platform --exclude int_product_catalog dim_product customer_360_view --vars '{"wap_phase":"pending"}'
& $Dbt test --target dev --select stg_clickstream_events stg_pos_transactions intermediate marts.marketing sessions_daily_platform --exclude int_product_catalog dim_product customer_360_view --vars '{"wap_phase":"pending"}'
Pop-Location
.\.venv-cloud\Scripts\python.exe $Ge --ge-root $GeRoot --checkpoint gold_layer_daily --pending-tables marketing.dim_customer,marketing.fact_customer_session,marketing.identity_graph,summary.sessions_daily_platform
.\.venv-cloud\Scripts\python.exe $Wap publish marketing

# Views resolve the freshly published live tables.
Push-Location transformation\dbt_project
& $Dbt run --target dev --select customer_360_view customer_360_serving
Pop-Location
```

`int_identity_resolution` is a thin view over the Spark-built Silver source;
dbt owns sessions, RFM, consent, Finance/Marketing Gold, and summary rollups.
If any test or GE command fails, stop: do not issue that set's `publish` command.

## Step 9 — Great Expectations live monitor

```powershell
$env:RS_SQLALCHEMY_URL = "redshift+psycopg2://rs_admin:<password>@<redshift_endpoint>:5439/dev"
cd quality\great_expectations
..\..\.venv-cloud\Scripts\great_expectations.exe checkpoint run gold_layer_daily
cd ..\..
```

All 11 suites should validate against Redshift (Gold marts + last-24h bronze
windows).

## Step 10 — Verify + capture evidence

```powershell
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action verify
```

Smoke checks: EMR state, S3 bronze object counts, Redshift row counts
(`dim_date`, `fact_sales`, `identity_graph`).

Suggested evidence captures (add to `docs/evidence/screenshots/`):

- EMR console: cluster RUNNING/WAITING with the three Flink steps and the
  completed Spark identity step
- Redshift Query Editor: row counts across bronze/staging/gold
- `dbt test` and GE checkpoint terminal output
- Optional local dashboard against Redshift: `streamlit run dashboard/app.py`
  with the `RS_*` vars set and mode `redshift` — the same UI reading cloud Gold

> Crop or avoid anything showing account IDs, endpoints, or secrets.

## Step 11 — Optional: Streamlit locally against Redshift

```powershell
$env:DASHBOARD_MODE = "redshift"
.\.venv\Scripts\streamlit.exe run dashboard/app.py
```

Same `RS_*` env vars as step 8; the app reads Gold/serving tables directly.

## Step 12 — Destroy (the money-saving step)

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action destroy
```

Confirm in the console that EMR, MSK, and Redshift are gone. Keep the
bootstrap stack (state bucket + budget alarm) — it costs cents.

## What this run does NOT include

| Skipped | Why | How to add later |
|---|---|---|
| MWAA / Airflow DAG runs | ~$300/mo floor, bills 24/7 | `enable_mwaa = true` + `apply`, then `-Action mwaa-sync`, set Airflow Variables (`-Action airflow-vars`), trigger `warehouse_daily_batch_pipeline` |
| App Runner dashboard + Cognito | needs ECR image push + domain/ACM for auth | `enable_dashboard = true` two-step rollout; see `dashboard/README.md` |
| Throughput claims | 60 s producer run is correctness, not load | follow the benchmark protocol in `docs/evidence/README.md` |

Record results in `docs/evidence/README.md` after the run.
