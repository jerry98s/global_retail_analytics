# Running on Amazon Redshift

Redshift Serverless is the **only** data warehouse (see
[ADR-005](decisions/ADR-005-warehouse-redshift.md)). Flink and the POS batch job
write Parquet to S3; Redshift reads bronze **in place via Spectrum** — no `COPY`
step.

Dev and prod use the **same platform stack** (`infra/terraform/modules/redshift`);
only per-env tfvars differ (RPU sizing, `allowed_cidrs`, database name).

---

## What provisions what

| Piece | Path | Purpose |
|---|---|---|
| Platform stack | `infra/terraform/` | MSK + EMR + Redshift + S3 (+ optional MWAA + dashboard) |
| Redshift module | `infra/terraform/modules/redshift/` | Serverless namespace/workgroup, IAM (S3+Glue), Glue catalog DB, usage limit |
| Spectrum SQL | `transformation/redshift/spectrum/bronze_external_tables.sql` | `CREATE EXTERNAL SCHEMA` + three bronze external tables |
| POS batch | `ingestion/batch/generate_pos_parquet.py` | Daily Parquet → `bronze/pos_transactions/` |
| Flink bronze | `streaming/flink_jobs/*_bronze_job.py` | Clickstream + inventory raw events |
| dbt | `transformation/dbt_project/` | Staging → marts on Redshift |
| Airflow | `warehouse_daily_batch_pipeline` DAG | POS → dbt → GE checkpoint (00:15 UTC) |
| Serving DDL | `transformation/redshift/` | Hand-written Gold tables + views |
| Metadata DDL | `transformation/redshift/metadata/` | Separate `metadata` DB (`meta.*`); not namespace `db_name` |
| Summary | dbt `marts.summary` → schema `summary` | Daily rollups from one Gold fact each |

---

## Deploy (dev or prod)

```powershell
# 1. Platform stack (includes Redshift)
cd infra\terraform\envs
copy dev.backend.hcl.example dev.backend.hcl  # set bucket = your state bucket
copy dev.tfvars.example dev.tfvars            # or prod.* equivalents
cd ..\..\..
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply

# 2. Sync MWAA assets + submit Flink jobs (lands Parquet in env buckets)
.\scripts\cloud\deploy_platform.ps1 -Env dev

# 3. Spectrum external schema + tables
#    Edit transformation/redshift/spectrum/bronze_external_tables.sql with terraform outputs:
#      redshift_iam_role_arn, bronze_bucket_name, redshift_glue_bronze_database
#    Run as rs_admin against database 'dev' (or 'prod').

# 4. Gold DDL (if not already applied)
#    transformation/redshift/ddl/*.sql

# 5. dbt — locally or via Airflow warehouse_daily_batch_pipeline
$env:RS_HOST="<redshift_endpoint>"; $env:RS_PORT="5439"
$env:RS_USER="rs_admin"; $env:RS_PASSWORD="<secret>"
$env:RS_DATABASE="dev"
cd transformation/dbt_project
cp profiles.yml.example profiles.yml   # or export RS_* as in profiles.yml.example
dbt deps && dbt run && dbt test

# 6. Metadata database (same workgroup; Query Editor must switch DB)
.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -MetadataOnly
# Run target/redshift_metadata_create_dev.sql while connected to analytics DB (dev)
# Run target/redshift_metadata_schema_dev.sql while connected to database metadata

# 7. MWAA (when enable_mwaa = true)
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars
# Set redshift_password in MWAA UI; confirm redshift_metadata_database=metadata
# Then trigger warehouse_daily_batch_pipeline
```

Tear down: `.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action destroy`

Operational metadata lives in database `metadata` (see
[ADR-008](decisions/ADR-008-metadata-database.md) and
[platform-layers.md](data-model/platform-layers.md)). Glue remains the
Iceberg/Spectrum catalog.

---

## Bronze Spectrum tables

All three dbt bronze sources are defined in `transformation/redshift/spectrum/bronze_external_tables.sql`:

| Spectrum table | S3 path suffix | Producer |
|---|---|---|
| `bronze.clickstream_events` | `iceberg/bronze/clickstream_events/data/` | Flink `clickstream_bronze_job` |
| `bronze.inventory_events` | `iceberg/bronze/inventory_events/data/` | Flink `inventory_bronze_job` |
| `bronze.pos_transactions` | `iceberg/bronze/pos_transactions/data/` | `generate_pos_parquet.py` |

Run the Spectrum script **after** Flink jobs and at least one POS batch run have
written Parquet files, otherwise `COUNT(*)` sanity queries return zero rows.

Flink also writes **silver** `inventory_hourly`. `finance.fact_inventory_snapshot`
reads from **silver** (kappa architecture — see [ADR-007](./decisions/ADR-007-inventory-kappa.md)):
the Flink `inventory_silver_job` is the single owner of hourly aggregation and
dedup, and the dbt mart only adds the running-balance window + surrogate-key joins.
Bronze `inventory_events` is still loaded into Spectrum for audit/replay, but no
Gold model reads from it directly.

Naming rules: [docs/data-model/naming-conventions.md](./data-model/naming-conventions.md).

---

## SQL dialect, keys, incremental strategy

Models use native Redshift SQL (`cast`, `dateadd`, `delete+insert`, `MERGE` for SCD2).
Surrogate keys via dbt macros; grains documented in `docs/data-model/dimensional-model.md`.

---

## Cost & security

- Dev: smaller base RPU (8), `publicly_accessible=true` in `dev.tfvars.example` for laptop dbt — tighten `allowed_cidrs` to your IP/32.
- Prod: larger RPUs, VPC-only Redshift, monthly RPU-hour usage limit in Terraform.
- Passwords only in git-ignored tfvars or `TF_VAR_redshift_admin_password`.

---

## Airflow Variables (MWAA)

After apply, run `.\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars` and set values in
the MWAA UI. Required for `warehouse_daily_batch_pipeline`:

- From Terraform: `emr_cluster_id`, `artifacts_bucket`, `redshift_*`, `pos_bronze_s3_path`, …
- Manual secrets: `redshift_user`, `redshift_password`

Full list: `terraform output airflow_variables` or `.cursor/rules/airflow.mdc`.
