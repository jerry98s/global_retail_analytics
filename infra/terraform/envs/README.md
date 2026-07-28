# Platform stack per-env config

The **platform** stack (`infra/terraform/`) is the single deployment path for
cloud **dev** and **prod**. Same modules, same scripts — only tfvars differ.

## Files

| File | Tracked? | Purpose |
|---|---|---|
| `dev.backend.hcl.example` / `prod.backend.hcl.example` | yes | Backend templates with a placeholder bucket |
| `dev.backend.hcl` / `prod.backend.hcl` | **no** (gitignored) | Your state bucket + key per env |
| `dev.tfvars.example` / `prod.tfvars.example` | yes | Templates with placeholders |
| `dev.tfvars` / `prod.tfvars` | **no** (gitignored) | Real VPC IDs, secrets |

Backend configs and tfvars are gitignored because they embed account-specific
values (state bucket name, account ID, VPC/subnet IDs, Redshift password). Only
the `*.example` templates are committed.

Adding **stage**: copy `dev.backend.hcl.example` → `stage.backend.hcl` (change key to
`platform/stage/terraform.tfstate`), copy `dev.tfvars.example` → `stage.tfvars`,
and add `stage` to `scripts/cloud/deploy_platform.ps1` / `scripts/cloud/run_terraform.ps1` ValidateSet if needed.

## First-time apply (dev)

The bootstrap stack creates the state bucket + lock table, so run it before the
platform stack's `init`.

```powershell
# 0. Bootstrap: state bucket + DynamoDB locks (once per AWS account)
cd infra\terraform\bootstrap
copy terraform.tfvars.example terraform.tfvars   # set state_bucket_name + budget_alert_email
cd ..\..\..
.\scripts\cloud\run_terraform.ps1 -Stack bootstrap -Action apply

# 1. Per-env backend + vars
cd infra\terraform\envs
copy dev.backend.hcl.example dev.backend.hcl   # set bucket = the state_bucket_name above
copy dev.tfvars.example dev.tfvars             # edit: VPC, subnets, Redshift password, enable_mwaa
cd ..\..\..

.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action init
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action plan
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply
```

## Post-apply runtime

```powershell
# Sync MWAA assets (DAGs, dbt, GE) + submit Flink jobs
.\scripts\cloud\deploy_platform.ps1 -Env dev

# Print Airflow Variable values for MWAA UI
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars

# Inspect stack
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action status
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action output
```

Then run `transformation/redshift/spectrum/bronze_external_tables.sql` in Redshift (see `docs/REDSHIFT.md`).

## Optional flags in tfvars

| Variable | Default | Purpose |
|---|---|---|
| `enable_mwaa` | `false` | Managed Airflow environment |
| `enable_dashboard` | `false` | App Runner Streamlit dashboard |

When `enable_mwaa = true`, set Airflow Variables from `terraform output airflow_variables`
plus `redshift_user` / `redshift_password` from tfvars.

## State key migration (existing prod deploys)

If you previously applied with `key = "prod/terraform.tfstate"`, either:

- leave the old state in place and keep using that key in `prod.backend.hcl`, or
- migrate once: `terraform state pull` → update backend key → `terraform state push`

New deploys should use `platform/prod/terraform.tfstate` as committed.
