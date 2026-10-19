# Dashboard — Streamlit

A Streamlit app that visualizes the platform's data in two modes:

| Mode | Source | Use |
|------|--------|-----|
| `local` | Iceberg Parquet from the local Flink stack (`LOCAL_ICEBERG_DIR`, default `/tmp/iceberg`) | local dev / demos |
| `redshift` (default) | Gold + serving tables on Redshift Serverless via `RS_*` env vars | cloud (App Runner) |

The mode is chosen by `DASHBOARD_MODE`. `data.py` holds all data access (no
Streamlit import, so it stays testable); `app.py` is the UI.

## Run locally

### Against the local Iceberg volume (Docker)

With the main stack up (`.\scripts\local\run_local_stack.ps1 -Task up` + Flink jobs producing data):

```powershell
docker compose -f infra/docker/compose/docker-compose.yml -f infra/docker/compose/docker-compose.dashboard.yml up -d --build dashboard
# open http://localhost:8501
```

This mounts the shared `flink-iceberg` volume read-only, so the dashboard reads
the same Parquet the Flink jobs commit.

### Against Redshift (from your machine)

```powershell
$env:DASHBOARD_MODE = "redshift"
$env:RS_HOST = "<workgroup-endpoint>"; $env:RS_DATABASE = "prod"
$env:RS_USER = "rs_admin"; $env:RS_PASSWORD = "<secret>"
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## Deploy on AWS (App Runner)

The `infra/terraform/modules/streamlit` module (wired into the platform stack)
creates an ECR repo, IAM roles, a Secrets Manager secret for `RS_PASSWORD`, an
optional VPC connector, and the App Runner service. App Runner needs the image
to exist at create time, so roll out in two steps:

```powershell
# 1) Create ECR (+ IAM/secret); service stays off
#    in prod.tfvars: enable_dashboard = true ; dashboard_create_service = false
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env prod -Action apply

# 2) Build + push the image to the ECR repo from the apply output
$repo = terraform -chdir=infra/terraform output -raw dashboard_ecr_repository_url
$region = "ap-southeast-1"
aws ecr get-login-password --region $region | docker login --username AWS --password-stdin ($repo -split '/')[0]
docker build -t "${repo}:latest" .\dashboard
docker push "${repo}:latest"

# 3) Turn the service on
#    in prod.tfvars: dashboard_create_service = true
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env prod -Action apply
terraform -chdir=infra/terraform output dashboard_url
```

### Reaching a VPC-only Redshift

When `redshift_publicly_accessible = false`, set
`dashboard_vpc_connector_subnet_ids` to private subnets and make sure those
subnets' CIDRs are inside `redshift_allowed_cidrs` so the App Runner VPC
connector can reach Redshift on `5439`.

### Login: Cognito + ALB in front of App Runner

By default the App Runner URL is public to anyone who finds it. Set
`dashboard_enable_auth = true` (see the commented block in
`infra/terraform/envs/prod.tfvars.example`) and Terraform instead makes the
service private and fronts it with an ALB that requires a Cognito Hosted UI
login:

```
internet -> ALB (HTTPS + Cognito) -> VPC interface endpoint -> App Runner (private)
```

Prerequisites, because they can't be created fully from Terraform:

- a **domain you control** (`dashboard_auth_domain_name`), and
- an **ACM certificate** for it in the stack's region
  (`dashboard_acm_certificate_arn`; request it in the ACM console and complete
  DNS validation before applying),
- **public subnets** for the ALB (`dashboard_alb_subnet_ids`, >= 2 AZs) and
  subnets for the endpoint ENIs (`dashboard_vpce_subnet_ids`; the same private
  subnets as the VPC connector are fine).

After apply:

```powershell
# 1) Point your domain at the ALB (CNAME or Route53 alias)
terraform -chdir=infra/terraform output dashboard_alb_dns_name

# 2) Create at least one login (users are not managed in Terraform, so no
#    passwords touch state). Cognito emails a temporary password.
$pool = terraform -chdir=infra/terraform output -raw dashboard_cognito_user_pool_id
aws cognito-idp admin-create-user --user-pool-id $pool --username you@example.com
```

Extra cost: ALB (~$16/mo + LCU usage) plus one interface endpoint per AZ
(~$7/mo each); Cognito stays in free tier at demo scale. HTTP:80 on the ALB
redirects to HTTPS, and `dashboard_auth_allowed_cidrs` can further restrict who
reaches the login page at all.
