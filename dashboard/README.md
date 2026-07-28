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

> App Runner ingress is public HTTPS. Put it behind your own auth (e.g. an
> identity-aware proxy) before exposing real data to the internet.
