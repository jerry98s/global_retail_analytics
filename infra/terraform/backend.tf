# Partial backend config — `key` is set at init time via
#   terraform init -reconfigure -backend-config=envs/<env>.backend.hcl
# so the same code base owns a separate state file per env. Use the
# `scripts/cloud/run_terraform.ps1` wrapper to make sure backend and tfvars stay in sync.
terraform {
  backend "s3" {}
}
