# Scripts

Operational scripts for the cloud deploy, plus the shared offline helpers.
The laptop stack (`run_local_stack.ps1`, demos, benchmarks) is on
`local-testing-version`; it is not in this branch.

## `scripts/cloud/`

| Script | Purpose |
|--------|---------|
| `deploy_platform.ps1` | The deploy entry point. `-Env dev|prod`, `-Action all\|flink\|spark\|mwaa-sync\|airflow-vars\|status\|verify`. Syncs MWAA plugins, submits Flink, syncs/submits the Spark identity job. |
| `run_terraform.ps1` | Terraform wrapper for the platform stack (init/plan/apply per env). |
| `run_cloud_stack.ps1` | Brings up / tears down the AWS resources for an env. |
| `run_msk_producers.ps1` | Runs the Kafka producers against MSK for a live run. |
| `bootstrap_redshift.ps1` | Generates the Redshift bootstrap SQL (DDL + seeds + Spectrum) from Terraform outputs, for Query Editor v2. |

## `scripts/common/`

| Script | Purpose |
|--------|---------|
| `metadata_observer.py` | Operational metadata collector. Writes `local_metadata.duckdb` (or the Redshift metadata DB), never Gold. Fail-open. |
| `run_ge_checkpoint.py` | Runs a Great Expectations checkpoint (Silver event contract) against Redshift (`RS_SQLALCHEMY_URL`). The GE CLI cannot interpolate env vars inside query strings, so this loads the checkpoint and calls the validator. |
| `generate_semantic_queries.py` | Compiles the dashboard's metric queries through MetricFlow (ADR-013) into `dashboard/semantic_queries.py`; CI `--check`. |
