# Environments

This branch runs the critical flow on a laptop. The AWS deploy (MSK, EMR,
Redshift, MWAA, Terraform) is on `main` and is not in this checkout.

| Flavour | Where | What runs |
|---|---|---|
| **local** | this branch | Docker Compose: Kafka + Flink + Spark, Iceberg on `.local/iceberg`, Gold in DuckDB |
| **cloud** | `main` | The same pipeline on AWS |

## Run it

```powershell
uv sync --group dev
.\scripts\local\run_local_stack.ps1 -Task all
```

`-Task all` starts Kafka, creates topics, starts Flink, produces events, writes
POS Parquet, runs the Spark identity job, loads Iceberg into DuckDB, builds
dbt, and runs quality checks. Start Flink before simulate yourself when you
run the tasks one at a time (`latest-offset`).

Kafka: `127.0.0.1:9092`. Flink UI: <http://localhost:8082>.

Default dbt source is `iceberg` (Flink and Spark Parquet). `-DbtSource seeds`
uses the CSV identity fixtures instead.

## What is shared

Flink jobs, the Spark identity job, dbt models, Airflow DAG definitions, and
Great Expectations suites are the pipeline. On this branch dbt targets DuckDB
(`--target local`). Profiles are copied from `profiles.yml.example` and are
not committed.
