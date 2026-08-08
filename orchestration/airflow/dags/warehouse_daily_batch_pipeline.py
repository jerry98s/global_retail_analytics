"""
Daily batch pipeline DAG.
Orchestrates: POS Parquet bronze → dbt staging → dbt SCD2 → dbt marts →
Redshift ANALYZE → dbt tests → row-count reconciliation → GE checkpoint →
Redshift usage check.

Schedule: 00:15 UTC daily (15-min buffer for late clickstream events).

Required Airflow Variables (seed from `.\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars`):
  - emr_cluster_id              : j-XXXXXXXXX (streaming DAG only; not used here)
  - artifacts_bucket            : retail-platform-<env>-artifacts
  - redshift_workgroup_name     : Redshift Serverless workgroup
  - redshift_database           : Redshift database name
  - redshift_metadata_database  : Operational metadata database (default metadata)
  - redshift_host               : Redshift workgroup endpoint (no port)
  - redshift_user               : Redshift admin user
  - redshift_secret_arn         : Secrets Manager ARN of the Redshift password.
                                  Tasks fetch the value at runtime, so the
                                  password itself is never an Airflow Variable.
  - pos_bronze_s3_path          : s3://<bronze-bucket>/iceberg/bronze/pos_transactions/

Optional Airflow Variables for row-count reconciliation (P2.5):
  - gold_row_counts_baseline    : JSON map of {"schema.table": row_count}; default "{}".
                                  Seeded automatically on the first clean run.
  - row_count_delta_threshold   : float in [0,1]; default 0.20 (warn on >20% day-over-day delta).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# MWAA loads modules under plugins/ onto sys.path, so this imports cleanly
# in the cloud runtime. Local `python -m compileall` skips Airflow imports
# (compileall does not execute imports).
from row_count_reconciliation import reconcile_gold_row_counts_task
from metadata_airflow import (
    collect_freshness_task,
    dbt_bash_with_metadata,
    ge_bash_with_metadata,
    on_dag_failure,
    on_dag_start,
    on_dag_success,
)

DEFAULT_ARGS = {
    "owner":            "data-platform",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
    "email":            ["data-platform@company.com"],
}


def _dbt_step(
    task_id: str,
    dbt_cmd: str,
    selector: str,
    *,
    vars_json: str | None = None,
    bootstrap: bool = False,
) -> BashOperator:
    """Factory for dbt BashOperator tasks; wraps result parsing in-process."""
    selector_arg = f"--select {selector}" if selector else ""
    vars_arg = f"--vars '{vars_json}'" if vars_json else ""
    dbt_cmd_str = f"dbt {dbt_cmd} {selector_arg} --target prod {vars_arg}".strip()
    if bootstrap:
        inner = (
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/dbt_project /tmp/dbt_project && "
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts && "
            "cp /tmp/dbt_project/profiles.yml.example /tmp/dbt_project/profiles.yml && "
            "cd /tmp/dbt_project && dbt deps && "
            f"{dbt_cmd_str}"
        )
    else:
        inner = f"cd /tmp/dbt_project && {dbt_cmd_str}"
    # No env= here: dbt_bash_with_metadata exports RS_HOST/RS_USER/RS_DATABASE
    # and resolves RS_PASSWORD from Secrets Manager inside the task shell.
    return BashOperator(
        task_id=task_id,
        bash_command=dbt_bash_with_metadata(inner),
    )


with DAG(
    dag_id           = "warehouse_daily_batch_pipeline",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "15 0 * * *",
    catchup          = False,
    tags             = ["batch", "core", "daily"],
    doc_md           = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )

    generate_pos_parquet = BashOperator(
        task_id      = "generate_pos_parquet",
        bash_command = """
            aws s3 sync s3://{{ var.value.artifacts_bucket }}/ingestion/batch /tmp/ingestion/batch
            python /tmp/ingestion/batch/generate_pos_parquet.py \
                --date {{ ds }} \
                --output-s3 {{ var.value.pos_bronze_s3_path }}
        """,
    )

    dbt_staging = _dbt_step(
        "dbt_staging_and_intermediate",
        "run",
        "staging intermediate",
        bootstrap=True,
        vars_json='{"run_date": "{{ ds }}"}',
    )

    dbt_scd2 = _dbt_step(
        "dbt_catalog_bihourly_product_scd2_refresh",
        "run",
        "int_product_catalog dim_product",
    )

    dbt_marts = _dbt_step(
        "dbt_mart_models",
        "run",
        # Finance marts + finance-owned summary rollups. Marketing sessions
        # summary is owned by marketing_hourly_customer_360_pipeline.
        "marts.finance sales_daily_store inventory_daily_product_store",
        vars_json='{"run_date": "{{ ds }}"}',
    )

    redshift_analyze = RedshiftDataOperator(
        task_id        = "redshift_analyze_statistics",
        workgroup_name = "{{ var.value.redshift_workgroup_name }}",
        database       = "{{ var.value.redshift_database }}",
        sql            = """
            ANALYZE finance;
            ANALYZE marketing;
            ANALYZE summary;
            ANALYZE serving;
        """,
    )

    dbt_tests = _dbt_step("dbt_tests", "test", "")

    row_count_reconcile = PythonOperator(
        task_id          = "row_count_reconciliation",
        python_callable  = reconcile_gold_row_counts_task,
    )

    metadata_freshness = PythonOperator(
        task_id="metadata_collect_freshness",
        python_callable=collect_freshness_task,
    )

    ge_checkpoint = BashOperator(
        task_id      = "ge_gold_checkpoint",
        # RS_SQLALCHEMY_URL is built by ge_bash_with_metadata from the secret.
        bash_command = ge_bash_with_metadata("""
            aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/quality/great_expectations /tmp/great_expectations
            aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts
            cd /tmp/great_expectations && \
            great_expectations checkpoint run gold_layer_daily
        """),
    )

    cost_check = RedshiftDataOperator(
        task_id        = "redshift_usage_check",
        workgroup_name = "{{ var.value.redshift_workgroup_name }}",
        database       = "{{ var.value.redshift_database }}",
        sql            = """
            SELECT COALESCE(SUM(compute_seconds) / 3600.0, 0) AS rpu_hours_today
            FROM sys_serverless_usage
            WHERE start_time >= CURRENT_DATE
        """,
    )

    (
        metadata_start
        >> generate_pos_parquet
        >> dbt_staging
        >> dbt_scd2
        >> dbt_marts
        >> redshift_analyze
        >> dbt_tests
        >> row_count_reconcile
        >> metadata_freshness
        >> ge_checkpoint
        >> cost_check
    )
