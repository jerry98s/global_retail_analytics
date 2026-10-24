"""
Daily batch pipeline DAG.
Orchestrates: POS Parquet bronze → Spectrum partition register → dbt staging
(owned models) → WAP clone live→pending → dbt marts (pending) → dbt tests
(pending) → row-count gate (pending) → WAP publish to live → Redshift ANALYZE →
Redshift usage check.

Each dbt task syncs its own copy of the project (`dbt_task_bash`), because
consecutive tasks can land on different MWAA workers. Staging/intermediate
models are built only by their owning DAG (`plugins/dbt_selectors.py`).
Failed tasks alert through SNS (`alert_topic_arn`); freshness SLAs are checked
hourly by `quality_hourly_ge_checkpoint`.

Write-Audit-Publish (ADR-009): live Gold is cloned into `*_pending`, dbt rebuilds
the marts there (`wap_phase='pending'`), dbt tests audit the pending copies,
and only then are they promoted into live `finance` / `summary`. A failing audit
leaves live untouched, so consumers keep reading the last good publish. The clone
is what preserves incremental state: without it every model would take its
full-refresh branch.

This DAG owns the finance marts and their summary rollups only.
`max_active_runs=1` serializes overlapping schedules/backfills so two runs
cannot drop/reclone the same `*_pending` tables. `marketing.dim_product`
belongs to `catalog_bihourly_product_scd2_refresh` and is
read live via the `wap_live_ref` macro; marketing marts belong to
`marketing_hourly_customer_360_pipeline`. `finance.dim_date` /
`finance.dim_store` are stable seed reference dims and are never published.

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
  - alert_topic_arn             : SNS topic for task-failure alerts

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
from alerting import notify_task_failure
from dbt_selectors import WAREHOUSE_LIVE_MODELS, select
from row_count_reconciliation import reconcile_gold_row_counts_task
from wap_publish import (
    FINANCE_SUMMARY_TABLES,
    clone_finance_summary_task,
    publish_finance_summary_task,
)
from metadata_airflow import (
    dbt_task_bash,
    on_dag_failure,
    on_dag_start,
    on_dag_success,
)

# Models this DAG owns end-to-end (clone -> build -> audit -> publish).
# marts.marketing and sessions_daily_platform belong to the hourly marketing DAG;
# dim_product belongs to catalog_bihourly_product_scd2_refresh.
OWNED_SELECT = (
    "marts.finance sales_daily_store inventory_daily_product_store inventory_position_daily"
)

# Audits must cover exactly what this DAG built. A bare `dbt test` would also run
# marketing suites whose pending tables this DAG never clones.
TEST_SELECT = select(WAREHOUSE_LIVE_MODELS, OWNED_SELECT)

# Same set as "schema.table" strings, for the reconciliation audit.
# Derived from the publish list so the two can never drift apart.
OWNED_TABLES = [f"{schema}.{table}" for schema, table in FINANCE_SUMMARY_TABLES]

DEFAULT_ARGS = {
    "owner":               "data-platform",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "retries":             2,
    "retry_delay":         timedelta(minutes=10),
    "execution_timeout":   timedelta(hours=1),
    "on_failure_callback": notify_task_failure,
}


def _dbt_step(
    task_id: str,
    dbt_cmd: str,
    selector: str,
    *,
    vars_json: str | None = None,
) -> BashOperator:
    """dbt BashOperator; the helper syncs the project and records run_results."""
    vars_arg = f" --vars '{vars_json}'" if vars_json else ""
    return BashOperator(
        task_id=task_id,
        bash_command=dbt_task_bash(f"{dbt_cmd} --select {selector}{vars_arg}"),
    )


with DAG(
    dag_id           = "warehouse_daily_batch_pipeline",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "15 0 * * *",
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["batch", "core", "daily"],
    doc_md           = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )

    # The generator imports ingestion.kafka.sim_entities, so the whole package
    # is synced and run as a module from the work directory.
    generate_pos_parquet = BashOperator(
        task_id      = "generate_pos_parquet",
        bash_command = """
            set -e
            WORK="$(mktemp -d /tmp/pos_task.XXXXXX)"
            trap 'rm -rf "$WORK"' EXIT
            aws s3 sync --only-show-errors s3://{{ var.value.artifacts_bucket }}/ingestion "$WORK/ingestion"
            cd "$WORK"
            python -m ingestion.batch.generate_pos_parquet \
                --date {{ ds }} \
                --output-s3 {{ var.value.pos_bronze_s3_path }}
        """,
    )

    # Spectrum does not auto-discover Hive partitions. Without this, today's
    # dt=<ds> directory is invisible and fact_sales incremental reads yesterday.
    # ADD IF NOT EXISTS keeps the task idempotent on DAG retry (S3 object is
    # overwritten in place; the partition location does not change).
    register_pos_partition = RedshiftDataOperator(
        task_id        = "register_pos_spectrum_partition",
        workgroup_name = "{{ var.value.redshift_workgroup_name }}",
        database       = "{{ var.value.redshift_database }}",
        sql            = """
            ALTER TABLE bronze.pos_transactions
            ADD IF NOT EXISTS PARTITION (dt='{{ ds }}')
            LOCATION '{{ var.value.pos_bronze_s3_path }}data/dt={{ ds }}/'
        """,
    )

    dbt_staging = _dbt_step(
        "dbt_staging_and_intermediate",
        "run",
        select(WAREHOUSE_LIVE_MODELS),
    )

    # WAP step 1: copy live Gold into the pending schemas so the incremental
    # models have their prior state and dbt takes the incremental branch.
    wap_clone = PythonOperator(
        task_id         = "wap_clone_live_to_pending",
        python_callable = clone_finance_summary_task,
    )

    dbt_marts = _dbt_step(
        "dbt_mart_models",
        "run",
        OWNED_SELECT,
        vars_json='{"wap_phase": "pending"}',
    )

    # Audits run against the pending schemas; live is untouched until publish.
    dbt_tests = _dbt_step(
        "dbt_tests", "test", TEST_SELECT, vars_json='{"wap_phase": "pending"}'
    )

    # Publish gate: an owned table that shrank or cannot be read fails here.
    row_count_reconcile = PythonOperator(
        task_id          = "row_count_reconciliation",
        python_callable  = reconcile_gold_row_counts_task,
        op_kwargs        = {"pending_tables": OWNED_TABLES},
    )

    # Audit gate passed — atomically promote pending marts into live schemas.
    wap_publish = PythonOperator(
        task_id         = "wap_publish_gold",
        python_callable = publish_finance_summary_task,
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
        >> register_pos_partition
        >> dbt_staging
        >> wap_clone
        >> dbt_marts
        >> dbt_tests
        >> row_count_reconcile
        >> wap_publish
        >> redshift_analyze
        >> cost_check
    )
