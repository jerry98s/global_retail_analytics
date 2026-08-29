"""
Hourly Customer 360 refresh DAG.
Runs Spark GraphFrames identity resolution (EMR) → WAP clone live→pending →
clickstream/POS staging → C360 intermediate → marketing marts (pending) →
audits → WAP publish → C360 view refresh, on an hourly cadence
(ADR-002 ~75min SLA).

Identity resolution (ADR-010): the Spark job
`spark/identity_resolution/identity_resolution_job.py` rebuilds
`silver.identity_resolution` (Iceberg) from bronze clickstream + POS before
dbt runs; `int_identity_resolution` is a thin view over that source, so the
rest of the C360 chain is unchanged.

Write-Audit-Publish (ADR-009): live marketing/summary Gold is cloned into
`marketing_pending` / `summary_pending`, dbt rebuilds the marts there
(`wap_phase='pending'`), dbt tests audit them, and they are promoted to live only
on success. A failing run leaves live `marketing` / `summary` untouched. The
clone is what lets the incremental models resume from prior state.

The audit gate here is dbt tests; Great Expectations monitors live Gold hourly
via `quality_hourly_ge_checkpoint`.

Owns marketing Gold: dim_customer, fact_customer_session, identity_graph,
customer_360_view, serving.customer_360_serving (+ intermediate identity/session/
RFM/consent). Finance facts remain on warehouse_daily_batch_pipeline
(`marts.finance` only). dim_product SCD2 + int_product_catalog stay on
catalog_bihourly_product_scd2_refresh (explicit --exclude).
Also builds `summary.sessions_daily_platform`.

Enable MWAA and set the same Redshift Variables as warehouse_daily_batch_pipeline,
plus `emr_cluster_id`, `bronze_iceberg_warehouse`, `silver_iceberg_warehouse`,
`checkpoints_bucket`, and `pos_bronze_s3_path` for the Spark step.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from metadata_airflow import (
    dbt_bash_with_metadata,
    on_dag_failure,
    on_dag_start,
    on_dag_success,
)
from wap_publish import clone_marketing_task, publish_marketing_task

# WAP: staging + intermediate write in place; marketing/summary mart *tables*
# route to *_pending via wap_phase. Exclusions:
#   int_product_catalog / dim_product — owned by catalog_bihourly_product_scd2_refresh.
#   customer_360_view / customer_360_serving — views are never published, so they
#   are built after the swap (SELECT_SERVING) against live tables.
SELECT_PENDING = (
    "stg_clickstream_events stg_pos_transactions intermediate "
    "marts.marketing sessions_daily_platform "
    "--exclude int_product_catalog dim_product customer_360_view"
)
VARS_PENDING = '{"wap_phase": "pending"}'

SELECT_SERVING = "customer_360_view customer_360_serving"

# ADR-010: Spark + GraphFrames identity resolution on the existing EMR
# cluster (Spark 3.4 on emr-6.15.0; Iceberg runtime matches Flink's 1.4.3).
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,"
    "graphframes:graphframes:0.8.3-spark3.4-s_2.12"
)


def _spark_identity_step() -> dict:
    """EMR step: rebuild silver.identity_resolution from bronze via GraphFrames."""
    cmd = (
        "aws s3 sync s3://{{ var.value.artifacts_bucket }}/spark /opt/spark-identity "
        "&& cd /opt/spark-identity/identity_resolution "
        f"&& spark-submit --master yarn --deploy-mode cluster --packages {SPARK_PACKAGES} "
        "--py-files graph_logic.py identity_resolution_job.py "
        "--bronze-warehouse {{ var.value.bronze_iceberg_warehouse }} "
        "--silver-warehouse {{ var.value.silver_iceberg_warehouse }} "
        "--pos-parquet-path {{ var.value.pos_bronze_s3_path }} "
        "--checkpoint-dir s3://{{ var.value.checkpoints_bucket }}/graphframes/"
    )
    return {
        "Name":            "spark-identity-resolution",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar":  "command-runner.jar",
            "Args": ["bash", "-lc", cmd],
        },
    }


DEFAULT_ARGS = {
    "owner":            "data-platform",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-platform@company.com"],
}

with DAG(
    dag_id           = "marketing_hourly_customer_360_pipeline",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "0 * * * *",
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["batch", "marketing", "hourly"],
    doc_md           = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )

    # ADR-010: refresh silver.identity_resolution (Iceberg) before dbt reads
    # it. Batch step — the DAG waits for COMPLETED, unlike the long-running
    # streaming Flink steps.
    spark_identity = EmrAddStepsOperator(
        task_id     = "spark_identity_resolution",
        job_flow_id = "{{ var.value.emr_cluster_id }}",
        steps       = [_spark_identity_step()],
    )

    wait_spark_identity = EmrStepSensor(
        task_id       = "wait_spark_identity_resolution",
        job_flow_id   = "{{ var.value.emr_cluster_id }}",
        step_id       = "{{ task_instance.xcom_pull('spark_identity_resolution')[0] }}",
        target_states = {"COMPLETED"},
        poke_interval = 30,
        timeout       = 1800,
    )

    # WAP step 1: clone live marketing/summary Gold into the pending schemas so
    # the incremental models resume from prior state instead of full-refreshing.
    wap_clone = PythonOperator(
        task_id         = "wap_clone_live_to_pending",
        python_callable = clone_marketing_task,
    )

    dbt_run_pending = BashOperator(
        task_id      = "dbt_customer_360_write_pending",
        bash_command = dbt_bash_with_metadata(f"""
            aws s3 sync s3://{{{{ var.value.artifacts_bucket }}}}/mwaa/dbt_project /tmp/dbt_project
            aws s3 sync s3://{{{{ var.value.artifacts_bucket }}}}/mwaa/scripts /tmp/scripts
            cp /tmp/dbt_project/profiles.yml.example /tmp/dbt_project/profiles.yml
            cd /tmp/dbt_project && \
            dbt deps && \
            dbt run --select {SELECT_PENDING} --vars '{VARS_PENDING}' --target prod
        """),
    )

    dbt_test_pending = BashOperator(
        task_id      = "dbt_customer_360_audit_pending",
        bash_command = dbt_bash_with_metadata(f"""
            cd /tmp/dbt_project && \
            dbt test --select {SELECT_PENDING} --vars '{VARS_PENDING}' --target prod
        """),
    )

    wap_publish = PythonOperator(
        task_id         = "wap_publish_marketing",
        python_callable = publish_marketing_task,
    )

    # Build the C360 views against the freshly published live marketing Gold.
    # Views are not WAP-published, so this is the only place they are created.
    dbt_serving = BashOperator(
        task_id      = "dbt_customer_360_serving_refresh",
        bash_command = dbt_bash_with_metadata(f"""
            cd /tmp/dbt_project && \
            dbt run --select {SELECT_SERVING} --target prod
        """),
    )

    (
        metadata_start
        >> spark_identity
        >> wait_spark_identity
        >> wap_clone
        >> dbt_run_pending
        >> dbt_test_pending
        >> wap_publish
        >> dbt_serving
    )
