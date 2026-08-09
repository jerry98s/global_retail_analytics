"""
Hourly Customer 360 refresh DAG.
Runs WAP clone live→pending → clickstream/POS staging → C360 intermediate →
marketing marts (pending) → audits → WAP publish → C360 view refresh, on an
hourly cadence (ADR-002 ~75min SLA).

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

Enable MWAA and set the same Redshift Variables as warehouse_daily_batch_pipeline.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    tags             = ["batch", "marketing", "hourly"],
    doc_md           = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
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
        >> wap_clone
        >> dbt_run_pending
        >> dbt_test_pending
        >> wap_publish
        >> dbt_serving
    )
