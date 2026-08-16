"""
Hourly Customer 360 refresh DAG.
Runs clickstream/POS staging → C360 intermediate → marketing marts (pending) →
audits → WAP publish → serving refresh, on an hourly cadence (ADR-002 ~75min
SLA).

Write-Audit-Publish (ADR-009): marketing Gold marts build into
`marketing_pending` / `summary_pending` (`wap_phase='pending'`), are audited
there by dbt tests, and are promoted to live only on success. A failing run
leaves live `marketing` / `summary` untouched.

Owns marketing Gold: dim_customer, fact_customer_session, identity_graph,
customer_360_view, serving.customer_360_serving (+ intermediate identity/session/
RFM/consent). Finance facts remain on warehouse_daily_batch_pipeline
(`marts.finance` only). dim_product SCD2 + int_product_catalog stay on
catalog_bihourly_product_scd2_refresh / warehouse (explicit --exclude).
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
from wap_publish import publish_marketing_task

# WAP: staging + intermediate write in place; marketing/summary marts route to
# *_pending via wap_phase. dim_product / int_product_catalog stay excluded
# (owned by catalog_bihourly_product_scd2_refresh / warehouse DAG).
SELECT_PENDING = (
    "stg_clickstream_events stg_pos_transactions intermediate "
    "marts.marketing sessions_daily_platform "
    "--exclude int_product_catalog dim_product"
)
VARS_PENDING = '{"wap_phase": "pending"}'

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

    # Rebuild the serving view against the freshly published live marketing Gold.
    dbt_serving = BashOperator(
        task_id      = "dbt_customer_360_serving_refresh",
        bash_command = dbt_bash_with_metadata("""
            cd /tmp/dbt_project && \
            dbt run --select customer_360_serving --target prod
        """),
    )

    metadata_start >> dbt_run_pending >> dbt_test_pending >> wap_publish >> dbt_serving
