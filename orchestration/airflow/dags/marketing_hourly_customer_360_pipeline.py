"""
Hourly Customer 360 refresh DAG.
Runs clickstream/POS staging → C360 intermediate → marketing marts on an hourly
cadence (ADR-002 ~75min SLA).

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

    dbt_customer_360 = BashOperator(
        task_id      = "dbt_customer_360_refresh",
        bash_command = dbt_bash_with_metadata("""
            aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/dbt_project /tmp/dbt_project
            aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts
            cp /tmp/dbt_project/profiles.yml.example /tmp/dbt_project/profiles.yml
            cd /tmp/dbt_project && \
            dbt deps && \
            dbt run --select stg_clickstream_events stg_pos_transactions intermediate marts.marketing sessions_daily_platform customer_360_serving --exclude int_product_catalog dim_product --target prod && \
            dbt test --select stg_clickstream_events stg_pos_transactions intermediate marts.marketing sessions_daily_platform customer_360_serving --exclude int_product_catalog dim_product --target prod
        """),
    )

    metadata_start >> dbt_customer_360
