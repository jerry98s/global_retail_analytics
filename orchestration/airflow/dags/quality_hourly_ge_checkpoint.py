"""
Great Expectations checkpoint DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from metadata_airflow import (
    ge_bash_with_metadata,
    on_dag_failure,
    on_dag_start,
    on_dag_success,
)

DAG_DOC_MD = """
## quality_hourly_ge_checkpoint

Runs the `gold_layer_daily` Great Expectations checkpoint hourly at :45
(off-peak between Flink silver writes at :00 and the daily batch at 02:00).

### Purpose
Re-validates Gold mart data quality on a cadence faster than the daily dbt
build, so downstream BI / Customer 360 dashboards catch issues (PK drift,
null marketing_consent, range violations on churn_risk_score, etc.) within
an hour rather than at the next daily run.

### Checkpoint
`quality/great_expectations/checkpoints/gold_layer_daily.yml` covers:
- `finance.fact_inventory_snapshot` (PK uniqueness, quantity ranges,
  `is_estimated` accepted_values)
- `finance.fact_sales` (line-item PK, revenue consistency, no-fact-to-fact)
- `marketing.dim_customer` (PK, consent flags, RFM/loyalty domain checks)
- `marketing.dim_product` (SCD2 invariants, valid_to >= valid_from)
- `marketing.fact_customer_session` (session PK, conversion flags)
- `marketing.identity_graph` (active edges, public-device exclusion)
- `marketing.customer_360_view` (derived columns, churn_risk_score range)
- `bronze.inventory_events` (last 24h, event_type domain)
- `bronze.clickstream_events` (last 24h, event_type / platform / schema_version)

### Idempotency
GE checkpoints are read-only validations — re-running produces the same
result for the same data state. Airflow retries (1 retry, 5 min delay) are
safe; a second run will not double-write anywhere.

### Failure handling
- A failed expectation emits a `ValidationException`. The BashOperator
  exits non-zero, the task is marked `failed`, and (in MWAA) an email is
  sent to the `data-platform` AlertEmail SNS subscription.
- The warehouse_daily_batch_pipeline DAG's `ge_checkpoint` task runs the same
  checkpoint after the dbt build — failures here likely indicate an
  upstream Silver/Iceberg regression, not a dbt build issue.
- For investigation: GE validation results are written to
  `s3://retail-platform-<env>-ge-results/<run_id>/` (configured in
  `great_expectations.yml`).

### Related
- `quality/great_expectations/checkpoints/gold_layer_daily.yml`
- `quality/great_expectations/expectations/*.json`
- `docs/runbooks/data-quality-failures.md`
"""

with DAG(
    dag_id="quality_hourly_ge_checkpoint",
    default_args={
        "owner": "data-platform",
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
        "email": ["data-platform@company.com"],
    },
    schedule_interval="45 * * * *",
    catchup=False,
    tags=["quality", "ge"],
    doc_md=DAG_DOC_MD,
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:
    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )
    run_gold_checkpoint = BashOperator(
        task_id="run_gold_layer_checkpoint",
        bash_command=ge_bash_with_metadata(
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts && "
            "cd /opt/airflow && "
            "great_expectations checkpoint run gold_layer_daily"
        ),
    )
    metadata_start >> run_gold_checkpoint
