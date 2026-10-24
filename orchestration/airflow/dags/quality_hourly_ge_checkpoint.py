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

Runs the `silver_events_daily` Great Expectations checkpoint hourly at :45
(off-peak between Flink silver writes at :00 and the daily batch at 02:00).

### Purpose
Checks the canonical Silver event contract between the hourly and daily
builds, so a producer or Spark promotion regression shows up within an hour.
Gold tables change only through a WAP publish, which each owning DAG gates
with dbt tests; GE does not re-check them.

### Checkpoint
`quality/great_expectations/checkpoints/silver_events_daily.yml` covers:
- `silver.inventory_events` (last 24h, event_type domain)
- `silver.clickstream_events` (last 24h, event_type / platform / schema_version)

### Idempotency
GE checkpoints are read-only validations — re-running produces the same
result for the same data state. Airflow retries (1 retry, 5 min delay) are
safe; a second run will not double-write anywhere.

### Failure handling
- A failed expectation emits a `ValidationException`. The BashOperator
  exits non-zero, the task is marked `failed`, and (in MWAA) an email is
  sent to the `data-platform` AlertEmail SNS subscription.
- For investigation: GE validation results are written to
  `s3://retail-platform-<env>-ge-results/<run_id>/` (configured in
  `great_expectations.yml`).

### Related
- `quality/great_expectations/checkpoints/silver_events_daily.yml`
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
    run_silver_checkpoint = BashOperator(
        task_id="run_silver_events_checkpoint",
        # The GE CLI cannot run runtime-query checkpoints; use the shared runner.
        bash_command=ge_bash_with_metadata(
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/quality/great_expectations /tmp/great_expectations && "
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts && "
            "python /tmp/scripts/common/run_ge_checkpoint.py "
            "--ge-root /tmp/great_expectations --checkpoint silver_events_daily"
        ),
    )
    metadata_start >> run_silver_checkpoint
