"""
Iceberg maintenance DAG.

Submits a Spark EMR step (spark/maintenance/iceberg_maintenance.py) that runs
Iceberg `rewrite_data_files` on the Flink streaming sinks and
`expire_snapshots` on every Bronze/Silver Iceberg table. Iceberg ships these
procedures for Spark only.

Schedule: 03:00 UTC daily — off-peak, after the warehouse_daily_batch_pipeline
finishes (~02:30 UTC). max_active_runs=1 plus the EMR step guard keep one run
in flight; a re-run on the same Iceberg state is a no-op.

Required Airflow Variables:
  - emr_cluster_id
  - artifacts_bucket
  - bronze_iceberg_warehouse
  - silver_iceberg_warehouse

Retention defaults (job arguments): Bronze 7 days, Silver 30 days, and the
last 5 snapshots of every table are always kept.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from alerting import notify_task_failure
from metadata_airflow import on_dag_failure, on_dag_start, on_dag_success

STEP_NAME = "iceberg-maintenance"
SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3"

DEFAULT_ARGS = {
    "owner":               "data-platform",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "execution_timeout":   timedelta(hours=1),
    "on_failure_callback": notify_task_failure,
}


def _iceberg_maintenance_step() -> dict:
    """EMR step: Spark Iceberg compaction + snapshot expiration."""
    cmd = (
        "aws s3 sync s3://{{ var.value.artifacts_bucket }}/spark /opt/spark-maintenance "
        "&& cd /opt/spark-maintenance/maintenance "
        f"&& spark-submit --master yarn --deploy-mode cluster --packages {SPARK_PACKAGES} "
        "iceberg_maintenance.py "
        "--bronze-warehouse {{ var.value.bronze_iceberg_warehouse }} "
        "--silver-warehouse {{ var.value.silver_iceberg_warehouse }}"
    )
    return {
        "Name":            STEP_NAME,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar":  "command-runner.jar",
            "Args": ["bash", "-lc", cmd],
        },
    }


def _no_existing_maintenance_step() -> bool:
    """Skip submission if a PENDING or RUNNING maintenance step exists."""
    cluster_id = Variable.get("emr_cluster_id")
    response = boto3.client("emr").list_steps(
        ClusterId=cluster_id,
        StepStates=["PENDING", "RUNNING"],
    )
    for step in response.get("Steps", []):
        if step.get("Name", "") == STEP_NAME:
            state = step.get("Status", {}).get("State", "UNKNOWN")
            print(f"EMR step {step['Id']} is {state}; skipping duplicate maintenance run.")
            return False
    return True


with DAG(
    dag_id              = "lakehouse_daily_iceberg_maintenance",
    default_args        = DEFAULT_ARGS,
    schedule_interval   = "0 3 * * *",
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["iceberg", "maintenance", "lakehouse"],
    doc_md              = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )

    check_no_existing = ShortCircuitOperator(
        task_id="check_no_existing_maintenance",
        python_callable=_no_existing_maintenance_step,
    )

    submit = EmrAddStepsOperator(
        task_id     = "submit_iceberg_maintenance",
        job_flow_id = "{{ var.value.emr_cluster_id }}",
        steps       = [_iceberg_maintenance_step()],
    )

    wait = EmrStepSensor(
        task_id       = "wait_iceberg_maintenance_completed",
        job_flow_id   = "{{ var.value.emr_cluster_id }}",
        step_id       = "{{ task_instance.xcom_pull('submit_iceberg_maintenance')[0] }}",
        target_states = {"COMPLETED"},
        poke_interval = 60,
        timeout       = 3000,
    )

    metadata_start >> check_no_existing >> submit >> wait
