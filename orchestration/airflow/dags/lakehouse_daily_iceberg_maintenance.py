"""
Iceberg maintenance DAG.

Submits a one-shot Flink batch job that runs Iceberg `rewrite_data_files`
(compaction) and `expire_snapshots` on all three Iceberg tables
(bronze.inventory_events, bronze.clickstream_events, silver.inventory_hourly).

Closes DL-D from the data lake checklist applied 2026-07-05.

Schedule: 03:00 UTC daily — off-peak, after the warehouse_daily_batch_pipeline
finishes (~02:30 UTC) and before the next hourly customer_360 run.

Idempotency:
  This DAG runs `flink run` (batch mode, not `run-application`) so the
  step exits when the maintenance procedures complete. A re-run for the
  same Iceberg state is a no-op (nothing to compact, nothing to expire).
  The ShortCircuitOperator guard prevents duplicate in-flight steps
  (same pattern as streaming_manual_flink_jobs DAG — P1.4).

Required Airflow Variables (same as streaming_manual_flink_jobs):
  - emr_cluster_id
  - artifacts_bucket
  - bronze_iceberg_warehouse
  - silver_iceberg_warehouse

Optional tuning Variables / env overrides (set as Airflow Variables and
pass through the step's env block):
  - ICEBERG_MIN_INPUT_FILES         (default 5)
  - ICEBERG_TARGET_FILE_SIZE_BYTES  (default 268435456 = 256MB)
  - ICEBERG_SNAPSHOTS_RETAIN_LAST   (default 5)
  - BRONZE_SNAPSHOT_RETENTION_DAYS  (default 7)
  - SILVER_SNAPSHOT_RETENTION_DAYS  (default 30)

Revisit triggers (per ADR-006):
  If Iceberg bronze/silver exceeds 10,000 data files per partition, or
  snapshot count exceeds 500, increase the daily maintenance frequency
  to twice-daily (06:00 + 18:00 UTC) or migrate the maintenance job to
  Spark on the same EMR cluster for tighter control over parallelism.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.models import Variable

from metadata_airflow import on_dag_failure, on_dag_start, on_dag_success


DEFAULT_ARGS = {
    "owner":            "data-platform",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-platform@company.com"],
}


def _iceberg_maintenance_step() -> dict:
    """Build an EMR step that runs the Iceberg maintenance Flink batch job."""
    cmd = (
        "export KAFKA_BOOTSTRAP_SERVERS='{{ var.value.msk_bootstrap_brokers }}' "
        "ICEBERG_BRONZE_WAREHOUSE='{{ var.value.bronze_iceberg_warehouse }}' "
        "ICEBERG_SILVER_WAREHOUSE='{{ var.value.silver_iceberg_warehouse }}' "
        "&& aws s3 sync s3://{{ var.value.artifacts_bucket }}/streaming /opt/flink-config-src "
        "&& flink run -t yarn-application "
        "-Dyarn.application.name=iceberg-maintenance "
        "-Dexecution.attached=true "
        "-pyfs /opt/flink-config-src/flink_jobs "
        "-py /opt/flink-config-src/flink_jobs/iceberg_maintenance.py"
    )
    return {
        "Name":            "iceberg-maintenance",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar":  "command-runner.jar",
            "Args": ["bash", "-lc", cmd],
        },
    }


def _no_existing_maintenance_step() -> bool:
    """Skip submission if a PENDING or RUNNING iceberg-maintenance step exists."""
    cluster_id = Variable.get("emr_cluster_id")
    emr_client = boto3.client("emr")
    response = emr_client.list_steps(
        ClusterId=cluster_id,
        StepStates=["PENDING", "RUNNING"],
    )
    for step in response.get("Steps", []):
        if step.get("Name", "").startswith("iceberg-maintenance"):
            state = step.get("Status", {}).get("State", "UNKNOWN")
            print(
                f"Existing EMR step '{step['Name']}' (id={step['Id']}) is {state}; "
                f"skipping submission to avoid duplicate maintenance run."
            )
            return False
    return True


with DAG(
    dag_id            = "lakehouse_daily_iceberg_maintenance",
    default_args      = DEFAULT_ARGS,
    schedule_interval = "0 3 * * *",
    catchup           = False,
    tags              = ["iceberg", "maintenance", "lakehouse"],
    doc_md            = __doc__,
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
        timeout       = 1800,  # 30 min — maintenance is bounded, not streaming.
    )

    metadata_start >> check_no_existing >> submit >> wait
