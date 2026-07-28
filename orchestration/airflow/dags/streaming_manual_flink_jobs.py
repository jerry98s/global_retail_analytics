"""Streaming Flink jobs DAG.

Submits the long-running PyFlink streaming jobs to the existing EMR cluster
as YARN application-mode steps. The DAG is `schedule_interval=None` because
the jobs are streaming; trigger it manually after each redeploy or via
`airflow dags trigger streaming_manual_flink_jobs`.

Prefer `scripts/cloud/deploy_platform.ps1 -Env <env>` for routine deploys — this DAG is the
Airflow-native equivalent when MWAA is enabled.

Required Airflow Variables (see `terraform output airflow_variables`):
  - emr_cluster_id
  - artifacts_bucket
  - checkpoints_bucket
  - bronze_iceberg_warehouse
  - silver_iceberg_warehouse
  - msk_bootstrap_brokers

Idempotency guard (P1.4 from docs/runbooks/dw-checklist-audit.md):
  Each branch starts with a ShortCircuitOperator that queries EMR for
  existing steps whose name starts with `flink-{name}`. If any are
  PENDING or RUNNING, the submit + wait tasks for that branch are
  skipped — preventing duplicate Flink submissions on DAG re-runs
  (YARN app names would otherwise collide and EMR would either reject
  the second step or queue it indefinitely).

Bronze vs silver parallel safety (P1.6):
  The three Flink jobs are submitted in parallel by design. There is
  no runtime data dependency between `inventory_bronze_job` and
  `inventory_silver_job` — both read Kafka directly with separate
  consumer groups and write to independent Iceberg tables
  (`bronze.inventory_events` and `silver.inventory_hourly`).
  After the kappa conversion (ADR-007), `fact_inventory_snapshot`
  reads from silver, so bronze is purely the audit/replay layer;
  a delayed bronze job does not block the gold mart. Sequential
  submission would add coupling without correctness benefit.
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
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": True,
    "email":            ["data-platform@company.com"],
}


def _flink_step(name: str, entry: str) -> dict:
    """Build an EMR step that runs a PyFlink job in YARN application mode."""
    cmd = (
        "export KAFKA_BOOTSTRAP_SERVERS='{{ var.value.msk_bootstrap_brokers }}' "
        "KAFKA_SECURITY_PROTOCOL='SASL_SSL' "
        "KAFKA_SASL_MECHANISM='AWS_MSK_IAM' "
        "ICEBERG_BRONZE_WAREHOUSE='{{ var.value.bronze_iceberg_warehouse }}' "
        "ICEBERG_SILVER_WAREHOUSE='{{ var.value.silver_iceberg_warehouse }}' "
        # Match Kafka partition counts (ingestion/kafka/topics.py):
        # inventory.events.v1=12, clickstream.events.v1=24.
        "INVENTORY_PARALLELISM='12' "
        "CLICKSTREAM_PARALLELISM='24' "
        "&& aws s3 sync s3://{{ var.value.artifacts_bucket }}/streaming /opt/flink-config-src "
        "&& flink run-application -t yarn-application "
        f"-Dyarn.application.name=flink-{name} "
        "-Dstate.checkpoints.dir=s3://{{ var.value.checkpoints_bucket }}/flink/checkpoints/"
        f"{name} "
        "-Dstate.savepoints.dir=s3://{{ var.value.checkpoints_bucket }}/flink/savepoints/"
        f"{name} "
        "-pyfs /opt/flink-config-src/flink_jobs "
        f"-py /opt/flink-config-src/flink_jobs/{entry}"
    )

    return {
        "Name":            f"flink-{name}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar":  "command-runner.jar",
            "Args": ["bash", "-lc", cmd],
        },
    }


def _no_existing_flink_step(step_name_prefix: str) -> bool:
    """Return False (skip submission) if a PENDING or RUNNING EMR step exists
    whose name starts with `step_name_prefix`.

    Guards against duplicate Flink job submissions on DAG re-runs: YARN app
    names are derived from the step name, so two steps with the same prefix
    would produce two Flink jobs with the same YARN application name and
    collide.
    """
    cluster_id = Variable.get("emr_cluster_id")
    emr_client = boto3.client("emr")
    response = emr_client.list_steps(
        ClusterId=cluster_id,
        StepStates=["PENDING", "RUNNING"],
    )
    for step in response.get("Steps", []):
        step_name = step.get("Name", "")
        if step_name.startswith(step_name_prefix):
            state = step.get("Status", {}).get("State", "UNKNOWN")
            print(
                f"Existing EMR step '{step_name}' (id={step['Id']}) is {state}; "
                f"skipping submission to avoid duplicate Flink job."
            )
            return False
    return True


with DAG(
    dag_id            = "streaming_manual_flink_jobs",
    default_args      = DEFAULT_ARGS,
    schedule_interval = None,
    catchup           = False,
    tags              = ["streaming", "flink", "emr"],
    doc_md            = __doc__,
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
) as dag:

    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )

    check_clickstream = ShortCircuitOperator(
        task_id="check_no_existing_clickstream_bronze",
        python_callable=_no_existing_flink_step,
        op_kwargs={"step_name_prefix": "flink-clickstream-bronze"},
    )

    submit_clickstream = EmrAddStepsOperator(
        task_id     = "submit_clickstream_bronze",
        job_flow_id = "{{ var.value.emr_cluster_id }}",
        steps       = [_flink_step("clickstream-bronze", "clickstream_bronze_job.py")],
    )

    wait_clickstream = EmrStepSensor(
        task_id       = "wait_clickstream_bronze_started",
        job_flow_id   = "{{ var.value.emr_cluster_id }}",
        step_id       = "{{ task_instance.xcom_pull('submit_clickstream_bronze')[0] }}",
        target_states = {"RUNNING", "COMPLETED"},
        poke_interval = 30,
        timeout       = 600,
    )

    check_inventory_bronze = ShortCircuitOperator(
        task_id="check_no_existing_inventory_bronze",
        python_callable=_no_existing_flink_step,
        op_kwargs={"step_name_prefix": "flink-inventory-bronze"},
    )

    submit_inventory_bronze = EmrAddStepsOperator(
        task_id     = "submit_inventory_bronze",
        job_flow_id = "{{ var.value.emr_cluster_id }}",
        steps       = [_flink_step("inventory-bronze", "inventory_bronze_job.py")],
    )

    wait_inventory_bronze = EmrStepSensor(
        task_id       = "wait_inventory_bronze_started",
        job_flow_id   = "{{ var.value.emr_cluster_id }}",
        step_id       = "{{ task_instance.xcom_pull('submit_inventory_bronze')[0] }}",
        target_states = {"RUNNING", "COMPLETED"},
        poke_interval = 30,
        timeout       = 600,
    )

    check_inventory_silver = ShortCircuitOperator(
        task_id="check_no_existing_inventory_silver",
        python_callable=_no_existing_flink_step,
        op_kwargs={"step_name_prefix": "flink-inventory-hourly"},
    )

    submit_inventory_silver = EmrAddStepsOperator(
        task_id     = "submit_inventory_silver",
        job_flow_id = "{{ var.value.emr_cluster_id }}",
        steps       = [_flink_step("inventory-hourly", "inventory_silver_job.py")],
    )

    wait_inventory_silver = EmrStepSensor(
        task_id       = "wait_inventory_silver_started",
        job_flow_id   = "{{ var.value.emr_cluster_id }}",
        step_id       = "{{ task_instance.xcom_pull('submit_inventory_silver')[0] }}",
        target_states = {"RUNNING", "COMPLETED"},
        poke_interval = 30,
        timeout       = 600,
    )

    metadata_start >> check_clickstream >> submit_clickstream >> wait_clickstream
    metadata_start >> check_inventory_bronze >> submit_inventory_bronze >> wait_inventory_bronze
    metadata_start >> check_inventory_silver >> submit_inventory_silver >> wait_inventory_silver
