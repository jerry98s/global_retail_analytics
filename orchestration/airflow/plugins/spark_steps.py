"""One spark-submit definition for the hourly and daily inventory modes.

``all`` is intentionally rejected. The local stack uses it to run both
workloads in one process; Airflow must not, or it would write the inventory
exports at the same time as the other mode.

Both DAGs submit through ``submit_spark_step_task`` in the ``emr_spark`` pool
(one slot). The pool alone is not a lock: a step keeps running on EMR after
its Airflow sensor times out. So the submit task, holding the slot, refuses
while any ``spark-hourly`` / ``spark-inventory`` step is still pending or
running on the cluster, and ``wait_spark_idle_task`` waits for that first.
Create the pool once on MWAA: ``airflow pools set emr_spark 1 "one Spark step at a time"``.

Kept free of Airflow imports so the rules are unit-testable offline.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

log = logging.getLogger(__name__)

# Spark 3.4 on emr-6.15.0. Iceberg matches Flink's 1.4.3. spark-sql-kafka puts
# the Kafka consumer on the classpath for the inventory coverage frontier, and
# aws-msk-iam-auth (the version Flink uses) signs it into MSK with IAM.
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,"
    "graphframes:graphframes:0.8.3-spark3.4-s_2.12,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
    "software.amazon.msk:aws-msk-iam-auth:1.1.9"
)
PY_FILES = (
    "graph_logic.py,canonical_events.py,consumer_exports.py,event_contract.py,"
    "kafka_frontier.py,pos_loyalty.py"
)
POOL = "emr_spark"
SCHEDULED_MODES = ("hourly", "inventory")
SPARK_STEP_NAMES = tuple(f"spark-{mode}" for mode in SCHEDULED_MODES)
ACTIVE_STEP_STATES = ("PENDING", "RUNNING", "CANCEL_PENDING")
IDLE_POKE_INTERVAL = timedelta(minutes=1)
IDLE_TIMEOUT = timedelta(hours=1)

INVENTORY_WAIT_TASK_ID = "wait_spark_inventory_canonical"
INVENTORY_OUTCOME_TASK_ID = "inventory_spark_outcome"
# What the finance DAG leaves out when the inventory step did not finish:
# inventory staging and intermediates, the inventory marts and their
# descendants and tests, and the Silver run-id check. POS staging still runs.
# Pending inventory copies are live clones, so the publish keeps them unchanged.
INVENTORY_HOLD_EXCLUDE = (
    "stg_inventory_events int_inventory_cutoff int_inventory_hourly "
    "int_inventory_reconciliation_hourly fact_inventory_snapshot+ "
    "inventory_publication+ inventory_canonical_run_is_one"
)


def spark_step(mode: str) -> dict:
    """EMR step dict for ``hourly`` (Customer 360) or ``inventory`` (daily canonical)."""
    if mode not in SCHEDULED_MODES:
        raise ValueError(
            f"Airflow spark_step mode must be hourly or inventory, not {mode!r}. "
            "all is the local stack only, so it cannot run beside a scheduled mode."
        )
    identity = (
        "--pos-loyalty-path {{ var.value.pos_loyalty_ids_s3_path }} "
        if mode == "hourly" else ""
    )
    kafka = (
        "--kafka-bootstrap-servers {{ var.value.msk_bootstrap_brokers }} "
        "--kafka-security-protocol SASL_SSL --kafka-sasl-mechanism AWS_MSK_IAM "
        "--acknowledge-kafka-loss \"{{ params.acknowledge_kafka_loss | join(',') }}\" "
        if mode == "inventory" else ""
    )
    cmd = (
        "aws s3 sync s3://{{ var.value.artifacts_bucket }}/spark /opt/spark-identity "
        "&& cd /opt/spark-identity/identity_resolution "
        f"&& spark-submit --master yarn --deploy-mode cluster --packages {SPARK_PACKAGES} "
        f"--py-files {PY_FILES} identity_resolution_job.py "
        f"--mode {mode} "
        "--bronze-warehouse {{ var.value.bronze_iceberg_warehouse }} "
        "--silver-warehouse {{ var.value.silver_iceberg_warehouse }} "
        f"{identity}"
        "--checkpoint-dir s3://{{ var.value.checkpoints_bucket }}/graphframes/ "
        f"{kafka}"
    )
    return {
        "Name":            f"spark-{mode}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar":  "command-runner.jar",
            "Args": ["bash", "-lc", cmd],
        },
    }


def active_spark_steps(client: Any, cluster_id: str) -> list[str]:
    """Pending or running hourly/inventory Spark steps on the cluster, as ``name (id)``."""
    request: dict[str, Any] = {"ClusterId": cluster_id, "StepStates": list(ACTIVE_STEP_STATES)}
    active: list[str] = []
    while True:
        page = client.list_steps(**request)
        active.extend(
            f"{step['Name']} ({step['Id']})"
            for step in page.get("Steps", [])
            if step.get("Name") in SPARK_STEP_NAMES
        )
        marker = page.get("Marker")
        if not marker:
            return active
        request["Marker"] = marker


def submit_when_idle(client: Any, cluster_id: str, steps: list[dict]) -> list[str]:
    """Add ``steps`` only when no other Spark step is on the cluster; returns the step ids."""
    active = active_spark_steps(client, cluster_id)
    if active:
        raise RuntimeError(
            f"A Spark step is still on EMR cluster {cluster_id}: {', '.join(active)}. "
            "Not submitting beside it."
        )
    return list(client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)["StepIds"])


def wait_spark_idle_task(cluster_id: str, **_: Any) -> bool:
    """PythonSensor callable: true once no hourly/inventory Spark step is on the cluster."""
    import boto3

    active = active_spark_steps(boto3.client("emr"), cluster_id)
    if active:
        log.info("Waiting for Spark steps to finish: %s", ", ".join(active))
    return not active


def submit_spark_step_task(cluster_id: str, steps: list[dict], **_: Any) -> list[str]:
    """PythonOperator callable run in the emr_spark pool. XCom is the step id list."""
    import boto3

    return submit_when_idle(boto3.client("emr"), cluster_id, steps)


def held_inventory_message(wait_state: str | None, run_id: str) -> tuple[str, str]:
    subject = "[airflow] warehouse_daily_batch_pipeline: inventory held"
    lines = [
        f"Run: {run_id}",
        f"{INVENTORY_WAIT_TASK_ID} ended {wait_state or 'without a state'}.",
        "The POS facts publish. fact_inventory_snapshot, its summaries and "
        "control.inventory_publication keep the last published cutoff.",
    ]
    return subject, "\n".join(lines)


def inventory_spark_outcome_task(**context: Any) -> str:
    """Return ``complete``, or alert and return ``held``. Never fails the run."""
    from alerting import publish_alert

    dag_run = context["dag_run"]
    state = getattr(dag_run.get_task_instance(INVENTORY_WAIT_TASK_ID), "state", None)
    if state == "success":
        return "complete"
    subject, message = held_inventory_message(state, dag_run.run_id)
    log.warning("%s\n%s", subject, message)
    publish_alert(subject, message)
    return "held"


def inventory_hold_exclude(outcome: str | None) -> str:
    """dbt ``--exclude`` suffix for the finance build and audit when inventory is held."""
    return f" --exclude {INVENTORY_HOLD_EXCLUDE}" if outcome == "held" else ""


AIRFLOW_MACROS = {"inventory_hold_exclude": inventory_hold_exclude}
