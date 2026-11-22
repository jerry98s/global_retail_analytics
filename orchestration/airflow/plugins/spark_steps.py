"""One spark-submit definition for the hourly and daily inventory modes.

``all`` is intentionally rejected. The local stack uses it to run both
workloads in one process; Airflow must not, or it would write the inventory
exports at the same time as the other mode.

Both DAGs run these steps in the ``emr_spark`` pool (one slot). Create it once
on MWAA: ``airflow pools set emr_spark 1 "one Spark step at a time"``.
"""
from __future__ import annotations

# Spark 3.4 on emr-6.15.0. Iceberg matches Flink's 1.4.3. spark-sql-kafka puts
# the Kafka consumer on the classpath for the inventory coverage frontier.
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,"
    "graphframes:graphframes:0.8.3-spark3.4-s_2.12,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
)
PY_FILES = (
    "graph_logic.py,canonical_events.py,consumer_exports.py,event_contract.py,kafka_frontier.py"
)
POOL = "emr_spark"
SCHEDULED_MODES = ("hourly", "inventory")


def spark_step(mode: str) -> dict:
    """EMR step dict for ``hourly`` (Customer 360) or ``inventory`` (daily canonical)."""
    if mode not in SCHEDULED_MODES:
        raise ValueError(
            f"Airflow spark_step mode must be hourly or inventory, not {mode!r}. "
            "all is the local stack only, so it cannot run beside a scheduled mode."
        )
    identity = (
        "--pos-parquet-path {{ var.value.pos_bronze_s3_path }} "
        if mode == "hourly" else ""
    )
    kafka = (
        "--kafka-bootstrap-servers {{ var.value.msk_bootstrap_brokers }} "
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
