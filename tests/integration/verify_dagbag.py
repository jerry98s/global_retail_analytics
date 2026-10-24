"""Load the DAG folder in real Airflow (CI, MWAA constraints).

Fails on any import error, a missing or unexpected DAG id, or a task without an
execution timeout or failure alert. Run with the plugins folder on Airflow's
plugins path, as MWAA extracts plugins.zip::

    python tests/integration/verify_dagbag.py orchestration/airflow/dags
"""
from __future__ import annotations

import sys

from airflow.models import DagBag

EXPECTED_DAGS = {
    "warehouse_daily_batch_pipeline",
    "marketing_hourly_customer_360_pipeline",
    "streaming_manual_flink_jobs",
    "catalog_bihourly_product_scd2_refresh",
    "quality_hourly_ge_checkpoint",
    "lakehouse_daily_iceberg_maintenance",
}


def main(dag_folder: str) -> int:
    bag = DagBag(dag_folder=dag_folder, include_examples=False)
    problems = [f"import error in {path}: {err}" for path, err in bag.import_errors.items()]
    found = set(bag.dag_ids)
    problems += [f"missing DAG {dag_id}" for dag_id in sorted(EXPECTED_DAGS - found)]
    problems += [f"unexpected DAG {dag_id}" for dag_id in sorted(found - EXPECTED_DAGS)]
    for dag in bag.dags.values():
        for task in dag.tasks:
            if task.execution_timeout is None:
                problems.append(f"{dag.dag_id}.{task.task_id}: no execution_timeout")
            if task.on_failure_callback is None:
                problems.append(f"{dag.dag_id}.{task.task_id}: no on_failure_callback")
    for problem in problems:
        print(f"FAIL: {problem}")
    if problems:
        return 1
    print(f"PASS: {len(found)} DAGs, {sum(len(d.tasks) for d in bag.dags.values())} tasks")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
