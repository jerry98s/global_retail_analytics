"""
Iceberg maintenance job — compaction + snapshot expiration.

Runs as a one-shot Flink batch job (not streaming) via the
`lakehouse_daily_iceberg_maintenance` Airflow DAG. Closes DL-D from the data lake
checklist applied 2026-07-05.

Operations per table (bronze.inventory_events, bronze.clickstream_events,
silver.inventory_hourly):

  1. rewrite_data_files  — merges small files into target-size files
     (128-256MB range). Threshold: min-input-files=5 so partitions with
     fewer than 5 small files are skipped (avoid no-op compaction cost).
  2. expire_snapshots    — physically deletes orphan data + metadata
     files for snapshots older than the retention window. Bronze uses a
     7-day window (high-volume, replay needs are short); Silver uses a
     30-day window (lower volume, longer replay value for the kappa
     path). At least 5 recent snapshots are always retained regardless
     of the cutoff, so a botched run can still roll back.

This job is idempotent: re-running for the same Iceberg state is a no-op
(rewrite_data_files finds nothing to compact; expire_snapshots finds
nothing to delete). It is safe to trigger manually after a backfill or
a producer burst.

Schedule: 03:00 UTC daily via Airflow — off-peak, after the
warehouse_daily_batch_pipeline finishes (~02:30 UTC) and before the next hourly
customer_360 run.

Run manually (cloud):
  aws emr add-steps --cluster-id <emr_cluster_id> --steps file://step.json
  # step.json builds the same `flink run` command the DAG emits — see
  # orchestration/airflow/dags/lakehouse_daily_iceberg_maintenance.py.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict

from pyflink.table import EnvironmentSettings, TableEnvironment

from _config import (  # noqa: E402
    load_simple_yaml,
    resolve_config_path,
)
from lake_names import (  # noqa: E402
    BRONZE_NAMESPACE,
    CLICKSTREAM_EVENTS,
    INVENTORY_EVENTS,
    INVENTORY_HOURLY,
    SILVER_NAMESPACE,
)

# Defaults — overridable via env vars so the Airflow DAG can tune without
# re-releasing the Flink job artifact.
BRONZE_SNAPSHOT_RETENTION_DAYS = int(os.environ.get("BRONZE_SNAPSHOT_RETENTION_DAYS", "7"))
SILVER_SNAPSHOT_RETENTION_DAYS = int(os.environ.get("SILVER_SNAPSHOT_RETENTION_DAYS", "30"))
SNAPSHOTS_RETAIN_LAST = int(os.environ.get("ICEBERG_SNAPSHOTS_RETAIN_LAST", "5"))
MIN_INPUT_FILES = os.environ.get("ICEBERG_MIN_INPUT_FILES", "5")
TARGET_FILE_SIZE_BYTES = os.environ.get("ICEBERG_TARGET_FILE_SIZE_BYTES", "268435456")  # 256MB


def _create_iceberg_catalog(
    t_env: TableEnvironment, cfg: Dict[str, Any]
) -> tuple[str, str]:
    """Register the Iceberg catalogs (bronze + silver) and return both names.

    The platform convention is one catalog per warehouse; bronze jobs use
    `iceberg_warehouse_bronze`, silver jobs use `iceberg_warehouse_silver`.
    For maintenance we register both as separate catalogs so a single
    Flink batch session can compact/expire across both layers."""
    bronze_catalog = str(cfg["iceberg_catalog_name"])
    t_env.execute_sql(
        f"""
        CREATE CATALOG {bronze_catalog} WITH (
          'type' = 'iceberg',
          'catalog-type' = '{cfg["iceberg_catalog_type"]}',
          'warehouse' = '{cfg["iceberg_warehouse_bronze"]}'
        )
        """
    )
    silver_catalog = bronze_catalog + "_silver"
    t_env.execute_sql(
        f"""
        CREATE CATALOG {silver_catalog} WITH (
          'type' = 'iceberg',
          'catalog-type' = '{cfg["iceberg_catalog_type"]}',
          'warehouse' = '{cfg["iceberg_warehouse_silver"]}'
        )
        """
    )
    return bronze_catalog, silver_catalog


def _run_procedure(t_env: TableEnvironment, sql: str, label: str) -> None:
    print(f"[iceberg-maintenance] {label}: {sql.strip()}", flush=True)
    # CALL procedures in Flink batch mode execute synchronously — execute_sql
    # blocks until the procedure completes and returns a TableResult with the
    # summary (files rewritten, snapshots expired, etc.). We don't need to
    # consume the result for the maintenance job — the log line above is the
    # audit trail. PyFlink's TableResult.await() cannot be called directly
    # because `await` is a Python keyword; batch-mode execute_sql already
    # blocks, so no explicit wait is needed.
    t_env.execute_sql(sql)


def _expire_snapshots_sql(
    catalog: str, table_ref: str, retention_days: int
) -> str:
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return (
        f"CALL {catalog}.system.expire_snapshots("
        f"  '{table_ref}',"
        f"  TIMESTAMP '{cutoff}',"
        f"  {SNAPSHOTS_RETAIN_LAST}"
        f")"
    )


def _rewrite_data_files_sql(catalog: str, table_ref: str) -> str:
    # Iceberg Flink procedure named-arg form. The options map sets
    # min-input-files (don't compact partitions with fewer than N small
    # files) and target-file-size-bytes (256MB soft cap, within the
    # 128-256MB band recommended by the data lake checklist).
    return (
        f"CALL {catalog}.system.rewrite_data_files("
        f"  '{table_ref}',"
        f"  options => map["
        f"    'min-input-files', '{MIN_INPUT_FILES}',"
        f"    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}'"
        f"  ]"
        f")"
    )


def run() -> None:
    cfg = load_simple_yaml(resolve_config_path("flink_conf.yaml"))
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)
    bronze_catalog, silver_catalog = _create_iceberg_catalog(t_env, cfg)

    # (catalog, namespace.table, retention_days, label)
    targets = [
        (bronze_catalog, f"{BRONZE_NAMESPACE}.{INVENTORY_EVENTS}", BRONZE_SNAPSHOT_RETENTION_DAYS, "bronze.inventory_events"),
        (bronze_catalog, f"{BRONZE_NAMESPACE}.{CLICKSTREAM_EVENTS}", BRONZE_SNAPSHOT_RETENTION_DAYS, "bronze.clickstream_events"),
        (silver_catalog, f"{SILVER_NAMESPACE}.{INVENTORY_HOURLY}", SILVER_SNAPSHOT_RETENTION_DAYS, "silver.inventory_hourly"),
    ]

    for catalog, table_ref, _, label in targets:
        _run_procedure(
            t_env,
            _rewrite_data_files_sql(catalog, table_ref),
            f"rewrite_data_files {label}",
        )

    for catalog, table_ref, retention_days, label in targets:
        _run_procedure(
            t_env,
            _expire_snapshots_sql(catalog, table_ref, retention_days),
            f"expire_snapshots {label} (retention={retention_days}d, retain_last={SNAPSHOTS_RETAIN_LAST})",
        )

    print(
        "[iceberg-maintenance] done — compaction + snapshot expiration "
        "completed for all 3 Iceberg tables.",
        flush=True,
    )


if __name__ == "__main__":
    run()
