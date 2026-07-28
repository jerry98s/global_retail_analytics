"""Fail-open Airflow helpers for the operational metadata database.

Requires ``scripts/common/metadata_observer.py`` on the worker (deploy syncs
it to ``/tmp/scripts/common`` and also copies it beside this plugin).
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _ensure_observer_path() -> None:
    candidates = [
        Path(__file__).resolve().parent,  # plugins/ (copied observer)
        Path("/tmp/scripts/common"),
        Path(__file__).resolve().parents[3] / "scripts" / "common",
    ]
    for directory in candidates:
        if (directory / "metadata_observer.py").is_file():
            path = str(directory)
            if path not in sys.path:
                sys.path.insert(0, path)
            return


def _observer() -> Any | None:
    try:
        _ensure_observer_path()
        import metadata_observer  # type: ignore

        return metadata_observer
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata_observer unavailable (fail-open): %s", exc)
        return None


def _writer(observer: Any) -> Any | None:
    try:
        from airflow.models import Variable

        return observer.RedshiftMetadataWriter(
            host=Variable.get("redshift_host"),
            port=int(Variable.get("redshift_port", default_var="5439")),
            database=Variable.get("redshift_metadata_database", default_var="metadata"),
            user=Variable.get("redshift_user"),
            password=Variable.get("redshift_password"),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata writer init failed (fail-open): %s", exc)
        return None


def execution_id_for_context(context: dict[str, Any]) -> str:
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = getattr(dag, "dag_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{dag_id}:{run_id}"))


def on_dag_start(*_args: Any, **context: Any) -> None:
    """DAG callback or PythonOperator entrypoint (accepts **context)."""
    if _args and isinstance(_args[0], dict) and not context:
        context = _args[0]
    observer = _observer()
    if observer is None:
        return
    writer = _writer(observer)
    if writer is None:
        return
    try:
        from airflow.models import Variable

        env = Variable.get("platform_environment", default_var="dev")
        execution_id = execution_id_for_context(context)
        dag = context.get("dag")
        dag_run = context.get("dag_run")
        external = bool(getattr(dag_run, "external_trigger", False))
        observer.start_pipeline_run(
            writer,
            execution_id=execution_id,
            pipeline_name=getattr(dag, "dag_id", "unknown"),
            environment=env,
            trigger_type="manual" if external else "schedule",
            orchestrator_run_id=getattr(dag_run, "run_id", None),
            logical_date=getattr(dag_run, "logical_date", None)
            or getattr(dag_run, "execution_date", None),
        )
        ti = context.get("ti")
        if ti is not None:
            ti.xcom_push(key="metadata_execution_id", value=execution_id)
        try:
            observer.seed_catalogs(writer)
        except Exception as seed_exc:  # noqa: BLE001
            log.warning("catalog seed failed (fail-open): %s", seed_exc)
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata on_dag_start failed (fail-open): %s", exc)
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def on_dag_success(context: dict[str, Any]) -> None:
    _finalize(context, status="SUCCESS")


def on_dag_failure(context: dict[str, Any]) -> None:
    err = context.get("exception")
    _finalize(context, status="FAILED", error_text=str(err) if err else "dag_failed")


def _finalize(
    context: dict[str, Any],
    *,
    status: str,
    error_text: str | None = None,
) -> None:
    observer = _observer()
    if observer is None:
        return
    writer = _writer(observer)
    if writer is None:
        return
    try:
        observer.finish_pipeline_run(
            writer,
            execution_id=execution_id_for_context(context),
            status=status,
            error_text=error_text,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata finalize failed (fail-open): %s", exc)
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def parse_dbt_run_results_task(**context: Any) -> int:
    observer = _observer()
    if observer is None:
        return 0
    writer = _writer(observer)
    if writer is None:
        return 0
    try:
        path = Path(
            os.environ.get(
                "DBT_RUN_RESULTS",
                "/tmp/dbt_project/target/run_results.json",
            )
        )
        results = observer.parse_dbt_run_results(path)
        return int(
            observer.record_dq_results(
                writer,
                execution_id=execution_id_for_context(context),
                results=results,
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("parse_dbt_run_results_task failed (fail-open): %s", exc)
        return 0
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def collect_freshness_task(**context: Any) -> int:
    observer = _observer()
    if observer is None:
        return 0
    writer = _writer(observer)
    if writer is None:
        return 0
    analytics = None
    try:
        from airflow.models import Variable
        import redshift_connector

        analytics = redshift_connector.connect(
            host=Variable.get("redshift_host"),
            port=int(Variable.get("redshift_port", default_var="5439")),
            database=Variable.get("redshift_database", default_var="prod"),
            user=Variable.get("redshift_user"),
            password=Variable.get("redshift_password"),
        )
        return int(
            observer.collect_table_freshness(
                writer,
                analytics,
                execution_id=execution_id_for_context(context),
            )
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("collect_freshness_task failed (fail-open): %s", exc)
        return 0
    finally:
        try:
            if analytics is not None:
                analytics.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def dbt_bash_with_metadata(inner_bash: str) -> str:
    """Run dbt bash, parse run_results in the same task, preserve exit code."""
    return f"""
set +e
{inner_bash}
DBT_RC=$?
set -e
EXEC_ID="{{{{ ti.xcom_pull(task_ids='metadata_start', key='metadata_execution_id') }}}}"
if [ -z "$EXEC_ID" ]; then EXEC_ID="{{{{ run_id }}}}"; fi
python /tmp/scripts/common/metadata_observer.py parse-dbt \\
  --backend redshift \\
  --execution-id "$EXEC_ID" \\
  --run-results /tmp/dbt_project/target/run_results.json \\
  --rs-host '{{{{ var.value.redshift_host }}}}' \\
  --rs-user '{{{{ var.value.redshift_user }}}}' \\
  --rs-password '{{{{ var.value.redshift_password }}}}' \\
  --rs-metadata-database '{{{{ var.value.redshift_metadata_database }}}}' \\
  || true
exit $DBT_RC
""".strip()


def ge_bash_with_metadata(inner_bash: str) -> str:
    """Run GE checkpoint bash and record a single suite outcome; preserve RC."""
    return f"""
set +e
{inner_bash}
GE_RC=$?
set -e
EXEC_ID="{{{{ ti.xcom_pull(task_ids='metadata_start', key='metadata_execution_id') }}}}"
if [ -z "$EXEC_ID" ]; then EXEC_ID="{{{{ run_id }}}}"; fi
if [ $GE_RC -eq 0 ]; then SUCCESS_FLAG=--success; else SUCCESS_FLAG=; fi
python /tmp/scripts/common/metadata_observer.py record-ge \\
  --backend redshift \\
  --execution-id "$EXEC_ID" \\
  --suite gold_layer_daily \\
  $SUCCESS_FLAG \\
  --rs-host '{{{{ var.value.redshift_host }}}}' \\
  --rs-user '{{{{ var.value.redshift_user }}}}' \\
  --rs-password '{{{{ var.value.redshift_password }}}}' \\
  --rs-metadata-database '{{{{ var.value.redshift_metadata_database }}}}' \\
  || true
exit $GE_RC
""".strip()
