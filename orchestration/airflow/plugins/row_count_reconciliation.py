"""Row-count publish gate for each WAP-owning DAG (P2.5, ADR-009).

Each owning DAG (warehouse, marketing, catalog) compares the row counts of the
pending Gold tables it owns to its own baseline, the counts of its last
successful publish, before its WAP publish.

This catches:
* Silent data loss — upstream dropped a partition, ``fact_sales`` dropped 50%.
* Volume spikes — a backfill ran, ``fact_sales`` grew 500%.

Gate rules
----------
* An owned table that cannot be read blocks the publish.
* An owned table that shrank by more than the threshold blocks the publish,
  unless the run acknowledges that table (``acknowledge_shrink``). An
  acknowledged shrink still alerts and is recorded as ``acknowledged``.
* Growth beyond the threshold alerts but never blocks.
* Whenever nothing is blocked, the counts become the new baseline once the
  publish succeeds, so a warning does not freeze the baseline.

Baseline persistence
--------------------
Each DAG stores its baseline in its own Airflow Variable,
``gold_row_counts_baseline_<scope>`` (scope: ``finance``, ``marketing``,
``catalog``), as a JSON map of ``"schema.table": row_count``. Until a scope's
Variable exists, its baseline is read from that scope's tables in the shared
``gold_row_counts_baseline`` Variable that earlier releases wrote, so a shrink
across the migration is still caught.

Configuration (Airflow Variables)
---------------------------------
* ``gold_row_counts_baseline_<scope>`` — written by the gate after publish.
* ``row_count_delta_threshold`` — float in [0, 1]; default ``0.20`` (20%).

Wiring
------
Each owning DAG runs ``reconcile_gold_row_counts_task`` after its dbt audit and
before its WAP publish, with ``save_baseline=False``, and saves the counts in a
separate ``save_row_count_baseline_task`` after publish, so a failed publish
never advances the baseline to counts that did not go live. The run parameter
``acknowledge_shrink`` (a list of ``schema.table``) is read from the DAG run.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 0.20
BASELINE_VAR_NAME = "gold_row_counts_baseline"
THRESHOLD_VAR_NAME = "row_count_delta_threshold"
STATUS_ACKNOWLEDGED = "acknowledged"


def baseline_var_name(scope: str) -> str:
    """Airflow Variable holding one owning DAG's baseline."""
    return f"{BASELINE_VAR_NAME}_{scope}"


def _query_row_counts(conn: Any, tables: list[str]) -> dict[str, int]:
    """COUNT(*) of each owned table's pending twin. Unqueryable -> -1.

    Keys stay on the live ``schema.table`` name so the trend is continuous
    across pending and live reads.
    """
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for key in tables:
            schema, table = key.split(".", 1)
            try:
                cur.execute(f"SELECT COUNT(*) FROM {schema}_pending.{table}")
                row = cur.fetchone()
                count = int(row[0]) if row and row[0] is not None else -1
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to query %s_pending.%s: %s", schema, table, exc)
                count = -1
            counts[key] = count
    return counts


def _parse_baseline(raw: str | None) -> dict[str, int]:
    try:
        parsed = json.loads(raw) if raw else {}
        return {k: int(v) for k, v in parsed.items()}
    except (json.JSONDecodeError, ValueError, TypeError, AttributeError) as exc:
        log.warning("Baseline parse failed (%s); starting fresh", exc)
        return {}


def _load_baseline(var_get: Any, scope: str, tables: list[str]) -> dict[str, int]:
    """The scope's baseline, else its tables from the shared legacy Variable.

    ``var_get`` is ``airflow.models.Variable.get``, injected so unit tests can
    stub it without the Airflow module.
    """
    raw = var_get(baseline_var_name(scope), default_var=None)
    if raw is not None:
        return _parse_baseline(raw)
    legacy = _parse_baseline(var_get(BASELINE_VAR_NAME, default_var=None))
    return {k: v for k, v in legacy.items() if k in tables}


def _resolve_threshold(var_get: Any) -> float:
    raw = var_get(THRESHOLD_VAR_NAME, default_var=str(DEFAULT_THRESHOLD))
    try:
        val = float(raw)
    except (ValueError, TypeError):
        return DEFAULT_THRESHOLD
    # Clamp to [0, 1] — values outside this range are nonsensical.
    return max(0.0, min(1.0, val))


def _delta_pct(prev: int | None, current: int) -> float | None:
    """Day-over-day delta as a fraction. None if uncomputable."""
    if prev is None or prev <= 0 or current < 0:
        return None
    return (current - prev) / prev


def reconcile_gold_row_counts(
    *,
    conn: Any,
    var_get: Any,
    var_set: Any,
    scope: str,
    tables: list[str],
    acknowledge_shrink: Iterable[str] = (),
    save_baseline: bool = True,
) -> dict[str, Any]:
    """Gate one DAG's pending Gold tables against that DAG's baseline.

    ``tables`` are the ``schema.table`` names the DAG owns and publishes.
    ``acknowledge_shrink`` names owned tables allowed to shrink in this run;
    a name the DAG does not own is rejected. A run with nothing blocked yields
    ``baseline_candidate``; it is saved here unless ``save_baseline`` is False,
    in which case the caller saves it with ``save_baseline_candidate`` once the
    audited tables are live.
    """
    if not tables:
        raise ValueError(f"Row-count gate for scope {scope!r} has no tables")
    acknowledged = set(acknowledge_shrink)
    unknown = sorted(acknowledged - set(tables))
    if unknown:
        raise ValueError(
            f"acknowledge_shrink names tables scope {scope!r} does not own: "
            + ", ".join(unknown)
        )

    current = _query_row_counts(conn, tables)
    baseline = _load_baseline(var_get, scope, tables)
    threshold = _resolve_threshold(var_get)

    deltas: dict[str, dict[str, Any]] = {}
    warned = False
    blocked: list[str] = []
    for key, count in current.items():
        prev = baseline.get(key)
        delta_pct = _delta_pct(prev, count)
        shrank = delta_pct is not None and delta_pct < -threshold
        if count < 0:
            blocked.append(key)
            status = "unavailable"
        elif shrank and key in acknowledged:
            warned = True
            status = STATUS_ACKNOWLEDGED
        elif shrank:
            blocked.append(key)
            warned = True
            status = "WARN"
        elif delta_pct is not None and delta_pct > threshold:
            warned = True
            status = "WARN"
        elif delta_pct is None:
            status = "no_baseline"
        else:
            status = "ok"
        log.log(
            logging.WARNING if status not in ("ok", "no_baseline") else logging.INFO,
            "Row-count %s for %s: %s -> %d (%s)",
            status,
            key,
            prev,
            count,
            "n/a" if delta_pct is None else f"{delta_pct * 100:+.1f}%",
        )
        deltas[key] = {
            "previous": prev,
            "current": count,
            "delta_pct": delta_pct,
            "status": status,
        }

    candidate = None if blocked else dict(current)
    if candidate is not None and save_baseline:
        var_set(baseline_var_name(scope), json.dumps(candidate))
        log.info("Baseline updated for %s.", scope)
    elif candidate is None:
        log.warning("Publish blocked for %s; baseline unchanged.", ", ".join(blocked))

    return {
        "scope": scope,
        "baseline_var": baseline_var_name(scope),
        "warned": warned,
        "blocked": blocked,
        "acknowledged": sorted(k for k, d in deltas.items() if d["status"] == STATUS_ACKNOWLEDGED),
        "threshold": threshold,
        "deltas": deltas,
        "baseline_candidate": candidate,
    }


def save_baseline_candidate(result: dict[str, Any] | None, *, var_set: Any) -> bool:
    """Persist a gate run's counts after publish; False when there are none."""
    candidate = (result or {}).get("baseline_candidate")
    if not candidate:
        log.warning("No baseline candidate from reconciliation; baseline unchanged.")
        return False
    var_set(result["baseline_var"], json.dumps(candidate))
    log.info("Baseline %s updated after publish.", result["baseline_var"])
    return True


def _emit_metadata(result: dict[str, Any], context: dict[str, Any] | None) -> None:
    """Best-effort DQ rows into the metadata database (fail-open)."""
    try:
        from metadata_airflow import _observer, _writer, execution_id_for_context

        observer = _observer()
        if observer is None:
            return
        writer = _writer(observer)
        if writer is None:
            return
        try:
            execution_id = (
                execution_id_for_context(context or {})
                if context
                else "row_count_reconciliation"
            )
            observer.record_reconciliation_results(
                writer,
                execution_id=execution_id,
                deltas=result.get("deltas") or {},
                warned=bool(result.get("warned")),
                blocked=result.get("blocked") or [],
            )
        finally:
            writer.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata emit from reconciliation failed (fail-open): %s", exc)


def _airflow_entrypoint(
    scope: str,
    tables: list[str],
    save_baseline: bool = True,
    **context: Any,
) -> dict[str, Any]:
    """Airflow PythonOperator entrypoint.

    Reads Redshift creds + baseline Variables from the Airflow runtime and the
    ``acknowledge_shrink`` list from the DAG run's params. Kept thin so the
    gate itself is testable without Airflow.
    """
    import redshift_connector
    from airflow.models import Variable

    from metadata_airflow import redshift_password

    acknowledge = (context.get("params") or {}).get("acknowledge_shrink") or []
    conn = redshift_connector.connect(
        host=Variable.get("redshift_host"),
        port=int(Variable.get("redshift_port", default_var="5439")),
        database=Variable.get("redshift_database", default_var="prod"),
        user=Variable.get("redshift_user"),
        password=redshift_password(),
    )
    # One failed COUNT must not abort the transaction and turn every later
    # count into -1.
    conn.autocommit = True
    try:
        result = reconcile_gold_row_counts(
            conn=conn,
            var_get=Variable.get,
            var_set=Variable.set,
            scope=scope,
            tables=tables,
            acknowledge_shrink=acknowledge,
            save_baseline=save_baseline,
        )
        _emit_metadata(result, context)
    finally:
        conn.close()

    if result["blocked"]:
        raise RuntimeError(
            "Row-count gate failed; publish blocked for "
            + ", ".join(result["blocked"])
            + f" (unreadable or shrank more than {result['threshold']:.0%};"
            " acknowledge an expected shrink with the acknowledge_shrink run param)"
        )
    if result["warned"]:
        from alerting import publish_alert

        publish_alert(
            f"[airflow] Gold row-count warning ({scope})",
            json.dumps(
                {k: v for k, v in result["deltas"].items()
                 if v["status"] in ("WARN", STATUS_ACKNOWLEDGED)},
                indent=2,
                default=str,
            ),
        )
    return result


def _save_baseline_entrypoint(
    reconcile_task_id: str = "row_count_reconciliation", **context: Any
) -> bool:
    """Airflow entrypoint: save the reconciliation's counts after WAP publish."""
    from airflow.models import Variable

    result = context["ti"].xcom_pull(task_ids=reconcile_task_id)
    return save_baseline_candidate(result, var_set=Variable.set)


# Airflow PythonOperator callables — referenced by the WAP-owning DAGs.
# Tests should import ``reconcile_gold_row_counts`` / ``save_baseline_candidate``
# and inject ``conn``/``var_get``/``var_set`` doubles.
reconcile_gold_row_counts_task = _airflow_entrypoint
save_row_count_baseline_task = _save_baseline_entrypoint
