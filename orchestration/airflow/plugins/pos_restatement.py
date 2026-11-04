"""POS restatement range for manual runs of warehouse_daily_batch_pipeline.

A run triggered with ``restate_from`` / ``restate_to`` (inclusive, YYYY-MM-DD)
keeps the delivered POS files, registers every partition in the range, and
passes the range to dbt so fact_sales and sales_daily_store replace exactly
those days. Without them the run is the normal daily load of ``ds``.

The DAG exposes these as Jinja macros; they import nothing from Airflow so the
validation is unit-testable.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse(name: str, value: Any) -> date:
    text = str(value)
    if not _ISO_DATE.match(text):
        raise ValueError(f"{name} must be YYYY-MM-DD, got {text!r}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} is not a real date: {text!r}") from exc


def restatement_range(params: dict[str, Any]) -> tuple[date, date] | None:
    """Validated (from, to) when both params are set, None when neither is."""
    start, end = params.get("restate_from"), params.get("restate_to")
    if start in (None, "") and end in (None, ""):
        return None
    if start in (None, "") or end in (None, ""):
        raise ValueError("restate_from and restate_to must be set together")
    first, last = _parse("restate_from", start), _parse("restate_to", end)
    if first > last:
        raise ValueError(f"restate_from {first} is after restate_to {last}")
    return first, last


def is_restatement(params: dict[str, Any]) -> bool:
    return restatement_range(params) is not None


def pos_partition_dates(params: dict[str, Any], ds: str) -> list[str]:
    """POS dt= partitions the run must register: the range, else just ``ds``."""
    window = restatement_range(params)
    if window is None:
        return [_parse("ds", ds).isoformat()]
    first, last = window
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


def pos_dbt_vars(params: dict[str, Any]) -> str:
    """dbt --vars JSON for the pending build and audit of this run."""
    dbt_vars: dict[str, str] = {"wap_phase": "pending"}
    window = restatement_range(params)
    if window is not None:
        dbt_vars["pos_restate_from"] = window[0].isoformat()
        dbt_vars["pos_restate_to"] = window[1].isoformat()
    return json.dumps(dbt_vars)


AIRFLOW_MACROS = {
    "pos_is_restatement": is_restatement,
    "pos_partition_dates": pos_partition_dates,
    "pos_dbt_vars": pos_dbt_vars,
}
