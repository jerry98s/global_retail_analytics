"""Finance waits for the day's product catalog publish (ADR-014).

``warehouse_daily_batch_pipeline`` reads the published ``marketing.dim_product``
through ``wap_live_ref``. Its ``wait_for_catalog_publish`` sensor waits for the
catalog DAG's publish task of the same day; ``catalog_publish_check`` then
alerts when the wait ended without that publish, and the run continues on the
last published ``dim_product``.

Kept free of Airflow imports so the mapping is unit-testable offline.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

log = logging.getLogger(__name__)

CATALOG_DAG_ID = "catalog_daily_product_scd2_refresh"
CATALOG_PUBLISH_TASK_ID = "wap_publish_dim_product"
WAIT_TASK_ID = "wait_for_catalog_publish"
WAIT_TIMEOUT = timedelta(hours=2)
POKE_INTERVAL = timedelta(minutes=5)


def _midnight(moment: datetime) -> datetime:
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)


def catalog_logical_date(logical_date: datetime, dag_run: Any = None, **_: Any) -> datetime:
    """Logical date of the catalog run that must publish before this finance run.

    Both DAGs are daily: the catalog at 00:00 UTC, finance at 01:15 UTC, so a
    scheduled finance run matches the catalog run of the same logical day. A
    manual run (a POS restatement) has its trigger time as logical date and no
    catalog run at that time, so it waits for the latest daily catalog run:
    the one that started at the most recent midnight.
    """
    if dag_run is not None and getattr(dag_run, "run_type", "scheduled") != "scheduled":
        started = getattr(dag_run, "start_date", None) or logical_date
        return _midnight(started) - timedelta(days=1)
    return _midnight(logical_date)


def stale_catalog_message(wait_state: str | None, run_id: str) -> tuple[str, str]:
    subject = "[airflow] warehouse_daily_batch_pipeline: dim_product not refreshed"
    lines = [
        f"Run: {run_id}",
        f"{WAIT_TASK_ID} ended {wait_state or 'without a state'}.",
        f"{CATALOG_DAG_ID}.{CATALOG_PUBLISH_TASK_ID} did not succeed within "
        f"{WAIT_TIMEOUT}. Finance continues on the last published marketing.dim_product.",
    ]
    return subject, "\n".join(lines)


def catalog_publish_check_task(**context: Any) -> str:
    """Return ``published``, or alert and return ``stale``. Never fails the run."""
    from alerting import publish_alert

    dag_run = context["dag_run"]
    wait = dag_run.get_task_instance(WAIT_TASK_ID)
    state = getattr(wait, "state", None)
    if state == "success":
        return "published"
    subject, message = stale_catalog_message(state, dag_run.run_id)
    log.warning("%s\n%s", subject, message)
    publish_alert(subject, message)
    return "stale"
