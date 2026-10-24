"""Operational alerts to the platform SNS topic.

MWAA has no SMTP server, so ``email_on_failure`` would send nothing. Task
failures and metadata breaches publish to the ``alert_topic_arn`` Airflow
Variable instead (the Terraform ``<project>-<env>-alerts`` topic, which the
on-call subscribes to via ``alert_email``).
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_SUBJECT_LIMIT = 100  # SNS rejects longer subjects.


def publish_alert(subject: str, message: str) -> bool:
    """Publish one alert; returns False (and logs) when it cannot be sent."""
    try:
        import boto3
        from airflow.models import Variable

        topic_arn = Variable.get("alert_topic_arn", default_var="")
        if not topic_arn:
            log.error("ALERT NOT SENT (alert_topic_arn unset): %s\n%s", subject, message)
            return False
        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject=subject[:_SUBJECT_LIMIT],
            Message=message,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.error("ALERT NOT SENT (%s): %s\n%s", exc, subject, message)
        return False


def failure_message(context: dict[str, Any]) -> tuple[str, str]:
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    subject = f"[airflow] {dag_id}.{task_id} failed"
    lines = [
        f"DAG: {dag_id}",
        f"Task: {task_id}",
        f"Run: {context.get('run_id', 'unknown')}",
        f"Try: {getattr(ti, 'try_number', 'unknown')}",
        f"Error: {context.get('exception')}",
        f"Log: {getattr(ti, 'log_url', '')}",
    ]
    return subject, "\n".join(lines)


def notify_task_failure(context: dict[str, Any]) -> None:
    """Task ``on_failure_callback``: fires once retries are exhausted."""
    publish_alert(*failure_message(context))
