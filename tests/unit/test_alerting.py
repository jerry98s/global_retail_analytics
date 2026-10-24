"""SNS alert publishing used by every DAG's on_failure_callback."""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from orchestration.airflow.plugins import alerting

pytestmark = pytest.mark.unit


@pytest.fixture
def runtime(monkeypatch):
    """Stub boto3 and airflow.models.Variable; returns (variables, sns client)."""
    variables: dict[str, str] = {}
    sns = MagicMock(name="sns")
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda service: sns
    models = types.ModuleType("airflow.models")
    models.Variable = types.SimpleNamespace(
        get=lambda name, default_var=None: variables.get(name, default_var)
    )
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.models", models)
    return variables, sns


def test_publishes_to_configured_topic_with_truncated_subject(runtime):
    variables, sns = runtime
    variables["alert_topic_arn"] = "arn:aws:sns:region:000000000000:alerts"
    assert alerting.publish_alert("x" * 150, "body") is True
    kwargs = sns.publish.call_args.kwargs
    assert kwargs["TopicArn"] == variables["alert_topic_arn"]
    assert len(kwargs["Subject"]) == 100
    assert kwargs["Message"] == "body"


def test_unset_topic_is_logged_not_raised(runtime, caplog):
    _variables, sns = runtime
    assert alerting.publish_alert("subject", "body") is False
    sns.publish.assert_not_called()
    assert "ALERT NOT SENT" in caplog.text


def test_sns_error_does_not_mask_the_task_failure(runtime, caplog):
    variables, sns = runtime
    variables["alert_topic_arn"] = "arn"
    sns.publish.side_effect = RuntimeError("throttled")
    assert alerting.publish_alert("subject", "body") is False
    assert "throttled" in caplog.text


def test_failure_message_names_dag_task_and_error():
    ti = types.SimpleNamespace(dag_id="d", task_id="t", try_number=2, log_url="http://log")
    subject, body = alerting.failure_message(
        {"task_instance": ti, "run_id": "r1", "exception": ValueError("boom")}
    )
    assert subject == "[airflow] d.t failed"
    assert "Run: r1" in body and "boom" in body and "http://log" in body
