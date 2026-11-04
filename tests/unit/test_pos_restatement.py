"""POS restatement params, post-publish run state, and baseline timing.

Covers the warehouse DAG's manual restatement path (pos_restatement plugin),
the ALL_DONE publish-outcome task, and saving the row-count baseline only
after publish. Airflow is not installed in unit CI, so DAG templates are
rendered with plain Jinja and the plugin macros.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import jinja2
import pytest

from orchestration.airflow.plugins import pos_restatement as pr
from orchestration.airflow.plugins.row_count_reconciliation import (
    BASELINE_VAR_NAME,
    reconcile_gold_row_counts,
    save_baseline_candidate,
)
from orchestration.airflow.plugins.wap_publish import require_publish_succeeded

pytestmark = pytest.mark.unit

_DAG = (
    Path(__file__).resolve().parents[2]
    / "orchestration" / "airflow" / "dags" / "warehouse_daily_batch_pipeline.py"
)
_SRC = _DAG.read_text(encoding="utf-8")

_RANGE = {"restate_from": "2026-10-01", "restate_to": "2026-10-03"}
_DAILY = {"restate_from": None, "restate_to": None}


# ---------- range validation ----------

def test_no_params_is_the_daily_load() -> None:
    assert pr.restatement_range(_DAILY) is None
    assert pr.restatement_range({}) is None
    assert pr.restatement_range({"restate_from": "", "restate_to": ""}) is None
    assert pr.pos_partition_dates(_DAILY, "2026-11-04") == ["2026-11-04"]
    assert json.loads(pr.pos_dbt_vars(_DAILY)) == {"wap_phase": "pending"}


def test_range_expands_inclusive_dates_and_dbt_vars() -> None:
    assert pr.pos_partition_dates(_RANGE, "2026-11-04") == [
        "2026-10-01", "2026-10-02", "2026-10-03",
    ]
    assert json.loads(pr.pos_dbt_vars(_RANGE)) == {
        "wap_phase": "pending",
        "pos_restate_from": "2026-10-01",
        "pos_restate_to": "2026-10-03",
    }


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"restate_from": "2026-10-01"}, "set together"),
        ({"restate_to": "2026-10-01"}, "set together"),
        ({"restate_from": "2026-10-03", "restate_to": "2026-10-01"}, "is after"),
        ({"restate_from": "20261001", "restate_to": "2026-10-03"}, "YYYY-MM-DD"),
        ({"restate_from": "2026-10-01';drop", "restate_to": "2026-10-03"}, "YYYY-MM-DD"),
        ({"restate_from": "2026-02-30", "restate_to": "2026-03-01"}, "not a real date"),
    ],
)
def test_invalid_ranges_fail_before_any_work(params: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        pr.restatement_range(params)


# ---------- DAG templates rendered with the plugin macros ----------

def _task_template(task_id: str, field: str) -> str:
    start = re.search(rf'task_id\s*=\s*"{task_id}"', _SRC)
    assert start, task_id
    match = re.search(field + r'\s*=\s*"""(.*?)"""', _SRC[start.end():], re.S)
    assert match, (task_id, field)
    return match.group(1)


def _render(template: str, params: dict) -> str:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return env.from_string(template).render(
        params=params,
        ds="2026-11-04",
        var={"value": {
            "artifacts_bucket": "artifacts-example",
            "pos_bronze_s3_path": "s3://bronze-example/iceberg/bronze/pos_transactions/",
        }},
        **pr.AIRFLOW_MACROS,
    )


def test_daily_run_generates_and_registers_only_ds() -> None:
    bash = _render(_task_template("generate_pos_parquet", "bash_command"), _DAILY)
    assert "exit 0" not in bash and "generate_pos_parquet" in bash
    sql = _render(_task_template("register_pos_spectrum_partition", "sql"), _DAILY)
    assert re.findall(r"PARTITION \(dt='([^']+)'\)", sql) == ["2026-11-04"]


def test_restatement_keeps_files_and_registers_the_range() -> None:
    bash = _render(_task_template("generate_pos_parquet", "bash_command"), _RANGE)
    assert bash.index("exit 0") < bash.index("python -m ingestion.batch.generate_pos_parquet")
    sql = _render(_task_template("register_pos_spectrum_partition", "sql"), _RANGE)
    assert sql.count("ADD IF NOT EXISTS") == 1
    assert re.findall(r"PARTITION \(dt='([^']+)'\)", sql) == [
        "2026-10-01", "2026-10-02", "2026-10-03",
    ]
    assert "data/dt=2026-10-02/'" in sql


def test_invalid_range_fails_the_first_task_render() -> None:
    with pytest.raises(ValueError):
        _render(_task_template("generate_pos_parquet", "bash_command"), {"restate_from": "2026-10-01"})


def test_post_publish_tasks_feed_an_all_done_outcome() -> None:
    assert ">> [save_baseline, redshift_analyze, cost_check]\n        >> gold_publish_outcome" in _SRC
    outcome = re.search(r"gold_publish_outcome = PythonOperator\((.*?)\n    \)", _SRC, re.S)
    assert outcome and "TriggerRule.ALL_DONE" in outcome.group(1)
    assert "require_publish_succeeded" in outcome.group(1)
    assert '"save_baseline": False' in _SRC


# ---------- publish outcome ----------

class _TI:
    def __init__(self, state: str | None) -> None:
        self.state = state


class _DagRun:
    def __init__(self, state: str | None) -> None:
        self._state = state

    def get_task_instance(self, task_id: str) -> _TI | None:
        return None if self._state is None else _TI(self._state)


def test_outcome_succeeds_when_publish_succeeded() -> None:
    assert "succeeded" in require_publish_succeeded(
        "wap_publish_gold", dag_run=_DagRun("success")
    )


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", None])
def test_outcome_fails_when_gold_was_not_published(state: str | None) -> None:
    with pytest.raises(RuntimeError, match="Gold was not published"):
        require_publish_succeeded("wap_publish_gold", dag_run=_DagRun(state))


# ---------- baseline saved only after publish ----------

class _Cursor:
    def __init__(self, counts: list[int]) -> None:
        self._counts = iter(counts)

    def execute(self, _sql: str) -> None:
        pass

    def fetchone(self) -> tuple[int]:
        return (next(self._counts),)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Conn:
    def __init__(self, counts: list[int]) -> None:
        self._cursor = _Cursor(counts)

    def cursor(self) -> _Cursor:
        return self._cursor


def test_audit_defers_baseline_until_publish_saves_it() -> None:
    store: dict[str, str] = {}
    result = reconcile_gold_row_counts(
        conn=_Conn([100] * 50),
        var_get=lambda name, default_var=None: store.get(name, default_var),
        var_set=store.__setitem__,
        save_baseline=False,
    )
    assert BASELINE_VAR_NAME not in store
    assert result["baseline_candidate"]

    assert save_baseline_candidate(result, var_set=store.__setitem__) is True
    assert json.loads(store[BASELINE_VAR_NAME]) == result["baseline_candidate"]


def test_no_candidate_leaves_baseline_unchanged() -> None:
    store = {BASELINE_VAR_NAME: '{"finance.fact_sales": 1}'}
    assert save_baseline_candidate(None, var_set=store.__setitem__) is False
    assert save_baseline_candidate(
        {"baseline_candidate": None}, var_set=store.__setitem__
    ) is False
    assert store[BASELINE_VAR_NAME] == '{"finance.fact_sales": 1}'
