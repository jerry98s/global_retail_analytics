"""Unit tests for the row-count publish gate pure functions (P2.5, ADR-009).

The Airflow entrypoint (``reconcile_gold_row_counts_task``) pulls in
``redshift_connector`` and ``airflow.models.Variable`` — both unavailable
in the offline unit suite. These tests target the dependency-injected
core (``reconcile_gold_row_counts``) with stub ``conn``/``var_get``/``var_set``
doubles, so they run under ``pytest -m unit`` without any cloud deps.
"""

from __future__ import annotations

import json

import pytest

from orchestration.airflow.plugins.row_count_reconciliation import (
    BASELINE_VAR_NAME,
    DEFAULT_THRESHOLD,
    THRESHOLD_VAR_NAME,
    _delta_pct,
    _resolve_threshold,
    baseline_var_name,
    reconcile_gold_row_counts,
    save_baseline_candidate,
)

pytestmark = pytest.mark.unit

_TABLES = ["marketing.dim_customer", "marketing.fact_customer_session"]
_BASELINE = {"marketing.dim_customer": 1_000, "marketing.fact_customer_session": 5_000}


# ---------- _delta_pct ----------


def test_delta_pct_returns_none_for_missing_prev() -> None:
    assert _delta_pct(None, 100) is None


def test_delta_pct_returns_none_for_zero_prev() -> None:
    # Avoid divide-by-zero; the previous baseline was empty/invalid.
    assert _delta_pct(0, 100) is None


def test_delta_pct_returns_none_for_negative_current() -> None:
    # -1 sentinel means "table unqueryable"; can't compute a meaningful delta.
    assert _delta_pct(100, -1) is None


def test_delta_pct_computes_growth() -> None:
    assert _delta_pct(100, 150) == pytest.approx(0.5)


def test_delta_pct_computes_shrinkage() -> None:
    assert _delta_pct(200, 150) == pytest.approx(-0.25)


def test_delta_pct_zero_when_unchanged() -> None:
    assert _delta_pct(100, 100) == 0.0


# ---------- _resolve_threshold ----------


def test_resolve_threshold_uses_default_when_missing() -> None:
    var_get = lambda _name, default_var=None: default_var  # noqa: E731
    assert _resolve_threshold(var_get) == DEFAULT_THRESHOLD


def test_resolve_threshold_parses_float_string() -> None:
    store = {THRESHOLD_VAR_NAME: "0.10"}
    var_get = lambda name, default_var=None: store.get(name, default_var)  # noqa: E731
    assert _resolve_threshold(var_get) == 0.10


def test_resolve_threshold_falls_back_on_invalid_string() -> None:
    store = {THRESHOLD_VAR_NAME: "not-a-number"}
    var_get = lambda name, default_var=None: store.get(name, default_var)  # noqa: E731
    assert _resolve_threshold(var_get) == DEFAULT_THRESHOLD


def test_resolve_threshold_clamps_to_unit_interval() -> None:
    for raw, expected in (("2.5", 1.0), ("-0.5", 0.0)):
        store = {THRESHOLD_VAR_NAME: raw}
        var_get = lambda name, default_var=None, s=store: s.get(name, default_var)  # noqa: E731
        assert _resolve_threshold(var_get) == expected


# ---------- reconcile_gold_row_counts (end-to-end with stubs) ----------


class _Cursor:
    """Answers ``SELECT COUNT(*) FROM <schema>.<table>`` from a dict; -1 raises."""

    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = counts
        self.queried: list[str] = []
        self._last = -1

    def execute(self, sql: str) -> None:
        relation = sql.rsplit(" ", 1)[1]
        self.queried.append(relation)
        self._last = self._counts.get(relation, -1)
        if self._last < 0:
            raise RuntimeError(f"relation {relation} does not exist")

    def fetchone(self) -> tuple[int]:
        return (self._last,)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Conn:
    def __init__(self, counts: dict[str, int]) -> None:
        self.cur = _Cursor({_pending(k): v for k, v in counts.items()})

    def cursor(self) -> _Cursor:
        return self.cur


def _pending(key: str) -> str:
    schema, table = key.split(".")
    return f"{schema}_pending.{table}"


def _store(baseline: dict[str, int] | None = None, *, legacy: dict[str, int] | None = None,
           threshold: str | None = None):
    store: dict[str, str] = {}
    if baseline is not None:
        store[baseline_var_name("marketing")] = json.dumps(baseline)
    if legacy is not None:
        store[BASELINE_VAR_NAME] = json.dumps(legacy)
    if threshold is not None:
        store[THRESHOLD_VAR_NAME] = threshold

    def var_get(name: str, default_var: str | None = None):
        return store.get(name, default_var)

    return var_get, store.__setitem__, store


def _run(counts: dict[str, int], var_get, var_set, **kwargs):
    conn = _Conn(counts)
    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set,
        scope="marketing", tables=list(_TABLES), **kwargs,
    )
    return result, conn


def _saved(store: dict[str, str]) -> dict[str, int]:
    return json.loads(store[baseline_var_name("marketing")])


def test_reads_only_owned_pending_tables() -> None:
    var_get, var_set, _ = _store()
    _, conn = _run(dict(_BASELINE), var_get, var_set)
    assert conn.cur.queried == ["marketing_pending.dim_customer",
                                "marketing_pending.fact_customer_session"]


def test_first_run_records_a_baseline_without_blocking() -> None:
    var_get, var_set, store = _store()
    result, _ = _run(dict(_BASELINE), var_get, var_set)
    assert result["blocked"] == [] and result["warned"] is False
    assert {d["status"] for d in result["deltas"].values()} == {"no_baseline"}
    assert _saved(store) == _BASELINE


def test_missing_scope_baseline_falls_back_to_legacy_shared_variable() -> None:
    legacy = {**_BASELINE, "finance.fact_sales": 9}
    var_get, var_set, store = _store(legacy=legacy)
    result, _ = _run({**_BASELINE, "marketing.dim_customer": 100}, var_get, var_set,
                     save_baseline=False)
    assert result["blocked"] == ["marketing.dim_customer"]
    assert "finance.fact_sales" not in result["deltas"]
    assert baseline_var_name("marketing") not in store


def test_scope_baseline_wins_over_legacy() -> None:
    var_get, var_set, _ = _store(baseline=dict(_BASELINE),
                                 legacy={"marketing.dim_customer": 1_000_000})
    result, _ = _run(dict(_BASELINE), var_get, var_set)
    assert result["deltas"]["marketing.dim_customer"]["status"] == "ok"


def test_unacknowledged_shrink_blocks_and_keeps_baseline() -> None:
    var_get, var_set, store = _store(baseline=dict(_BASELINE))
    result, _ = _run({**_BASELINE, "marketing.dim_customer": 200}, var_get, var_set)
    assert result["blocked"] == ["marketing.dim_customer"]
    assert result["baseline_candidate"] is None
    assert _saved(store) == _BASELINE


def test_acknowledged_shrink_publishes_and_becomes_the_baseline() -> None:
    var_get, var_set, store = _store(baseline=dict(_BASELINE))
    result, _ = _run({**_BASELINE, "marketing.dim_customer": 200}, var_get, var_set,
                     acknowledge_shrink=["marketing.dim_customer"])
    assert result["blocked"] == []
    assert result["warned"] is True
    assert result["acknowledged"] == ["marketing.dim_customer"]
    assert result["deltas"]["marketing.dim_customer"]["status"] == "acknowledged"
    assert _saved(store)["marketing.dim_customer"] == 200


def test_acknowledgement_only_covers_the_named_table() -> None:
    var_get, var_set, _ = _store(baseline=dict(_BASELINE))
    result, _ = _run({"marketing.dim_customer": 200, "marketing.fact_customer_session": 10},
                     var_get, var_set, acknowledge_shrink=["marketing.dim_customer"])
    assert result["blocked"] == ["marketing.fact_customer_session"]


def test_growth_warns_but_still_advances_the_baseline() -> None:
    var_get, var_set, store = _store(baseline=dict(_BASELINE))
    result, _ = _run({**_BASELINE, "marketing.fact_customer_session": 50_000},
                     var_get, var_set)
    assert result["warned"] is True and result["blocked"] == []
    assert result["deltas"]["marketing.fact_customer_session"]["status"] == "WARN"
    assert _saved(store)["marketing.fact_customer_session"] == 50_000


def test_unreadable_table_blocks_even_when_acknowledged() -> None:
    var_get, var_set, _ = _store(baseline=dict(_BASELINE))
    result, _ = _run({"marketing.fact_customer_session": 5_000}, var_get, var_set,
                     acknowledge_shrink=["marketing.dim_customer"])
    assert result["blocked"] == ["marketing.dim_customer"]
    assert result["deltas"]["marketing.dim_customer"]["status"] == "unavailable"


def test_acknowledging_a_table_the_scope_does_not_own_is_rejected() -> None:
    var_get, var_set, _ = _store()
    with pytest.raises(ValueError, match="finance.fact_sales"):
        _run(dict(_BASELINE), var_get, var_set, acknowledge_shrink=["finance.fact_sales"])


def test_empty_table_list_is_rejected() -> None:
    var_get, var_set, _ = _store()
    with pytest.raises(ValueError, match="no tables"):
        reconcile_gold_row_counts(conn=_Conn({}), var_get=var_get, var_set=var_set,
                                  scope="marketing", tables=[])


def test_custom_threshold_applies() -> None:
    var_get, var_set, _ = _store(baseline=dict(_BASELINE), threshold="0.05")
    result, _ = _run({**_BASELINE, "marketing.dim_customer": 900}, var_get, var_set)
    assert result["threshold"] == 0.05
    assert result["blocked"] == ["marketing.dim_customer"]


def test_corrupt_baseline_starts_fresh() -> None:
    var_get, var_set, store = _store()
    store[baseline_var_name("marketing")] = "not-valid-json{"
    result, _ = _run(dict(_BASELINE), var_get, var_set)
    assert result["blocked"] == []
    assert _saved(store) == _BASELINE


def test_save_candidate_writes_the_scope_variable() -> None:
    var_get, var_set, store = _store(baseline=dict(_BASELINE))
    result, _ = _run({**_BASELINE, "marketing.dim_customer": 1_050}, var_get, var_set,
                     save_baseline=False)
    assert _saved(store) == _BASELINE
    assert save_baseline_candidate(result, var_set=var_set) is True
    assert _saved(store)["marketing.dim_customer"] == 1_050
    assert BASELINE_VAR_NAME not in store
