"""Unit tests for the row-count reconciliation pure functions (P2.5).

The Airflow entrypoint (``reconcile_gold_row_counts_task``) pulls in
``redshift_connector`` and ``airflow.models.Variable`` — both unavailable
in the offline unit suite. These tests target the dependency-injected
core (``reconcile_gold_row_counts``) with stub ``conn``/``var_get``/``var_set``
doubles, so they run under ``pytest -m unit`` without any cloud deps.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from orchestration.airflow.plugins.row_count_reconciliation import (
    BASELINE_VAR_NAME,
    DEFAULT_THRESHOLD,
    GOLD_MARTS,
    THRESHOLD_VAR_NAME,
    _delta_pct,
    _resolve_threshold,
    reconcile_gold_row_counts,
)

pytestmark = pytest.mark.unit

# Counts aligned to GOLD_MARTS order (11 marts including summary.*).
_DEFAULT_COUNTS = [3653, 20, 100_000, 720, 5_000, 100, 8_000, 50_000, 500, 200, 1_000]

_DEFAULT_BASELINE = {
    "finance.dim_date": 3653,
    "finance.dim_store": 20,
    "finance.fact_sales": 100_000,
    "finance.fact_inventory_snapshot": 720,
    "marketing.dim_customer": 5_000,
    "marketing.dim_product": 100,
    "marketing.identity_graph": 8_000,
    "marketing.fact_customer_session": 50_000,
    "summary.sales_daily_store": 500,
    "summary.inventory_daily_product_store": 200,
    "summary.sessions_daily_platform": 1_000,
}


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
    # 100 -> 150 is +50%
    assert _delta_pct(100, 150) == pytest.approx(0.5)


def test_delta_pct_computes_shrinkage() -> None:
    # 200 -> 150 is -25%
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


def test_resolve_threshold_clamps_above_one() -> None:
    # Nonsensical threshold (>100%) — clamp to 1.0.
    store = {THRESHOLD_VAR_NAME: "2.5"}
    var_get = lambda name, default_var=None: store.get(name, default_var)  # noqa: E731
    assert _resolve_threshold(var_get) == 1.0


def test_resolve_threshold_clamps_below_zero() -> None:
    store = {THRESHOLD_VAR_NAME: "-0.5"}
    var_get = lambda name, default_var=None: store.get(name, default_var)  # noqa: E731
    assert _resolve_threshold(var_get) == 0.0


def test_gold_marts_include_summary_and_marketing_dim_product() -> None:
    assert ("marketing", "dim_product") in GOLD_MARTS
    assert ("summary", "sales_daily_store") in GOLD_MARTS
    assert ("summary", "sessions_daily_platform") in GOLD_MARTS


# ---------- reconcile_gold_row_counts (end-to-end with stubs) ----------


@dataclass
class _StubCursor:
    """Yields successive row counts for ``SELECT COUNT(*)`` queries."""

    counts: list[int]
    _idx: int = 0

    def execute(self, _sql: str) -> None:
        pass  # SQL not validated here — the test inspects call sequence via _idx

    def fetchone(self) -> tuple[int]:
        idx = self._idx
        self._idx += 1
        return (self.counts[idx],)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _StubConn:
    """Returns a single cursor that yields canned COUNT results in order."""

    def __init__(self, counts: list[int]) -> None:
        self._cursor = _StubCursor(counts=counts)

    def cursor(self) -> _StubCursor:
        return self._cursor

    def close(self) -> None:
        pass


def _make_var_stores(baseline: dict[str, int] | None = None,
                     threshold: str | None = None):
    """Build (var_get, var_store) pairs backed by an in-memory dict."""
    store: dict[str, str] = {}
    if baseline is not None:
        store[BASELINE_VAR_NAME] = json.dumps(baseline)
    if threshold is not None:
        store[THRESHOLD_VAR_NAME] = threshold

    def var_get(name: str, default_var: str | None = None):
        return store.get(name, default_var)

    def var_set(name: str, value: str):
        store[name] = value

    return var_get, var_set, store


def test_reconcile_seeds_baseline_on_first_run() -> None:
    # No prior baseline — should not warn, and should persist a new baseline.
    conn = _StubConn(counts=list(_DEFAULT_COUNTS))
    var_get, var_set, store = _make_var_stores(baseline=None)

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    assert result["warned"] is False
    persisted = json.loads(store[BASELINE_VAR_NAME])
    assert persisted["finance.dim_date"] == 3653
    assert persisted["marketing.dim_product"] == 100
    assert persisted["marketing.fact_customer_session"] == 50_000
    assert persisted["summary.sales_daily_store"] == 500
    assert len(persisted) == len(GOLD_MARTS)


def test_reconcile_warns_on_large_drop() -> None:
    # fact_sales dropped from 100k to 30k — -70%, well above the 20% threshold.
    counts = [3653, 20, 30_000, 720, 5_000, 100, 8_000, 50_000, 500, 200, 1_000]
    conn = _StubConn(counts=counts)
    var_get, var_set, store = _make_var_stores(baseline=dict(_DEFAULT_BASELINE))

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    assert result["warned"] is True
    assert result["deltas"]["finance.fact_sales"]["status"] == "WARN"
    assert result["deltas"]["finance.fact_sales"]["delta_pct"] == pytest.approx(-0.7)
    # Baseline NOT updated — previous baseline preserved.
    persisted = json.loads(store[BASELINE_VAR_NAME])
    assert persisted["finance.fact_sales"] == 100_000


def test_reconcile_warns_on_large_spike() -> None:
    # fact_customer_session spiked from 50k to 300k — +500%.
    baseline = {"marketing.fact_customer_session": 50_000}
    counts = [3653, 20, 100_000, 720, 5_000, 100, 8_000, 300_000, 500, 200, 1_000]
    conn = _StubConn(counts=counts)
    var_get, var_set, _store = _make_var_stores(baseline=baseline)

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    assert result["warned"] is True
    assert result["deltas"]["marketing.fact_customer_session"]["status"] == "WARN"
    # Other marts have no baseline entries -> status "no_baseline", not "WARN".
    assert result["deltas"]["finance.dim_date"]["status"] == "no_baseline"


def test_reconcile_no_warning_when_within_threshold() -> None:
    # Small day-over-day growth (+5%) — under the 20% threshold.
    counts = [3653, 20, 105_000, 720, 5_000, 100, 8_000, 50_000, 500, 200, 1_000]
    conn = _StubConn(counts=counts)
    var_get, var_set, store = _make_var_stores(baseline=dict(_DEFAULT_BASELINE))

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    assert result["warned"] is False
    # Baseline updated to the new counts.
    persisted = json.loads(store[BASELINE_VAR_NAME])
    assert persisted["finance.fact_sales"] == 105_000


def test_reconcile_respects_custom_threshold() -> None:
    # Tighter 5% threshold; +10% growth should warn.
    counts = [3653, 20, 110_000, 720, 5_000, 100, 8_000, 50_000, 500, 200, 1_000]
    conn = _StubConn(counts=counts)
    var_get, var_set, _store = _make_var_stores(
        baseline=dict(_DEFAULT_BASELINE), threshold="0.05"
    )

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    assert result["warned"] is True
    assert result["threshold"] == 0.05
    assert result["deltas"]["finance.fact_sales"]["status"] == "WARN"


def test_reconcile_handles_corrupt_baseline_gracefully() -> None:
    # Baseline Variable contains invalid JSON — should start fresh, not crash.
    store = {BASELINE_VAR_NAME: "not-valid-json{"}
    var_get = lambda name, default_var=None: store.get(name, default_var)  # noqa: E731
    var_set = lambda name, value: store.__setitem__(name, value)  # noqa: E731
    conn = _StubConn(counts=list(_DEFAULT_COUNTS))

    result = reconcile_gold_row_counts(
        conn=conn, var_get=var_get, var_set=var_set
    )

    # First run with no usable baseline -> no warning, baseline seeded.
    assert result["warned"] is False
    persisted = json.loads(store[BASELINE_VAR_NAME])
    assert persisted["finance.fact_sales"] == 100_000
