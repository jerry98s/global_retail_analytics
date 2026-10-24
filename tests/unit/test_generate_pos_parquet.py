"""Unit tests for ingestion.batch.generate_pos_parquet (P3.6 — determinism)."""

from __future__ import annotations

import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# generate_pos_parquet lives under ingestion/batch — add the repo root to
# sys.path so the module imports cleanly without a package install.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _stub_heavy_deps() -> None:
    """Stub pyarrow + boto3 so the module imports without those wheels.

    The determinism tests only exercise `generate_rows` and `PosLine.generate`,
    which never touch the Parquet/S3 code paths. Installing pyarrow just to
    run a pure-Python RNG test would be disproportionate in CI.
    """
    for mod_name in ("pyarrow", "pyarrow.parquet", "boto3"):
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


@pytest.fixture(scope="module")
def gen_module():
    _stub_heavy_deps()
    return importlib.import_module("ingestion.batch.generate_pos_parquet")


def test_two_runs_same_date_produce_identical_rows(gen_module) -> None:
    """Same --date -> identical transaction_ids, line counts, and measures."""
    txn_date = date(2026, 7, 4)
    first = gen_module.generate_rows(txn_date, transaction_count=50, max_lines=5)
    second = gen_module.generate_rows(txn_date, transaction_count=50, max_lines=5)

    assert len(first) == len(second)
    for a, b in zip(first, second):
        assert a.transaction_id == b.transaction_id
        assert a.line_item_number == b.line_item_number
        assert a.store_id == b.store_id
        assert a.product_id == b.product_id
        assert a.loyalty_id == b.loyalty_id
        assert a.quantity_sold == b.quantity_sold
        assert a.gross_revenue == b.gross_revenue
        assert a.net_revenue == b.net_revenue
        assert a.gross_margin == b.gross_margin
        assert a.is_voided == b.is_voided


def test_different_dates_produce_different_rows(gen_module) -> None:
    """Two different --date values should not collide on transaction_id."""
    a = gen_module.generate_rows(date(2026, 7, 4), 10, 3)
    b = gen_module.generate_rows(date(2026, 7, 5), 10, 3)
    a_ids = {r.transaction_id for r in a}
    b_ids = {r.transaction_id for r in b}
    assert a_ids.isdisjoint(b_ids)


def test_explicit_seed_overrides_date_derived(gen_module) -> None:
    """--seed <int> should produce a distinct draw vs. the date-derived default."""
    txn_date = date(2026, 7, 4)
    default = gen_module.generate_rows(txn_date, 25, 4)
    override = gen_module.generate_rows(txn_date, 25, 4, seed_override=424242)
    # At least one row should differ on either txn_id or a measure — checking
    # the whole list for inequality is the stronger assertion.
    default_dump = [(r.transaction_id, r.quantity_sold, r.gross_revenue) for r in default]
    override_dump = [(r.transaction_id, r.quantity_sold, r.gross_revenue) for r in override]
    assert default_dump != override_dump


def test_seed_is_deterministic_for_same_override(gen_module) -> None:
    """Two runs with the same --seed should match (independent of date)."""
    a = gen_module.generate_rows(date(2026, 7, 4), 20, 3, seed_override=7)
    b = gen_module.generate_rows(date(2026, 7, 5), 20, 3, seed_override=7)
    # Measures (quantity/revenue) are driven by `random` post-seed, so they
    # match; transaction_id is uuid5(date, n) so they differ by design.
    for ra, rb in zip(a, b):
        assert ra.quantity_sold == rb.quantity_sold
        assert ra.gross_revenue == rb.gross_revenue
        assert ra.store_id == rb.store_id


def test_store_and_loyalty_are_transaction_level(gen_module) -> None:
    """Every line in a ticket shares one store and one (possibly absent) loyalty ID."""
    rows = gen_module.generate_rows(date(2026, 7, 4), transaction_count=200, max_lines=5)
    by_txn: dict[str, set[tuple[str, object]]] = {}
    for row in rows:
        by_txn.setdefault(row.transaction_id, set()).add((row.store_id, row.loyalty_id))
    assert any(len([r for r in rows if r.transaction_id == t]) > 1 for t in by_txn)
    assert all(len(header) == 1 for header in by_txn.values())


def test_line_measures_follow_product_catalog(gen_module) -> None:
    """gross = qty x list price, net within a 0-20% discount, margin = net - qty x cost."""
    from ingestion.kafka.sim_entities import PRODUCT_CATALOG

    rows = gen_module.generate_rows(date(2026, 7, 4), transaction_count=100, max_lines=4)
    for row in rows:
        product = PRODUCT_CATALOG[row.product_id]
        qty = row.quantity_sold
        assert row.gross_revenue == (product.list_price * qty).quantize(Decimal("0.01"))
        assert row.gross_revenue * Decimal("0.79") <= row.net_revenue <= row.gross_revenue
        assert row.gross_margin == (row.net_revenue - product.unit_cost * qty).quantize(Decimal("0.01"))
        assert row.gross_margin > 0
