"""Product master: simulator catalog and the dbt seed stay in sync."""

from __future__ import annotations

from decimal import Decimal

from ingestion.batch import generate_product_master as gpm
from ingestion.kafka.sim_entities import OPENING_STOCK, PRODUCT_CATALOG, PRODUCTS, STORES


def test_catalog_covers_every_simulated_product() -> None:
    assert set(PRODUCT_CATALOG) == set(PRODUCTS)


def test_catalog_is_deterministic() -> None:
    from ingestion.kafka import sim_entities

    assert sim_entities._build_catalog() == PRODUCT_CATALOG


def test_cost_below_list_price() -> None:
    for attrs in PRODUCT_CATALOG.values():
        assert Decimal("0") < attrs.unit_cost < attrs.list_price


def test_committed_seed_matches_catalog() -> None:
    committed = gpm.OUTPUT.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == gpm.render_csv()


def test_opening_stock_covers_every_product_store() -> None:
    assert set(OPENING_STOCK) == {(p, s) for p in PRODUCTS for s in STORES}
    assert all(qty > 0 for qty in OPENING_STOCK.values())


def test_opening_stock_is_deterministic() -> None:
    from ingestion.kafka import sim_entities

    assert sim_entities._build_opening_stock() == OPENING_STOCK


def test_committed_opening_balance_matches_entities() -> None:
    committed = gpm.OPENING_BALANCE_OUTPUT.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == gpm.render_opening_balance_csv()
