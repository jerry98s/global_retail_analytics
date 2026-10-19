"""Entity ID ranges for the producer simulators and POS batch generator.

Single source of truth for STORE / PRODUCT / LOYALTY ID ranges so the
inventory producer, POS producer, POS Parquet batch generator, and any
future sim all draw from the same population.

Importing this module from a producer sim:
    from ingestion.kafka.sim_entities import STORES, PRODUCTS, LOYALTY_IDS

Importing the platform enum (which is schema-bound, not simulation-bound):
    from streaming.flink_jobs.event_types import PLATFORMS
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

# 20 stores — matches `the store list on main` seed rows.
# Update both files together if the store count changes.
STORES: list[str] = [f"STORE-{i:03d}" for i in range(1, 21)]

# 500 products — attributes live in PRODUCT_CATALOG below and are exported to
# the dbt seed `seeds/reference/product_master.csv` by
# `ingestion/batch/generate_product_master.py` (CI runs --check).
PRODUCTS: list[str] = [f"PROD-{i:04d}" for i in range(1, 501)]

# 10,000 loyalty IDs — synthetic customer anchor space for the identity graph.
# Real deployments replace this with the loyalty member dimension.
LOYALTY_IDS: list[str] = [f"LOYAL-{i:06d}" for i in range(1, 10_001)]


@dataclass(frozen=True)
class ProductAttributes:
    product_id: str
    sku: str
    product_name: str
    brand: str
    category_l1: str
    category_l2: str
    supplier_id: str
    list_price: Decimal
    unit_cost: Decimal


# (category_l1, category_l2 options, list price band)
_CATEGORY_TREE: tuple[tuple[str, tuple[str, ...], tuple[float, float]], ...] = (
    ("GROCERY", ("SNACKS", "BEVERAGES", "PANTRY"), (2.0, 25.0)),
    ("HOME", ("CLEANING", "KITCHEN"), (5.0, 60.0)),
    ("PERSONAL_CARE", ("SKIN_CARE", "HAIR_CARE"), (4.0, 40.0)),
    ("ELECTRONICS", ("ACCESSORIES", "AUDIO"), (15.0, 150.0)),
)
_BRANDS: tuple[str, ...] = ("HOUSE_BRAND", "ACME", "NORTHWIND", "CONTOSO", "FABRIKAM")
_SUPPLIERS: tuple[str, ...] = tuple(f"SUP-{i:03d}" for i in range(1, 26))
_CENT = Decimal("0.01")

# Own RNG instance: the catalog must not depend on (or disturb) the global
# `random` state that generate_pos_parquet seeds per --date.
_CATALOG_SEED = 20_260_101


def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(_CENT, rounding=ROUND_HALF_UP)


def _build_catalog() -> dict[str, ProductAttributes]:
    rng = random.Random(_CATALOG_SEED)
    catalog: dict[str, ProductAttributes] = {}
    for index, product_id in enumerate(PRODUCTS, start=1):
        category_l1, subcategories, (low, high) = rng.choice(_CATEGORY_TREE)
        category_l2 = rng.choice(subcategories)
        brand = rng.choice(_BRANDS)
        list_price = _money(rng.uniform(low, high))
        unit_cost = _money(list_price * Decimal(str(round(rng.uniform(0.45, 0.75), 4))))
        catalog[product_id] = ProductAttributes(
            product_id=product_id,
            sku=f"SKU-{index:06d}",
            product_name=f"{brand.replace('_', ' ').title()} {category_l2.replace('_', ' ').title()} {index:04d}",
            brand=brand,
            category_l1=category_l1,
            category_l2=category_l2,
            supplier_id=rng.choice(_SUPPLIERS),
            list_price=list_price,
            unit_cost=unit_cost,
        )
    return catalog


PRODUCT_CATALOG: dict[str, ProductAttributes] = _build_catalog()


# Shelf count per (product_id, store_id) before the first streamed movement.
# Exported to `seeds/reference/inventory_opening_balance.csv`; the inventory
# fact adds it to the running sum of stream deltas.
_OPENING_STOCK_SEED = 20_260_102
_OPENING_STOCK_RANGE = (20, 120)


def _build_opening_stock() -> dict[tuple[str, str], int]:
    rng = random.Random(_OPENING_STOCK_SEED)
    return {
        (product_id, store_id): rng.randint(*_OPENING_STOCK_RANGE)
        for product_id in PRODUCTS
        for store_id in STORES
    }


OPENING_STOCK: dict[tuple[str, str], int] = _build_opening_stock()
