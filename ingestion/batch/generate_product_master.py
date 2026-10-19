"""Export simulator reference data to the dbt seeds under seeds/reference/.

`ingestion.kafka.sim_entities` is the single definition of:
- PRODUCT_CATALOG -> product_master.csv. The POS generator prices line items
  from it, and dbt's `int_product_catalog` reads the exported seed, so SCD2 on
  `dim_product` tracks catalog attributes instead of values derived from sales.
- OPENING_STOCK -> inventory_opening_balance.csv. `fact_inventory_snapshot`
  adds it to the running sum of streamed inventory deltas.

Usage (from the repo root):
    python -m ingestion.batch.generate_product_master          # rewrite CSVs
    python -m ingestion.batch.generate_product_master --check  # CI: fail on drift
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ingestion.kafka.sim_entities import OPENING_STOCK, PRODUCT_CATALOG

_REPO = Path(__file__).resolve().parents[2]
SEED_DIR = _REPO / "transformation" / "dbt_project" / "seeds" / "reference"
OUTPUT = SEED_DIR / "product_master.csv"
OPENING_BALANCE_OUTPUT = SEED_DIR / "inventory_opening_balance.csv"

COLUMNS = [
    "product_id",
    "sku",
    "product_name",
    "brand",
    "category_l1",
    "category_l2",
    "supplier_id",
    "list_price",
    "unit_cost",
]


def render_csv() -> str:
    lines = [",".join(COLUMNS)]
    for product_id in sorted(PRODUCT_CATALOG):
        attrs = PRODUCT_CATALOG[product_id]
        lines.append(",".join(str(getattr(attrs, column)) for column in COLUMNS))
    return "\n".join(lines) + "\n"


def render_opening_balance_csv() -> str:
    lines = ["product_id,store_id,opening_qty"]
    for (product_id, store_id), qty in sorted(OPENING_STOCK.items()):
        lines.append(f"{product_id},{store_id},{qty}")
    return "\n".join(lines) + "\n"


SEEDS = {
    OUTPUT: render_csv,
    OPENING_BALANCE_OUTPUT: render_opening_balance_csv,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true",
                        help="Do not write; fail if a committed CSV is stale.")
    args = parser.parse_args()

    if args.check:
        stale = []
        for path, render in SEEDS.items():
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing.replace("\r\n", "\n") != render():
                stale.append(path)
        for path in stale:
            print(
                f"ERROR: {path} is stale. Regenerate with "
                "`python -m ingestion.batch.generate_product_master`.",
                file=sys.stderr,
            )
        if stale:
            return 1
        print(f"OK: {len(SEEDS)} reference seeds match sim_entities.")
        return 0

    SEED_DIR.mkdir(parents=True, exist_ok=True)
    for path, render in SEEDS.items():
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(render())
        print(f"Wrote {path}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
