"""Regenerate the dbt seed fixture seeds/silver/identity_resolution.csv.

The DuckDB CI chain (`-DbtSource seeds`) has no Spark runtime, so the
fixture-mode identity chain reads ``source('silver', 'identity_resolution')``
from this committed CSV instead of from the Spark job's Iceberg output.

The CSV is *generated* from seeds/bronze/*.csv via graph_logic — the same
rules the PySpark/GraphFrames job executes — so the fixture cannot drift
from the engine without `--check` failing in CI.

Usage:
    python spark/identity_resolution/generate_fixture.py          # rewrite CSV
    python spark/identity_resolution/generate_fixture.py --check  # CI: fail on drift
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from graph_logic import resolve_identity_graph

_REPO = Path(__file__).resolve().parents[2]
SEEDS = _REPO / "transformation" / "dbt_project" / "seeds"
OUTPUT = SEEDS / "silver" / "identity_resolution.csv"

COLUMNS = [
    "identifier_type",
    "identifier_value",
    "customer_key",
    "confidence_score",
    "resolution_method",
    "is_public_device",
    "component_rep_node",
    "component_rep_type",
]


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def render_rows() -> list[dict]:
    clickstream = _read_csv(SEEDS / "bronze" / "clickstream_events.csv")
    pos = _read_csv(SEEDS / "bronze" / "pos_transactions.csv")
    _, resolution, _ = resolve_identity_graph(clickstream, pos)
    return [
        {
            "identifier_type": row.identifier_type,
            "identifier_value": row.identifier_value,
            "customer_key": row.customer_key,
            "confidence_score": f"{row.confidence_score:.4f}",
            "resolution_method": row.resolution_method,
            "is_public_device": str(row.is_public_device).lower(),
            "component_rep_node": row.component_rep_node,
            "component_rep_type": row.component_rep_type,
        }
        for row in sorted(
            resolution, key=lambda r: (r.identifier_type, r.identifier_value)
        )
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true",
                        help="Do not write; fail if the committed CSV is stale.")
    args = parser.parse_args()

    rows = render_rows()
    rendered = [",".join(COLUMNS)]
    rendered += [",".join(str(row[col]) for col in COLUMNS) for row in rows]
    content = "\n".join(rendered) + "\n"

    if args.check:
        existing = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if existing != content:
            print(
                f"ERROR: {OUTPUT} is stale. Regenerate with "
                "`python spark/identity_resolution/generate_fixture.py`.",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {OUTPUT} matches graph_logic output ({len(rows)} rows).")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {OUTPUT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
