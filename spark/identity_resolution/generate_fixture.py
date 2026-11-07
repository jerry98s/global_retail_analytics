"""Regenerate the dbt seed fixtures under seeds/silver/.

The DuckDB CI chain (`-DbtSource seeds`) has no Spark runtime, so the
fixture-mode chain reads ``source('silver', 'identity_resolution')`` and
``source('silver', 'device_session_associations')`` from committed CSVs
instead of from the Spark job's output.

The CSVs are *generated* from seeds/bronze/*.csv via graph_logic — the same
rules the PySpark/GraphFrames job executes — so the fixtures cannot drift
from the engine without `--check` failing in CI.

Usage:
    python spark/identity_resolution/generate_fixture.py          # rewrite CSVs
    python spark/identity_resolution/generate_fixture.py --check  # CI: fail on drift
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import asdict, fields
from pathlib import Path

from graph_logic import DeviceSessionAssociation, ResolutionRow, device_session_associations, resolve_identity_graph

_REPO = Path(__file__).resolve().parents[2]
SEEDS = _REPO / "transformation" / "dbt_project" / "seeds"
OUTPUTS = {
    "identity_resolution": SEEDS / "silver" / "identity_resolution.csv",
    "device_session_associations": SEEDS / "silver" / "device_session_associations.csv",
}


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _cell(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _render(rows: list, row_type: type) -> str:
    columns = [field.name for field in fields(row_type)]
    lines = [",".join(columns)]
    lines += [",".join(_cell(asdict(row)[column]) for column in columns) for row in rows]
    return "\n".join(lines) + "\n"


def render() -> dict[str, str]:
    clickstream = _read_csv(SEEDS / "bronze" / "clickstream_events.csv")
    pos = _read_csv(SEEDS / "bronze" / "pos_transactions.csv")
    _, resolution, _ = resolve_identity_graph(clickstream, pos)
    resolution = sorted(resolution, key=lambda r: (r.identifier_type, r.identifier_value))
    return {
        "identity_resolution": _render(resolution, ResolutionRow),
        "device_session_associations": _render(device_session_associations(clickstream), DeviceSessionAssociation),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true",
                        help="Do not write; fail if a committed CSV is stale.")
    args = parser.parse_args()

    stale = []
    for name, content in render().items():
        path = OUTPUTS[name]
        if args.check:
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing != content:
                stale.append(path)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"Wrote {content.count(chr(10)) - 1} rows to {path}.")

    if stale:
        for path in stale:
            print(f"ERROR: {path} is stale. Regenerate with "
                  "`python spark/identity_resolution/generate_fixture.py`.", file=sys.stderr)
        return 1
    if args.check:
        print(f"OK: {len(OUTPUTS)} fixtures match graph_logic output.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
