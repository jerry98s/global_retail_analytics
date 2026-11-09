"""Fingerprint business results in the local DuckDB file and compare two replays.

A fingerprint is, per table, the row count and an order-independent hash of the
business columns. Run-specific columns (export run ids, Spark ``computed_at``,
``last_updated_at``) are left out and doubles are rounded, so two runs over the
same Bronze, POS files and evaluation date must produce identical fingerprints.

    python scripts/local/replay_check.py snapshot --out .local/replay/a.json
    python scripts/local/replay_check.py compare .local/replay/a.json .local/replay/b.json

``snapshot`` reads Gold from the ``*_pending`` copies, i.e. after ``-Task dbt``
and before publish. ``-Task replay`` in ``run_local_stack.ps1`` drives both.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from orchestration.airflow.plugins.wap_publish import WAP_TABLES  # noqa: E402

RUN_COLUMNS = frozenset({"export_run_id", "run", "computed_at", "last_updated_at"})

SPARK_EXPORTS = [
    ("silver", "clickstream_events"),
    ("silver", "identity_resolution"),
    ("silver", "identity_edges"),
    ("silver", "loyalty_link_claims"),
    ("silver", "device_session_associations"),
]
BUSINESS_MODELS = [
    ("intermediate", "int_identity_resolution"),
    ("intermediate", "int_identity_changes"),
    ("intermediate", "int_session_reconstruction"),
    ("intermediate", "int_customer_consent"),
    ("intermediate", "int_rfm_scoring"),
    ("intermediate", "int_order_reconciliation"),
]


def _column_expr(name: str, data_type: str) -> str:
    quoted = f'"{name}"'
    return f"round({quoted}, 9)" if data_type in ("DOUBLE", "FLOAT", "REAL") else quoted


def fingerprint_table(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> dict:
    columns = con.execute(
        "select column_name, data_type from information_schema.columns "
        "where table_schema = ? and table_name = ? order by ordinal_position",
        [schema, table],
    ).fetchall()
    if not columns:
        raise ValueError(f"{schema}.{table} does not exist")
    kept = [(name, data_type) for name, data_type in columns if name not in RUN_COLUMNS]
    row_hash = f"hash({', '.join(_column_expr(n, t) for n, t in kept)})"
    rows, digest = con.execute(
        f'select count(*), coalesce(sum({row_hash}::hugeint), 0)::varchar from "{schema}"."{table}"'
    ).fetchone()
    return {"rows": rows, "hash": digest, "columns": [n for n, _ in kept]}


def snapshot(con: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    tables = SPARK_EXPORTS + BUSINESS_MODELS + [(f"{schema}_pending", table) for schema, table in WAP_TABLES]
    return {f"{schema}.{table}": fingerprint_table(con, schema, table) for schema, table in tables}


def compare(first: dict[str, dict], second: dict[str, dict]) -> list[str]:
    differences = []
    for name in sorted(first.keys() | second.keys()):
        a, b = first.get(name), second.get(name)
        if a is None or b is None:
            differences.append(f"{name}: only in {'first' if b is None else 'second'} snapshot")
        elif a["columns"] != b["columns"]:
            differences.append(f"{name}: columns changed")
        elif a["rows"] != b["rows"]:
            differences.append(f"{name}: rows {a['rows']} -> {b['rows']}")
        elif a["hash"] != b["hash"]:
            differences.append(f"{name}: same {a['rows']} rows, different content")
    return differences


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("--duckdb", type=Path, default=_REPO / "transformation" / "dbt_project" / "local_retail.duckdb")
    snap.add_argument("--out", type=Path, required=True)
    cmp_ = sub.add_parser("compare")
    cmp_.add_argument("first", type=Path)
    cmp_.add_argument("second", type=Path)
    args = parser.parse_args(argv)

    if args.command == "snapshot":
        con = duckdb.connect(str(args.duckdb), read_only=True)
        try:
            result = snapshot(con)
        finally:
            con.close()
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
        for name, entry in result.items():
            print(f"  {name:48s} {entry['rows']:>10,} rows")
        print(f"Fingerprint of {len(result)} tables written to {args.out}")
        return 0

    first = json.loads(args.first.read_text(encoding="utf-8"))
    second = json.loads(args.second.read_text(encoding="utf-8"))
    differences = compare(first, second)
    if differences:
        print(f"REPLAY MISMATCH — {len(differences)} of {len(first)} tables differ:")
        for line in differences:
            print(f"  - {line}")
        return 1
    print(f"REPLAY MATCH — {len(first)} tables identical across both runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
