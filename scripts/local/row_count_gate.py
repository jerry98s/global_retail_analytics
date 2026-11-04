"""Local row-count publish gate: the warehouse DAG's gate against DuckDB.

Runs the shared ``reconcile_gold_row_counts`` from the Airflow plugin over the
tables the warehouse DAG owns (``FINANCE_SUMMARY_TABLES``), read from their
``*_pending`` schemas before WAP publish. The Airflow Variables become one
local JSON file. As on cloud, the audit never saves the baseline; ``save``
does that after a successful publish.

Usage::

    python scripts/local/row_count_gate.py audit --duckdb <db> --state <json>
    python scripts/local/row_count_gate.py save --state <json>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from orchestration.airflow.plugins.row_count_reconciliation import (  # noqa: E402
    reconcile_gold_row_counts,
    save_baseline_candidate,
)
from orchestration.airflow.plugins.wap_publish import FINANCE_SUMMARY_TABLES  # noqa: E402

_LAST_AUDIT = "last_audit"
_VARIABLES = "variables"


def _read_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {_VARIABLES: {}}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def _var_store(state: dict[str, Any]):
    variables = state.setdefault(_VARIABLES, {})

    def var_get(name: str, default_var: str | None = None) -> str | None:
        return variables.get(name, default_var)

    def var_set(name: str, value: str) -> None:
        variables[name] = value

    return var_get, var_set


def audit(duckdb_path: Path, state_path: Path) -> int:
    import duckdb

    state = _read_state(state_path)
    var_get, var_set = _var_store(state)
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        result = reconcile_gold_row_counts(
            conn=con,
            var_get=var_get,
            var_set=var_set,
            pending_tables=[f"{s}.{t}" for s, t in FINANCE_SUMMARY_TABLES],
            save_baseline=False,
        )
    finally:
        con.close()
    state[_LAST_AUDIT] = result
    _write_state(state_path, state)
    for key, item in result["deltas"].items():
        print(f"  {key}: {item['previous']} -> {item['current']} ({item['status']})")
    if result["blocked"]:
        print(
            "Row-count gate failed; publish blocked for " + ", ".join(result["blocked"])
            + f" (unreadable or shrank more than {result['threshold']:.0%})",
            file=sys.stderr,
        )
        return 1
    return 0


def save(state_path: Path) -> int:
    state = _read_state(state_path)
    _, var_set = _var_store(state)
    saved = save_baseline_candidate(state.pop(_LAST_AUDIT, None), var_set=var_set)
    _write_state(state_path, state)
    print("Row-count baseline saved." if saved else "No clean audit; baseline unchanged.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    audit_p = sub.add_parser("audit", help="Gate pending Gold before publish.")
    audit_p.add_argument("--duckdb", type=Path, required=True)
    audit_p.add_argument("--state", type=Path, required=True)
    save_p = sub.add_parser("save", help="Save the last clean audit after publish.")
    save_p.add_argument("--state", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "audit":
        return audit(args.duckdb, args.state)
    return save(args.state)


if __name__ == "__main__":
    raise SystemExit(main())
