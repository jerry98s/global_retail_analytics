"""Local row-count publish gate: each owning DAG's gate against DuckDB.

Local ``-Task quality`` publishes every WAP scope (finance, marketing, catalog)
in one step, so the audit runs the shared ``reconcile_gold_row_counts`` once per
scope over that scope's ``*_pending`` tables, each against its own baseline.
The Airflow Variables become one local JSON file. As on cloud, the audit never
saves a baseline; ``save`` does that after a successful publish.

``--acknowledge-shrink schema.table`` (repeatable) lets a named table shrink
past the threshold in this run only; the gate reports it as ``acknowledged``.
``--scope`` (repeatable) audits one owning DAG instead of all three, so a
blocked marketing table does not block finance.

Usage::

    python scripts/local/row_count_gate.py audit --duckdb <db> --state <json> [--scope <name> ...] [--acknowledge-shrink <schema.table> ...]
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
from orchestration.airflow.plugins.wap_publish import WAP_SCOPES  # noqa: E402

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


def _scope_tables() -> dict[str, list[str]]:
    return {scope: [f"{s}.{t}" for s, t in tables] for scope, tables in WAP_SCOPES.items()}


def audit(
    duckdb_path: Path,
    state_path: Path,
    acknowledge_shrink: list[str],
    scopes: list[str] | None = None,
) -> int:
    import duckdb

    available = _scope_tables()
    unknown_scopes = sorted(set(scopes or []) - set(available))
    if unknown_scopes:
        print("--scope is not an owning DAG: " + ", ".join(unknown_scopes), file=sys.stderr)
        return 2
    chosen = {scope: available[scope] for scope in scopes} if scopes else available
    owned = {key for tables in chosen.values() for key in tables}
    unknown = sorted(set(acknowledge_shrink) - owned)
    if unknown:
        print("--acknowledge-shrink is outside this audit's scope: " + ", ".join(unknown),
              file=sys.stderr)
        return 2

    state = _read_state(state_path)
    var_get, var_set = _var_store(state)
    results: dict[str, Any] = {}
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        for scope, tables in chosen.items():
            results[scope] = reconcile_gold_row_counts(
                conn=con,
                var_get=var_get,
                var_set=var_set,
                scope=scope,
                tables=tables,
                acknowledge_shrink=[k for k in acknowledge_shrink if k in tables],
                save_baseline=False,
            )
    finally:
        con.close()
    state[_LAST_AUDIT] = results
    _write_state(state_path, state)

    blocked: list[str] = []
    for scope, result in results.items():
        print(f"[{scope}]")
        for key, item in result["deltas"].items():
            print(f"  {key}: {item['previous']} -> {item['current']} ({item['status']})")
        blocked += result["blocked"]
    if blocked:
        threshold = next(iter(results.values()))["threshold"]
        print(
            "Row-count gate failed; publish blocked for " + ", ".join(blocked)
            + f" (unreadable or shrank more than {threshold:.0%}; pass"
            " -AcknowledgeShrink <schema.table> for an expected shrink)",
            file=sys.stderr,
        )
        return 1
    return 0


def save(state_path: Path) -> int:
    state = _read_state(state_path)
    _, var_set = _var_store(state)
    results = state.pop(_LAST_AUDIT, None) or {}
    saved = [scope for scope, result in results.items()
             if save_baseline_candidate(result, var_set=var_set)]
    _write_state(state_path, state)
    print("Row-count baseline saved for: " + ", ".join(saved) if saved
          else "No unblocked audit; baseline unchanged.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    audit_p = sub.add_parser("audit", help="Gate pending Gold before publish.")
    audit_p.add_argument("--duckdb", type=Path, required=True)
    audit_p.add_argument("--state", type=Path, required=True)
    audit_p.add_argument("--scope", action="append", default=[], choices=tuple(WAP_SCOPES),
                         help="Owning DAG to audit. Repeatable. Default: all of them.")
    audit_p.add_argument("--acknowledge-shrink", action="append", default=[],
                         metavar="SCHEMA.TABLE")
    save_p = sub.add_parser("save", help="Save the last unblocked audit after publish.")
    save_p.add_argument("--state", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "audit":
        return audit(args.duckdb, args.state, args.acknowledge_shrink, args.scope or None)
    return save(args.state)


if __name__ == "__main__":
    raise SystemExit(main())
