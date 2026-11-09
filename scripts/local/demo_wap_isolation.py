"""Show one blocked Gold scope leaving the others free to publish.

Works on a copy of the local DuckDB file, so the real warehouse is not changed.
The copy is cloned live to pending, then marketing is broken twice:

1. A duplicate session fails the marketing dbt test. Marketing is not published.
2. ``dim_customer`` shrinks past the row-count threshold. The marketing gate
   blocks. The finance gate passes, and finance publishes.

After each break the marketing live tables match their starting fingerprint.
Finance's publish is the finance ownership set only.

    .\\.venv\\Scripts\\python.exe scripts/local/demo_wap_isolation.py
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from orchestration.airflow.plugins.dbt_selectors import quality_selection  # noqa: E402
from orchestration.airflow.plugins.wap_publish import (  # noqa: E402
    WAP_SCOPES,
    clone_live_to_pending,
    publish_gold,
)

_SRC = _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
_DIR = _REPO / ".local" / "wap_isolation"
_COPY = _DIR / "local_retail.duckdb"
_STATE = _DIR / "gate.json"
_MARKETING = [f"{schema}.{table}" for schema, table in WAP_SCOPES["marketing"]]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _check(failures: list[str], name: str, ok: bool, detail: object = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  {detail}" if not ok else ""), flush=True)
    if not ok:
        failures.append(f"{name}: {detail}")


def _fingerprint(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    replay = _load("replay_check", _REPO / "scripts" / "local" / "replay_check.py")
    return {name: replay.fingerprint_table(con, *name.split("."))["hash"] for name in _MARKETING}


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(_COPY))


def _clone(*scopes: str) -> None:
    tables = [table for scope in scopes for table in WAP_SCOPES[scope]]
    con = _connect()
    try:
        clone_live_to_pending(con, tables, dialect="duckdb")
    finally:
        con.close()


def _gate(scope: str) -> int:
    return subprocess.run(
        [
            sys.executable, str(_REPO / "scripts" / "local" / "row_count_gate.py"),
            "audit", "--duckdb", str(_COPY), "--state", str(_STATE), "--scope", scope,
        ],
        cwd=_REPO,
    ).returncode


def _dbt_marketing_test() -> int:
    selection = quality_selection("marketing")
    command = [
        sys.executable, "-m", "dbt", "test", "--profiles-dir", ".", "--target", "local",
        "--vars", '{"wap_phase": "pending"}', "--select", str(selection["select"]),
    ]
    exclude = list(selection["exclude"])
    if exclude:
        command += ["--exclude", *exclude]
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(_COPY)
    return subprocess.run(command, cwd=_REPO / "transformation" / "dbt_project", env=env).returncode


def _publish_finance() -> list[str]:
    con = _connect()
    try:
        return publish_gold(con, WAP_SCOPES["finance"], dialect="duckdb")["published"]
    finally:
        con.close()


def _prepare() -> dict[str, str]:
    if not _SRC.exists():
        raise SystemExit(f"ERROR: {_SRC} not found. Run the local stack first.")
    if _DIR.exists():
        shutil.rmtree(_DIR)
    _DIR.mkdir(parents=True)
    source = duckdb.connect(str(_SRC))
    try:
        source.execute("CHECKPOINT")
    finally:
        source.close()
    shutil.copyfile(_SRC, _COPY)
    _clone("finance", "marketing", "catalog")
    con = _connect()
    try:
        start = _fingerprint(con)
        con.execute(
            "insert into finance_pending.fact_sales "
            "select * replace ('WAP-ISO-1' as transaction_id) "
            "from finance_pending.fact_sales limit 1"
        )
    finally:
        con.close()
    return start


def _break_duplicate() -> None:
    con = _connect()
    try:
        con.execute(
            "insert into marketing_pending.fact_customer_session "
            "select * from marketing_pending.fact_customer_session limit 1"
        )
    finally:
        con.close()


def _break_shrink() -> None:
    _clone("finance", "marketing")
    con = _connect()
    try:
        con.execute(
            "delete from marketing_pending.dim_customer where customer_key in ("
            "select customer_key from marketing_pending.dim_customer "
            "limit (select cast(count(*) * 0.8 as bigint) from marketing_pending.dim_customer))"
        )
    finally:
        con.close()


def main() -> int:
    print("Copying the warehouse and cloning live Gold to pending", flush=True)
    start = _prepare()
    gate = _load("row_count_gate", _REPO / "scripts" / "local" / "row_count_gate.py")
    if gate.audit(_COPY, _STATE, []) != 0 or gate.save(_STATE) != 0:
        print("ERROR: the clean copy did not pass the row-count gate.", file=sys.stderr)
        return 1
    failures: list[str] = []

    print("Break 1: a duplicate session fails the marketing test", flush=True)
    _break_duplicate()
    test_code = _dbt_marketing_test()
    _check(failures, "marketing dbt test fails", test_code != 0, test_code)
    published = _publish_finance() if test_code != 0 else []
    _check(failures, "finance publishes while marketing is not published",
           published == [f"{s}.{t}" for s, t in WAP_SCOPES["finance"]], published)
    con = _connect()
    try:
        _check(failures, "marketing live tables are unchanged after the failed test",
               _fingerprint(con) == start)
    finally:
        con.close()

    print("Break 2: dim_customer shrinks past the threshold", flush=True)
    _break_shrink()
    marketing_gate = _gate("marketing")
    finance_gate = _gate("finance")
    _check(failures, "marketing row-count gate blocks", marketing_gate == 1, marketing_gate)
    _check(failures, "finance row-count gate passes", finance_gate == 0, finance_gate)
    published = _publish_finance() if marketing_gate == 1 and finance_gate == 0 else []
    _check(failures, "finance publishes again while marketing stays blocked",
           published == [f"{s}.{t}" for s, t in WAP_SCOPES["finance"]], published)
    con = _connect()
    try:
        _check(failures, "marketing live tables are unchanged after the blocked shrink",
               _fingerprint(con) == start)
        sales = con.execute(
            "select count(*) from finance.fact_sales where transaction_id = 'WAP-ISO-1'"
        ).fetchone()[0]
        _check(failures, "the finance row added in pending was published", sales == 1, sales)
    finally:
        con.close()

    print()
    if failures:
        print(f"FAIL — {len(failures)} check(s). The real warehouse was not used.")
        return 1
    print("PASS — a blocked marketing scope left finance free to publish")
    return 0


if __name__ == "__main__":
    sys.exit(main())
