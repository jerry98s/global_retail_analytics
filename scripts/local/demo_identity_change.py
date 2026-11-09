"""Show an identity change propagating through the local stack.

Phase 1 publishes three separate customers: an account that signed in without
verifying its card, and two cards that each bought something in store. Phase 2
verifies the account to both cards. The lower card stays the anchor, so the
next build must:

- rekey the account and the higher card onto the lower card
- restate the account's session onto that customer
- rekey the higher card's POS lines onto it
- drop the two old customer keys from dim_customer

Requires Kafka and Flink already running, and these ids absent from the
published graph. Phase 1 is published before phase 2 is built, because
int_identity_changes compares against the published graph. The phase 2 checks
read the pending Gold; the build is published only after they pass.

    .\\.venv\\Scripts\\python.exe scripts/local/demo_identity_change.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from ingestion.batch.generate_pos_parquet import write_identity_change_parquet  # noqa: E402
from ingestion.kafka.sim_entities import (  # noqa: E402
    IDENTITY_CHANGE_ACCOUNT,
    IDENTITY_CHANGE_CARD,
    IDENTITY_CHANGE_CLIENT,
    IDENTITY_CHANGE_OTHER_CARD,
)

_DB = _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
_IDS = (IDENTITY_CHANGE_ACCOUNT, IDENTITY_CHANGE_CARD, IDENTITY_CHANGE_OTHER_CARD)
_BRONZE_WAIT_SECONDS = 60


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("DUCKDB_PATH", None)
    return env


def _run(cmd: list[str]) -> None:
    print(">>", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=_REPO, env=_env())
    if result.returncode != 0:
        raise SystemExit(f"command failed with exit code {result.returncode}: {' '.join(cmd)}")


def _stack(task: str) -> None:
    _run(["powershell", "-NoProfile", "-File", str(_REPO / "scripts" / "local" / "run_local_stack.ps1"), "-Task", task])


def _produce(phase: int) -> None:
    _run([
        sys.executable, "-m", "ingestion.kafka.producer_sim.clickstream_producer",
        "--scenario", "identity-change", "--phase", str(phase),
    ])


def _query(sql: str, params: list | None = None) -> list[tuple]:
    con = duckdb.connect(str(_DB), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def _published_keys() -> dict[str, tuple]:
    rows = _query(
        "select identifier_value, customer_key, resolution_method from marketing.identity_graph "
        "where identifier_value in (?, ?, ?)",
        list(_IDS),
    )
    return {value: (key, method) for value, key, method in rows}


def _check(failures: list[str], name: str, ok: bool, detail: object = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  {detail}" if not ok else ""), flush=True)
    if not ok:
        failures.append(f"{name}: {detail}")


def _phase1(keys: dict[str, tuple]) -> list[str]:
    failures: list[str] = []
    account, card, other = (keys.get(i) for i in _IDS)
    distinct = {item[0] for item in keys.values()}
    _check(failures, "three identifiers, three customers",
           account and card and other and len(distinct) == 3, keys)
    if not account:
        return failures
    _check(failures, "account is standalone", account[1] == "customer_id_standalone", account)
    _check(failures, "both cards anchor themselves",
           card[1] == "component_anchor" and other[1] == "component_anchor", (card, other))
    sessions = _query(
        "select customer_key, attribution_status from marketing.fact_customer_session where client_id = ?",
        [IDENTITY_CHANGE_CLIENT],
    )
    _check(failures, "the sign-in session belongs to the account",
           sessions == [(account[0], "attributed")], sessions)
    sales = dict(_query(
        "select loyalty_id, customer_key from finance.fact_sales where loyalty_id in (?, ?)",
        [IDENTITY_CHANGE_CARD, IDENTITY_CHANGE_OTHER_CARD],
    ))
    _check(failures, "each card's purchase belongs to that card",
           sales == {IDENTITY_CHANGE_CARD: card[0], IDENTITY_CHANGE_OTHER_CARD: other[0]}, sales)
    return failures


def _phase2(before: dict[str, tuple]) -> list[str]:
    failures: list[str] = []
    account_key, card_key, other_key = (before[i][0] for i in _IDS)
    changes = {
        value: (change, published, current)
        for value, change, published, current in _query(
            "select identifier_value, change_type, published_customer_key, current_customer_key "
            "from intermediate.int_identity_changes where identifier_value in (?, ?, ?)",
            list(_IDS),
        )
    }
    _check(failures, "account rekeyed onto the anchor card",
           changes.get(IDENTITY_CHANGE_ACCOUNT) == ("rekeyed", account_key, card_key),
           changes.get(IDENTITY_CHANGE_ACCOUNT))
    _check(failures, "higher card rekeyed onto the anchor card",
           changes.get(IDENTITY_CHANGE_OTHER_CARD) == ("rekeyed", other_key, card_key),
           changes.get(IDENTITY_CHANGE_OTHER_CARD))
    _check(failures, "anchor card's key did not move", IDENTITY_CHANGE_CARD not in changes, changes)
    sessions = _query(
        "select customer_key, attribution_status from marketing_pending.fact_customer_session "
        "where client_id = ? order by session_start_time",
        [IDENTITY_CHANGE_CLIENT],
    )
    _check(failures, "all three sessions restated onto the anchor",
           len(sessions) == 3 and all(row == (card_key, "attributed") for row in sessions), sessions)
    sales = dict(_query(
        "select loyalty_id, customer_key from finance_pending.fact_sales where loyalty_id in (?, ?)",
        [IDENTITY_CHANGE_CARD, IDENTITY_CHANGE_OTHER_CARD],
    ))
    _check(failures, "both purchases now belong to the anchor",
           sales == {IDENTITY_CHANGE_CARD: card_key, IDENTITY_CHANGE_OTHER_CARD: card_key}, sales)
    present = dict(_query(
        "select customer_key, count(*) from marketing_pending.dim_customer "
        "where customer_key in (?, ?, ?) group by 1",
        [account_key, card_key, other_key],
    ))
    _check(failures, "dim_customer keeps the anchor and drops the old keys",
           present == {card_key: 1}, present)
    return failures


def main() -> int:
    if not _DB.exists():
        print(f"ERROR: {_DB} not found. Run the local stack first.", file=sys.stderr)
        return 1
    existing = _published_keys()
    if existing:
        print(f"ERROR: demo ids are already published: {sorted(existing)}. This demo runs once.", file=sys.stderr)
        return 2

    print("Phase 1: unverified sign-in and one purchase on each card", flush=True)
    path = write_identity_change_parquet(_REPO / ".local" / "iceberg" / "bronze" / "pos_transactions", date.today())
    print(f"  wrote {path}", flush=True)
    _produce(1)
    print(f"  waiting {_BRONZE_WAIT_SECONDS}s for the Flink checkpoint", flush=True)
    time.sleep(_BRONZE_WAIT_SECONDS)
    _stack("spark")
    _stack("dbt")
    _stack("quality")

    before = _published_keys()
    print("Phase 1 published", flush=True)
    failures = _phase1(before)
    if failures:
        print(f"FAIL — phase 1: {len(failures)} check(s)")
        return 1
    (_REPO / ".local" / "identity_change").mkdir(parents=True, exist_ok=True)
    (_REPO / ".local" / "identity_change" / "phase1.json").write_text(
        json.dumps({value: {"customer_key": key, "method": method} for value, (key, method) in before.items()}, indent=2),
        encoding="utf-8",
    )

    print("Phase 2: the account verifies both cards", flush=True)
    _produce(2)
    print(f"  waiting {_BRONZE_WAIT_SECONDS}s for the Flink checkpoint", flush=True)
    time.sleep(_BRONZE_WAIT_SECONDS)
    _stack("spark")
    _stack("dbt")
    print("Phase 2 pending", flush=True)
    failures = _phase2(before)
    if failures:
        print(f"FAIL — phase 2: {len(failures)} check(s). Pending Gold was not published.")
        return 1
    _stack("quality")
    print("PASS — identity change propagated and the build was published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
