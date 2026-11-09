"""Verify the Customer 360 acceptance scenarios and rules in the local DuckDB file.

Runs after an Iceberg-mode local run that included the acceptance scenarios
(``clickstream_producer --scenario acceptance``)::

    .\\scripts\\local\\run_local_stack.ps1 -Task all
    .\\.venv\\Scripts\\python.exe tests/integration/verify_customer_360.py [--pending]

``--pending`` reads the Gold tables from their ``*_pending`` copies, so a build
can be checked after ``-Task dbt`` and before ``-Task quality`` publishes (and
drops) them. Intermediate models are always live.

Scenarios (devices CLIENT-SCN-000N from the producer):
  1. Journey        : anonymous view, verified login, cart, checkout, logout form
                      one attributed, converted session for CUST-000001 / LOYAL-000001
  2. Inactivity gap : one producer session splits into two anonymous instances
  3. Shared tablet  : CUST-000002 and CUST-000003 keep separate customers
  4. Dual accounts  : CUST-000004 and CUST-010004 merge through LOYAL-000004
  5. Consent change : CUST-000005 ends with marketing false, analytics true
  6. Unverified     : CUST-000006 stays standalone
  7. Late and retry : the late event joins its session; the retry is not counted
Rules:
  - devices are never identities; legacy sessions are never attributed
  - consent equals the latest explicit statement, else false
  - Customer 360 serves only customers with marketing consent
  - every loyalty sales line has a customer key
  - RFM scores run 1 to 5 and only score POS customers
  - every checkout order is unconfirmed and appears once
  - the session rollup counts anonymous sessions
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import duckdb

_DB = Path(__file__).resolve().parents[2] / "transformation" / "dbt_project" / "local_retail.duckdb"


class Checker:
    def __init__(self, con: duckdb.DuckDBPyConnection, pending: bool) -> None:
        self.con = con
        self.suffix = "_pending" if pending else ""
        self.failures: list[str] = []
        self.passed = 0

    def gold(self, schema: str, table: str) -> str:
        return f"{schema}{self.suffix}.{table}"

    def rows(self, sql: str, params: list[Any] | None = None) -> list[tuple]:
        return self.con.execute(sql, params or []).fetchall()

    def scalar(self, sql: str, params: list[Any] | None = None) -> Any:
        return self.con.execute(sql, params or []).fetchone()[0]

    def check(self, name: str, ok: bool, detail: Any = "") -> None:
        if ok:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failures.append(f"{name}: {detail}")
            print(f"  FAIL  {name}  {detail}")

    def key(self, identifier_type: str, value: str) -> int | None:
        row = self.rows(
            "select customer_key from intermediate.int_identity_resolution "
            "where identifier_type = ? and identifier_value = ?",
            [identifier_type, value],
        )
        return row[0][0] if row else None

    def sessions(self, client_id: str) -> list[tuple]:
        return self.rows(
            f"select f.session_id, f.producer_session_id, f.customer_key, f.attribution_status, "
            f"r.event_count, f.converted from {self.gold('marketing', 'fact_customer_session')} f "
            f"join intermediate.int_session_reconstruction r on r.session_id = f.session_id "
            f"where f.client_id = ? order by f.session_start_time",
            [client_id],
        )


def scenarios(c: Checker) -> None:
    print("Scenarios")
    journey_key = c.key("customer_id", "CUST-000001")
    journey = c.sessions("CLIENT-SCN-0001")
    c.check("journey: account and card share a customer",
            journey_key is not None and journey_key == c.key("loyalty_id", "LOYAL-000001"),
            (journey_key, c.key("loyalty_id", "LOYAL-000001")))
    c.check("journey: every session attributed to that customer",
            bool(journey) and all(s[2] == journey_key and s[3] == "attributed" for s in journey), journey)
    c.check("journey: the pre-login view is in the converted session",
            any(s[4] == 5 and s[5] for s in journey), journey)

    gap = c.sessions("CLIENT-SCN-0002")
    instances: dict[str, set[str]] = {}
    for session_id, producer_session_id, *_ in gap:
        instances.setdefault(producer_session_id, set()).add(session_id)
    c.check("inactivity gap: one producer session splits into instances",
            bool(instances) and all(len(v) >= 2 for v in instances.values()), instances)
    c.check("inactivity gap: no login, so anonymous",
            bool(gap) and all(s[2] is None and s[3] == "anonymous" for s in gap), gap)

    a, b = c.key("customer_id", "CUST-000002"), c.key("customer_id", "CUST-000003")
    tablet_keys = {s[2] for s in c.sessions("CLIENT-SCN-0003") if s[3] == "attributed"}
    c.check("shared tablet: the two people stay separate", None not in (a, b) and a != b, (a, b))
    c.check("shared tablet: sessions belong to exactly those two", tablet_keys == {a, b}, tablet_keys)
    shared = {r[0] for r in c.rows("select client_id from intermediate.int_identity_shared_devices")}
    c.check("shared tablet: listed as a shared device", "CLIENT-SCN-0003" in shared, sorted(shared)[:5])

    dual = {c.key("customer_id", "CUST-000004"), c.key("customer_id", "CUST-010004"),
            c.key("loyalty_id", "LOYAL-000004")}
    c.check("dual accounts: both accounts merge through the card", len(dual) == 1 and None not in dual, dual)

    consent = c.rows(
        f"select marketing_consent, analytics_consent from {c.gold('marketing', 'dim_customer')} "
        "where customer_key = ?", [c.key("customer_id", "CUST-000005")])
    c.check("consent change: marketing false, analytics true", consent == [(False, True)], consent)

    unverified = c.key("customer_id", "CUST-000006")
    c.check("unverified claim: no merge with the claimed card",
            unverified is not None and unverified != c.key("loyalty_id", "LOYAL-000099"), unverified)

    late = c.sessions("CLIENT-SCN-0008")
    c.check("late and retry: one anonymous session of three events",
            bool(late) and all(s[4] == 3 and s[2] is None for s in late), late)


def rules(c: Checker) -> None:
    print("Rules")
    facts = c.gold("marketing", "fact_customer_session")
    dim = c.gold("marketing", "dim_customer")
    c.check("devices are never identities", c.scalar(
        f"select count(*) from {c.gold('marketing', 'identity_graph')} where identifier_type = 'client_id'") == 0)
    c.check("legacy sessions are never attributed", c.scalar(
        f"select count(*) from {facts} where is_legacy and customer_key is not null") == 0)
    c.check("customer key set only on attributed sessions", c.scalar(
        f"select count(*) from {facts} where (attribution_status = 'attributed') <> (customer_key is not null)") == 0)
    c.check("one row per session", c.scalar(
        f"select count(*) - count(distinct session_id) from {facts}") == 0)
    c.check("consent equals the latest explicit statement, else false", c.scalar(f"""
        select count(*) from {dim} d
        left join intermediate.int_customer_consent s on s.customer_key = d.customer_key
        where d.marketing_consent <> coalesce(s.marketing_consent, false)
           or d.analytics_consent <> coalesce(s.analytics_consent, false)""") == 0)
    c.check("Customer 360 serves only marketing-consented customers", c.scalar(f"""
        select count(*) from marketing.customer_360_view v
        join {dim} d on d.customer_key = v.customer_key
        where not d.marketing_consent""") == 0)
    c.check("every loyalty sales line has a customer key", c.scalar(
        f"select count(*) from {c.gold('finance', 'fact_sales')} "
        "where loyalty_id is not null and customer_key is null") == 0)
    c.check("RFM scores run 1 to 5", c.scalar("""
        select count(*) from intermediate.int_rfm_scoring
        where least(r_score, f_score, m_score) < 1 or greatest(r_score, f_score, m_score) > 5""") == 0)
    c.check("RFM scores only customers with a loyalty card", c.scalar("""
        select count(*) from intermediate.int_rfm_scoring r
        where not exists (select 1 from intermediate.int_identity_resolution i
                          where i.customer_key = r.customer_key and i.identifier_type = 'loyalty_id')""") == 0)
    orders = c.rows("""
        select count(*), count(distinct order_id),
               count(*) filter (where reconciliation_status <> 'unconfirmed')
        from intermediate.int_order_reconciliation""")[0]
    c.check("checkout orders appear once and are unconfirmed",
            orders[0] > 0 and orders[0] == orders[1] and orders[2] == 0, orders)
    totals = c.rows(f"""
        select (select sum(session_count) from {c.gold('summary', 'sessions_daily_platform')}),
               (select count(*) from {facts})""")[0]
    c.check("session rollup counts every session, anonymous included", totals[0] == totals[1], totals)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pending", action="store_true", help="read Gold from the *_pending copies")
    parser.add_argument("--duckdb", type=Path, default=_DB)
    args = parser.parse_args(argv)
    if not args.duckdb.exists():
        print(f"ERROR: {args.duckdb} not found. Run the local stack first.", file=sys.stderr)
        return 1

    con = duckdb.connect(str(args.duckdb), read_only=True)
    if args.pending and not con.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'marketing_pending' and table_name = 'fact_customer_session'"
    ).fetchone()[0]:
        con.close()
        print("ERROR: no pending Gold; publish drops it. Run -Task dbt, then check before -Task quality.",
              file=sys.stderr)
        return 1
    try:
        c = Checker(con, args.pending)
        scenarios(c)
        rules(c)
    finally:
        con.close()

    print()
    if c.failures:
        print(f"FAIL — {len(c.failures)} of {len(c.failures) + c.passed} checks failed")
        return 1
    print(f"PASS — {c.passed} Customer 360 checks ({'pending' if args.pending else 'live'} Gold)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
