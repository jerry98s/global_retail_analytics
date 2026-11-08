"""Verify identity output from the local DuckDB simulation.

Requires fixture-mode seeds (not Iceberg fidelity mode)::

    .\\scripts\\local\\run_local_stack.ps1 -Task dbt -DbtSource seeds

Runs after `dbt run --target local --select +identity_graph int_identity_shared_devices`.
Checks the scenarios encoded in seeds/bronze/clickstream_events.csv:
  1. Verified link   : C1001 verified to L1001 -> one customer, anchored on loyalty:L1001
  2. Two accounts    : C2002 and C2003 both verified to L2002 -> one customer
  3. Shared tablet   : C3001 and C3002 on c-tab keep separate customer_keys;
                       c-tab is listed in int_identity_shared_devices
  4. Unverified claim: C4001 claimed L1001 without verifying -> standalone
  5. Legacy no-merge : v1.2 customer_id L1001 equals a loyalty id but stays its
                       own customer
  6. Devices         : no client_id appears as an identity anywhere

The fixture-mode chain reads the generated seeds/silver/*.csv. Edge and
component correctness is covered by tests/unit/test_spark_identity_resolution.py
against the same scenarios.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def main() -> int:
    db_path = Path(__file__).resolve().parents[2] / "transformation" / "dbt_project" / "local_retail.duckdb"
    if not db_path.exists():
        print(f"ERROR: {db_path} not found. Run `dbt run --target local` first.", file=sys.stderr)
        return 1

    con = duckdb.connect(str(db_path), read_only=True)
    failures: list[str] = []

    print("=" * 80)
    print("marketing.identity_graph")
    print("=" * 80)
    rows = con.execute("""
        select identifier_type, identifier_value, customer_key, confidence_score, resolution_method
        from marketing.identity_graph
        order by identifier_type, identifier_value
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:12s}  {r[1]:10s}  ck={r[2]:10d}  conf={r[3]}  method={r[4]}")
    key = {(r[0], r[1]): r[2] for r in rows}
    method = {(r[0], r[1]): r[4] for r in rows}

    def same(a, b):
        return key.get(a) is not None and key.get(a) == key.get(b)

    if not same(("customer_id", "C1001"), ("loyalty_id", "L1001")):
        failures.append("Scenario 1 FAILED: C1001 should share L1001's customer_key")
    if method.get(("loyalty_id", "L1001")) != "component_anchor":
        failures.append("Scenario 1 FAILED: loyalty:L1001 should anchor its component")
    if not (same(("customer_id", "C2002"), ("loyalty_id", "L2002"))
            and same(("customer_id", "C2003"), ("loyalty_id", "L2002"))):
        failures.append("Scenario 2 FAILED: C2002, C2003 and L2002 should share one customer_key")
    if same(("customer_id", "C3001"), ("customer_id", "C3002")):
        failures.append("Scenario 3 FAILED: C3001 and C3002 share a device but must stay separate")
    if same(("customer_id", "C4001"), ("loyalty_id", "L1001")):
        failures.append("Scenario 4 FAILED: an unverified claim merged C4001 into L1001")
    if method.get(("customer_id", "C4001")) != "customer_id_standalone":
        failures.append("Scenario 4 FAILED: C4001 should be standalone")
    if same(("customer_id", "L1001"), ("loyalty_id", "L1001")):
        failures.append("Scenario 5 FAILED: a v1.2 customer_id merged by value equality")
    if any(identifier_type == "client_id" for identifier_type, _ in key):
        failures.append("Scenario 6 FAILED: a client_id appears in identity_graph")

    print()
    print("=" * 80)
    print("intermediate.int_identity_shared_devices")
    print("=" * 80)
    shared = con.execute("""
        select client_id, account_count, has_ambiguous_instance
        from intermediate.int_identity_shared_devices order by client_id
    """).fetchall()
    for r in shared:
        print(f"  {r[0]:10s}  accounts={r[1]}  ambiguous={r[2]}")
    if {r[0] for r in shared} != {"c-tab", "c-fff"}:
        failures.append(f"Scenario 3 FAILED: expected shared devices c-tab and c-fff, got {shared}")

    resolution_clients = con.execute(
        "select count(*) from intermediate.int_identity_resolution where identifier_type = 'client_id'"
    ).fetchone()[0]
    if resolution_clients:
        failures.append(f"Scenario 6 FAILED: {resolution_clients} client_id rows in int_identity_resolution")

    print()
    print("=" * 80)
    if failures:
        print(f"FAIL — {len(failures)} check(s) failed:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PASS — all identity scenarios verified locally in DuckDB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
