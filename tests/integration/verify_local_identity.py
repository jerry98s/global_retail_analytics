"""Verify identity graph output from the local DuckDB simulation.

Requires fixture-mode seeds (not Iceberg fidelity mode)::

    .\\scripts\\local\\run_local_stack.ps1 -Task dbt -DbtSource seeds

Runs after `dbt run --target local --select +identity_graph`.
Checks the scenarios encoded in seeds/bronze/clickstream_events.csv:
  1. Loyalty match    : loyalty:L1001 <-> customer:L1001 via POS loyalty_value_match
  2. Session link     : client:c-aaa  <-> customer:C2001 via clickstream session_link
  3. Multi-hop closure: loyalty:L1001, customer:L1001, client:c-aaa, customer:C2001 all in one component
  4. Public device    : client:c-shared excluded from identity_graph (10 distinct customers >= threshold)
  5. Singleton client : client:c-solo in int_identity_resolution with method='device_only'
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
    print("int_identity_public_devices (should contain c-shared only)")
    print("=" * 80)
    rows = con.execute("select client_id, distinct_customer_count from intermediate.int_identity_public_devices order by client_id").fetchall()
    for r in rows:
        print(f"  {r[0]:20s}  distinct_customers={r[1]}")
    public_devices = {r[0] for r in rows}
    if "c-shared" not in public_devices:
        failures.append("Scenario 4 FAILED: c-shared not in int_identity_public_devices")
    if len(public_devices) != 1:
        failures.append(f"Scenario 4 FAILED: expected 1 public device, got {len(public_devices)}: {public_devices}")

    print()
    print("=" * 80)
    print("int_identity_edges (session_link + loyalty_value_match; c-shared excluded)")
    print("=" * 80)
    rows = con.execute("select src, dst, edge_type from intermediate.int_identity_edges order by edge_type, src, dst").fetchall()
    for r in rows:
        print(f"  {r[0]:30s} -> {r[1]:30s}  [{r[2]}]")
    edges = {(r[0], r[1]) for r in rows}
    # Expected edges:
    #   session_link: client:c-aaa -> customer:L1001, client:c-aaa -> customer:C2001, client:c-bbb -> customer:L2002
    #   loyalty_value_match: loyalty:L1001 -> customer:L1001, loyalty:L2002 -> customer:L2002
    expected_session = {
        ("client:c-aaa", "customer:L1001"),
        ("client:c-aaa", "customer:C2001"),
        ("client:c-bbb", "customer:L2002"),
    }
    expected_loyalty = {
        ("loyalty:L1001", "customer:L1001"),
        ("loyalty:L2002", "customer:L2002"),
    }
    found_session = {(r[0], r[1]) for r in rows if r[2] == "session_link"}
    found_loyalty = {(r[0], r[1]) for r in rows if r[2] == "loyalty_value_match"}
    if found_session != expected_session:
        failures.append(f"Session-link edges mismatch. Expected {expected_session}, got {found_session}")
    if found_loyalty != expected_loyalty:
        failures.append(f"Loyalty-match edges mismatch. Expected {expected_loyalty}, got {found_loyalty}")
    # c-shared should be excluded from edges (public device)
    if any("c-shared" in (s + d) for s, d in edges):
        failures.append("Scenario 4 FAILED: c-shared appears in int_identity_edges (should be excluded)")

    print()
    print("=" * 80)
    print("int_identity_components (component rep per node)")
    print("=" * 80)
    rows = con.execute("""
        select node, identifier_type, identifier_value, component_rep_type, component_rep_node, customer_key
        from intermediate.int_identity_components
        order by node
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:30s}  type={r[1]:12s} rep_type={r[3]:12s} rep={r[4]:30s}  ck={r[5]}")
    # Scenario 3: loyalty:L1001, customer:L1001, client:c-aaa, customer:C2001 all share component_rep_node='loyalty:L1001'
    rep_by_node = {r[0]: r[4] for r in rows}
    same_component_nodes = ["loyalty:L1001", "customer:L1001", "client:c-aaa", "customer:C2001"]
    reps = {rep_by_node.get(n) for n in same_component_nodes}
    if len(reps) != 1 or reps != {"loyalty:L1001"}:
        failures.append(f"Scenario 3 FAILED: expected all of {same_component_nodes} to share rep='loyalty:L1001', got reps={reps}")

    # Same for L2002 component
    same_component_l2002 = ["loyalty:L2002", "customer:L2002", "client:c-bbb"]
    reps_l2002 = {rep_by_node.get(n) for n in same_component_l2002}
    if len(reps_l2002) != 1 or reps_l2002 != {"loyalty:L2002"}:
        failures.append(f"Scenario (L2002) FAILED: expected rep='loyalty:L2002', got {reps_l2002}")

    print()
    print("=" * 80)
    print("int_identity_resolution (per-identifier: confidence, method, is_public_device)")
    print("=" * 80)
    rows = con.execute("""
        select identifier_type, identifier_value, customer_key, confidence_score, resolution_method, is_public_device
        from intermediate.int_identity_resolution
        order by identifier_type, identifier_value
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:12s}  {r[1]:30s}  ck={r[2]:10d}  conf={r[3]}  method={r[4]:24s}  pub={r[5]}")
    method_by_node = {(r[0], r[1]): r[4] for r in rows}
    conf_by_node = {(r[0], r[1]): float(r[3]) for r in rows}
    # Scenario 1: loyalty:L1001 -> method='component_anchor', confidence=1.0
    if method_by_node.get(("loyalty_id", "L1001")) != "component_anchor":
        failures.append(f"Scenario 1 FAILED: loyalty:L1001 method expected 'component_anchor', got '{method_by_node.get(('loyalty_id', 'L1001'))}'")
    if abs(conf_by_node.get(("loyalty_id", "L1001"), 0) - 1.0) > 0.001:
        failures.append(f"Scenario 1 FAILED: loyalty:L1001 confidence expected 1.0, got {conf_by_node.get(('loyalty_id', 'L1001'))}")
    # Scenario 2: client:c-aaa -> method='session_linked', confidence=0.85
    if method_by_node.get(("client_id", "c-aaa")) != "session_linked":
        failures.append(f"Scenario 2 FAILED: client:c-aaa method expected 'session_linked', got '{method_by_node.get(('client_id', 'c-aaa'))}'")
    if abs(conf_by_node.get(("client_id", "c-aaa"), 0) - 0.85) > 0.001:
        failures.append(f"Scenario 2 FAILED: client:c-aaa confidence expected 0.85, got {conf_by_node.get(('client_id', 'c-aaa'))}")
    # Scenario 5: client:c-solo -> method='device_only', confidence=0.5
    if method_by_node.get(("client_id", "c-solo")) != "device_only":
        failures.append(f"Scenario 5 FAILED: client:c-solo method expected 'device_only', got '{method_by_node.get(('client_id', 'c-solo'))}'")
    if abs(conf_by_node.get(("client_id", "c-solo"), 0) - 0.5) > 0.001:
        failures.append(f"Scenario 5 FAILED: client:c-solo confidence expected 0.5, got {conf_by_node.get(('client_id', 'c-solo'))}")
    # Scenario 4: client:c-shared -> method='public_device_excluded', confidence=0.3, is_public_device=true
    if method_by_node.get(("client_id", "c-shared")) != "public_device_excluded":
        failures.append(f"Scenario 4 FAILED: client:c-shared method expected 'public_device_excluded', got '{method_by_node.get(('client_id', 'c-shared'))}'")
    if abs(conf_by_node.get(("client_id", "c-shared"), 0) - 0.3) > 0.001:
        failures.append(f"Scenario 4 FAILED: client:c-shared confidence expected 0.3, got {conf_by_node.get(('client_id', 'c-shared'))}")
    pub_flag = {(r[0], r[1]): r[5] for r in rows}
    if pub_flag.get(("client_id", "c-shared")) is not True:
        failures.append("Scenario 4 FAILED: client:c-shared is_public_device expected true")

    print()
    print("=" * 80)
    print("marketing.identity_graph (final mart; public devices EXCLUDED)")
    print("=" * 80)
    rows = con.execute("""
        select identifier_type, identifier_value, customer_key, confidence_score, resolution_method, is_public_device, is_active
        from marketing.identity_graph
        order by identifier_type, identifier_value
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:12s}  {r[1]:30s}  ck={r[2]:10d}  conf={r[3]}  method={r[4]:24s}  pub={r[5]}  active={r[6]}")
    graph_ids = {(r[0], r[1]) for r in rows}
    # c-shared must be EXCLUDED from identity_graph
    if ("client_id", "c-shared") in graph_ids:
        failures.append("Scenario 4 FAILED: client:c-shared appears in marketing.identity_graph (must be excluded)")
    # Expected members of identity_graph: all nodes EXCEPT public devices
    expected_in_graph = {
        ("loyalty_id", "L1001"),
        ("loyalty_id", "L2002"),
        ("customer_id", "L1001"),
        ("customer_id", "C2001"),
        ("customer_id", "L2002"),
        ("customer_id", "C-pub-01"),
        ("customer_id", "C-pub-02"),
        ("customer_id", "C-pub-03"),
        ("customer_id", "C-pub-04"),
        ("customer_id", "C-pub-05"),
        ("customer_id", "C-pub-06"),
        ("customer_id", "C-pub-07"),
        ("customer_id", "C-pub-08"),
        ("customer_id", "C-pub-09"),
        ("customer_id", "C-pub-10"),
        ("client_id", "c-aaa"),
        ("client_id", "c-bbb"),
        ("client_id", "c-solo"),
    }
    missing = expected_in_graph - graph_ids
    extra = graph_ids - expected_in_graph
    if missing:
        failures.append(f"identity_graph missing expected rows: {missing}")
    if extra:
        failures.append(f"identity_graph has unexpected rows: {extra}")

    print()
    print("=" * 80)
    if failures:
        print(f"FAIL — {len(failures)} check(s) failed:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PASS — all identity graph scenarios verified locally in DuckDB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
