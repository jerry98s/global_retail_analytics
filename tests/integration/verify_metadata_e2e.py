"""One-shot local summary + metadata validation after dbt/quality.

Requires a prior local Iceberg dbt + quality run::

    .\\scripts\\local\\run_local_stack.ps1 -Task dbt -DbtSource iceberg
    .\\scripts\\local\\run_local_stack.ps1 -Task quality

Then::

    .\\.venv\\Scripts\\python.exe tests/integration/verify_metadata_e2e.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_REPO = Path(__file__).resolve().parents[2]
analytics = _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
meta = _REPO / "transformation" / "dbt_project" / "local_metadata.duckdb"


def main() -> int:
    assert analytics.is_file(), f"missing {analytics}"
    assert meta.is_file(), f"missing {meta}"

    a = duckdb.connect(str(analytics), read_only=True)
    m = duckdb.connect(str(meta), read_only=True)

    print("=== SUMMARY ===")
    ok = True
    checks = [
        ("summary.sales_daily_store", "date_key, store_key"),
        (
            "summary.inventory_daily_product_store",
            "snapshot_date_key, product_key, store_key",
        ),
        ("summary.sessions_daily_platform", "session_date_key, platform"),
    ]
    for table, grain in checks:
        n = a.execute(f"select count(*) from {table}").fetchone()[0]
        dup = a.execute(
            f"select count(*) from (select {grain}, count(*) c from {table} "
            f"group by {grain} having count(*) > 1)"
        ).fetchone()[0]
        status = "OK" if n > 0 and dup == 0 else "FAIL"
        if status != "OK":
            ok = False
        print(f"{status} {table}: rows={n} grain_dups={dup}")

    print("=== METADATA pipeline_run ===")
    runs = m.execute(
        "select execution_id, pipeline_name, status, duration_seconds, "
        "started_at, ended_at from meta.pipeline_run order by started_at"
    ).fetchdf()
    print(runs.to_string(index=False))
    success_runs = m.execute(
        "select count(*) from meta.pipeline_run where status='SUCCESS'"
    ).fetchone()[0]
    if success_runs < 1:
        ok = False
        print("FAIL: no SUCCESS pipeline_run")

    print("=== Freshness by layer ===")
    fresh = m.execute(
        "select schema_name, count(*) as observations, "
        "sum(case when row_count is not null and row_count > 0 then 1 else 0 end) "
        "as nonzero from meta.table_freshness group by 1 order by 1"
    ).fetchdf()
    print(fresh.to_string(index=False))
    for layer in ["bronze", "silver", "finance", "marketing", "summary"]:
        n = m.execute(
            "select count(*) from meta.table_freshness where schema_name = ?",
            [layer],
        ).fetchone()[0]
        if n < 1:
            print(f"FAIL freshness missing layer {layer}")
            ok = False

    print("=== DQ by system/status ===")
    dq = m.execute(
        "select check_system, status, count(*) n from meta.dq_check_result "
        "group by 1,2 order by 1,2"
    ).fetchdf()
    print(dq.to_string(index=False))
    dbt_n = m.execute(
        "select count(*) from meta.dq_check_result where check_system='dbt'"
    ).fetchone()[0]
    ge_n = m.execute(
        "select count(*) from meta.dq_check_result where check_system='ge'"
    ).fetchone()[0]
    if dbt_n < 1:
        ok = False
        print("FAIL: no dbt DQ rows")
    if ge_n < 1:
        ok = False
        print("FAIL: no GE DQ rows")

    # Serving layer (dbt-managed customer_360_serving)
    print("=== SERVING ===")
    try:
        n_serving = a.execute(
            "select count(*) from serving.customer_360_serving"
        ).fetchone()[0]
        print(f"OK serving.customer_360_serving: rows={n_serving}")
        if n_serving < 1:
            ok = False
            print("FAIL: serving.customer_360_serving empty")
    except Exception as exc:  # noqa: BLE001 — smoke check
        ok = False
        print(f"FAIL serving.customer_360_serving: {exc}")

    a.close()
    m.close()
    print("OVERALL:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
