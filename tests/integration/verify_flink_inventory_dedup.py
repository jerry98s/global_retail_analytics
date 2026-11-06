"""Run the inventory Silver job's real dedup + window SQL on a bounded input.

Runs inside the local Flink image (PyFlink 1.17.1)::

    docker run --rm -v <repo>/streaming:/opt/streaming -v <repo>/tests:/opt/tests \
        retail-analytics/flink:1.17.1 python /opt/tests/integration/verify_flink_inventory_dedup.py

Input, one product/store (UTC unless noted):
  e1 09:05 -5 (no fractional seconds), e1 again (identical retry),
  e2 09:20:00.25 +10, e1 09:40 +100 (same id, different payload),
  e3 18:10+08:00 -2 (= 10:10 UTC), e4 unparseable timestamp +999.
A retry must count once, a conflicting copy must not replace the first
delivery, offsets convert to UTC and a bad timestamp must not crash the job,
so hour 09 = +5 (received 10) and hour 10 = -2. The plan must be
append-only, because silver.inventory_hourly is an append Iceberg table.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/streaming/flink_jobs")

from pyflink.table import EnvironmentSettings, ExplainDetail, TableEnvironment  # noqa: E402
from pyflink.common import RowKind  # noqa: E402

from inventory_silver_job import event_ts_sql, hourly_select_sql  # noqa: E402

EVENTS = [
    ("e1", "2026-11-04T09:05:00+00:00", -5),
    ("e1", "2026-11-04T09:05:00+00:00", -5),
    ("e2", "2026-11-04T09:20:00.250000+00:00", 10),
    ("e1", "2026-11-04T09:40:00+00:00", 100),
    ("e3", "2026-11-04T18:10:00+08:00", -2),
    ("e4", "not-a-timestamp", 999),
]
EXPECTED = {(20261104, 9, 5, 10), (20261104, 10, -2, 0)}


def main() -> int:
    src_dir = Path(tempfile.mkdtemp())
    (src_dir / "events.json").write_text(
        "".join(
            json.dumps({"event_id": eid, "event_time": ts, "store_id": "S1",
                        "product_id": "P1", "qty_delta": qty}) + "\n"
            for eid, ts, qty in EVENTS
        )
    )
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")
    t_env.execute_sql(f"""
        CREATE TABLE inventory_events (
          event_id STRING,
          event_time STRING,
          event_ts AS {event_ts_sql()},
          store_id STRING,
          product_id STRING,
          qty_delta INT,
          WATERMARK FOR event_ts AS event_ts - INTERVAL '60' SECOND
        ) WITH ('connector' = 'filesystem', 'path' = '{src_dir}', 'format' = 'json')
    """)
    select = hourly_select_sql(source="inventory_events", window_interval="INTERVAL '1' HOUR")

    plan = t_env.explain_sql(select, ExplainDetail.CHANGELOG_MODE)
    physical = plan.split("== Optimized Physical Plan ==", 1)[-1].split("== Optimized Execution Plan ==", 1)[0]
    print(physical.strip())
    modes = {m for m in ("changelogMode=[I]", "changelogMode=[I,UB,UA]", "changelogMode=[I,UA]",
                         "changelogMode=[I,UB,UA,D]", "changelogMode=[I,D]") if m in physical}
    append_only = modes == {"changelogMode=[I]"}
    print(f"changelog modes in plan: {sorted(modes)}; append-only: {append_only}")

    rows, kinds = set(), []
    with t_env.execute_sql(select).collect() as results:
        for row in results:
            kinds.append(row.get_row_kind())
            rows.add((row[0], row[1], row[4], row[5]))
    print(f"row kinds: {[k.name for k in kinds]}")
    print(f"rows (date_key, hour, qty_delta_hour, qty_received_hour): {sorted(rows)}")

    ok = append_only and all(k == RowKind.INSERT for k in kinds) and rows == EXPECTED
    print("PASS" if ok else f"FAIL: expected append-only rows {sorted(EXPECTED)}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
