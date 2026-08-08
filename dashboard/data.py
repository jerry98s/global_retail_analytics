"""Data-access layer for the retail analytics dashboard.

Two modes, selected by the ``DASHBOARD_MODE`` environment variable:

* ``redshift`` (default) — query the Gold/serving tables on Amazon Redshift
  Serverless via ``redshift_connector`` (same ``RS_*`` env vars as dbt and the
  Airflow cost sensor).
* ``local`` — read the Parquet files written by the local Flink jobs into the
  Iceberg warehouse directory (``LOCAL_ICEBERG_DIR``, default ``/tmp/iceberg``).

This module deliberately avoids importing Streamlit so it can be unit-tested
and reused outside the UI.
"""

from __future__ import annotations

import glob
import os
from typing import Optional

import pandas as pd

MODE_REDSHIFT = "redshift"
MODE_LOCAL = "local"

# Local Iceberg paths relative to LOCAL_ICEBERG_DIR — see docs/data-model/naming-conventions.md
_LOCAL_TABLES = {
    "clickstream": "bronze/clickstream_events/data",
    "inventory": "silver/inventory_hourly/data",
}


def get_mode() -> str:
    """Return the active dashboard mode (``redshift`` or ``local``)."""
    mode = os.environ.get("DASHBOARD_MODE", MODE_REDSHIFT).strip().lower()
    return mode if mode in (MODE_REDSHIFT, MODE_LOCAL) else MODE_REDSHIFT


# --------------------------------------------------------------------------- #
# Local mode (Iceberg Parquet)
# --------------------------------------------------------------------------- #
def _local_dir() -> str:
    return os.environ.get("LOCAL_ICEBERG_DIR", "/tmp/iceberg")


def load_local_table(name: str) -> pd.DataFrame:
    """Read a local Iceberg table's Parquet data files into a DataFrame.

    Returns an empty DataFrame when no Parquet files exist yet (e.g. the Flink
    job has not produced a committed snapshot).
    """
    if name not in _LOCAL_TABLES:
        raise ValueError(f"Unknown local table '{name}'. Choices: {sorted(_LOCAL_TABLES)}")
    # Iceberg identity partitions nest under data/event_date=.../ — recursive glob.
    pattern = os.path.join(_local_dir(), _LOCAL_TABLES[name], "**", "*.parquet")
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


# --------------------------------------------------------------------------- #
# Redshift mode
# --------------------------------------------------------------------------- #
def _redshift_connect():
    """Open a Redshift Serverless connection from RS_* environment variables."""
    import redshift_connector  # imported lazily so local mode needs no driver

    missing = [v for v in ("RS_HOST", "RS_USER", "RS_PASSWORD") if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            "Missing Redshift environment variables: " + ", ".join(missing)
        )
    return redshift_connector.connect(
        host=os.environ["RS_HOST"],
        port=int(os.environ.get("RS_PORT", "5439")),
        database=os.environ.get("RS_DATABASE", "prod"),
        user=os.environ["RS_USER"],
        password=os.environ["RS_PASSWORD"],
    )


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only SQL query against Redshift and return a DataFrame."""
    conn = _redshift_connect()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetch_dataframe()
    finally:
        conn.close()


def redshift_healthcheck() -> Optional[str]:
    """Return None when Redshift is reachable, else a short error string."""
    try:
        run_query("SELECT 1")
        return None
    except Exception as exc:  # noqa: BLE001 - surfaced to the UI as a banner
        return str(exc)


# Named analytical queries over the Gold/serving layer. Each is defensive: the
# UI wraps calls in try/except so a missing table (dbt not yet run) degrades to
# an informational message rather than a crash.
SALES_BY_DAY = """
    SELECT date_key,
           SUM(net_revenue)  AS net_revenue,
           SUM(quantity_sold) AS units,
           COUNT(*)          AS line_items
    FROM finance.fact_sales
    WHERE is_voided = FALSE
    GROUP BY date_key
    ORDER BY date_key
"""

SALES_BY_STORE = """
    SELECT s.store_id,
           s.store_name,
           SUM(f.net_revenue) AS net_revenue
    FROM finance.fact_sales f
    JOIN finance.dim_store s ON f.store_key = s.store_key
    WHERE f.is_voided = FALSE
    GROUP BY s.store_id, s.store_name
    ORDER BY net_revenue DESC
    LIMIT 20
"""

INVENTORY_LATEST = """
    SELECT snapshot_date_key,
           snapshot_hour,
           SUM(quantity_on_hand)   AS quantity_on_hand,
           SUM(quantity_available) AS quantity_available
    FROM finance.fact_inventory_snapshot
    GROUP BY snapshot_date_key, snapshot_hour
    ORDER BY snapshot_date_key, snapshot_hour
"""

SESSION_FUNNEL = """
    SELECT platform,
           COUNT(*)                                       AS sessions,
           SUM(page_view_count)                           AS page_views,
           SUM(product_view_count)                        AS product_views,
           SUM(add_to_cart_count)                         AS add_to_carts,
           SUM(CASE WHEN converted THEN 1 ELSE 0 END)     AS conversions
    FROM marketing.fact_customer_session
    GROUP BY platform
    ORDER BY sessions DESC
"""

CUSTOMER_360 = """
    SELECT customer_key,
           loyalty_tier,
           rfm_segment,
           churn_risk_score,
           total_lifetime_value
    FROM serving.customer_360_serving
    LIMIT 500
"""


# --------------------------------------------------------------------------- #
# Inventory snapshot display helpers
# --------------------------------------------------------------------------- #
# snapshot_date_key is a Kimball integer date (YYYYMMDD, e.g. 20250613).
# snapshot_hour is the hour (0–23) when the Flink tumbling window started.
INVENTORY_SNAPSHOT_HELP = (
    "**Snapshot time** — each row is one Flink tumbling-window aggregate. "
    "`snapshot_date_key` is the warehouse date key (`YYYYMMDD`); "
    "`snapshot_hour` is the hour that window started (0 = midnight, 14 = 2 pm). "
    "On the local-testing branch the window is 1 minute (not hourly), but the "
    "hour still reflects when that minute bucket began."
)


def format_date_key(date_key: int | float | str) -> str:
    """Turn a Kimball date_key (YYYYMMDD int) into an ISO date string."""
    try:
        digits = str(int(date_key))
    except (TypeError, ValueError):
        return str(date_key)
    if len(digits) != 8:
        return digits
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def format_snapshot_hour(hour: int | float | str) -> str:
    """Turn snapshot_hour (0–23) into a clock label like ``14:00``."""
    try:
        h = int(hour)
    except (TypeError, ValueError):
        return str(hour)
    if h < 0 or h > 23:
        return str(hour)
    return f"{h:02d}:00"


def enrich_inventory_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable snapshot columns and drop raw warehouse keys from view.

    Accepts both schemas:
      * Redshift ``finance.fact_inventory_snapshot`` — already has
        ``quantity_on_hand`` (running balance) and ``quantity_available``.
      * Local silver ``silver.inventory_hourly`` — has ``qty_delta_hour`` and
        ``qty_received_hour`` (per-hour deltas, NOT running balances). We
        compute the running balance here so downstream display code can treat
        both modes uniformly.
    """
    if df.empty:
        return df

    out = df.copy()
    if "snapshot_date_key" in out.columns:
        out["snapshot_date"] = out["snapshot_date_key"].map(format_date_key)
    if "snapshot_hour" in out.columns:
        out["snapshot_time"] = out["snapshot_hour"].map(format_snapshot_hour)
    if "snapshot_date" in out.columns and "snapshot_time" in out.columns:
        out["snapshot_at"] = out["snapshot_date"] + " " + out["snapshot_time"]

    # Local-silver path: derive running balance from hourly deltas.
    # Mirrors what dbt computes in fact_inventory_snapshot.sql so the dashboard
    # shows the same numbers in both modes.
    if (
        "quantity_on_hand" not in out.columns
        and "qty_delta_hour" in out.columns
        and "snapshot_date_key" in out.columns
        and "snapshot_hour" in out.columns
    ):
        group_cols = [
            c for c in ("product_id", "store_id") if c in out.columns
        ]
        ordered = out.sort_values(
            group_cols + ["snapshot_date_key", "snapshot_hour"]
        )
        ordered["quantity_on_hand"] = ordered.groupby(group_cols)[
            "qty_delta_hour"
        ].cumsum()
        ordered["quantity_available"] = ordered["quantity_on_hand"].clip(lower=0)
        out = ordered

    preferred = [
        "snapshot_at",
        "snapshot_date",
        "snapshot_time",
        "product_id",
        "store_id",
        "quantity_on_hand",
        "quantity_available",
        "qty_delta_hour",
        "qty_received_hour",
        "is_estimated",
    ]
    cols = [c for c in preferred if c in out.columns]
    cols += [c for c in out.columns if c not in cols and c not in ("snapshot_date_key", "snapshot_hour")]
    return out[cols]


def inventory_snapshot_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate inventory totals per snapshot window for charts."""
    enriched = enrich_inventory_snapshots(df)
    if enriched.empty or "snapshot_at" not in enriched.columns:
        return pd.DataFrame()
    agg = {
        "quantity_on_hand": "sum",
        "quantity_available": "sum",
    }
    if "product_id" in enriched.columns:
        agg["product_id"] = "count"
    summary = (
        enriched.groupby("snapshot_at", as_index=False)
        .agg(agg)
        .rename(columns={"product_id": "sku_count"})
        .sort_values("snapshot_at")
    )
    return summary
