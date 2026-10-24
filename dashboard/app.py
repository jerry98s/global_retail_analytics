"""Streamlit dashboard for the Global Retail Analytics platform.

Runs in three modes (see data.py):
  * redshift — Gold/serving tables on Redshift Serverless (cloud / App Runner)
  * duckdb   — the same Gold/serving tables in the local dbt DuckDB file
  * local    — Iceberg Parquet from the local Flink stack

Sales, inventory and session numbers come from MetricFlow-compiled SQL
(semantic_queries.py), not hand-written aggregates.

Run locally:
    $env:DASHBOARD_MODE = "duckdb"; streamlit run dashboard/app.py
Container entrypoint is the same module (see dashboard/Dockerfile).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

import data

st.set_page_config(page_title="Global Retail Analytics", page_icon="📊", layout="wide")

MODE = data.get_mode()


@st.cache_data(ttl=300, show_spinner=False)
def _cached_query(sql: str) -> pd.DataFrame:
    return data.run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_semantic(name: str) -> pd.DataFrame:
    return data.semantic_query(name)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_local(table: str) -> pd.DataFrame:
    return data.load_local_table(table)


def _safe_query(sql: str) -> tuple[pd.DataFrame, str | None]:
    try:
        return _cached_query(sql), None
    except Exception as exc:  # noqa: BLE001 - shown as an info banner
        return pd.DataFrame(), str(exc)


def _safe_semantic(name: str) -> tuple[pd.DataFrame, str | None]:
    try:
        return _cached_semantic(name), None
    except Exception as exc:  # noqa: BLE001 - shown as an info banner
        return pd.DataFrame(), str(exc)


def _missing(table: str, err: str) -> None:
    st.info(
        f"`{table}` is not queryable yet. Run dbt to build it.\n\n"
        f"Details: {err}"
    )


def render_header() -> None:
    st.title("📊 Global Retail Analytics")
    badge = {
        data.MODE_REDSHIFT: "🟢 Redshift",
        data.MODE_DUCKDB: "🟢 Local DuckDB",
    }.get(MODE, "🟡 Local Iceberg")
    st.caption(f"Data source: **{badge}**  ·  Kimball Gold over POS, inventory, clickstream")

    if MODE in data.GOLD_MODES:
        err = data.warehouse_healthcheck()
        if err:
            st.error(f"Cannot reach the Gold warehouse ({MODE}).\n\n{err}")
            st.stop()


def render_gold() -> None:
    tab_sales, tab_inventory, tab_customer = st.tabs(["Sales", "Inventory", "Customer 360"])

    with tab_sales:
        st.subheader("Sales")
        by_day, err = _safe_semantic("sales_by_day")
        if err:
            _missing("summary.sales_daily_store", err)
        elif by_day.empty:
            st.warning("No sales rows yet.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Net revenue", f"${by_day['net_revenue'].sum():,.0f}")
            c2.metric("Units sold", f"{int(by_day['units_sold'].sum()):,}")
            c3.metric("Transactions", f"{int(by_day['transaction_count'].sum()):,}")
            st.line_chart(by_day.set_index("metric_time__day")["net_revenue"])

        by_store, err = _safe_semantic("sales_by_store")
        if not err and not by_store.empty:
            st.subheader("Top stores by net revenue")
            st.bar_chart(by_store.set_index("store__store_name")["net_revenue"])

    with tab_inventory:
        st.subheader("Inventory position")
        st.caption(
            "On-hand is the end-of-day balance of every product/store, carried "
            "forward on days without movement, summed across products and stores."
        )
        current, err = _safe_semantic("inventory_current")
        by_day, _ = _safe_semantic("inventory_by_day")
        if err:
            _missing("summary.inventory_position_daily", err)
        elif current.empty or by_day.empty:
            st.warning("No inventory rows yet.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Quantity on hand", f"{int(current['inventory_on_hand'].iloc[0]):,}")
            c2.metric("Quantity available", f"{int(current['inventory_available'].iloc[0]):,}")
            c3.metric("As of", str(pd.Timestamp(by_day["metric_time__day"].iloc[-1]).date()))
            daily = by_day.set_index("metric_time__day")
            st.line_chart(daily["inventory_on_hand"])
            st.subheader("Stockout hours per day")
            st.bar_chart(daily["stockout_hours"])

        by_store, err = _safe_semantic("inventory_by_store")
        if not err and not by_store.empty:
            st.subheader("On hand by store")
            st.bar_chart(by_store.set_index("store__store_name")["inventory_on_hand"])

    with tab_customer:
        st.subheader("Customer 360 & funnel")
        funnel, err = _safe_semantic("session_funnel")
        if err:
            _missing("summary.sessions_daily_platform", err)
        elif not funnel.empty:
            st.dataframe(
                funnel.rename(columns={"platform_day__platform": "platform"}),
                use_container_width=True,
                hide_index=True,
            )

        c360, err = _safe_query(data.CUSTOMER_360)
        if err:
            _missing("serving.customer_360_serving", err)
        elif not c360.empty:
            st.subheader("Customers by RFM segment")
            st.bar_chart(c360.groupby("rfm_segment").size())


def render_local() -> None:
    st.info(
        "Local mode reads Parquet from the Flink Iceberg warehouse "
        f"(`{data._local_dir()}`). Only clickstream Bronze and inventory Silver "
        "exist here; set DASHBOARD_MODE=duckdb for the local Gold marts."
    )
    tab_click, tab_inventory = st.tabs(["Clickstream (Bronze)", "Inventory (Silver)"])

    with tab_click:
        df = _cached_local("clickstream")
        if df.empty:
            st.warning("No clickstream Parquet yet. Run the Flink job + producer.")
        else:
            st.metric("Clickstream events", f"{len(df):,}")
            if "event_type" in df:
                st.bar_chart(df.groupby("event_type").size().sort_values(ascending=False))
            if "platform" in df:
                st.subheader("By platform")
                st.bar_chart(df.groupby("platform").size())
            st.dataframe(df.head(50), use_container_width=True)

    with tab_inventory:
        df = _cached_local("inventory")
        if df.empty:
            st.warning("No inventory snapshot Parquet yet. Run the inventory Flink job.")
        else:
            st.caption(data.INVENTORY_SNAPSHOT_HELP)
            display = data.enrich_inventory_snapshots(df)
            summary = data.inventory_snapshot_summary(df)
            c1, c2, c3 = st.columns(3)
            if "quantity_on_hand" in df:
                c1.metric("Quantity on hand", f"{int(df['quantity_on_hand'].sum()):,}")
            if "quantity_available" in df:
                c2.metric("Quantity available", f"{int(df['quantity_available'].sum()):,}")
            if not summary.empty:
                c3.metric("Latest snapshot", summary["snapshot_at"].iloc[-1])
            if not summary.empty:
                st.subheader("Inventory over time")
                st.line_chart(summary.set_index("snapshot_at")["quantity_on_hand"])
            st.dataframe(display.head(50), use_container_width=True, hide_index=True)


def main() -> None:
    render_header()
    if MODE in data.GOLD_MODES:
        render_gold()
    else:
        render_local()


if __name__ == "__main__":
    main()
