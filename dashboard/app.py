"""Streamlit dashboard for the Global Retail Analytics platform.

Runs in two modes (see data.py):
  * redshift — Gold/serving tables on Redshift Serverless (cloud / App Runner)
  * local    — Iceberg Parquet from the local Flink stack

Run locally:
    streamlit run dashboard/app.py
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
def _cached_local(table: str) -> pd.DataFrame:
    return data.load_local_table(table)


def _safe_query(sql: str) -> tuple[pd.DataFrame, str | None]:
    try:
        return _cached_query(sql), None
    except Exception as exc:  # noqa: BLE001 - shown as an info banner
        return pd.DataFrame(), str(exc)


def _missing(table: str, err: str) -> None:
    st.info(
        f"`{table}` is not queryable yet. Run dbt against Redshift to build it.\n\n"
        f"Details: {err}"
    )


def render_header() -> None:
    st.title("📊 Global Retail Analytics")
    badge = "🟢 Redshift" if MODE == data.MODE_REDSHIFT else "🟡 Local Iceberg"
    st.caption(f"Data source: **{badge}**  ·  Kimball Gold over POS, inventory, clickstream")

    if MODE == data.MODE_REDSHIFT:
        err = data.redshift_healthcheck()
        if err:
            st.error(f"Cannot reach Redshift. Check RS_* settings / network.\n\n{err}")
            st.stop()


def render_redshift() -> None:
    tab_sales, tab_inventory, tab_customer = st.tabs(["Sales", "Inventory", "Customer 360"])

    with tab_sales:
        st.subheader("Sales")
        by_day, err = _safe_query(data.SALES_BY_DAY)
        if err:
            _missing("finance.fact_sales", err)
        elif by_day.empty:
            st.warning("No sales rows yet.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Net revenue", f"${by_day['net_revenue'].sum():,.0f}")
            c2.metric("Units sold", f"{int(by_day['units'].sum()):,}")
            c3.metric("Line items", f"{int(by_day['line_items'].sum()):,}")
            st.line_chart(by_day.set_index("date_key")["net_revenue"])

        by_store, err = _safe_query(data.SALES_BY_STORE)
        if not err and not by_store.empty:
            st.subheader("Top stores by net revenue")
            st.bar_chart(by_store.set_index("store_name")["net_revenue"])

    with tab_inventory:
        st.subheader("Inventory snapshots")
        st.caption(data.INVENTORY_SNAPSHOT_HELP)
        inv, err = _safe_query(data.INVENTORY_LATEST)
        if err:
            _missing("finance.fact_inventory_snapshot", err)
        elif inv.empty:
            st.warning("No inventory rows yet.")
        else:
            display = data.enrich_inventory_snapshots(inv)
            summary = data.inventory_snapshot_summary(inv)
            c1, c2, c3 = st.columns(3)
            c1.metric("Quantity on hand", f"{int(inv['quantity_on_hand'].sum()):,}")
            c2.metric("Quantity available", f"{int(inv['quantity_available'].sum()):,}")
            if not summary.empty:
                c3.metric("Latest snapshot", summary["snapshot_at"].iloc[-1])
            if not summary.empty:
                st.line_chart(summary.set_index("snapshot_at")["quantity_on_hand"])
            st.dataframe(display, use_container_width=True, hide_index=True)

    with tab_customer:
        st.subheader("Customer 360 & funnel")
        funnel, err = _safe_query(data.SESSION_FUNNEL)
        if err:
            _missing("marketing.fact_customer_session", err)
        elif not funnel.empty:
            st.dataframe(funnel, use_container_width=True)

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
        "exist locally; the Gold marts live on Redshift."
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
    if MODE == data.MODE_REDSHIFT:
        render_redshift()
    else:
        render_local()


if __name__ == "__main__":
    main()
