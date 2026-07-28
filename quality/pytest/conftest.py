"""
Shared fixtures for the data quality test suite.
All Redshift connections use environment variables — no hardcoded credentials.
"""

import os
import pytest
import pandas as pd
import redshift_connector


@pytest.fixture(scope="session")
def redshift_conn():
    """
    Session-scoped Redshift connection.
    Requires env vars: RS_HOST, RS_USER, RS_PASSWORD,
                       optionally RS_PORT, RS_DATABASE.
    """
    conn = redshift_connector.connect(
        host=os.environ["RS_HOST"],
        port=int(os.environ.get("RS_PORT", "5439")),
        database=os.environ.get("RS_DATABASE", "prod"),
        user=os.environ["RS_USER"],
        password=os.environ["RS_PASSWORD"],
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def dim_product_df(redshift_conn) -> pd.DataFrame:
    """Full dim_product loaded into memory for set-level assertions."""
    query = """
        SELECT
            product_key,
            product_id,
            effective_from,
            effective_to,
            is_current,
            record_hash,
            category_l1,
            category_l2,
            unit_cost,
            supplier_id
        FROM marketing.dim_product
        ORDER BY product_id, effective_from
    """
    return pd.read_sql(query, redshift_conn)


@pytest.fixture(scope="module")
def fact_sales_sample_df(redshift_conn) -> pd.DataFrame:
    """Rolling 90-day sample of fact_sales for referential tests."""
    query = """
        SELECT
            transaction_id,
            line_item_number,
            date_key,
            product_key,
            store_key,
            customer_key,
            net_revenue,
            is_voided
        FROM finance.fact_sales
        WHERE date_key >= CAST(
            TO_CHAR(DATEADD(day, -90, CURRENT_DATE), 'YYYYMMDD') AS INTEGER
        )
    """
    return pd.read_sql(query, redshift_conn)


@pytest.fixture(scope="module")
def identity_graph_df(redshift_conn) -> pd.DataFrame:
    """Full identity_graph for resolution correctness tests."""
    query = """
        SELECT
            customer_key,
            identifier_type,
            identifier_value,
            confidence_score,
            is_active
        FROM marketing.identity_graph
        WHERE is_active = TRUE
    """
    return pd.read_sql(query, redshift_conn)


@pytest.fixture(scope="module")
def fact_customer_session_df(redshift_conn) -> pd.DataFrame:
    """Recent fact_customer_session rows for session logic tests."""
    query = """
        SELECT session_id, session_date_key, customer_key, client_id, converted, order_id
        FROM (
            SELECT
                session_id,
                session_date_key,
                customer_key,
                client_id,
                converted,
                order_id,
                ROW_NUMBER() OVER (
                    PARTITION BY session_id
                    ORDER BY session_date_key DESC
                ) AS rn
            FROM marketing.fact_customer_session
        )
        WHERE rn = 1
    """
    return pd.read_sql(query, redshift_conn)


@pytest.fixture(scope="module")
def dim_store_df(redshift_conn) -> pd.DataFrame:
    """Store dimension keys for referential integrity tests."""
    query = """
        SELECT
            store_key,
            store_id
        FROM finance.dim_store
    """
    return pd.read_sql(query, redshift_conn)
