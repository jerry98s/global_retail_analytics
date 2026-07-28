"""
Referential integrity tests across facts and dimensions.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


def test_fact_sales_product_keys_exist(
    fact_sales_sample_df: pd.DataFrame, dim_product_df: pd.DataFrame
) -> None:
    fact_keys = set(fact_sales_sample_df["product_key"].dropna().astype(int).tolist())
    dim_keys = set(dim_product_df["product_key"].dropna().astype(int).tolist())
    missing = sorted(fact_keys - dim_keys)
    assert not missing, f"fact_sales has product_key values missing in dim_product: {missing[:20]}"


def test_fact_sales_store_keys_exist(
    fact_sales_sample_df: pd.DataFrame, dim_store_df: pd.DataFrame
) -> None:
    fact_keys = set(fact_sales_sample_df["store_key"].dropna().astype(int).tolist())
    dim_keys = set(dim_store_df["store_key"].dropna().astype(int).tolist())
    missing = sorted(fact_keys - dim_keys)
    assert not missing, f"fact_sales has store_key values missing in dim_store: {missing[:20]}"
