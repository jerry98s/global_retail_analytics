"""
Session reconstruction correctness tests.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


def test_no_overlapping_sessions_per_client(
    fact_customer_session_df: pd.DataFrame,
) -> None:
    rows = fact_customer_session_df.dropna(subset=["client_id", "session_id"]).copy()
    if rows.empty:
        return

    duplicate_session_ids = rows.duplicated(subset=["session_id"], keep=False)
    violations = rows[duplicate_session_ids].sort_values(["client_id", "session_id"])
    assert violations.empty, (
        "Session overlap detected (duplicate session_id rows):\n"
        + violations.to_string(index=False)
    )


def test_converted_implies_order_id_present(
    fact_customer_session_df: pd.DataFrame,
) -> None:
    converted = fact_customer_session_df[fact_customer_session_df["converted"].eq(True)]
    violations = converted[
        converted["order_id"].isna() | (converted["order_id"].astype(str).str.strip() == "")
    ]
    assert violations.empty, (
        "Converted sessions missing order_id:\n"
        + violations.to_string(index=False)
    )
