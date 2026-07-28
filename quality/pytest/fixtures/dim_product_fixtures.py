"""
Helper fixtures for dim_product-related tests.
"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_dim_product_scd2_frame() -> pd.DataFrame:
    """Small in-memory SCD2 sample for unit-style checks."""
    return pd.DataFrame(
        [
            {
                "product_key": 1,
                "product_id": "PROD-0001",
                "effective_from": "2024-01-01",
                "effective_to": "2024-06-01",
                "is_current": False,
                "record_hash": "a" * 64,
            },
            {
                "product_key": 2,
                "product_id": "PROD-0001",
                "effective_from": "2024-06-01",
                "effective_to": None,
                "is_current": True,
                "record_hash": "b" * 64,
            },
        ]
    )
