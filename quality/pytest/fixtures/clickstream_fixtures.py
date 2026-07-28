"""
Helper fixtures for clickstream/session tests.
"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_clickstream_events_frame() -> pd.DataFrame:
    """Small event sample representing one converting session."""
    return pd.DataFrame(
        [
            {
                "event_id": "evt-1",
                "session_id": "sess-1",
                "client_id": "client-1",
                "event_type": "page_view",
                "order_id": None,
            },
            {
                "event_id": "evt-2",
                "session_id": "sess-1",
                "client_id": "client-1",
                "event_type": "checkout",
                "order_id": "order-123",
            },
        ]
    )
