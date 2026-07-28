"""
Identity graph correctness tests.

Covers:
  - No duplicate (type, value) pairs
  - No loyalty_id mapped to multiple customer_keys
  - Public devices are excluded from marketing.identity_graph
  - Component merging: identifiers sharing a loyalty anchor share a customer_key
    (verified via int_identity_resolution expectations rather than the
    public-device-filtered identity_graph).
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


def test_no_duplicate_identifier_pairs(identity_graph_df: pd.DataFrame) -> None:
    dupes = identity_graph_df.duplicated(
        subset=["identifier_type", "identifier_value"], keep=False
    )
    violations = identity_graph_df[dupes]
    assert violations.empty, (
        "Duplicate identifier mappings found:\n"
        + violations.sort_values(
            ["identifier_type", "identifier_value"]
        ).to_string(index=False)
    )


def test_no_shared_loyalty_ids(identity_graph_df: pd.DataFrame) -> None:
    loyalty = identity_graph_df[
        identity_graph_df["identifier_type"].str.lower() == "loyalty_id"
    ].copy()
    if loyalty.empty:
        return

    collisions = (
        loyalty.groupby("identifier_value")["customer_key"]
        .nunique()
        .reset_index(name="customer_keys")
    )
    violations = collisions[collisions["customer_keys"] > 1]
    assert violations.empty, (
        "Loyalty IDs mapped to multiple customer_keys:\n"
        + violations.to_string(index=False)
    )


def test_public_devices_excluded_from_graph(identity_graph_df: pd.DataFrame) -> None:
    """marketing.identity_graph must not contain rows for public devices."""
    if "is_public_device" not in identity_graph_df.columns:
        # Older snapshots without the column — pass vacuously
        return
    flagged = identity_graph_df[identity_graph_df["is_public_device"]]
    assert flagged.empty, (
        f"Public devices leaked into identity_graph ({len(flagged)} rows):\n"
        + flagged.head(20).to_string(index=False)
    )


def test_component_merge_for_session_linked_clients(
    identity_graph_df: pd.DataFrame,
) -> None:
    """Every session_linked client_id must share a customer_key with at least
    one loyalty_id or customer_id in the same graph (component merge invariant)."""
    session_linked = identity_graph_df[
        identity_graph_df["resolution_method"] == "session_linked"
    ]
    if session_linked.empty:
        return

    known_keys = set(
        identity_graph_df[
            identity_graph_df["identifier_type"].isin(["loyalty_id", "customer_id"])
        ]["customer_key"]
    )
    orphans = session_linked[~session_linked["customer_key"].isin(known_keys)]
    assert orphans.empty, (
        f"session_linked client_ids with no loyalty/customer in same component "
        f"({len(orphans)} rows):\n"
        + orphans.head(20).to_string(index=False)
    )
