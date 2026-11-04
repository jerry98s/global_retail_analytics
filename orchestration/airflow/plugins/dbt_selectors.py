"""dbt selections per owning DAG for the live staging/intermediate layer.

Staging and intermediate models are not WAP-routed: they build in place. Each
model has exactly one building DAG, so two schedules never rebuild the same
incremental model or view concurrently. Readers in other DAGs use whatever the
owner last built. ``tests/unit/test_dbt_selectors.py`` checks the sets are
disjoint and cover every staging/intermediate model.
"""

from __future__ import annotations

WAREHOUSE_LIVE_MODELS: tuple[str, ...] = (
    "stg_pos_transactions",
    "stg_inventory_events",
    "int_inventory_batch_coverage",
    "int_inventory_hourly",
    "int_inventory_reconciliation_hourly",
)

MARKETING_LIVE_MODELS: tuple[str, ...] = (
    "stg_clickstream_events",
    "int_identity_resolution",
    "int_identity_public_devices",
    "int_session_reconstruction",
    "int_rfm_scoring",
    "int_customer_consent",
)

CATALOG_LIVE_MODELS: tuple[str, ...] = (
    "int_product_catalog",
)


def select(*groups: tuple[str, ...] | str) -> str:
    """Space-joined dbt ``--select`` argument from model tuples and selectors."""
    parts: list[str] = []
    for group in groups:
        parts.extend([group] if isinstance(group, str) else group)
    return " ".join(parts)
