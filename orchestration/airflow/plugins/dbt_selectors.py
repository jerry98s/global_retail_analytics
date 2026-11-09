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
    "int_inventory_coverage_gaps",
    "int_inventory_hourly",
    "int_inventory_reconciliation_hourly",
)

MARKETING_LIVE_MODELS: tuple[str, ...] = (
    "stg_clickstream_events",
    "int_identity_resolution",
    "int_identity_changes",
    "int_identity_shared_devices",
    "int_session_reconstruction",
    "int_order_reconciliation",
    "int_rfm_scoring",
    "int_consent_events",
    "int_customer_consent",
)

CATALOG_LIVE_MODELS: tuple[str, ...] = (
    "int_product_catalog",
)

# Marketing-owned marts. The summary models are each built from one marketing
# Gold table. dim_product stays with the catalog DAG.
MARKETING_MART_SELECT = (
    "marts.marketing sessions_daily_platform consent_identity_daily customer_segments"
)


def select(*groups: tuple[str, ...] | str) -> str:
    """Space-joined dbt ``--select`` argument from model tuples and selectors."""
    parts: list[str] = []
    for group in groups:
        parts.extend([group] if isinstance(group, str) else group)
    return " ".join(parts)


# Pending selections for one owning DAG. Marketing excludes dim_product.
QUALITY_SELECTIONS: dict[str, dict[str, object]] = {
    "finance": {
        "select": select(
            WAREHOUSE_LIVE_MODELS,
            "marts.finance sales_daily_store inventory_daily_product_store inventory_position_daily",
        ),
        "exclude": [],
    },
    "marketing": {
        "select": select(MARKETING_LIVE_MODELS, MARKETING_MART_SELECT),
        "exclude": ["dim_product", "customer_360_view"],
    },
    "catalog": {
        "select": select(CATALOG_LIVE_MODELS, "dim_product"),
        "exclude": [],
    },
}


def quality_selection(scope: str) -> dict[str, object]:
    """dbt test selection for one owning DAG. Raises for an unknown scope."""
    try:
        return QUALITY_SELECTIONS[scope]
    except KeyError:
        raise ValueError(f"Unsupported quality scope: {scope}") from None
