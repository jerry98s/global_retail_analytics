"""Each staging/intermediate model is built by exactly one DAG."""
from __future__ import annotations

from pathlib import Path

import pytest

from orchestration.airflow.plugins.dbt_selectors import (
    CATALOG_LIVE_MODELS,
    MARKETING_LIVE_MODELS,
    MARKETING_MART_SELECT,
    WAREHOUSE_LIVE_MODELS,
    quality_selection,
    select,
)

pytestmark = pytest.mark.unit

_MODELS = Path(__file__).resolve().parents[2] / "transformation/dbt_project/models"
_GROUPS = (WAREHOUSE_LIVE_MODELS, MARKETING_LIVE_MODELS, CATALOG_LIVE_MODELS)


def test_owner_sets_are_disjoint():
    owned = [model for group in _GROUPS for model in group]
    assert len(owned) == len(set(owned))


def test_every_staging_and_intermediate_model_has_an_owner():
    models = {
        path.stem
        for layer in ("staging", "intermediate")
        for path in (_MODELS / layer).rglob("*.sql")
    }
    owned = {model for group in _GROUPS for model in group}
    assert models == owned


def test_select_joins_groups_and_raw_selectors():
    assert select(("a", "b"), "marts.finance") == "a b marts.finance"


def test_quality_scopes_use_the_owning_dag_selections():
    dags = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"
    marketing = (dags / "marketing_hourly_customer_360_pipeline.py").read_text(encoding="utf-8")
    warehouse = (dags / "warehouse_daily_batch_pipeline.py").read_text(encoding="utf-8")
    catalog = (dags / "catalog_bihourly_product_scd2_refresh.py").read_text(encoding="utf-8")

    assert quality_selection("marketing") == {
        "select": select(MARKETING_LIVE_MODELS, MARKETING_MART_SELECT),
        "exclude": ["dim_product", "customer_360_view"],
    }
    assert "MARKETING_MART_SELECT" in marketing
    assert "consent_identity_daily customer_segments" in MARKETING_MART_SELECT
    assert quality_selection("finance")["select"] == select(
        WAREHOUSE_LIVE_MODELS,
        "marts.finance sales_daily_store inventory_daily_product_store inventory_position_daily",
    )
    assert "OWNED_SELECT" in warehouse and "marts.finance sales_daily_store" in warehouse
    assert quality_selection("catalog") == {
        "select": select(CATALOG_LIVE_MODELS, "dim_product"),
        "exclude": [],
    }
    assert 'select(CATALOG_LIVE_MODELS, "dim_product")' in catalog
    with pytest.raises(ValueError):
        quality_selection("serving")
