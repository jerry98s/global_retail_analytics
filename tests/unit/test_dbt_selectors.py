"""Each staging/intermediate model is built by exactly one DAG."""
from __future__ import annotations

from pathlib import Path

import pytest

from orchestration.airflow.plugins.dbt_selectors import (
    CATALOG_LIVE_MODELS,
    MARKETING_LIVE_MODELS,
    WAREHOUSE_LIVE_MODELS,
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
