"""Unit tests for dbt incremental model schema-evolution config (DL-I from the
data lake checklist applied 2026-07-05).

The data lake checklist item 4.1 says: when adding new columns to an
existing table, ensure they are NULLABLE (or have a default) so historical
data can be read without crashing. In dbt, this is enforced by setting
`on_schema_change` on every incremental model. The safe values are:

  - 'append_new_columns'  — adds new columns from the SELECT list as
    NULLABLE. Existing columns are untouched. This is the conservative
    default and matches the checklist item 4.2 ("avoid renaming or
    dropping columns directly") because it never drops a column.
  - 'sync_all_columns'    — adds new columns AND removes columns that
    disappear from the SELECT list. Useful for Type 1 dimensions where
    a removed column is intentional, but carries the risk item 4.2
    warns about. Allowed with documentation.

The unsafe values are:
  - 'ignore' (default if unset) — silently keeps the old schema, so new
    columns never land in the target. This is the most dangerous setting
    because it produces silent data loss.
  - 'fail' — raises an error on schema change, blocking deploys. Too
    aggressive for a CI/CD pipeline that expects additive evolution.

This test enforces that every `materialized='incremental'` model under
`transformation/dbt_project/models/` declares `on_schema_change` with one
of the two safe values.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODELS_DIR = _REPO_ROOT / "transformation" / "dbt_project" / "models"

# Matches the dbt `{{ config(...) }}` block in a .sql model file.
_CONFIG_BLOCK_RE = re.compile(
    r"\{\{\s*config\s*\((.*?)\)\s*\}\}",
    re.DOTALL,
)

# Extracts `materialized='...'` (or with double quotes / extra spaces).
_MATERIALIZED_RE = re.compile(
    r"materialized\s*=\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)

# Extracts `on_schema_change='...'`.
_ON_SCHEMA_CHANGE_RE = re.compile(
    r"on_schema_change\s*=\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)

_ALLOWED_ON_SCHEMA_CHANGE = {"append_new_columns", "sync_all_columns"}


def _incremental_models() -> list[Path]:
    """Return every .sql file under models/ that declares materialized='incremental'."""
    out: list[Path] = []
    for path in _MODELS_DIR.rglob("*.sql"):
        src = path.read_text(encoding="utf-8")
        m_cfg = _CONFIG_BLOCK_RE.search(src)
        if m_cfg is None:
            continue
        m_mat = _MATERIALIZED_RE.search(m_cfg.group(1))
        if m_mat and m_mat.group(1).lower() == "incremental":
            out.append(path)
    return out


def _format_parametrized_ids(paths: list[Path]) -> list[str]:
    return [str(p.relative_to(_REPO_ROOT)) for p in paths]


_INCREMENTAL_MODELS = _incremental_models()
_IDS = _format_parametrized_ids(_INCREMENTAL_MODELS)


@pytest.mark.parametrize("model_path", _INCREMENTAL_MODELS, ids=_IDS)
def test_incremental_model_declares_safe_on_schema_change(model_path: Path) -> None:
    """Every `materialized='incremental'` model must set `on_schema_change`
    to a safe value (`append_new_columns` or `sync_all_columns`). The
    dbt default `ignore` silently drops new columns — data lake checklist
    item 4.1 requires additive schema evolution to land in the target."""
    src = model_path.read_text(encoding="utf-8")
    cfg_match = _CONFIG_BLOCK_RE.search(src)
    assert cfg_match is not None, (
        f"{model_path}: no dbt config block found — every incremental "
        f"model needs a `{{ config(materialized='incremental', ...) }}` block."
    )
    cfg = cfg_match.group(1)
    osc_match = _ON_SCHEMA_CHANGE_RE.search(cfg)
    assert osc_match is not None, (
        f"{model_path}: incremental model is missing `on_schema_change`. "
        f"Without it dbt defaults to 'ignore', which silently drops new "
        f"columns — data lake checklist item 4.1 violation."
    )
    value = osc_match.group(1).lower()
    assert value in _ALLOWED_ON_SCHEMA_CHANGE, (
        f"{model_path}: on_schema_change='{value}' is not in the safe set "
        f"{sorted(_ALLOWED_ON_SCHEMA_CHANGE)}. 'ignore' silently drops new "
        f"columns; 'fail' blocks deploys. Use 'append_new_columns' for "
        f"fact tables and intermediate models (default), or "
        f"'sync_all_columns' for Type 1 dimensions where a removed column "
        f"is intentional."
    )


def test_all_incremental_models_covered() -> None:
    """Sanity check: we found at least the known incremental models. If
    this test fails, either a model was deleted (update the count down) or
    a new model was added without `materialized='incremental'` declared
    properly (the parametrized test above will catch the missing
    on_schema_change; this test catches accidental removal of the
    incremental materialization itself)."""
    # Known count as of 2026-07-18 (summary layer):
    #   fact_sales, fact_inventory_snapshot, fact_customer_session,
    #   dim_customer, dim_product, identity_graph,
    #   int_rfm_scoring, int_identity_resolution, int_session_reconstruction,
    #   sales_daily_store, inventory_daily_product_store, sessions_daily_platform
    assert len(_INCREMENTAL_MODELS) == 12, (
        f"Expected 12 incremental models, found {len(_INCREMENTAL_MODELS)}: "
        f"{[str(p.relative_to(_REPO_ROOT)) for p in _INCREMENTAL_MODELS]}"
    )
