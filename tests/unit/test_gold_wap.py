"""Unit + static-contract tests for Gold Write-Audit-Publish (ADR-009).

Covers:
- dbt macro routing: wap_phase=pending sends Gold schemas to *_pending.
- wap_prior_state(): incremental anchors read live (not empty pending).
- wap_publish helper: canonical table list, exclusions, Redshift SQL builder.
- run_ge_checkpoint.apply_schema_suffix(): Gold mart queries retargeted,
  reference dims / views / Bronze untouched.
- DAG contract: warehouse/marketing/catalog wire pending -> audit -> publish.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAGS = _REPO / "orchestration" / "airflow" / "dags"
_PLUGINS = _REPO / "orchestration" / "airflow" / "plugins"

WAP_GOLD_SCHEMAS = ("finance", "marketing", "summary")
GOLD_INCREMENTAL_MODELS = [
    "models/marts/finance/fact_sales.sql",
    "models/marts/finance/fact_inventory_snapshot.sql",
    "models/marts/marketing/dim_product.sql",
    "models/marts/marketing/fact_customer_session.sql",
    "models/marts/summary/sales_daily_store.sql",
    "models/marts/summary/inventory_daily_product_store.sql",
    "models/marts/summary/sessions_daily_platform.sql",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestGenerateSchemaNameWap:
    def test_pending_suffixes_gold_schemas(self) -> None:
        src = _read(_DBT / "macros" / "generate_schema_name.sql")
        assert "wap_phase" in src
        assert "'finance', 'marketing', 'summary'" in src
        assert "_pending" in src
        # The Gold schema list MUST live inside the macro. A top-level
        # `{% set %}` in a macros file is ignored (UnexpectedJinjaBlock) and
        # silently skips pending routing — caught in the first local E2E.
        macro_idx = src.index("{% macro generate_schema_name")
        set_idx = src.index("wap_gold_schemas")
        assert set_idx > macro_idx

    def test_wap_prior_state_strips_pending_suffix(self) -> None:
        src = _read(_DBT / "macros" / "generate_schema_name.sql")
        assert "macro wap_prior_state" in src
        # Strips the 8-char "_pending" suffix to point at the live schema.
        assert "rel.schema[:-8]" in src
        assert "endswith('_pending')" in src


class TestIncrementalSelfRefsReadLive:
    @pytest.mark.parametrize("model", GOLD_INCREMENTAL_MODELS)
    def test_incremental_anchor_uses_wap_prior_state(self, model: str) -> None:
        src = _read(_DBT / model)
        assert "wap_prior_state()" in src, (
            f"{model}: incremental anchor must read from live via "
            "wap_prior_state(), not the empty pending {{ this }}."
        )

    @pytest.mark.parametrize("model", GOLD_INCREMENTAL_MODELS)
    def test_no_bare_this_incremental_anchor(self, model: str) -> None:
        src = _read(_DBT / model)
        # Inside an is_incremental block, a bare "from {{ this }}" would read
        # the empty pending relation. Comments/docstrings may still mention it.
        body = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        assert "from {{ this }}" not in body, (
            f"{model}: found bare 'from {{{{ this }}}}' — use wap_prior_state()."
        )


class TestWapPublishHelper:
    def test_canonical_table_list(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        tables = set(wap_publish.WAP_TABLES)
        for schema, table in tables:
            assert schema in WAP_GOLD_SCHEMAS
        # Reference dims and views are never published.
        assert ("finance", "dim_date") not in tables
        assert ("finance", "dim_store") not in tables
        assert ("marketing", "customer_360_view") not in tables
        # Sanity: expected marts are present.
        assert ("finance", "fact_sales") in tables
        assert ("marketing", "dim_product") in tables
        assert ("summary", "sessions_daily_platform") in tables

    def test_redshift_publish_statements_swap(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        stmts = wap_publish.build_redshift_publish_statements(
            [("finance", "fact_sales")]
        )
        joined = "\n".join(stmts)
        assert "ALTER TABLE finance.fact_sales RENAME TO fact_sales__wap_old" in joined
        assert "ALTER TABLE finance_pending.fact_sales SET SCHEMA finance" in joined
        assert "DROP TABLE IF EXISTS finance.fact_sales__wap_old" in joined
        assert joined.count("BEGIN") >= 1 and joined.count("COMMIT") >= 1

    def test_publish_aborts_when_pending_missing(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Cur:
            def execute(self, *a: object) -> None:
                pass

            def fetchone(self) -> tuple[int]:
                return (0,)  # nothing exists

            def close(self) -> None:
                pass

            def __enter__(self) -> "_Cur":
                return self

            def __exit__(self, *a: object) -> None:
                pass

        class _Conn:
            def cursor(self) -> _Cur:
                return _Cur()

        with pytest.raises(RuntimeError, match="pending table"):
            wap_publish.publish_gold(
                _Conn(), [("finance", "fact_sales")], dialect="redshift"
            )


class TestGeSchemaSuffix:
    def test_mart_queries_suffixed_dims_not(self) -> None:
        from scripts.common.run_ge_checkpoint import apply_schema_suffix

        q = (
            "SELECT * FROM finance.fact_sales WHERE x = 1; "
            "SELECT * FROM finance.dim_date; "
            "SELECT * FROM marketing.dim_product; "
            "SELECT * FROM marketing.customer_360_view; "
            "SELECT * FROM bronze.clickstream_events"
        )
        out = apply_schema_suffix(q, "_pending")
        assert "finance_pending.fact_sales" in out
        assert "marketing_pending.dim_product" in out
        # Reference dims, views, and Bronze stay on live.
        assert "finance.dim_date" in out
        assert "marketing.customer_360_view" in out
        assert "bronze.clickstream_events" in out
        assert "finance_pending.dim_date" not in out

    def test_empty_suffix_is_noop(self) -> None:
        from scripts.common.run_ge_checkpoint import apply_schema_suffix

        q = "SELECT * FROM finance.fact_sales"
        assert apply_schema_suffix(q, "") == q

    def test_idempotent_no_double_suffix(self) -> None:
        from scripts.common.run_ge_checkpoint import apply_schema_suffix

        q = "SELECT * FROM finance_pending.fact_sales"
        out = apply_schema_suffix(q, "_pending")
        assert "finance_pending_pending" not in out


class TestDagWapContract:
    def test_warehouse_pending_before_publish(self) -> None:
        src = _read(_DAGS / "warehouse_daily_batch_pipeline.py")
        assert '"wap_phase": "pending"' in src
        assert "wap_publish" in src
        assert 'op_kwargs        = {"schema_suffix": "_pending"}' in src
        assert "--schema-suffix _pending" in src
        # Publish precedes ANALYZE.
        assert src.index("wap_publish") < src.index("redshift_analyze")

    def test_marketing_pending_publish_serving(self) -> None:
        src = _read(_DAGS / "marketing_hourly_customer_360_pipeline.py")
        assert "wap_phase" in src
        assert "wap_publish_marketing" in src
        assert "customer_360_serving" in src
        # Publish precedes the serving refresh.
        assert src.index("wap_publish") < src.index("dbt_serving")

    def test_catalog_publishes_dim_product(self) -> None:
        src = _read(_DAGS / "catalog_bihourly_product_scd2_refresh.py")
        assert "wap_phase" in src and "pending" in src
        assert "publish_dim_product_task" in src

    def test_hourly_ge_stays_on_live(self) -> None:
        src = _read(_DAGS / "quality_hourly_ge_checkpoint.py")
        # The live monitor must NOT retarget at pending.
        assert "--schema-suffix" not in src
        assert "wap_phase" not in src
