"""Unit + static-contract tests for Gold Write-Audit-Publish (ADR-009).

Covers:
- dbt macro routing: wap_phase=pending sends Gold *tables* to *_pending;
  views stay live.
- wap_live_ref(): cross-DAG Gold reads resolve to the live schema.
- Incremental models anchor on ``{{ this }}`` (pending is a live clone).
- wap_publish helper: clone SQL, publish SQL, disjoint per-DAG ownership.
- GE pending retargeting: only listed tables are rewritten.
- DAG + local-stack contract: clone -> write pending -> audit -> publish.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAGS = _REPO / "orchestration" / "airflow" / "dags"
_PLUGINS = _REPO / "orchestration" / "airflow" / "plugins"
_STACK = _REPO / "scripts" / "local" / "run_local_stack.ps1"

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
GOLD_DIST_SORT = {
    "models/marts/finance/fact_sales.sql": ("product_key", "date_key"),
    "models/marts/finance/fact_inventory_snapshot.sql": ("product_key", "snapshot_date_key"),
    "models/marts/marketing/dim_product.sql": ("product_key", "product_id"),
    "models/marts/marketing/dim_customer.sql": ("customer_key", "loyalty_id"),
    "models/marts/marketing/fact_customer_session.sql": ("customer_key", "session_date_key"),
    "models/marts/marketing/identity_graph.sql": ("customer_key", "identifier_type"),
}


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

    def test_views_are_not_redirected_to_pending(self) -> None:
        src = _read(_DBT / "macros" / "generate_schema_name.sql")
        assert "is_view" in src
        assert "materialized == 'view'" in src
        assert "not is_view" in src

    def test_wap_live_ref_strips_pending_suffix(self) -> None:
        src = _read(_DBT / "macros" / "generate_schema_name.sql")
        assert "macro wap_live_ref" in src
        assert "rel.schema[:-8]" in src
        assert "endswith('_pending')" in src
        assert "wap_prior_state" not in src


class TestIncrementalSelfRefsUseThis:
    @pytest.mark.parametrize("model", GOLD_INCREMENTAL_MODELS)
    def test_incremental_anchor_uses_this(self, model: str) -> None:
        src = _read(_DBT / model)
        assert "{{ this }}" in src, (
            f"{model}: after the live→pending clone, incremental lookbacks "
            "must read {{ this }} (the pending clone), not a live alias."
        )
        assert "wap_prior_state" not in src

    def test_finance_facts_join_live_dim_product(self) -> None:
        for model in (
            "models/marts/finance/fact_sales.sql",
            "models/marts/finance/fact_inventory_snapshot.sql",
        ):
            src = _read(_DBT / model)
            assert "wap_live_ref('dim_product')" in src, (
                f"{model}: dim_product is owned by the catalog DAG; join live."
            )

    def test_cross_dag_relationship_tests_use_live_source(self) -> None:
        for rel_path in (
            "tests/fact_sales.yml",
            "tests/summary.yml",
        ):
            src = _read(_DBT / rel_path)
            assert "wap_live_ref" not in src, (
                f"{rel_path}: nested wap_live_ref in generic tests breaks dbt compile"
            )
            assert "source('gold_marketing', 'dim_product')" in src
        sources = _read(_DBT / "models" / "staging" / "_sources.yml")
        assert "name: gold_marketing" in sources
        assert "name: dim_product" in sources

    def test_dim_product_does_not_live_ref_itself(self) -> None:
        src = _read(_DBT / "models/marts/marketing/dim_product.sql")
        assert "wap_live_ref" not in src


class TestGoldDistSortPreserved:
    @pytest.mark.parametrize("model,dist,sort", [
        (path, dist, sort) for path, (dist, sort) in GOLD_DIST_SORT.items()
    ])
    def test_model_declares_dist_and_sort(self, model: str, dist: str, sort: str) -> None:
        src = _read(_DBT / model)
        assert f"dist='{dist}'" in src, f"{model} missing dist='{dist}'"
        assert f"dist='{dist}'" in src, f"{model} missing dist='{dist}'"
        assert "sort=" in src and sort in src, f"{model} missing sort containing {sort}"


class TestLateBindingViews:
    def test_dbt_project_sets_bind_false(self) -> None:
        src = _read(_DBT / "dbt_project.yml")
        assert "+bind: false" in src

    def test_redshift_serving_views_are_late_binding(self) -> None:
        for name in ("customer_360_serving.sql", "dim_product_current.sql"):
            src = _read(_REPO / "transformation" / "redshift" / "views" / name)
            assert "WITH NO SCHEMA BINDING" in src
            assert "DROP VIEW IF EXISTS" in src


class TestWapPublishHelper:
    def test_canonical_table_list(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        tables = set(wap_publish.WAP_TABLES)
        for schema, table in tables:
            assert schema in WAP_GOLD_SCHEMAS
        assert ("finance", "dim_date") not in tables
        assert ("finance", "dim_store") not in tables
        assert ("marketing", "customer_360_view") not in tables
        assert ("finance", "fact_sales") in tables
        assert ("marketing", "dim_product") in tables
        assert ("summary", "sessions_daily_platform") in tables

    def test_per_dag_subsets_are_disjoint_and_cover_wap_tables(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        finance = set(wap_publish.FINANCE_SUMMARY_TABLES)
        marketing = set(wap_publish.MARKETING_TABLES)
        catalog = set(wap_publish.DIM_PRODUCT_TABLES)
        assert not (finance & marketing)
        assert not (finance & catalog)
        assert not (marketing & catalog)
        assert finance | marketing | catalog == set(wap_publish.WAP_TABLES)
        # dim_product is catalog-only — warehouse must not publish it.
        assert ("marketing", "dim_product") not in finance
        assert ("marketing", "dim_product") in catalog

    def test_redshift_clone_statements_use_like(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        stmts = wap_publish.build_redshift_clone_statements(
            [("finance", "fact_sales")]
        )
        joined = "\n".join(stmts)
        assert "DROP TABLE IF EXISTS finance_pending.fact_sales" in joined
        assert "CREATE TABLE finance_pending.fact_sales (LIKE finance.fact_sales)" in joined
        assert "INSERT INTO finance_pending.fact_sales SELECT * FROM finance.fact_sales" in joined

    def test_redshift_publish_statements_swap(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        stmts = wap_publish.build_redshift_publish_statements(
            [("finance", "fact_sales"), ("summary", "sales_daily_store")]
        )
        joined = "\n".join(stmts)
        assert stmts.count("COMMIT") == 1
        assert joined.index("SET SCHEMA finance") < joined.index("COMMIT")
        assert joined.index("SET SCHEMA summary") < joined.index("COMMIT")
        assert joined.index("COMMIT") < joined.rindex(
            "DROP TABLE IF EXISTS finance.fact_sales__wap_old"
        )
        assert "ALTER TABLE finance.fact_sales RENAME TO fact_sales__wap_old" in joined
        assert "ALTER TABLE finance_pending.fact_sales SET SCHEMA finance" in joined

    def test_publish_aborts_when_pending_missing(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Cur:
            def execute(self, *a: object) -> None:
                pass

            def fetchone(self) -> tuple[int]:
                return (0,)

            def close(self) -> None:
                pass

        class _Conn:
            def cursor(self) -> _Cur:
                return _Cur()

            def commit(self) -> None:
                pass

            def rollback(self) -> None:
                pass

        with pytest.raises(RuntimeError, match="pending table"):
            wap_publish.publish_gold(
                _Conn(), [("finance", "fact_sales")], dialect="redshift"
            )

    def test_publish_preflights_set_before_any_swap(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Conn:
            def __init__(self) -> None:
                self.existing = {("finance_pending", "fact_sales")}
                self.sql: list[str] = []
                self.commits = 0
                self._count = 0

            def cursor(self) -> "_Conn":
                return self

            def execute(self, sql: str, params: tuple[str, str] | None = None) -> None:
                self.sql.append(sql)
                if params is not None:
                    self._count = 1 if params in self.existing else 0

            def fetchone(self) -> tuple[int]:
                return (self._count,)

            def close(self) -> None:
                pass

            def commit(self) -> None:
                self.commits += 1

            def rollback(self) -> None:
                pass

        conn = _Conn()
        with pytest.raises(RuntimeError, match="fact_inventory_snapshot"):
            wap_publish.publish_gold(
                conn,
                [
                    ("finance", "fact_sales"),
                    ("finance", "fact_inventory_snapshot"),
                ],
                dialect="redshift",
            )
        mutating = [
            s for s in conn.sql if s.startswith(("DROP", "ALTER", "CREATE"))
        ]
        assert mutating == []
        assert conn.commits == 0

    def test_publish_commits_whole_set_once(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Conn:
            def __init__(self) -> None:
                self.existing = {
                    ("finance_pending", "fact_sales"),
                    ("finance_pending", "fact_inventory_snapshot"),
                    ("finance", "fact_sales"),
                    ("finance", "fact_inventory_snapshot"),
                }
                self.sql: list[str] = []
                self.commits = 0
                self._count = 0

            def cursor(self) -> "_Conn":
                return self

            def execute(self, sql: str, params: tuple[str, str] | None = None) -> None:
                self.sql.append(sql)
                if params is not None:
                    self._count = 1 if params in self.existing else 0

            def fetchone(self) -> tuple[int]:
                return (self._count,)

            def close(self) -> None:
                pass

            def commit(self) -> None:
                self.commits += 1

            def rollback(self) -> None:
                pass

        conn = _Conn()
        result = wap_publish.publish_gold(
            conn,
            [
                ("finance", "fact_sales"),
                ("finance", "fact_inventory_snapshot"),
            ],
            dialect="redshift",
        )
        assert result["count"] == 2
        assert conn.commits == 2  # one swap transaction, one __wap_old drop
        assert sum("SET SCHEMA finance" in s for s in conn.sql) == 2

    def test_clone_skips_missing_live_table(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Cur:
            def execute(self, *a: object) -> None:
                pass

            def fetchone(self) -> tuple[int]:
                return (0,)

            def close(self) -> None:
                pass

        class _Conn:
            def __init__(self) -> None:
                self.commits = 0

            def cursor(self) -> _Cur:
                return _Cur()

            def commit(self) -> None:
                self.commits += 1

            def rollback(self) -> None:
                pass

        conn = _Conn()
        result = wap_publish.clone_live_to_pending(
            conn, [("finance", "fact_sales")], dialect="redshift"
        )
        assert result["cloned"] == []
        assert result["skipped"] == ["finance.fact_sales"]
        assert conn.commits >= 1


class TestGePendingRetarget:
    def test_parse_table_list(self) -> None:
        from scripts.common.run_ge_checkpoint import parse_table_list

        assert parse_table_list("finance.fact_sales, summary.sales_daily_store") == [
            ("finance", "fact_sales"),
            ("summary", "sales_daily_store"),
        ]
        assert parse_table_list("") == []
        with pytest.raises(ValueError):
            parse_table_list("finance")

    def test_retarget_only_listed_tables(self) -> None:
        from scripts.common.run_ge_checkpoint import retarget_query

        q = (
            "SELECT * FROM finance.fact_sales WHERE x = 1; "
            "SELECT * FROM finance.dim_date; "
            "SELECT * FROM marketing.dim_product; "
            "SELECT * FROM marketing.customer_360_view; "
            "SELECT * FROM bronze.clickstream_events"
        )
        out = retarget_query(q, [("finance", "fact_sales"), ("marketing", "dim_product")])
        assert "finance_pending.fact_sales" in out
        assert "marketing_pending.dim_product" in out
        assert "finance.dim_date" in out
        assert "marketing.customer_360_view" in out
        assert "bronze.clickstream_events" in out
        assert "finance_pending.dim_date" not in out

    def test_prepare_validations_filters_and_retargets(self) -> None:
        from scripts.common.run_ge_checkpoint import prepare_validations

        validations = [
            {"suite": "fact_sales", "query": "SELECT * FROM finance.fact_sales", "asset": "a", "datasource": "d"},
            {"suite": "dim_date", "query": "SELECT * FROM finance.dim_date", "asset": "b", "datasource": "d"},
            {"suite": "c360", "query": "SELECT * FROM marketing.customer_360_view", "asset": "c", "datasource": "d"},
        ]
        selected = prepare_validations(validations, [("finance", "fact_sales")])
        assert len(selected) == 1
        assert selected[0]["suite"] == "fact_sales"
        assert "finance_pending.fact_sales" in selected[0]["query"]

    def test_empty_pending_tables_is_live_mode(self) -> None:
        from scripts.common.run_ge_checkpoint import prepare_validations

        validations = [
            {"suite": "fact_sales", "query": "SELECT * FROM finance.fact_sales", "asset": "a", "datasource": "d"},
        ]
        assert prepare_validations(validations, []) == validations


class TestDagWapContract:
    def test_warehouse_clone_then_pending_then_publish(self) -> None:
        src = _read(_DAGS / "warehouse_daily_batch_pipeline.py")
        assert "clone_finance_summary_task" in src
        assert '"wap_phase": "pending"' in src
        assert "wap_publish" in src
        assert "pending_tables" in src
        assert "--pending-tables" in src
        assert src.index("wap_clone") < src.index("dbt_marts")
        assert src.index("wap_publish") < src.index("redshift_analyze")
        # Warehouse no longer builds or publishes dim_product.
        assert "int_product_catalog dim_product" not in src
        assert "max_active_runs" in src and "max_active_runs  = 1" in src

    def test_marketing_clone_pending_publish_serving(self) -> None:
        src = _read(_DAGS / "marketing_hourly_customer_360_pipeline.py")
        assert "clone_marketing_task" in src
        assert "wap_phase" in src
        assert "wap_publish_marketing" in src
        assert "customer_360_serving" in src
        assert src.index("wap_clone") < src.index("dbt_run_pending")
        assert src.index("wap_publish") < src.index("dbt_serving")
        assert "max_active_runs  = 1" in src

    def test_catalog_clones_and_publishes_dim_product(self) -> None:
        src = _read(_DAGS / "catalog_bihourly_product_scd2_refresh.py")
        assert "clone_dim_product_task" in src
        assert "publish_dim_product_task" in src
        assert "wap_phase" in src and "pending" in src
        assert src.index("clone_dim_product") < src.index("refresh_dim_product")
        assert "max_active_runs=1" in src

    def test_hourly_ge_stays_on_live(self) -> None:
        src = _read(_DAGS / "quality_hourly_ge_checkpoint.py")
        assert "--pending-tables" not in src
        assert "--schema-suffix" not in src
        assert "wap_phase" not in src

    def test_local_stack_matches_plugin_table_list(self) -> None:
        from orchestration.airflow.plugins.wap_publish import WAP_TABLES

        stack = _read(_STACK)
        for schema, table in WAP_TABLES:
            assert f"'{schema}.{table}'" in stack
        assert "Invoke-WapCloneLocal" in stack
        assert "clone_live_to_pending" in stack
        assert "--pending-tables" in stack
        assert "Invoke-WapPublishLocal" in stack
