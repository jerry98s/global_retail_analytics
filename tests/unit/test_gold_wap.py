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
WAP_GOLD_SCHEMAS = ("finance", "marketing", "summary")
GOLD_INCREMENTAL_MODELS = [
    "models/marts/finance/fact_sales.sql",
    "models/marts/finance/fact_inventory_snapshot.sql",
    "models/marts/marketing/dim_product.sql",
    "models/marts/marketing/fact_customer_session.sql",
    "models/marts/summary/sales_daily_store.sql",
    "models/marts/summary/inventory_daily_product_store.sql",
    "models/marts/summary/inventory_position_daily.sql",
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

    def test_dim_product_changed_versions_use_last_seen_date(self) -> None:
        # product_key = hash(product_id + effective_from). If a changed
        # product's new version reused the catalog's effective_from (earliest
        # activity), it would collide with the existing version's key — the
        # 2026-08-31 pending-audit failure (938 duplicate product_key rows).
        src = _read(_DBT / "models/marts/marketing/dim_product.sql")
        assert "s.effective_from as new_effective_from" not in src
        compact = " ".join(src.split())
        assert (
            "greatest( s.last_seen_date, cast({{ dateadd_unit('day', 1, 'e.effective_from') }} as date) )"
            " as new_effective_from" in compact
        )
        assert (
            "when ec.product_id is not null then greatest( sp.last_seen_date, "
            "cast({{ dateadd_unit('day', 1, 'ec.effective_from') }} as date) )" in compact
        )

    def test_product_catalog_attributes_come_from_master(self) -> None:
        # Sales-derived attributes (average selling price) change on every
        # load and would open a new SCD2 version per product per day.
        src = _read(_DBT / "models/intermediate/int_product_catalog.sql")
        assert "ref('product_master')" in src
        assert "net_revenue" not in src
        assert "quantity_sold" not in src

    def test_fact_sales_customer_key_drift_test_present(self) -> None:
        src = _read(_DBT / "tests/fact_sales_customer_key_matches_identity.sql")
        assert "severity='warn'" in src
        assert "ref('int_identity_resolution')" in src
        assert "i.customer_key <> f.customer_key" in src

    def test_inventory_on_hand_starts_from_opening_balance(self) -> None:
        # Without an opening count every product/store starts at zero and
        # the first sale reads as negative stock.
        src = _read(_DBT / "models/marts/finance/fact_inventory_snapshot.sql")
        assert "ref('inventory_opening_balance')" in src
        assert "coalesce(cast(o.opening_qty as numeric(38, 0)), 0)" in src

    def test_inventory_reconciliation_test_present(self) -> None:
        model = _read(_DBT / "models/intermediate/int_inventory_reconciliation_hourly.sql")
        assert "source('silver', 'inventory_events')" in model
        assert "source('silver', 'inventory_hourly')" in model
        assert "late_dropped_events" in model
        test = _read(_DBT / "tests/inventory_hourly_reconciles_canonical_events.sql")
        assert "severity='warn'" in test
        assert "unexplained_qty <> 0" in test
        # The open hour is incomplete on the Spark side; comparing it is noise.
        assert "is_batch_covered" in test
        assert "ref('int_inventory_batch_coverage')" in model


class TestInventoryLambda:
    """ADR-012: Spark owns covered hours, Flink only the hours after."""

    def test_fact_reads_combined_hourly_not_flink_directly(self) -> None:
        src = _read(_DBT / "models/marts/finance/fact_inventory_snapshot.sql")
        assert "ref('int_inventory_hourly')" in src
        assert "source('silver', 'inventory_hourly')" not in src

    def test_hourly_splits_on_batch_coverage(self) -> None:
        src = _read(_DBT / "models/intermediate/int_inventory_hourly.sql")
        assert "source('silver', 'inventory_events')" in src
        assert "source('silver', 'inventory_hourly')" in src
        assert "ref('int_inventory_batch_coverage')" in src
        compact = " ".join(src.split())
        assert "false as is_estimated from batch_hourly" in compact
        assert "true as is_estimated from speed_hourly" in compact
        # No coverage yet (Spark never ran) must keep every Flink hour.
        assert "c.batch_start_hour_key is null" in compact

    def test_coverage_closes_hours_with_flink_watermark(self) -> None:
        src = _read(_DBT / "models/intermediate/int_inventory_batch_coverage.sql")
        assert "var('inventory_silver_watermark_delay_seconds', 60)" in src
        assert "min(cast(kafka_timestamp as timestamp))" in src
        assert "max(cast(kafka_timestamp as timestamp))" in src

    def test_fact_rewrites_changed_past_hours(self) -> None:
        # An append-only "hours after max" filter would never apply a Spark
        # correction to an hour that is already loaded.
        src = _read(_DBT / "models/marts/finance/fact_inventory_snapshot.sql")
        compact = " ".join(src.split())
        assert "max(snapshot_date_key * 100 + snapshot_hour)" not in compact
        assert "e.quantity_on_hand <> f.quantity_on_hand" in compact
        assert "e.is_estimated <> f.is_estimated" in compact
        # Keep the stored key so delete+insert replaces the row in place.
        assert "coalesce(e.product_key, p.product_key)" in compact
        # Stored hours that lost their movement are re-emitted, not left stale.
        assert "cast(0 as numeric(38, 0)) as qty_delta_hour" in compact

    def test_inventory_summary_rewrites_changed_days(self) -> None:
        src = _read(_DBT / "models/marts/summary/inventory_daily_product_store.sql")
        compact = " ".join(src.split())
        assert "max(snapshot_date_key)" not in compact
        assert "t.ending_on_hand_qty <> d.ending_on_hand_qty" in compact
        assert "t.stockout_hour_count <> d.stockout_hour_count" in compact

    def test_is_estimated_allows_speed_layer_hours(self) -> None:
        yml = _read(_DBT / "tests/fact_sales.yml")
        assert "values: [false, true]" in yml


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

    def test_laptop_cli_scopes_reuse_dag_ownership(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        assert wap_publish.WAP_SCOPES == {
            "finance": wap_publish.FINANCE_SUMMARY_TABLES,
            "marketing": wap_publish.MARKETING_TABLES,
            "catalog": wap_publish.DIM_PRODUCT_TABLES,
        }

    def test_redshift_clone_statements_use_like(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        stmts = wap_publish.build_redshift_clone_statements(
            [("finance", "fact_sales")]
        )
        joined = "\n".join(stmts)
        assert "DROP TABLE IF EXISTS finance_pending.fact_sales" in joined
        assert "CREATE TABLE finance_pending.fact_sales (LIKE finance.fact_sales)" in joined
        assert "INSERT INTO finance_pending.fact_sales SELECT * FROM finance.fact_sales" in joined

    def test_redshift_publish_statements_keep_live_table(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        stmts = wap_publish.build_publish_statements(
            "finance", "fact_sales", ["sale_id", "amount"], live_exists=True
        )
        assert stmts == [
            "DELETE FROM finance.fact_sales",
            "INSERT INTO finance.fact_sales (sale_id, amount) "
            "SELECT sale_id, amount FROM finance_pending.fact_sales",
        ]
        first = wap_publish.build_publish_statements(
            "finance", "fact_sales", ["sale_id"], live_exists=False
        )
        assert first[0] == "CREATE TABLE finance.fact_sales (LIKE finance_pending.fact_sales)"
        joined = " ".join(stmts + first)
        assert "SET SCHEMA" not in joined
        assert "RENAME" not in joined

    @staticmethod
    def _duckdb_with(tables: dict[str, list[tuple[int, str]]]) -> object:
        import duckdb

        conn = duckdb.connect()
        for fqn, rows in tables.items():
            schema = fqn.split(".", 1)[0]
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(f"CREATE TABLE {fqn} (id INTEGER, label VARCHAR)")
            for row in rows:
                conn.execute(f"INSERT INTO {fqn} VALUES (?, ?)", list(row))
        return conn

    def test_publish_replaces_live_rows_and_drops_pending(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "finance.fact_sales": [(1, "old")],
            "finance_pending.fact_sales": [(1, "new"), (2, "new")],
            "summary_pending.sales_daily_store": [(9, "first")],
        })
        conn.execute("CREATE SCHEMA summary")
        result = wap_publish.publish_gold(
            conn,
            [("finance", "fact_sales"), ("summary", "sales_daily_store")],
            dialect="duckdb",
        )
        assert result["published"] == ["finance.fact_sales", "summary.sales_daily_store"]
        assert conn.execute("SELECT * FROM finance.fact_sales ORDER BY id").fetchall() == [
            (1, "new"),
            (2, "new"),
        ]
        assert conn.execute("SELECT * FROM summary.sales_daily_store").fetchall() == [
            (9, "first")
        ]
        pending_left = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema LIKE '%_pending'"
        ).fetchone()
        assert pending_left == (0,)

    def test_publish_aborts_when_pending_missing(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "finance.fact_sales": [(1, "live")],
            "finance_pending.fact_sales": [(1, "new")],
        })
        with pytest.raises(RuntimeError, match="fact_inventory_snapshot"):
            wap_publish.publish_gold(
                conn,
                [("finance", "fact_sales"), ("finance", "fact_inventory_snapshot")],
                dialect="duckdb",
            )
        assert conn.execute("SELECT label FROM finance.fact_sales").fetchall() == [("live",)]

    def test_publish_aborts_on_column_drift_before_any_write(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "finance.fact_sales": [(1, "live")],
            "finance_pending.fact_sales": [(1, "new")],
        })
        conn.execute("ALTER TABLE finance_pending.fact_sales ADD COLUMN extra INTEGER")
        with pytest.raises(RuntimeError, match="columns differ"):
            wap_publish.publish_gold(conn, [("finance", "fact_sales")], dialect="duckdb")
        assert conn.execute("SELECT label FROM finance.fact_sales").fetchall() == [("live",)]

    def test_publish_failure_rolls_back_whole_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "finance.fact_sales": [(1, "live")],
            "finance_pending.fact_sales": [(1, "new")],
            "finance.fact_inventory_snapshot": [(1, "live")],
            "finance_pending.fact_inventory_snapshot": [(1, "new")],
        })
        real_build = wap_publish.build_publish_statements

        def _failing_second(schema: str, table: str, *a: object, **kw: object) -> list[str]:
            if table == "fact_inventory_snapshot":
                return ["INSERT INTO finance.no_such_table VALUES (1)"]
            return real_build(schema, table, *a, **kw)

        monkeypatch.setattr(wap_publish, "build_publish_statements", _failing_second)
        with pytest.raises(Exception, match="no_such_table"):
            wap_publish.publish_gold(
                conn,
                [("finance", "fact_sales"), ("finance", "fact_inventory_snapshot")],
                dialect="duckdb",
            )
        assert conn.execute("SELECT label FROM finance.fact_sales").fetchall() == [("live",)]

    def test_clone_skips_missing_live_table(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        class _Cur:
            def execute(self, *a: object) -> None:
                pass

            def fetchall(self) -> list[tuple[int]]:
                return [(0,)]

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


class TestDagWapContract:
    def test_warehouse_clone_then_pending_then_publish(self) -> None:
        src = _read(_DAGS / "warehouse_daily_batch_pipeline.py")
        assert "clone_finance_summary_task" in src
        assert '"wap_phase": "pending"' in src
        assert "wap_publish" in src
        assert "pending_tables" in src
        # dbt tests are the publish gate; GE no longer re-audits Gold.
        assert "--pending-tables" not in src
        assert src.index("dbt_tests") < src.index("wap_publish_gold")
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

    def test_ci_compiles_pending_finance_catalog_and_marketing(self) -> None:
        src = _read(_REPO / ".github" / "workflows" / "ci.yml")
        assert "--vars '{\"wap_phase\": \"pending\"}'" in src
        assert "path:models/marts/finance" in src
        assert "dim_product" in src
        assert "marts.marketing" in src
        assert "--exclude int_product_catalog dim_product customer_360_view" in src

    def test_catalog_clones_and_publishes_dim_product(self) -> None:
        src = _read(_DAGS / "catalog_bihourly_product_scd2_refresh.py")
        assert "clone_dim_product_task" in src
        assert "publish_dim_product_task" in src
        assert "wap_phase" in src and "pending" in src
        assert src.index("clone_dim_product") < src.index("refresh_dim_product")
        assert "max_active_runs=1" in src

    def test_hourly_ge_stays_on_live(self) -> None:
        src = _read(_DAGS / "quality_hourly_ge_checkpoint.py")
        assert "run_ge_checkpoint.py" in src
        assert "--checkpoint silver_events_daily" in src
        assert "great_expectations checkpoint run" not in src
        assert "--pending-tables" not in src
        assert "--schema-suffix" not in src
        assert "wap_phase" not in src
