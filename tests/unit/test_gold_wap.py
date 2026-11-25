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
WAP_GOLD_SCHEMAS = ("finance", "marketing", "summary", "control")
GOLD_INCREMENTAL_MODELS = [
    "models/marts/finance/fact_sales.sql",
    "models/marts/finance/fact_returns.sql",
    "models/marts/finance/fact_inventory_snapshot.sql",
    "models/marts/marketing/dim_product.sql",
    "models/marts/marketing/fact_customer_session.sql",
    "models/marts/summary/sales_daily_store.sql",
    "models/marts/summary/inventory_daily_product_store.sql",
    "models/marts/summary/inventory_position_daily.sql",
]
GOLD_DIST_SORT = {
    "models/marts/finance/fact_sales.sql": ("product_key", "date_key"),
    "models/marts/finance/fact_returns.sql": ("product_key", "date_key"),
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
        macro = _read(_DBT / "macros" / "product_as_of.sql")
        assert "wap_live_ref('dim_product')" in macro, (
            "dim_product is owned by the catalog DAG; join live."
        )
        for model in (
            "models/marts/finance/fact_sales.sql",
            "models/marts/finance/fact_returns.sql",
            "models/marts/finance/fact_inventory_snapshot.sql",
        ):
            src = _read(_DBT / model)
            assert "{{ product_versions() }}" in src, model
            assert "product_as_of(" in src, model
            assert "is_current" not in src, f"{model}: join dim_product as-of (ADR-016)"

    def test_cross_dag_relationship_tests_use_live_source(self) -> None:
        for rel_path in (
            "tests/fact_sales.yml",
            "tests/fact_returns.yml",
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
        # Hours at or after the cutoff are not in Gold; comparing them is noise.
        assert "is_batch_covered" in test
        assert "ref('int_inventory_cutoff')" in model


class TestInventoryLambda:
    """ADR-012: Spark owns covered hours, Flink only the hours after."""

    def test_fact_reads_combined_hourly_not_flink_directly(self) -> None:
        src = _read(_DBT / "models/marts/finance/fact_inventory_snapshot.sql")
        assert "ref('int_inventory_hourly')" in src
        assert "source('silver', 'inventory_hourly')" not in src

    def test_hourly_is_canonical_below_the_cutoff(self) -> None:
        src = _read(_DBT / "models/intermediate/int_inventory_hourly.sql")
        assert "source('silver', 'inventory_events')" in src
        assert "source('silver', 'inventory_hourly')" not in src
        assert "ref('int_inventory_cutoff')" in src
        compact = " ".join(src.split())
        assert "false as is_estimated" in compact
        assert "e.snapshot_date_key * 100 + e.snapshot_hour < c.closed_hour_exclusive" in compact

    def test_cutoff_is_the_spark_summary_row(self) -> None:
        src = _read(_DBT / "models/intermediate/int_inventory_cutoff.sql")
        assert "source('silver', 'inventory_cutoff')" in src
        assert "closed_hour_exclusive" in src
        assert "coverage_status" in src

    def test_expected_partitions_match_topic_config(self) -> None:
        import yaml

        from ingestion.kafka.topics import PARTITIONS_BY_TOPIC, TOPIC_INVENTORY

        project = yaml.safe_load(_read(_DBT / "dbt_project.yml"))
        assert project["vars"]["inventory_kafka_partitions"] == PARTITIONS_BY_TOPIC[TOPIC_INVENTORY]

    def test_fact_rewrites_changed_past_hours(self) -> None:
        # An append-only "hours after max" filter would never apply a Spark
        # correction to an hour that is already loaded.
        src = _read(_DBT / "models/marts/finance/fact_inventory_snapshot.sql")
        compact = " ".join(src.split())
        assert "max(snapshot_date_key * 100 + snapshot_hour)" not in compact
        assert "e.quantity_on_hand <> f.quantity_on_hand" in compact
        assert "e.is_estimated <> f.is_estimated" in compact
        # The stored movement must stay the one the balance was built from.
        assert "e.qty_delta_hour <> f.qty_delta_hour" in compact
        assert "e.qty_delta_hour is null" in compact
        # delete+insert keys on product_id, so a re-keyed hour replaces its row.
        assert "'snapshot_hour', 'product_id', 'store_key'" in compact
        assert "e.product_key <> p.product_key" in compact
        # Stored hours that lost their movement are re-emitted, not left stale.
        assert "cast(0 as numeric(38, 0)) as qty_delta_hour" in compact

    def test_inventory_summary_rewrites_changed_days(self) -> None:
        src = _read(_DBT / "models/marts/summary/inventory_daily_product_store.sql")
        compact = " ".join(src.split())
        assert "max(snapshot_date_key)" not in compact
        assert "t.ending_on_hand_qty <> d.ending_on_hand_qty" in compact
        # A re-keyed fact day (ADR-016) must not leave its old product_key row.
        assert "delete from {{ this }} where not exists" in compact
        # Stockout duration needs dense hours: only inventory_position_daily has it.
        assert "stockout_hour_count" not in compact

    def test_is_estimated_is_always_false_in_gold(self) -> None:
        yml = _read(_DBT / "tests/fact_sales.yml")
        assert "values: [false]" in yml
        assert "values: [false, true]" not in yml


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
        assert ("summary", "consent_identity_daily") in tables
        assert ("summary", "customer_segments") in tables

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

    def test_publish_adds_new_pending_columns_to_live(self) -> None:
        # on_schema_change='append_new_columns' appends to the pending clone;
        # the publish carries the new column to live in the same transaction.
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "summary.sales_daily_store": [(1, "live")],
            "summary_pending.sales_daily_store": [(1, "new")],
        })
        conn.execute(
            "ALTER TABLE summary_pending.sales_daily_store ADD COLUMN return_amount DECIMAL(18,4)"
        )
        conn.execute("UPDATE summary_pending.sales_daily_store SET return_amount = 12.5")
        wap_publish.publish_gold(conn, [("summary", "sales_daily_store")], dialect="duckdb")
        assert conn.execute(
            "SELECT id, label, return_amount FROM summary.sales_daily_store"
        ).fetchall() == [(1, "new", 12.5)]
        types = wap_publish._column_types(conn, "summary", "sales_daily_store", "duckdb")
        assert types["return_amount"] == "DECIMAL(18,4)"

    def test_publish_aborts_when_pending_lacks_a_live_column(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        conn = self._duckdb_with({
            "finance.fact_sales": [(1, "live")],
            "finance_pending.fact_sales": [(1, "new")],
        })
        conn.execute("ALTER TABLE finance.fact_sales ADD COLUMN retired INTEGER")
        with pytest.raises(RuntimeError, match=r"pending lacks live columns \['retired'\]"):
            wap_publish.publish_gold(conn, [("finance", "fact_sales")], dialect="duckdb")
        assert conn.execute("SELECT label FROM finance.fact_sales").fetchall() == [("live",)]

    def test_redshift_column_types_keep_length_and_precision(self) -> None:
        from orchestration.airflow.plugins import wap_publish

        rows = [
            ("return_status", "character varying", 256, None, None),
            ("return_amount", "numeric", None, 18, 4),
            ("return_transaction_count", "bigint", None, 64, 0),
            ("voided_at", "timestamp without time zone", None, None, None),
        ]

        class _Cur:
            def execute(self, *a: object) -> None:
                pass

            def fetchall(self) -> list[tuple[object, ...]]:
                return rows

            def close(self) -> None:
                pass

        class _Conn:
            def cursor(self) -> _Cur:
                return _Cur()

        assert wap_publish._column_types(_Conn(), "summary", "t", "redshift") == {
            "return_status": "varchar(256)",
            "return_amount": "numeric(18,4)",
            "return_transaction_count": "bigint",
            "voided_at": "timestamp without time zone",
        }
        stmts = wap_publish.build_publish_statements(
            "summary", "t", ["id", "return_amount"], live_exists=True,
            add_columns=[("return_amount", "numeric(18,4)")],
        )
        assert stmts[:2] == [
            "ALTER TABLE summary.t ADD COLUMN return_amount numeric(18,4)",
            "DELETE FROM summary.t",
        ]

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
        # Pending vars come from pos_restatement.pos_dbt_vars (tested there).
        assert src.count("{{ pos_dbt_vars(params) }}") == 2
        assert "wap_publish" in src
        assert '"scope": "finance", "tables": OWNED_TABLES' in src
        assert src.index("row_count_reconcile\n") < src.index(">> wap_publish")
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

    @pytest.mark.parametrize("dag, scope, publish", [
        ("marketing_hourly_customer_360_pipeline.py", "marketing", "wap_publish"),
        ("catalog_daily_product_scd2_refresh.py", "catalog", "publish_dim_product"),
    ])
    def test_owning_dag_gates_its_own_tables_before_publish(self, dag, scope, publish) -> None:
        src = _read(_DAGS / dag)
        chain = src.rsplit("metadata_start\n", 1)[1]
        assert f'"scope": "{scope}", "tables": OWNED_TABLES, "save_baseline": False' in src
        assert chain.index("row_count_reconcile") < chain.index(publish) < chain.index("save_baseline")
        assert '"acknowledge_shrink": Param(' in src

    def test_ci_compiles_pending_finance_catalog_and_marketing(self) -> None:
        src = _read(_REPO / ".github" / "workflows" / "ci.yml")
        assert "--vars '{\"wap_phase\": \"pending\"}'" in src
        assert "path:models/marts/finance" in src
        assert "dim_product" in src
        assert "marts.marketing" in src
        assert "--exclude int_product_catalog dim_product customer_360_view" in src

    def test_catalog_clones_and_publishes_dim_product(self) -> None:
        src = _read(_DAGS / "catalog_daily_product_scd2_refresh.py")
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
