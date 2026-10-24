"""Static checks for the dbt semantic layer and the dashboard that reads it (ADR-013).

Offline: parses YAML and source files only. The MetricFlow compile itself is
checked in CI by ``generate_semantic_queries.py --check`` after ``dbt compile``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

from scripts.common.generate_semantic_queries import DASHBOARD_QUERIES, DIALECTS

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SEMANTIC = _REPO / "transformation" / "dbt_project" / "models" / "semantic"
_DASHBOARD = _REPO / "dashboard"


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _semantic_models() -> dict[str, dict]:
    return {sm["name"]: sm for sm in _yaml(_SEMANTIC / "semantic_models.yml")["semantic_models"]}


def _metrics() -> dict[str, dict]:
    return {m["name"]: m for m in _yaml(_SEMANTIC / "metrics.yml")["metrics"]}


def _measure_index() -> dict[str, tuple[str, dict]]:
    index = {}
    for sm_name, sm in _semantic_models().items():
        for measure in sm.get("measures", []):
            index[measure["name"]] = (sm_name, measure)
    return index


def _metric_measures(metric: dict) -> list[str]:
    params = metric["type_params"]
    if metric["type"] == "simple":
        measure = params["measure"]
        return [measure if isinstance(measure, str) else measure["name"]]
    if metric["type"] == "ratio":
        return [
            m
            for side in (params["numerator"], params["denominator"])
            for m in _metric_measures(_metrics()[side if isinstance(side, str) else side["name"]])
        ]
    raise AssertionError(f"unhandled metric type {metric['type']}")


def _generated_queries() -> dict[str, dict[str, str]]:
    tree = ast.parse((_DASHBOARD / "semantic_queries.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and node.target.id == "QUERIES":
            return ast.literal_eval(node.value)
    raise AssertionError("QUERIES not found in dashboard/semantic_queries.py")


class TestSemanticModels:
    def test_semantic_models_read_summary_tables_or_dim_store_only(self) -> None:
        allowed = {
            "sales_daily_store",
            "inventory_position_daily",
            "sessions_daily_platform",
            "dim_store",
        }
        for name, sm in _semantic_models().items():
            ref = re.fullmatch(r"ref\('(\w+)'\)", sm["model"])
            assert ref, name
            assert ref.group(1) in allowed, f"{name} must not read {ref.group(1)}"
            assert not ref.group(1).startswith("fact_"), name

    def test_every_semantic_model_with_measures_has_a_time_dimension(self) -> None:
        for name, sm in _semantic_models().items():
            if not sm.get("measures"):
                continue
            agg_time = sm["defaults"]["agg_time_dimension"]
            dims = {d["name"]: d for d in sm["dimensions"]}
            assert dims[agg_time]["type"] == "time", name
            assert dims[agg_time]["type_params"]["time_granularity"] == "day", name

    def test_time_expressions_are_portable(self) -> None:
        # expr is not Jinja-rendered, so it must run on DuckDB and Redshift as-is.
        for name, sm in _semantic_models().items():
            for dim in sm.get("dimensions", []):
                if dim["type"] != "time":
                    continue
                expr = dim["expr"]
                assert "{{" not in expr, name
                for dialect_only in ("strptime", "strftime", "to_date", "to_char", "::"):
                    assert dialect_only not in expr, f"{name}.{dim['name']} uses {dialect_only}"

    def test_on_hand_measures_are_semi_additive(self) -> None:
        inventory = _semantic_models()["inventory_position_daily"]
        measures = {m["name"]: m for m in inventory["measures"]}
        for name in ("on_hand_qty", "available_qty"):
            nad = measures[name]["non_additive_dimension"]
            assert nad["name"] == inventory["defaults"]["agg_time_dimension"]
            assert nad["window_choice"] == "max"
        assert "non_additive_dimension" not in measures["stockout_hours"]

    def test_time_spine_is_declared(self) -> None:
        models = {m["name"]: m for m in _yaml(_SEMANTIC / "semantic_models.yml")["models"]}
        spine = models["metricflow_time_spine"]
        assert spine["time_spine"]["standard_granularity_column"] == "date_day"
        assert (_SEMANTIC / "metricflow_time_spine.sql").is_file()

    def test_metrics_reference_existing_measures(self) -> None:
        measures = _measure_index()
        for name, metric in _metrics().items():
            for measure in _metric_measures(metric):
                assert measure in measures, f"{name} -> unknown measure {measure}"


class TestMetricCatalogAlignment:
    def test_metric_catalog_yaml_holds_only_operational_metrics(self) -> None:
        catalog = _yaml(_REPO / "metadata" / "catalog" / "metric_catalog.yml")["metrics"]
        for entry in catalog:
            assert entry["source_relation"].startswith("meta."), entry["metric_name"]

    def test_catalog_rows_derive_from_semantic_layer(self) -> None:
        from scripts.common import metadata_observer as mo

        layer = mo.load_layer_catalog(_REPO / "metadata" / "catalog")
        rows = {r["metric_name"]: r for r in mo.semantic_metric_rows(_SEMANTIC, layer)}
        assert set(rows) == set(_metrics())
        on_hand = rows["inventory_on_hand"]
        assert on_hand["source_relation"] == "summary.inventory_position_daily"
        assert on_hand["expression"] == "SUM(ending_on_hand_qty)"
        assert on_hand["grain"] == "(snapshot_date_key, product_id, store_id)"
        assert rows["session_conversion_rate"]["expression"] == (
            "CAST(SUM(converted_sessions) AS DOUBLE PRECISION) / NULLIF(SUM(session_count), 0)"
        )


class TestGeneratedDashboardQueries:
    def test_every_query_is_compiled_for_every_dialect(self) -> None:
        queries = _generated_queries()
        assert set(queries) == set(DIALECTS)
        for dialect in DIALECTS:
            assert set(queries[dialect]) == set(DASHBOARD_QUERIES), dialect

    def test_relations_are_live_and_database_free(self) -> None:
        for dialect, queries in _generated_queries().items():
            for name, sql in queries.items():
                assert "_pending" not in sql, f"{dialect}.{name}"
                assert "local_retail" not in sql, f"{dialect}.{name}"
                assert '"summary".' in sql or '"finance".' in sql, f"{dialect}.{name}"

    def test_dashboard_queries_use_known_metrics(self) -> None:
        metrics = _metrics()
        for name, spec in DASHBOARD_QUERIES.items():
            for metric in spec["metrics"]:
                assert metric in metrics, f"{name} -> {metric}"

    def test_on_hand_queries_filter_to_latest_day(self) -> None:
        for dialect, queries in _generated_queries().items():
            for name in ("inventory_current", "inventory_by_store"):
                assert "MAX(" in queries[name], f"{dialect}.{name}"


class TestDashboardReadsSemanticLayer:
    def test_no_hand_written_metric_sql_over_facts(self) -> None:
        src = (_DASHBOARD / "data.py").read_text(encoding="utf-8")
        assert "fact_sales" not in src
        assert "fact_inventory_snapshot" not in src.split("def enrich_inventory_snapshots")[0]
        assert "fact_customer_session" not in src
        assert "from semantic_queries import QUERIES" in src

    def test_gold_tabs_use_semantic_queries(self) -> None:
        app = (_DASHBOARD / "app.py").read_text(encoding="utf-8")
        for name in DASHBOARD_QUERIES:
            assert f'_safe_semantic("{name}")' in app, name

    def test_container_ships_generated_queries(self) -> None:
        dockerfile = (_DASHBOARD / "Dockerfile").read_text(encoding="utf-8")
        assert "semantic_queries.py" in dockerfile
        assert "duckdb" in (_DASHBOARD / "requirements.txt").read_text(encoding="utf-8")

    def test_ci_checks_generated_queries_before_pending_compile(self) -> None:
        ci = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        check = ci.index("generate_semantic_queries.py --check")
        assert ci.index("dbt compile full project") < check
        assert check < ci.index("dbt compile pending finance and catalog")


class TestInventoryPositionDaily:
    def test_position_is_dense_and_carried_forward(self) -> None:
        src = (
            _REPO / "transformation" / "dbt_project" / "models" / "marts" / "summary"
            / "inventory_position_daily.sql"
        ).read_text(encoding="utf-8")
        assert "cross join pairs" in src
        assert "full outer join opening" in src
        assert "last_value(e.day_end_qty ignore nulls)" in src
        assert "ref('inventory_opening_balance')" in src
        assert "wap_live_ref('dim_product')" in src
        assert "unique_key=['snapshot_date_key', 'product_id', 'store_id']" in src
