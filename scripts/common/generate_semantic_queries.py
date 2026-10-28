"""Compile the dashboard's metric queries through MetricFlow (ADR-013).

Reads the dbt semantic manifest (``dbt parse`` writes
``transformation/dbt_project/target/semantic_manifest.json``) and asks the
MetricFlow engine to compile each query in ``DASHBOARD_QUERIES`` to DuckDB and
Redshift SQL. Nothing connects to a warehouse: the SQL client is compile-only.
The result is written to ``dashboard/semantic_queries.py``, which the dashboard
executes as-is, so it needs neither MetricFlow nor a dbt profile at runtime.

Relations are rendered as ``"schema"."table"`` without the database, so the
same SQL runs on local_retail.duckdb and on the dev/prod Redshift databases.

Usage:
    python scripts/common/generate_semantic_queries.py          # rewrite
    python scripts/common/generate_semantic_queries.py --check  # CI drift check
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "transformation" / "dbt_project" / "target" / "semantic_manifest.json"
DEFAULT_OUTPUT = REPO_ROOT / "dashboard" / "semantic_queries.py"

# Every dashboard metric query. Names, group-bys and metric names here are the
# column names the dashboard reads back.
DASHBOARD_QUERIES: dict[str, dict[str, Any]] = {
    "sales_by_day": {
        "metrics": ["net_revenue", "units_sold", "transaction_count"],
        "group_by": ["metric_time__day"],
        "order_by": ["metric_time__day"],
    },
    "sales_by_store": {
        "metrics": ["net_revenue"],
        "group_by": ["store__store_name"],
        "order_by": ["-net_revenue"],
        "limit": 20,
    },
    "inventory_by_day": {
        "metrics": ["ending_on_hand_qty", "ending_available_qty", "stockout_hour_count"],
        "group_by": ["metric_time__day"],
        "order_by": ["metric_time__day"],
    },
    "inventory_current": {
        "metrics": ["ending_on_hand_qty", "ending_available_qty"],
    },
    "inventory_by_store": {
        "metrics": ["ending_on_hand_qty"],
        "group_by": ["store__store_name"],
        "order_by": ["-ending_on_hand_qty"],
        "limit": 20,
    },
    "session_funnel": {
        "metrics": [
            "session_count",
            "product_view_count",
            "add_to_cart_count",
            "converted_sessions",
            "conversion_rate",
        ],
        "group_by": ["platform_day__platform"],
        "order_by": ["-session_count"],
    },
}

DIALECTS = ("duckdb", "redshift")


def _strip_database(node: Any, schemas: set[str]) -> None:
    """Rewrite every node_relation in the manifest JSON to "schema"."alias"."""
    if isinstance(node, dict):
        if {"alias", "schema_name", "relation_name"} <= node.keys():
            schemas.add(node["schema_name"])
            node["database"] = None
            node["relation_name"] = f'"{node["schema_name"]}"."{node["alias"]}"'
        for value in node.values():
            _strip_database(value, schemas)
    elif isinstance(node, list):
        for value in node:
            _strip_database(value, schemas)


class _CompileOnlySqlClient:
    """SqlClient that only supplies the dialect; MetricFlow never executes."""

    def __init__(self, dialect: str) -> None:
        from metricflow.protocols.sql_client import SqlEngine
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
        from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer

        engines = {
            "duckdb": (SqlEngine.DUCKDB, DuckDbSqlPlanRenderer()),
            "redshift": (SqlEngine.REDSHIFT, RedshiftSqlPlanRenderer()),
        }
        self._engine, self._renderer = engines[dialect]

    @property
    def sql_engine_type(self) -> Any:
        return self._engine

    @property
    def sql_plan_renderer(self) -> Any:
        return self._renderer

    def query(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("compile-only SqlClient cannot execute SQL")

    execute = query
    dry_run = query

    def close(self) -> None:
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f"%({bind_parameter_key})s"


def compile_queries(manifest_path: Path) -> dict[str, dict[str, str]]:
    from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
    from metricflow_semantics.model.dbt_manifest_parser import (
        parse_manifest_from_dbt_generated_manifest,
    )
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    schemas: set[str] = set()
    _strip_database(raw, schemas)
    pending = sorted(s for s in schemas if s.endswith("_pending"))
    if pending:
        raise ValueError(
            f"{manifest_path} was parsed with wap_phase=pending ({', '.join(pending)}); "
            "rerun `dbt parse` without --vars so relations point at live schemas."
        )
    manifest = parse_manifest_from_dbt_generated_manifest(manifest_json_string=json.dumps(raw))

    compiled: dict[str, dict[str, str]] = {}
    for dialect in DIALECTS:
        engine = MetricFlowEngine(
            semantic_manifest_lookup=SemanticManifestLookup(manifest),
            sql_client=_CompileOnlySqlClient(dialect),
        )
        compiled[dialect] = {}
        for name, spec in DASHBOARD_QUERIES.items():
            request = MetricFlowQueryRequest.create(
                metric_names=spec["metrics"],
                group_by_names=spec.get("group_by"),
                order_by_names=spec.get("order_by"),
                limit=spec.get("limit"),
            )
            statement = engine.explain(request).sql_statement
            if statement.bind_parameter_set.param_dict:
                raise ValueError(f"{name}: bind parameters are not supported")
            compiled[dialect][name] = statement.without_descriptions.sql.strip()
    return compiled


def render_module(compiled: dict[str, dict[str, str]]) -> str:
    lines = [
        '"""MetricFlow-compiled SQL for the dashboard. Generated file, do not edit.',
        "",
        "Regenerate after changing transformation/dbt_project/models/semantic/*.yml",
        "or DASHBOARD_QUERIES:",
        "    dbt parse  (in transformation/dbt_project)",
        "    python scripts/common/generate_semantic_queries.py",
        "CI runs the generator with --check.",
        '"""',
        "",
        "QUERIES: dict[str, dict[str, str]] = {",
    ]
    for dialect, queries in compiled.items():
        lines.append(f"    {dialect!r}: {{")
        for name, sql in queries.items():
            if '"""' in sql or "\\" in sql:
                raise ValueError(f"{dialect}.{name}: SQL cannot be embedded in a triple-quoted string")
            lines.append(f'        {name!r}: """')
            lines.extend(sql.splitlines())
            lines.append('""",')
        lines.append("    },")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true", help="fail if --output is stale")
    args = parser.parse_args(argv)

    if not args.manifest.is_file():
        print(f"{args.manifest} not found; run `dbt parse` first.", file=sys.stderr)
        return 2

    rendered = render_module(compile_queries(args.manifest))
    if args.check:
        current = args.output.read_text(encoding="utf-8") if args.output.is_file() else ""
        if current != rendered:
            print(
                f"{args.output} is stale; run python scripts/common/generate_semantic_queries.py",
                file=sys.stderr,
            )
            return 1
        print(f"{args.output} is up to date ({len(DASHBOARD_QUERIES)} queries x {len(DIALECTS)} dialects)")
        return 0

    args.output.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
