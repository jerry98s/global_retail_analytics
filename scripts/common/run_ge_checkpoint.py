"""Great Expectations checkpoint runner with WAP pending-table retargeting (ADR-009).

The GE CLI (``great_expectations checkpoint run``) cannot override a checkpoint's
``runtime_parameters.query`` at run time, and GE 0.18 does not interpolate
environment variables inside query strings. This module loads the checkpoint
YAML, optionally rewrites the Gold table references, and validates each batch
through ``context.get_validator(...)`` — the same explicit path
``scripts/local/run_ge_local.py`` uses against DuckDB, so both environments
exercise one shared implementation instead of undocumented checkpoint internals.

Usage::

    # Audit exactly the tables a DAG is about to publish, in their pending
    # schemas. Validations for any other table are skipped.
    python scripts/common/run_ge_checkpoint.py \\
        --ge-root /tmp/great_expectations \\
        --checkpoint gold_layer_daily \\
        --pending-tables finance.fact_sales,summary.sales_daily_store

    # Monitor live Gold: every validation in the checkpoint, no rewriting.
    python scripts/common/run_ge_checkpoint.py \\
        --ge-root /tmp/great_expectations \\
        --checkpoint gold_layer_daily

``--pending-tables`` takes an explicit ``schema.table`` list rather than a blanket
schema suffix. Each DAG owns a disjoint set of Gold tables, so a blanket suffix
would point the warehouse DAG's audit at ``marketing_pending`` tables it never
built. The list is generated from the same per-DAG constants the publish task
uses (``orchestration/airflow/plugins/wap_publish.py``).

Exit code is 0 when every selected validation passes, 1 otherwise, so the calling
BashOperator fails the task the way the GE CLI would.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger("run_ge_checkpoint")

PENDING_SUFFIX = "_pending"


def parse_table_list(raw: str) -> list[tuple[str, str]]:
    """Parse ``"finance.fact_sales,summary.sales_daily_store"`` into pairs."""
    tables: list[tuple[str, str]] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        if token.count(".") != 1:
            raise ValueError(f"Expected schema.table, got {token!r}")
        schema, table = token.split(".")
        tables.append((schema, table))
    return tables


def retarget_query(query: str, tables: list[tuple[str, str]]) -> str:
    """Rewrite ``schema.table`` -> ``schema_pending.table`` for the given tables.

    Only the exact pairs supplied are rewritten, so reference dims, views, and
    Bronze external tables in the same query are left alone. Idempotent.
    """
    out = query
    for schema, table in tables:
        pattern = rf"\b{re.escape(schema)}\.{re.escape(table)}\b"
        out = re.sub(pattern, f"{schema}{PENDING_SUFFIX}.{table}", out)
    return out


def query_targets(query: str, tables: list[tuple[str, str]]) -> bool:
    """True if the query references any of ``tables`` (live or pending form)."""
    for schema, table in tables:
        live = rf"\b{re.escape(schema)}\.{re.escape(table)}\b"
        pending = rf"\b{re.escape(schema)}{PENDING_SUFFIX}\.{re.escape(table)}\b"
        if re.search(live, query) or re.search(pending, query):
            return True
    return False


def load_validations(checkpoint_path: Path) -> list[dict[str, str]]:
    """Flatten a checkpoint YAML into ``{suite, query, asset, datasource}`` dicts."""
    raw = yaml.safe_load(checkpoint_path.read_text(encoding="utf-8"))
    out: list[dict[str, str]] = []
    for item in raw.get("validations") or []:
        batch_request = item.get("batch_request") or {}
        params = batch_request.get("runtime_parameters") or {}
        query = params.get("query")
        suite = item.get("expectation_suite_name")
        asset = batch_request.get("data_asset_name") or suite
        if not query or not suite:
            raise ValueError(f"Invalid validation entry in {checkpoint_path}: {item}")
        out.append(
            {
                "suite": str(suite),
                "query": " ".join(str(query).split()),
                "asset": str(asset),
                "datasource": str(batch_request.get("datasource_name") or ""),
            }
        )
    return out


def prepare_validations(
    validations: list[dict[str, str]],
    pending_tables: list[tuple[str, str]],
) -> list[dict[str, str]]:
    """Select and retarget the validations for a WAP pending audit.

    With no pending tables this is a no-op (live monitoring mode). Otherwise only
    validations touching the owned tables survive, each rewritten at pending.
    """
    if not pending_tables:
        return validations
    selected: list[dict[str, str]] = []
    for item in validations:
        if not query_targets(item["query"], pending_tables):
            continue
        selected.append({**item, "query": retarget_query(item["query"], pending_tables)})
    return selected


def _print_failures(suite: str, result: Any) -> None:
    print(f"FAILED suite: {suite}", file=sys.stderr)
    for exp in getattr(result, "results", None) or []:
        if getattr(exp, "success", True):
            continue
        cfg = exp.expectation_config
        etype = getattr(cfg, "expectation_type", cfg)
        info = getattr(exp, "exception_info", None) or {}
        if isinstance(info, dict) and info.get("raised_exception"):
            print(f"  - {etype}: EXCEPTION {info.get('exception_message')}", file=sys.stderr)
        else:
            print(f"  - {etype}: {getattr(cfg, 'kwargs', {})}", file=sys.stderr)


def run_checkpoint(
    ge_root: str,
    checkpoint_name: str,
    pending_tables: list[tuple[str, str]],
) -> bool:
    """Validate each selected batch; return True iff all suites passed."""
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    root = Path(ge_root)
    checkpoint_path = root / "checkpoints" / f"{checkpoint_name}.yml"
    if not checkpoint_path.is_file():
        raise FileNotFoundError(f"Checkpoint missing: {checkpoint_path}")

    (root / "uncommitted" / "validations").mkdir(parents=True, exist_ok=True)

    validations = prepare_validations(load_validations(checkpoint_path), pending_tables)
    if not validations:
        raise RuntimeError(
            f"No validations in {checkpoint_name} match the requested tables "
            f"{pending_tables}. Check the --pending-tables list against the checkpoint."
        )

    context = gx.get_context(context_root_dir=str(root))
    all_ok = True
    for item in validations:
        batch_request = RuntimeBatchRequest(
            datasource_name=item["datasource"],
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=item["asset"],
            runtime_parameters={"query": item["query"]},
            batch_identifiers={"default_identifier_name": "wap"},
        )
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=item["suite"],
        )
        result = validator.validate()
        if result.success:
            print(f"PASS suite: {item['suite']}")
        else:
            all_ok = False
            _print_failures(item["suite"], result)
    return all_ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ge-root", required=True, help="great_expectations/ root dir")
    parser.add_argument("--checkpoint", required=True, help="checkpoint name")
    parser.add_argument(
        "--pending-tables",
        default="",
        help=(
            "Comma-separated schema.table list to audit in its *_pending schema. "
            "Empty = validate every batch against live."
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        tables = parse_table_list(args.pending_tables)
        ok = run_checkpoint(args.ge_root, args.checkpoint, tables)
    except Exception as exc:  # noqa: BLE001
        import traceback

        log.error("checkpoint run failed: %s", exc)
        traceback.print_exc()
        return 1
    log.info("checkpoint %s success=%s", args.checkpoint, ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
