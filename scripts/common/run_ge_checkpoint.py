"""Great Expectations checkpoint runner for the Silver event checks.

The GE CLI (``great_expectations checkpoint run``) cannot override a checkpoint's
``runtime_parameters.query`` at run time, and GE 0.18 does not interpolate
environment variables inside query strings. This module loads the checkpoint
YAML and validates each batch through ``context.get_validator(...)`` — the same
explicit path ``scripts/local/run_ge_local.py`` uses against DuckDB, so both
environments exercise one shared implementation instead of undocumented
checkpoint internals.

Gold tables are not validated here: each owning DAG gates its WAP publish with
dbt tests (ADR-009).

Usage::

    python scripts/common/run_ge_checkpoint.py \\
        --ge-root /tmp/great_expectations \\
        --checkpoint silver_events_daily

Exit code is 0 when every validation passes, 1 otherwise, so the calling
BashOperator fails the task the way the GE CLI would.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger("run_ge_checkpoint")


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


def run_checkpoint(ge_root: str, checkpoint_name: str) -> bool:
    """Validate each batch in the checkpoint; return True iff all suites passed."""
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    root = Path(ge_root)
    checkpoint_path = root / "checkpoints" / f"{checkpoint_name}.yml"
    if not checkpoint_path.is_file():
        raise FileNotFoundError(f"Checkpoint missing: {checkpoint_path}")

    (root / "uncommitted" / "validations").mkdir(parents=True, exist_ok=True)

    validations = load_validations(checkpoint_path)
    if not validations:
        raise RuntimeError(f"Checkpoint {checkpoint_name} has no validations")

    context = gx.get_context(context_root_dir=str(root))
    all_ok = True
    for item in validations:
        batch_request = RuntimeBatchRequest(
            datasource_name=item["datasource"],
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=item["asset"],
            runtime_parameters={"query": item["query"]},
            batch_identifiers={"default_identifier_name": checkpoint_name},
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
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        ok = run_checkpoint(args.ge_root, args.checkpoint)
    except Exception as exc:  # noqa: BLE001
        import traceback

        log.error("checkpoint run failed: %s", exc)
        traceback.print_exc()
        return 1
    log.info("checkpoint %s success=%s", args.checkpoint, ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
