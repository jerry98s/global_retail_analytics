"""Schema-suffix-aware Great Expectations checkpoint runner (ADR-009).

The GE CLI (``great_expectations checkpoint run``) has no flag to override a
checkpoint's ``runtime_parameters.query`` at run time, and the 0.18.x YAML
config does not interpolate environment variables inside query strings. This
wrapper loads the checkpoint, optionally rewrites the Gold schema qualifiers in
every batch query, and runs it in-process.

Usage (called from Airflow bash and the local stack):

    python scripts/common/run_ge_checkpoint.py \\
        --ge-root /tmp/great_expectations \\
        --checkpoint gold_layer_daily \\
        --schema-suffix _pending          # audit pending Gold before publish

    python scripts/common/run_ge_checkpoint.py \\
        --ge-root /tmp/great_expectations \\
        --checkpoint gold_layer_daily     # monitor live Gold (hourly DAG)

``--schema-suffix _pending`` retargets the Gold mart queries
(``finance.fact_sales`` -> ``finance_pending.fact_sales``) so the daily
warehouse DAG audits the pending build before WAP publish. Reference dims
(``finance.dim_date`` / ``finance.dim_store``) and Bronze stream windows are
NOT suffixed — they are not published through WAP, and Bronze is a Spectrum
external schema outside the pending scope.

Exit code is 0 on success, 1 on any failed expectation, so the calling
BashOperator fails the task the same way the GE CLI would.
"""

from __future__ import annotations

import argparse
import logging
import sys

log = logging.getLogger("run_ge_checkpoint")

# Gold schemas whose mart tables are WAP-published. Only these get suffixed.
_GOLD_MART_SCHEMAS = ("finance", "marketing", "summary")

# Tables inside those schemas that are NOT published through WAP (stable seed
# reference dims, views rebuilt after publish). Their queries stay on live.
_NO_SUFFIX_TABLES = ("dim_date", "dim_store", "customer_360_view")


def apply_schema_suffix(query: str, suffix: str) -> str:
    """Rewrite Gold mart schema qualifiers in a batch query.

    ``FROM finance.fact_sales`` -> ``FROM finance_pending.fact_sales`` when
    ``suffix='_pending'``. Reference dims, views, and Bronze are untouched.
    Idempotent: an already-suffixed schema is not double-suffixed.
    """
    if not suffix:
        return query
    out = query
    for schema in _GOLD_MART_SCHEMAS:
        # Rewrite "schema.table" for every mart table in the schema except the
        # excluded reference dims / views. We do a targeted token replacement.
        marker = f"{schema}."
        idx = 0
        while True:
            pos = out.find(marker, idx)
            if pos == -1:
                break
            # Skip if already suffixed (schema_pending.)
            if out[max(0, pos - len(suffix)):pos] == suffix:
                idx = pos + len(marker)
                continue
            # Read the table token following "schema."
            end = pos + len(marker)
            start_table = end
            while end < len(out) and (out[end].isalnum() or out[end] == "_"):
                end += 1
            table = out[start_table:end]
            if table in _NO_SUFFIX_TABLES:
                idx = end
                continue
            out = out[:pos] + f"{schema}{suffix}.{table}" + out[end:]
            idx = pos + len(f"{schema}{suffix}.{table}")
    return out


def run_checkpoint(ge_root: str, checkpoint_name: str, schema_suffix: str) -> bool:
    """Run a GE checkpoint; return True iff all validations succeeded."""
    import great_expectations as ge

    context = ge.get_context(context_root_dir=ge_root)
    checkpoint = context.get_checkpoint(checkpoint_name)

    if schema_suffix:
        # Clone the config and rewrite each batch query so we never mutate the
        # on-disk checkpoint the hourly live monitor relies on.
        config = checkpoint.config
        for validation in config.validations:
            batch_request = validation.get("batch_request") or {}
            runtime = batch_request.get("runtime_parameters") or {}
            query = runtime.get("query")
            if query:
                runtime["query"] = apply_schema_suffix(query, schema_suffix)
        result = context.run_checkpoint(
            checkpoint_name=None,
            ge_cloud_id=None,
            **config.to_json_dict(),
        )
    else:
        result = checkpoint.run()

    return bool(result.success)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ge-root", required=True, help="great_expectations/ root dir")
    parser.add_argument("--checkpoint", required=True, help="checkpoint name")
    parser.add_argument(
        "--schema-suffix",
        default="",
        help="suffix appended to Gold mart schemas (e.g. _pending). Empty = live.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        ok = run_checkpoint(args.ge_root, args.checkpoint, args.schema_suffix)
    except Exception as exc:  # noqa: BLE001
        log.error("checkpoint run failed: %s", exc)
        return 1
    log.info("checkpoint %s success=%s", args.checkpoint, ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
