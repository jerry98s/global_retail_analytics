"""Run the local Great Expectations gold checkpoint against DuckDB.

Cloud uses ``gold_layer_daily`` (Redshift + SqlAlchemy). Local uses the same
expectation suites with DuckDB-portable SQL from ``gold_layer_local.yml``, but
loads each query into a Pandas batch — GE 0.18's SqlAlchemy path against
duckdb-engine raises ``MetricResolutionError: list index out of range``.

Usage::

    python scripts/local/run_ge_local.py
    python scripts/local/run_ge_local.py --duckdb path/to/local_retail.duckdb
    python scripts/local/run_ge_local.py --execution-id <uuid>
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[2]
_GE_ROOT = _REPO / "quality" / "great_expectations"
_DEFAULT_DUCKDB = _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
_DEFAULT_METADATA = (
    _REPO / "transformation" / "dbt_project" / "local_metadata.duckdb"
)
_CHECKPOINT = _GE_ROOT / "checkpoints" / "gold_layer_local.yml"

# Allow importing scripts.common.metadata_observer
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _load_validations(checkpoint_path: Path) -> list[dict[str, str]]:
    raw = yaml.safe_load(checkpoint_path.read_text(encoding="utf-8"))
    out: list[dict[str, str]] = []
    for item in raw.get("validations") or []:
        br = item.get("batch_request") or {}
        params = br.get("runtime_parameters") or {}
        query = params.get("query")
        suite = item.get("expectation_suite_name")
        asset = br.get("data_asset_name") or suite
        if not query or not suite:
            raise ValueError(f"Invalid validation entry in {checkpoint_path}: {item}")
        out.append(
            {
                "suite": str(suite),
                "query": " ".join(str(query).split()),
                "asset": str(asset),
            }
        )
    return out


def _print_failures(suite: str, validation_result: Any) -> None:
    print(f"FAILED suite: {suite}", file=sys.stderr)
    results = getattr(validation_result, "results", None) or []
    for exp in results:
        if getattr(exp, "success", True):
            continue
        cfg = exp.expectation_config
        etype = getattr(cfg, "expectation_type", cfg)
        kwargs = getattr(cfg, "kwargs", {})
        ei = getattr(exp, "exception_info", None) or {}
        if isinstance(ei, dict) and ei.get("raised_exception"):
            print(f"  - {etype}: EXCEPTION {ei.get('exception_message')}", file=sys.stderr)
        else:
            print(f"  - {etype}: {kwargs}", file=sys.stderr)


def _record_ge(
    *,
    execution_id: str | None,
    metadata_duckdb: Path,
    suite: str,
    success: bool,
    failed_count: int | None,
    duration_seconds: float,
    target_object: str,
) -> None:
    if not execution_id:
        return
    try:
        from scripts.common.metadata_observer import (
            DuckDBMetadataWriter,
            record_ge_suite_result,
        )

        writer = DuckDBMetadataWriter(metadata_duckdb)
        try:
            writer.ensure_schema()
            record_ge_suite_result(
                writer,
                execution_id=execution_id,
                suite_name=suite,
                success=success,
                failed_count=failed_count,
                duration_seconds=duration_seconds,
                target_object=target_object,
                detail={"suite": suite, "success": success},
            )
        finally:
            writer.close()
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: metadata GE record failed (ignored): {exc}", file=sys.stderr)


def run_checkpoint(
    duckdb_path: Path,
    *,
    execution_id: str | None = None,
    metadata_duckdb: Path = _DEFAULT_METADATA,
    schema_suffix: str = "",
) -> bool:
    if not duckdb_path.is_file():
        raise FileNotFoundError(
            f"DuckDB not found: {duckdb_path}. Run "
            "`run_local_stack.ps1 -Task dbt` first."
        )
    if not _CHECKPOINT.is_file():
        raise FileNotFoundError(f"Checkpoint missing: {_CHECKPOINT}")

    # ADR-009: reuse the shared Gold-schema suffixer so local GE audits the
    # pending build (finance_pending.*) the same way the cloud wrapper does.
    from scripts.common.run_ge_checkpoint import apply_schema_suffix

    # Satisfy ${...} expansion for unused SqlAlchemy datasources at context load.
    placeholder = f"duckdb:///{duckdb_path.resolve().as_posix()}"
    os.environ.setdefault("DUCKDB_SQLALCHEMY_URL", placeholder)
    os.environ.setdefault("RS_SQLALCHEMY_URL", placeholder)
    # Keep GE/tqdm progress off stderr (breaks PowerShell Stop-on-NativeError).
    os.environ.setdefault("TQDM_DISABLE", "1")

    (_GE_ROOT / "uncommitted" / "validations").mkdir(parents=True, exist_ok=True)

    import duckdb
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    validations = _load_validations(_CHECKPOINT)
    if schema_suffix:
        for item in validations:
            item["query"] = " ".join(
                apply_schema_suffix(item["query"], schema_suffix).split()
            )
    context = gx.get_context(context_root_dir=str(_GE_ROOT))
    con = duckdb.connect(str(duckdb_path), read_only=True)
    all_ok = True
    try:
        for item in validations:
            suite = item["suite"]
            query = item["query"]
            asset = item["asset"]
            started = time.perf_counter()
            df = con.execute(query).fetchdf()
            batch_request = RuntimeBatchRequest(
                datasource_name="pandas_local",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=asset,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": "local"},
            )
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite,
            )
            result = validator.validate()
            ok = bool(result.success)
            duration = time.perf_counter() - started
            failed = 0
            for exp in getattr(result, "results", None) or []:
                if not getattr(exp, "success", True):
                    failed += 1
            _record_ge(
                execution_id=execution_id,
                metadata_duckdb=metadata_duckdb,
                suite=suite,
                success=ok,
                failed_count=failed,
                duration_seconds=duration,
                target_object=asset,
            )
            if ok:
                print(f"PASS suite: {suite} (rows={len(df)})")
            else:
                all_ok = False
                _print_failures(suite, result)
    finally:
        con.close()
    return all_ok


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--duckdb",
        type=Path,
        default=_DEFAULT_DUCKDB,
        help="Path to local_retail.duckdb (after dbt run)",
    )
    p.add_argument(
        "--execution-id",
        default=None,
        help="Optional metadata execution_id for DQ result capture",
    )
    p.add_argument(
        "--metadata-duckdb",
        type=Path,
        default=_DEFAULT_METADATA,
        help="Path to local_metadata.duckdb",
    )
    p.add_argument(
        "--schema-suffix",
        default="",
        help="Suffix appended to Gold mart schemas (e.g. _pending) for the WAP audit. Empty = live.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        ok = run_checkpoint(
            args.duckdb,
            execution_id=args.execution_id,
            metadata_duckdb=args.metadata_duckdb,
            schema_suffix=args.schema_suffix,
        )
    except Exception as exc:  # noqa: BLE001
        import traceback

        print(f"GE local checkpoint failed to run: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1
    if ok:
        print("GE gold_layer_local: SUCCESS")
        return 0
    print("GE gold_layer_local: FAILED", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
