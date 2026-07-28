"""Shared operational metadata collector (local DuckDB + cloud Redshift).

Writes to a separate metadata database/file — never into Gold schemas.
All write helpers are fail-open: exceptions are logged and swallowed so a
metadata outage cannot flip a successful business pipeline to failed.

Usage (CLI)::

    python scripts/common/metadata_observer.py init-local
    python scripts/common/metadata_observer.py start-run --backend local ...
    python scripts/common/metadata_observer.py finish-run --backend local ...
    python scripts/common/metadata_observer.py parse-dbt --backend local ...
    python scripts/common/metadata_observer.py collect-freshness --backend local ...
    python scripts/common/metadata_observer.py seed-catalog --backend local
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import yaml

log = logging.getLogger("metadata_observer")

COLLECTOR_VERSION = "1.0.0"
_REPO = Path(__file__).resolve().parents[2]
_DEFAULT_CATALOG_DIR = _REPO / "metadata" / "catalog"
_DEFAULT_ANALYTICS_DUCKDB = (
    _REPO / "transformation" / "dbt_project" / "local_retail.duckdb"
)
_DEFAULT_METADATA_DUCKDB = (
    _REPO / "transformation" / "dbt_project" / "local_metadata.duckdb"
)

# Objects profiled for freshness / row counts (schema, table, ts_expr|None, sla_minutes|None)
DEFAULT_FRESHNESS_TARGETS: list[tuple[str, str, str | None, int | None]] = [
    ("bronze", "clickstream_events", "event_time", 60),
    ("bronze", "inventory_events", "event_time", 60),
    ("bronze", "pos_transactions", "transaction_date", 1440),
    ("silver", "inventory_hourly", None, 120),
    ("finance", "fact_sales", None, 1440),
    ("finance", "fact_inventory_snapshot", None, 1440),
    ("finance", "dim_store", None, None),
    ("marketing", "dim_product", None, None),
    ("marketing", "fact_customer_session", None, 120),
    ("marketing", "dim_customer", None, None),
    ("summary", "sales_daily_store", None, 1440),
    ("summary", "inventory_daily_product_store", None, 1440),
    ("summary", "sessions_daily_platform", None, 120),
]

_DUCKDB_DDL = """
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.layer_catalog (
    object_fqn VARCHAR NOT NULL PRIMARY KEY,
    object_type VARCHAR NOT NULL,
    platform_layer VARCHAR NOT NULL,
    domain VARCHAR,
    owner VARCHAR,
    grain VARCHAR,
    timestamp_column VARCHAR,
    freshness_sla_minutes INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    collector_version VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.metric_catalog (
    metric_name VARCHAR NOT NULL PRIMARY KEY,
    description VARCHAR,
    source_relation VARCHAR NOT NULL,
    expression VARCHAR NOT NULL,
    grain VARCHAR,
    unit VARCHAR,
    owner VARCHAR,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    collector_version VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.pipeline_run (
    execution_id VARCHAR NOT NULL PRIMARY KEY,
    orchestrator_run_id VARCHAR,
    pipeline_name VARCHAR NOT NULL,
    environment VARCHAR NOT NULL,
    trigger_type VARCHAR,
    logical_date TIMESTAMP,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status VARCHAR NOT NULL,
    duration_seconds INTEGER,
    error_text VARCHAR,
    collector_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta.table_freshness (
    execution_id VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    row_count BIGINT,
    max_event_ts TIMESTAMP,
    lag_minutes INTEGER,
    sla_status VARCHAR,
    measured_at TIMESTAMP NOT NULL,
    collector_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (execution_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS meta.dq_check_result (
    execution_id VARCHAR NOT NULL,
    check_system VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    target_object VARCHAR,
    status VARCHAR NOT NULL,
    failed_count INTEGER,
    duration_seconds DOUBLE,
    detail_json VARCHAR,
    measured_at TIMESTAMP NOT NULL,
    collector_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (execution_id, check_system, check_name)
);
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_execution_id() -> str:
    return str(uuid.uuid4())


def fail_open(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run ``fn``; log and return None on any exception."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata write failed (fail-open): %s", exc, exc_info=True)
        return None


def _clip(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@dataclass(frozen=True)
class DqCheckResult:
    check_system: str
    check_name: str
    target_object: str | None
    status: str
    failed_count: int | None = None
    duration_seconds: float | None = None
    detail: dict[str, Any] | None = None


class MetadataWriter:
    """Minimal write API shared by DuckDB and Redshift adapters."""

    def ensure_schema(self) -> None:
        raise NotImplementedError

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        raise NotImplementedError

    def fetchone(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class DuckDBMetadataWriter(MetadataWriter):
    def __init__(self, path: Path) -> None:
        import duckdb

        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._con = duckdb.connect(str(path))

    def ensure_schema(self) -> None:
        self._con.execute(_DUCKDB_DDL)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        if params is None:
            self._con.execute(sql)
        else:
            self._con.execute(sql, list(params))

    def fetchone(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        if params is None:
            return self._con.execute(sql).fetchone()
        return self._con.execute(sql, list(params)).fetchone()

    def close(self) -> None:
        self._con.close()


def _qmark_to_pyformat(sql: str) -> str:
    """Convert ``?`` placeholders to ``%s`` for redshift_connector / psycopg."""
    return sql.replace("%", "%%").replace("?", "%s")


class RedshiftMetadataWriter(MetadataWriter):
    def __init__(
        self,
        *,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5439,
    ) -> None:
        import redshift_connector

        self._conn = redshift_connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        self._conn.autocommit = True

    def ensure_schema(self) -> None:
        # Cloud schema is created by transformation/redshift/metadata DDL.
        # Only ensure meta schema exists for resilience.
        with self._conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS meta")

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self._conn.cursor() as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(_qmark_to_pyformat(sql), tuple(params))

    def fetchone(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        with self._conn.cursor() as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(_qmark_to_pyformat(sql), tuple(params))
            return cur.fetchone()

    def close(self) -> None:
        self._conn.close()


def open_writer(backend: str, args: argparse.Namespace) -> MetadataWriter:
    if backend == "local":
        path = Path(args.metadata_duckdb)
        writer = DuckDBMetadataWriter(path)
        writer.ensure_schema()
        return writer
    if backend == "redshift":
        writer = RedshiftMetadataWriter(
            host=args.rs_host or os.environ["RS_HOST"],
            database=args.rs_metadata_database
            or os.environ.get("RS_METADATA_DATABASE", "metadata"),
            user=args.rs_user or os.environ["RS_USER"],
            password=args.rs_password or os.environ["RS_PASSWORD"],
            port=int(args.rs_port or os.environ.get("RS_PORT", "5439")),
        )
        writer.ensure_schema()
        return writer
    raise ValueError(f"Unknown backend: {backend}")


def open_analytics_reader(backend: str, args: argparse.Namespace) -> Any:
    """Return a connection with .execute/.fetchone-style usage for profiling."""
    if backend == "local":
        import duckdb

        path = Path(args.analytics_duckdb)
        if not path.is_file():
            raise FileNotFoundError(f"Analytics DuckDB not found: {path}")
        return duckdb.connect(str(path), read_only=True)

    import redshift_connector

    return redshift_connector.connect(
        host=args.rs_host or os.environ["RS_HOST"],
        port=int(args.rs_port or os.environ.get("RS_PORT", "5439")),
        database=args.rs_database or os.environ.get("RS_DATABASE", "dev"),
        user=args.rs_user or os.environ["RS_USER"],
        password=args.rs_password or os.environ["RS_PASSWORD"],
    )


def seed_catalogs(
    writer: MetadataWriter,
    catalog_dir: Path = _DEFAULT_CATALOG_DIR,
) -> None:
    now = _iso(utc_now())
    layer_path = catalog_dir / "layer_catalog.yml"
    metric_path = catalog_dir / "metric_catalog.yml"

    if layer_path.is_file():
        data = yaml.safe_load(layer_path.read_text(encoding="utf-8")) or {}
        for obj in data.get("objects") or []:
            fqn = obj["object_fqn"]
            writer.execute(
                "DELETE FROM meta.layer_catalog WHERE object_fqn = ?", (fqn,)
            )
            writer.execute(
                """
                INSERT INTO meta.layer_catalog (
                    object_fqn, object_type, platform_layer, domain, owner, grain,
                    timestamp_column, freshness_sla_minutes, is_active,
                    created_at, updated_at, collector_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fqn,
                    obj["object_type"],
                    obj["platform_layer"],
                    obj.get("domain"),
                    obj.get("owner"),
                    obj.get("grain"),
                    obj.get("timestamp_column"),
                    obj.get("freshness_sla_minutes"),
                    bool(obj.get("is_active", True)),
                    now,
                    now,
                    COLLECTOR_VERSION,
                ),
            )

    if metric_path.is_file():
        data = yaml.safe_load(metric_path.read_text(encoding="utf-8")) or {}
        for met in data.get("metrics") or []:
            name = met["metric_name"]
            writer.execute(
                "DELETE FROM meta.metric_catalog WHERE metric_name = ?", (name,)
            )
            writer.execute(
                """
                INSERT INTO meta.metric_catalog (
                    metric_name, description, source_relation, expression, grain,
                    unit, owner, is_active, created_at, updated_at, collector_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    met.get("description"),
                    met["source_relation"],
                    met["expression"],
                    met.get("grain"),
                    met.get("unit"),
                    met.get("owner"),
                    bool(met.get("is_active", True)),
                    now,
                    now,
                    COLLECTOR_VERSION,
                ),
            )


def start_pipeline_run(
    writer: MetadataWriter,
    *,
    execution_id: str,
    pipeline_name: str,
    environment: str,
    trigger_type: str | None = None,
    orchestrator_run_id: str | None = None,
    logical_date: datetime | None = None,
) -> None:
    now = utc_now()
    writer.execute(
        "DELETE FROM meta.pipeline_run WHERE execution_id = ?", (execution_id,)
    )
    writer.execute(
        """
        INSERT INTO meta.pipeline_run (
            execution_id, orchestrator_run_id, pipeline_name, environment,
            trigger_type, logical_date, started_at, ended_at, status,
            duration_seconds, error_text, collector_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'RUNNING', NULL, NULL, ?, ?, ?)
        """,
        (
            execution_id,
            orchestrator_run_id,
            pipeline_name,
            environment,
            trigger_type,
            _iso(logical_date),
            _iso(now),
            COLLECTOR_VERSION,
            _iso(now),
            _iso(now),
        ),
    )


def finish_pipeline_run(
    writer: MetadataWriter,
    *,
    execution_id: str,
    status: str,
    error_text: str | None = None,
) -> None:
    now = utc_now()
    row = writer.fetchone(
        "SELECT started_at FROM meta.pipeline_run WHERE execution_id = ?",
        (execution_id,),
    )
    duration = None
    if row and row[0] is not None:
        started = row[0]
        if isinstance(started, str):
            started = datetime.fromisoformat(started.replace("Z", ""))
        if isinstance(started, datetime):
            duration = int((now - started.replace(tzinfo=None)).total_seconds())

    writer.execute(
        """
        UPDATE meta.pipeline_run
        SET ended_at = ?,
            status = ?,
            duration_seconds = ?,
            error_text = ?,
            updated_at = ?,
            collector_version = ?
        WHERE execution_id = ?
        """,
        (
            _iso(now),
            status,
            duration,
            _clip(error_text, 4000),
            _iso(now),
            COLLECTOR_VERSION,
            execution_id,
        ),
    )


def parse_dbt_run_results(path: Path) -> list[DqCheckResult]:
    """Parse dbt ``target/run_results.json`` into DQ rows (success and failure)."""
    if not path.is_file():
        raise FileNotFoundError(f"dbt run_results not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    results: list[DqCheckResult] = []
    for item in payload.get("results") or []:
        unique_id = str(item.get("unique_id") or item.get("name") or "unknown")
        status_raw = str(item.get("status") or "error").lower()
        if status_raw in {"pass", "success"}:
            status = "pass"
        elif status_raw in {"warn", "warning"}:
            status = "warn"
        elif status_raw in {"skipped", "skip"}:
            status = "skipped"
        else:
            status = "fail" if status_raw in {"fail", "error"} else status_raw

        timing = item.get("execution_time")
        failures = item.get("failures")
        if failures is None:
            failures = item.get("failed_rows")
        relation = None
        node = item.get("relation_name") or item.get("name")
        if node:
            relation = str(node)

        detail = {
            "unique_id": unique_id,
            "message": _clip(str(item.get("message") or ""), 500),
            "adapter_response": item.get("adapter_response"),
        }
        results.append(
            DqCheckResult(
                check_system="dbt",
                check_name=_clip(unique_id, 512) or unique_id,
                target_object=relation,
                status=status,
                failed_count=int(failures) if failures is not None else None,
                duration_seconds=float(timing) if timing is not None else None,
                detail=detail,
            )
        )
    return results


def record_dq_results(
    writer: MetadataWriter,
    *,
    execution_id: str,
    results: Iterable[DqCheckResult],
    measured_at: datetime | None = None,
) -> int:
    measured = _iso(measured_at or utc_now())
    count = 0
    for r in results:
        check_name = _clip(r.check_name, 512) or r.check_name
        writer.execute(
            """
            DELETE FROM meta.dq_check_result
            WHERE execution_id = ? AND check_system = ? AND check_name = ?
            """,
            (execution_id, r.check_system, check_name),
        )
        writer.execute(
            """
            INSERT INTO meta.dq_check_result (
                execution_id, check_system, check_name, target_object, status,
                failed_count, duration_seconds, detail_json, measured_at,
                collector_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                execution_id,
                r.check_system,
                check_name,
                _clip(r.target_object, 256),
                r.status,
                r.failed_count,
                r.duration_seconds,
                _clip(json.dumps(r.detail or {}, default=str), 8000),
                measured,
                COLLECTOR_VERSION,
                measured,
            ),
        )
        count += 1
    return count


def record_ge_suite_result(
    writer: MetadataWriter,
    *,
    execution_id: str,
    suite_name: str,
    success: bool,
    target_object: str | None = None,
    failed_count: int | None = None,
    duration_seconds: float | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    record_dq_results(
        writer,
        execution_id=execution_id,
        results=[
            DqCheckResult(
                check_system="ge",
                check_name=suite_name,
                target_object=target_object or suite_name,
                status="pass" if success else "fail",
                failed_count=failed_count,
                duration_seconds=duration_seconds,
                detail=detail,
            )
        ],
    )


def _reader_execute(conn: Any, sql: str) -> Any:
    """DuckDB connection vs redshift_connector cursor compatibility."""
    if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
        return conn.execute(sql).fetchone()
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()


def collect_table_freshness(
    writer: MetadataWriter,
    analytics_conn: Any,
    *,
    execution_id: str,
    targets: Sequence[tuple[str, str, str | None, int | None]] | None = None,
    measured_at: datetime | None = None,
) -> int:
    measured = measured_at or utc_now()
    measured_s = _iso(measured)
    written = 0
    for schema, table, ts_col, sla in targets or DEFAULT_FRESHNESS_TARGETS:
        row_count: int | None = None
        max_ts: datetime | None = None
        lag: int | None = None
        sla_status = "unknown"
        try:
            row = _reader_execute(
                analytics_conn, f"SELECT COUNT(*) FROM {schema}.{table}"
            )
            row_count = int(row[0]) if row and row[0] is not None else 0
            if ts_col:
                row_ts = _reader_execute(
                    analytics_conn,
                    f"SELECT MAX({ts_col}) FROM {schema}.{table}",
                )
                if row_ts and row_ts[0] is not None:
                    max_ts = row_ts[0]
                    if isinstance(max_ts, str):
                        max_ts = datetime.fromisoformat(max_ts.replace("Z", ""))
                    if isinstance(max_ts, datetime):
                        lag = int(
                            (measured - max_ts.replace(tzinfo=None)).total_seconds()
                            / 60
                        )
            if sla is None:
                sla_status = "ok" if row_count is not None else "unknown"
            elif lag is None:
                sla_status = "unknown"
            elif lag <= sla:
                sla_status = "ok"
            elif lag <= sla * 2:
                sla_status = "warn"
            else:
                sla_status = "breach"
        except Exception as exc:  # noqa: BLE001
            log.warning("freshness probe failed for %s.%s: %s", schema, table, exc)
            sla_status = "unknown"

        writer.execute(
            """
            DELETE FROM meta.table_freshness
            WHERE execution_id = ? AND schema_name = ? AND table_name = ?
            """,
            (execution_id, schema, table),
        )
        writer.execute(
            """
            INSERT INTO meta.table_freshness (
                execution_id, schema_name, table_name, row_count, max_event_ts,
                lag_minutes, sla_status, measured_at, collector_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                execution_id,
                schema,
                table,
                row_count,
                _iso(max_ts) if isinstance(max_ts, datetime) else max_ts,
                lag,
                sla_status,
                measured_s,
                COLLECTOR_VERSION,
                measured_s,
            ),
        )
        written += 1
    return written


def record_reconciliation_results(
    writer: MetadataWriter,
    *,
    execution_id: str,
    deltas: dict[str, dict[str, Any]],
    warned: bool,
) -> int:
    results = [
        DqCheckResult(
            check_system="reconciliation",
            check_name=f"row_count:{key}",
            target_object=key,
            status="warn" if item.get("status") == "WARN" else "pass",
            failed_count=1 if item.get("status") == "WARN" else 0,
            detail=item,
        )
        for key, item in deltas.items()
    ]
    results.append(
        DqCheckResult(
            check_system="reconciliation",
            check_name="row_count_reconciliation_summary",
            target_object="gold",
            status="warn" if warned else "pass",
            failed_count=1 if warned else 0,
            detail={"warned": warned, "mart_count": len(deltas)},
        )
    )
    return record_dq_results(writer, execution_id=execution_id, results=results)


def _with_retries(fn: Callable[[], Any], attempts: int = 3, delay: float = 0.5) -> Any:
    last: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last = exc
            if i + 1 < attempts:
                time.sleep(delay * (i + 1))
    assert last is not None
    raise last


def _add_backend_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--backend",
        choices=("local", "redshift"),
        default="local",
    )
    p.add_argument("--metadata-duckdb", type=Path, default=_DEFAULT_METADATA_DUCKDB)
    p.add_argument("--analytics-duckdb", type=Path, default=_DEFAULT_ANALYTICS_DUCKDB)
    p.add_argument("--catalog-dir", type=Path, default=_DEFAULT_CATALOG_DIR)
    p.add_argument("--rs-host", default=None)
    p.add_argument("--rs-port", default=None)
    p.add_argument("--rs-user", default=None)
    p.add_argument("--rs-password", default=None)
    p.add_argument("--rs-database", default=None)
    p.add_argument("--rs-metadata-database", default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-local", help="Create local metadata DuckDB schema")
    _add_backend_args(p_init)

    p_seed = sub.add_parser("seed-catalog", help="Upsert YAML catalogs")
    _add_backend_args(p_seed)

    p_start = sub.add_parser("start-run", help="Insert RUNNING pipeline_run row")
    _add_backend_args(p_start)
    p_start.add_argument("--execution-id", required=True)
    p_start.add_argument("--pipeline", required=True)
    p_start.add_argument("--environment", default="local")
    p_start.add_argument("--trigger", default="manual")
    p_start.add_argument("--orchestrator-run-id", default=None)

    p_finish = sub.add_parser("finish-run", help="Finalize pipeline_run status")
    _add_backend_args(p_finish)
    p_finish.add_argument("--execution-id", required=True)
    p_finish.add_argument(
        "--status",
        required=True,
        choices=("SUCCESS", "FAILED", "SKIPPED"),
    )
    p_finish.add_argument("--error", default=None)

    p_dbt = sub.add_parser("parse-dbt", help="Parse dbt run_results.json into DQ rows")
    _add_backend_args(p_dbt)
    p_dbt.add_argument("--execution-id", required=True)
    p_dbt.add_argument(
        "--run-results",
        type=Path,
        default=_REPO / "transformation" / "dbt_project" / "target" / "run_results.json",
    )

    p_fresh = sub.add_parser("collect-freshness", help="Profile analytics tables")
    _add_backend_args(p_fresh)
    p_fresh.add_argument("--execution-id", required=True)

    p_ge = sub.add_parser("record-ge", help="Record one GE suite result")
    _add_backend_args(p_ge)
    p_ge.add_argument("--execution-id", required=True)
    p_ge.add_argument("--suite", required=True)
    p_ge.add_argument("--success", action="store_true")
    p_ge.add_argument("--failed-count", type=int, default=None)
    p_ge.add_argument("--target-object", default=None)

    p_new = sub.add_parser("new-id", help="Print a new execution UUID")
    _add_backend_args(p_new)

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = build_parser().parse_args(argv)
    if args.command == "new-id":
        print(new_execution_id())
        return 0

    def _run() -> int:
        writer = open_writer(args.backend, args)
        try:
            if args.command == "init-local":
                writer.ensure_schema()
                seed_catalogs(writer, args.catalog_dir)
                print(f"Initialized metadata store: {args.metadata_duckdb}")
                return 0
            if args.command == "seed-catalog":
                seed_catalogs(writer, args.catalog_dir)
                print("Catalog seed complete")
                return 0
            if args.command == "start-run":
                start_pipeline_run(
                    writer,
                    execution_id=args.execution_id,
                    pipeline_name=args.pipeline,
                    environment=args.environment,
                    trigger_type=args.trigger,
                    orchestrator_run_id=args.orchestrator_run_id,
                )
                print(f"Started pipeline_run {args.execution_id}")
                return 0
            if args.command == "finish-run":
                finish_pipeline_run(
                    writer,
                    execution_id=args.execution_id,
                    status=args.status,
                    error_text=args.error,
                )
                print(f"Finished pipeline_run {args.execution_id} status={args.status}")
                return 0
            if args.command == "parse-dbt":
                results = parse_dbt_run_results(args.run_results)
                n = record_dq_results(
                    writer, execution_id=args.execution_id, results=results
                )
                print(f"Recorded {n} dbt DQ rows from {args.run_results}")
                return 0
            if args.command == "collect-freshness":
                analytics = open_analytics_reader(args.backend, args)
                try:
                    n = collect_table_freshness(
                        writer,
                        analytics,
                        execution_id=args.execution_id,
                    )
                finally:
                    analytics.close()
                print(f"Recorded {n} freshness rows")
                return 0
            if args.command == "record-ge":
                record_ge_suite_result(
                    writer,
                    execution_id=args.execution_id,
                    suite_name=args.suite,
                    success=bool(args.success),
                    failed_count=args.failed_count,
                    target_object=args.target_object,
                )
                print(f"Recorded GE suite {args.suite}")
                return 0
            raise SystemExit(f"Unhandled command {args.command}")
        finally:
            writer.close()

    # CLI always fail-open for write commands except new-id (already returned).
    try:
        return _with_retries(_run)
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata CLI failed (fail-open exit 0): %s", exc, exc_info=True)
        print(f"WARNING: metadata observer failed (ignored): {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
