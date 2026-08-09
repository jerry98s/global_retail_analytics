"""Write-Audit-Publish publish helper for Gold marts (ADR-009).

dbt builds Gold marts into ``*_pending`` schemas (``wap_phase='pending'``),
dbt tests + Great Expectations audit them there, and only then does this
helper promote them into the live ``finance`` / ``marketing`` / ``summary``
schemas. A failing audit never touches live, so consumers keep reading the
last good publish.

Publish strategy per table (Redshift):

    BEGIN;
    DROP TABLE IF EXISTS live.table__wap_old;
    ALTER TABLE live.table RENAME TO table__wap_old;      -- only if live exists
    ALTER TABLE live_pending.table SET SCHEMA live;       -- lands as live.table
    COMMIT;
    DROP TABLE IF EXISTS live.table__wap_old;

DuckDB has no ``SET SCHEMA`` and ``ALTER TABLE ... RENAME TO`` cannot move
across schemas, so the local path uses
``CREATE OR REPLACE TABLE live.table AS SELECT * FROM live_pending.table``
plus a drop of the pending copy, all inside one transaction. It is a copy
rather than a pointer swap; acceptable for the local DuckDB simulation.

The canonical WAP table list lives here (``WAP_TABLES``) so cloud Airflow and
the local stack share one definition. Reference dims ``finance.dim_date`` /
``finance.dim_store`` are deliberately excluded — they are stable seed data,
not dbt-built marts. ``customer_360_view`` / ``serving.*`` are views over live
Gold and are refreshed by the marketing DAG after publish, not renamed.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# (live schema, table) — every entry is published from "<schema>_pending".
WAP_TABLES: list[tuple[str, str]] = [
    ("finance", "fact_sales"),
    ("finance", "fact_inventory_snapshot"),
    ("marketing", "dim_customer"),
    ("marketing", "dim_product"),
    ("marketing", "fact_customer_session"),
    ("marketing", "identity_graph"),
    ("summary", "sales_daily_store"),
    ("summary", "inventory_daily_product_store"),
    ("summary", "sessions_daily_platform"),
]

# Subsets so each DAG publishes only what it built.
FINANCE_SUMMARY_TABLES: list[tuple[str, str]] = [
    ("finance", "fact_sales"),
    ("finance", "fact_inventory_snapshot"),
    ("marketing", "dim_product"),  # SCD2 built by warehouse DAG
    ("summary", "sales_daily_store"),
    ("summary", "inventory_daily_product_store"),
]

MARKETING_TABLES: list[tuple[str, str]] = [
    ("marketing", "dim_customer"),
    ("marketing", "fact_customer_session"),
    ("marketing", "identity_graph"),
    ("summary", "sessions_daily_platform"),
]

DIM_PRODUCT_TABLES: list[tuple[str, str]] = [
    ("marketing", "dim_product"),
]


def _pending_schema(schema: str) -> str:
    return f"{schema}_pending"


def build_redshift_publish_statements(tables: list[tuple[str, str]]) -> list[str]:
    """SQL statements promoting pending tables into live schemas (Redshift).

    Each table is swapped in its own transaction so one failure cannot leave a
    half-published set; the old live copy is kept as ``table__wap_old`` until
    the swap commits, then dropped.
    """
    statements: list[str] = []
    for schema, table in tables:
        pending = _pending_schema(schema)
        live_fqn = f"{schema}.{table}"
        old_fqn = f"{schema}.{table}__wap_old"
        pending_fqn = f"{pending}.{table}"
        statements.extend(
            [
                "BEGIN",
                f"DROP TABLE IF EXISTS {old_fqn}",
                f"ALTER TABLE {live_fqn} RENAME TO {table}__wap_old",
                f"ALTER TABLE {pending_fqn} SET SCHEMA {schema}",
                "COMMIT",
                f"DROP TABLE IF EXISTS {old_fqn}",
            ]
        )
    return statements


def _table_exists(conn: Any, schema: str, table: str, dialect: str) -> bool:
    if dialect == "duckdb":
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
    else:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        row = cur.fetchone()
        cur.close()
    return bool(row and int(row[0]) > 0)


def _exec(conn: Any, sql: str, dialect: str) -> None:
    if dialect == "duckdb":
        conn.execute(sql)
    else:
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()


def publish_gold(
    conn: Any,
    tables: list[tuple[str, str]],
    *,
    dialect: str = "redshift",
) -> dict[str, Any]:
    """Promote pending Gold tables into live schemas.

    ``conn`` is a ``redshift_connector`` connection (cloud) or a ``duckdb``
    connection (local). ``dialect`` selects the swap mechanics.

    Raises if a pending table is missing (nothing to audit/publish). A missing
    live table is fine — that is the first-ever publish and the rename of live
    is simply skipped.
    """
    published: list[str] = []
    for schema, table in tables:
        pending = _pending_schema(schema)
        if not _table_exists(conn, pending, table, dialect):
            raise RuntimeError(
                f"WAP publish aborted: pending table {pending}.{table} does not exist. "
                "Run dbt with wap_phase='pending' and pass audits before publishing."
            )

        live_exists = _table_exists(conn, schema, table, dialect)
        live_fqn = f"{schema}.{table}"
        old_fqn = f"{schema}.{table}__wap_old"
        pending_fqn = f"{pending}.{table}"

        if dialect == "duckdb":
            _exec(conn, "BEGIN TRANSACTION", dialect)
            try:
                _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)
                if live_exists:
                    _exec(conn, f"ALTER TABLE {live_fqn} RENAME TO {table}__wap_old", dialect)
                _exec(
                    conn,
                    f"CREATE TABLE {live_fqn} AS SELECT * FROM {pending_fqn}",
                    dialect,
                )
                _exec(conn, f"DROP TABLE {pending_fqn}", dialect)
                _exec(conn, "COMMIT", dialect)
            except Exception:
                _exec(conn, "ROLLBACK", dialect)
                raise
            if live_exists:
                _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)
        else:
            _exec(conn, "BEGIN", dialect)
            try:
                _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)
                if live_exists:
                    _exec(conn, f"ALTER TABLE {live_fqn} RENAME TO {table}__wap_old", dialect)
                _exec(conn, f"ALTER TABLE {pending_fqn} SET SCHEMA {schema}", dialect)
                _exec(conn, "COMMIT", dialect)
            except Exception:
                _exec(conn, "ROLLBACK", dialect)
                raise
            if live_exists:
                _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)

        published.append(live_fqn)
        log.info("WAP published %s (live existed: %s)", live_fqn, live_exists)

    return {"published": published, "count": len(published)}


def _airflow_entrypoint(tables: list[tuple[str, str]], **context: Any) -> dict[str, Any]:
    """Airflow PythonOperator entrypoint.

    ``tables`` is passed via the operator's ``op_kwargs`` so each DAG publishes
    only the tables it built. Redshift creds resolve the same way as
    ``row_count_reconciliation._airflow_entrypoint``.
    """
    import redshift_connector
    from airflow.models import Variable

    from metadata_airflow import redshift_password

    conn = redshift_connector.connect(
        host=Variable.get("redshift_host"),
        port=int(Variable.get("redshift_port", default_var="5439")),
        database=Variable.get("redshift_database", default_var="prod"),
        user=Variable.get("redshift_user"),
        password=redshift_password(),
    )
    try:
        result = publish_gold(conn, tables, dialect="redshift")
    finally:
        conn.close()

    _emit_metadata(result, context)
    return result


def _emit_metadata(result: dict[str, Any], context: dict[str, Any] | None) -> None:
    """Best-effort audit rows into the metadata database (fail-open)."""
    try:
        from metadata_airflow import _observer, _writer, execution_id_for_context

        observer = _observer()
        if observer is None:
            return
        writer = _writer(observer)
        if writer is None:
            return
        try:
            execution_id = (
                execution_id_for_context(context or {}) if context else "wap_publish"
            )
            record = getattr(observer, "record_wap_publish", None)
            if record is not None:
                record(
                    writer,
                    execution_id=execution_id,
                    published=result.get("published") or [],
                )
        finally:
            writer.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata emit from WAP publish failed (fail-open): %s", exc)


# Airflow PythonOperator callables — referenced by the warehouse, marketing,
# and catalog DAGs. Tests should call ``publish_gold`` directly with a stub
# connection rather than importing these.
def publish_finance_summary_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(FINANCE_SUMMARY_TABLES, **context)


def publish_marketing_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(MARKETING_TABLES, **context)


def publish_dim_product_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(DIM_PRODUCT_TABLES, **context)
