"""Write-Audit-Publish helper for Gold marts (ADR-009).

Three phases, one per Airflow task:

1. **Clone** — ``clone_live_to_pending`` copies each live Gold table into its
   ``*_pending`` twin.
2. **Write + audit** — dbt runs with ``wap_phase='pending'`` so models build into
   the pending clones; dbt tests and Great Expectations validate them there.
3. **Publish** — ``publish_gold`` replaces each live table's rows with the
   audited pending rows. A failing audit never touches live, so consumers keep
   reading the last good publish.

Why the clone phase exists
--------------------------
Gold marts are ``incremental``. dbt's ``is_incremental()`` is false whenever the
target relation does not exist, so a pending schema that starts empty would make
every model take its full-refresh branch. For facts that is merely wasteful
(Bronze can be replayed), but ``marketing.dim_product`` is an SCD2 accumulator
whose history exists *only in the table itself* — a full-refresh branch rebuilds
it as current-only rows and **the version history is destroyed on publish**.

Cloning live into pending first means the pending relation exists and already
holds prior state, so ``is_incremental()`` is true and ``{{ this }}`` is correct
for every model. Live remains the source of truth for accumulated state; pending
is ephemeral and is re-cloned every run, so a failed audit is discarded rather
than carried forward into the next build.

Cost: on Redshift the clone is a real copy (there is no zero-copy clone), so each
run rewrites the Gold tables once. Acceptable at this project's volumes; a
Snowflake/Iceberg deployment would use zero-copy clones or branches instead.

Clone strategy per table (Redshift)::

    DROP TABLE IF EXISTS live_pending.table;
    CREATE TABLE live_pending.table (LIKE live.table);  -- copies DISTKEY/SORTKEY
    INSERT INTO live_pending.table SELECT * FROM live.table;

``CREATE TABLE ... LIKE`` is what preserves the distribution and sort keys
defined in ``transformation/redshift/ddl/``; a plain CTAS would silently drop
them.

Publish strategy, one transaction for the whole DAG-owned set::

    -- preflight: every pending table exists and its columns match live
    DELETE FROM live.table;                              -- live exists
    CREATE TABLE live.table (LIKE live_pending.table);   -- first publish only
    INSERT INTO live.table (cols) SELECT cols FROM live_pending.table;
    -- ...repeat for every owned table, then COMMIT once...

Redshift has no ``ALTER TABLE ... SET SCHEMA``, and ``ALTER TABLE APPEND``
cannot run inside a transaction block, so a cross-schema pointer swap cannot
publish the set atomically. Delete + insert keeps the live table's OID, grants,
owner and DIST/SORT keys; readers see the previous publish until COMMIT. The
cost is one copy of each owned table per publish. Pending copies are dropped
in a follow-up transaction so a drop failure cannot undo the publish.

A column mismatch between pending and live (a model changed shape) aborts the
publish before any write: the live DDL must be migrated deliberately.

Each owning DAG also sets ``max_active_runs=1`` so a second run cannot drop
and reclone ``*_pending`` while the first run is still building or publishing.

Dependent views stay late-binding (``+bind: false`` in ``dbt_project.yml`` and
``WITH NO SCHEMA BINDING`` on the serving views) so a deliberate live DDL
migration can drop and recreate a table without dropping its views.

Table ownership
---------------
Each Gold table has exactly one owning DAG. ``marketing.dim_product`` belongs to
``catalog_bihourly_product_scd2_refresh`` alone — the warehouse DAG reads it from
the live schema via the ``wap_live_ref`` dbt macro instead of rebuilding it, so
the two DAGs can never write the same pending table concurrently.

Reference dims ``finance.dim_date`` / ``finance.dim_store`` are excluded
entirely: they are stable seed data, not dbt-built marts. ``customer_360_view``
and ``serving.*`` are views over live Gold, refreshed after publish.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

# (live schema, table) — every entry is cloned from and published to "<schema>_pending".
WAP_TABLES: list[tuple[str, str]] = [
    ("finance", "fact_sales"),
    ("finance", "fact_inventory_snapshot"),
    ("marketing", "dim_customer"),
    ("marketing", "dim_product"),
    ("marketing", "fact_customer_session"),
    ("marketing", "identity_graph"),
    ("summary", "sales_daily_store"),
    ("summary", "inventory_daily_product_store"),
    ("summary", "inventory_position_daily"),
    ("summary", "sessions_daily_platform"),
]

# Per-DAG subsets. These are disjoint by design — see "Table ownership" above.
# Each DAG clones, builds, audits, and publishes exactly its own tables.
FINANCE_SUMMARY_TABLES: list[tuple[str, str]] = [
    ("finance", "fact_sales"),
    ("finance", "fact_inventory_snapshot"),
    ("summary", "sales_daily_store"),
    ("summary", "inventory_daily_product_store"),
    ("summary", "inventory_position_daily"),
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

WAP_SCOPES: dict[str, list[tuple[str, str]]] = {
    "finance": FINANCE_SUMMARY_TABLES,
    "marketing": MARKETING_TABLES,
    "catalog": DIM_PRODUCT_TABLES,
}


def _pending_schema(schema: str) -> str:
    return f"{schema}_pending"


def build_redshift_clone_statements(tables: list[tuple[str, str]]) -> list[str]:
    """SQL statements cloning live tables into their pending twins (Redshift).

    ``CREATE TABLE ... LIKE`` copies column definitions plus DISTKEY/SORTKEY, so
    the publish swap preserves the tuning declared in the hand-written DDL.
    """
    statements: list[str] = []
    for schema, table in tables:
        pending_fqn = f"{_pending_schema(schema)}.{table}"
        live_fqn = f"{schema}.{table}"
        statements.extend(
            [
                f"DROP TABLE IF EXISTS {pending_fqn}",
                f"CREATE TABLE {pending_fqn} (LIKE {live_fqn})",
                f"INSERT INTO {pending_fqn} SELECT * FROM {live_fqn}",
            ]
        )
    return statements


def build_publish_statements(
    schema: str,
    table: str,
    columns: list[str],
    *,
    live_exists: bool,
    dialect: str = "redshift",
) -> list[str]:
    """Statements replacing one live table's rows with its pending rows.

    The caller runs every owned table's statements inside one transaction.
    """
    live_fqn = f"{schema}.{table}"
    pending_fqn = f"{_pending_schema(schema)}.{table}"
    cols = ", ".join(columns)
    if live_exists:
        create = [f"DELETE FROM {live_fqn}"]
    elif dialect == "duckdb":
        create = [f"CREATE TABLE {live_fqn} AS SELECT * FROM {pending_fqn} WHERE 1 = 0"]
    else:
        create = [f"CREATE TABLE {live_fqn} (LIKE {pending_fqn})"]
    return [*create, f"INSERT INTO {live_fqn} ({cols}) SELECT {cols} FROM {pending_fqn}"]


def _query(conn: Any, sql: str, params: tuple[str, ...], dialect: str) -> list[tuple[Any, ...]]:
    if dialect == "duckdb":
        return conn.execute(sql.replace("%s", "?"), list(params)).fetchall()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return list(rows)


def _table_exists(conn: Any, schema: str, table: str, dialect: str) -> bool:
    rows = _query(
        conn,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
        dialect,
    )
    return bool(rows and int(rows[0][0]) > 0)


def _columns(conn: Any, schema: str, table: str, dialect: str) -> list[str]:
    rows = _query(
        conn,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
        dialect,
    )
    return [str(r[0]) for r in rows]


def _exec(conn: Any, sql: str, dialect: str) -> None:
    if dialect == "duckdb":
        conn.execute(sql)
    else:
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()


def _begin(conn: Any, dialect: str) -> None:
    # redshift_connector is DB-API with autocommit off: a transaction is already
    # open, and an explicit BEGIN would nest. Commit/rollback go through the
    # connection so nothing is left uncommitted when the connection closes.
    if dialect == "duckdb":
        _exec(conn, "BEGIN TRANSACTION", dialect)


def _commit(conn: Any, dialect: str) -> None:
    if dialect == "duckdb":
        _exec(conn, "COMMIT", dialect)
    else:
        conn.commit()


def _rollback(conn: Any, dialect: str) -> None:
    if dialect == "duckdb":
        _exec(conn, "ROLLBACK", dialect)
    else:
        conn.rollback()


def clone_live_to_pending(
    conn: Any,
    tables: list[tuple[str, str]],
    *,
    dialect: str = "redshift",
) -> dict[str, Any]:
    """Copy live Gold tables into their pending twins before a dbt pending build.

    Gives dbt an existing pending relation holding prior state, so incremental
    models take their incremental branch and SCD2 history survives (see module
    docstring). Any stale pending table from an aborted run is dropped first, so
    a failed audit is never carried into the next build.

    A live table that does not exist yet (first-ever run on a fresh environment)
    is skipped: pending stays absent and dbt performs its initial load.
    """
    cloned: list[str] = []
    skipped: list[str] = []
    for schema, table in tables:
        pending = _pending_schema(schema)
        pending_fqn = f"{pending}.{table}"
        live_fqn = f"{schema}.{table}"

        _exec(conn, f"CREATE SCHEMA IF NOT EXISTS {pending}", dialect)

        live_exists = _table_exists(conn, schema, table, dialect)

        _begin(conn, dialect)
        try:
            _exec(conn, f"DROP TABLE IF EXISTS {pending_fqn}", dialect)
            if live_exists:
                if dialect == "duckdb":
                    _exec(
                        conn,
                        f"CREATE TABLE {pending_fqn} AS SELECT * FROM {live_fqn}",
                        dialect,
                    )
                else:
                    # LIKE (not CTAS) so DISTKEY/SORTKEY survive the later swap.
                    _exec(conn, f"CREATE TABLE {pending_fqn} (LIKE {live_fqn})", dialect)
                    _exec(
                        conn,
                        f"INSERT INTO {pending_fqn} SELECT * FROM {live_fqn}",
                        dialect,
                    )
                cloned.append(live_fqn)
            else:
                skipped.append(live_fqn)
            _commit(conn, dialect)
        except Exception:
            _rollback(conn, dialect)
            raise

        log.info("WAP clone %s -> %s (live existed: %s)", live_fqn, pending_fqn, live_exists)

    return {"cloned": cloned, "skipped": skipped, "count": len(cloned)}


def publish_gold(
    conn: Any,
    tables: list[tuple[str, str]],
    *,
    dialect: str = "redshift",
) -> dict[str, Any]:
    """Replace live Gold rows with the audited pending rows.

    ``conn`` is a ``redshift_connector`` connection (cloud) or a ``duckdb``
    connection (local).

    Raises before any write if a pending table is missing or its columns differ
    from live. A missing live table is the first-ever publish and is created
    from the pending definition. The whole owned set commits once, so a failure
    on a later table leaves every live table on the previous publish.
    """
    missing = [
        f"{_pending_schema(schema)}.{table}"
        for schema, table in tables
        if not _table_exists(conn, _pending_schema(schema), table, dialect)
    ]
    if missing:
        raise RuntimeError(
            "WAP publish aborted: pending table(s) do not exist: "
            + ", ".join(missing)
            + ". Run the clone task and dbt with wap_phase='pending' before publishing."
        )

    plan: list[tuple[str, str, bool, list[str]]] = []
    drift: list[str] = []
    for schema, table in tables:
        pending_cols = _columns(conn, _pending_schema(schema), table, dialect)
        live_exists = _table_exists(conn, schema, table, dialect)
        if live_exists:
            live_cols = _columns(conn, schema, table, dialect)
            if sorted(live_cols) != sorted(pending_cols):
                drift.append(
                    f"{schema}.{table} live={live_cols} pending={pending_cols}"
                )
            plan.append((schema, table, True, live_cols))
        else:
            plan.append((schema, table, False, pending_cols))
    if drift:
        raise RuntimeError(
            "WAP publish aborted: pending columns differ from live; migrate the live "
            "DDL first: " + "; ".join(drift)
        )

    published: list[str] = []
    _begin(conn, dialect)
    try:
        for schema, table, live_exists, columns in plan:
            for sql in build_publish_statements(
                schema, table, columns, live_exists=live_exists, dialect=dialect
            ):
                _exec(conn, sql, dialect)
            published.append(f"{schema}.{table}")
        _commit(conn, dialect)
    except Exception:
        _rollback(conn, dialect)
        raise

    # Separate transaction: the publish is already durable, so failing to drop
    # the pending copies must not roll it back. The next clone drops them anyway.
    try:
        _begin(conn, dialect)
        for schema, table, _, _ in plan:
            _exec(conn, f"DROP TABLE IF EXISTS {_pending_schema(schema)}.{table}", dialect)
        _commit(conn, dialect)
    except Exception as exc:  # noqa: BLE001
        _rollback(conn, dialect)
        log.warning("WAP published %s but could not drop pending copies: %s", published, exc)

    for schema, table, live_exists, _ in plan:
        log.info("WAP published %s.%s (live existed: %s)", schema, table, live_exists)

    return {"published": published, "count": len(published)}


def _connect_redshift() -> Any:
    import redshift_connector
    from airflow.models import Variable

    from metadata_airflow import redshift_password

    return redshift_connector.connect(
        host=Variable.get("redshift_host"),
        port=int(Variable.get("redshift_port", default_var="5439")),
        database=Variable.get("redshift_database", default_var="prod"),
        user=Variable.get("redshift_user"),
        password=redshift_password(),
    )


def _connect_redshift_env() -> Any:
    """Connect for the documented no-MWAA laptop workflow."""
    import redshift_connector

    required = ("RS_HOST", "RS_USER", "RS_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing Redshift environment variables: {', '.join(missing)}")
    return redshift_connector.connect(
        host=os.environ["RS_HOST"],
        port=int(os.environ.get("RS_PORT", "5439")),
        database=os.environ.get("RS_DATABASE", "dev"),
        user=os.environ["RS_USER"],
        password=os.environ["RS_PASSWORD"],
    )


def run_cli(phase: str, scope: str) -> dict[str, Any]:
    """Run one WAP phase from a laptop using the canonical ownership sets."""
    if phase not in {"clone", "publish"}:
        raise ValueError(f"Unsupported WAP phase: {phase}")
    if scope not in WAP_SCOPES:
        raise ValueError(f"Unsupported WAP scope: {scope}")

    conn = _connect_redshift_env()
    try:
        if phase == "clone":
            return clone_live_to_pending(conn, WAP_SCOPES[scope], dialect="redshift")
        return publish_gold(conn, WAP_SCOPES[scope], dialect="redshift")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clone or publish a Redshift Gold WAP ownership set"
    )
    parser.add_argument("phase", choices=("clone", "publish"))
    parser.add_argument("scope", choices=tuple(WAP_SCOPES))
    args = parser.parse_args()
    print(json.dumps(run_cli(args.phase, args.scope), sort_keys=True))


def _airflow_entrypoint(
    tables: list[tuple[str, str]],
    phase: str,
    **context: Any,
) -> dict[str, Any]:
    """Airflow PythonOperator entrypoint for the clone and publish phases.

    ``tables`` is passed via the operator's ``op_kwargs`` so each DAG touches
    only the tables it owns. Redshift creds resolve the same way as
    ``row_count_reconciliation._airflow_entrypoint``.
    """
    conn = _connect_redshift()
    try:
        if phase == "clone":
            result = clone_live_to_pending(conn, tables, dialect="redshift")
        else:
            result = publish_gold(conn, tables, dialect="redshift")
    finally:
        conn.close()

    if phase == "publish":
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
            observer.record_wap_publish(
                writer,
                execution_id=execution_id,
                published=result.get("published") or [],
            )
        finally:
            writer.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata emit from WAP publish failed (fail-open): %s", exc)


# Airflow PythonOperator callables — referenced by the warehouse, marketing, and
# catalog DAGs. Tests should call clone_live_to_pending / publish_gold directly
# with a stub connection rather than importing these.
def clone_finance_summary_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(FINANCE_SUMMARY_TABLES, "clone", **context)


def clone_marketing_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(MARKETING_TABLES, "clone", **context)


def clone_dim_product_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(DIM_PRODUCT_TABLES, "clone", **context)


def publish_finance_summary_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(FINANCE_SUMMARY_TABLES, "publish", **context)


def publish_marketing_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(MARKETING_TABLES, "publish", **context)


def publish_dim_product_task(**context: Any) -> dict[str, Any]:
    return _airflow_entrypoint(DIM_PRODUCT_TABLES, "publish", **context)


if __name__ == "__main__":
    main()
