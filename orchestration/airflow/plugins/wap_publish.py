"""Write-Audit-Publish helper for Gold marts (ADR-009).

Three phases, one per Airflow task:

1. **Clone** — ``clone_live_to_pending`` copies each live Gold table into its
   ``*_pending`` twin.
2. **Write + audit** — dbt runs with ``wap_phase='pending'`` so models build into
   the pending clones; dbt tests and Great Expectations validate them there.
3. **Publish** — ``publish_gold`` swaps each audited pending table into the live
   schema. A failing audit never touches live, so consumers keep reading the
   last good publish.

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
them and the publish swap would leave an untuned table behind.

Publish strategy per table (Redshift)::

    DROP TABLE IF EXISTS live.table__wap_old;
    ALTER TABLE live.table RENAME TO table__wap_old;   -- only if live exists
    ALTER TABLE live_pending.table SET SCHEMA live;    -- lands as live.table
    -- commit, then drop the old copy

The swap requires every dependent view to be late-binding — a bound Redshift
view follows the renamed table by OID and would block the drop. That is why
``dbt_project.yml`` sets ``+bind: false`` and the hand-written serving views use
``WITH NO SCHEMA BINDING``.

DuckDB has no ``SET SCHEMA`` and cannot rename across schemas, so the local path
copies pending into live and drops the pending copy inside one transaction. It
is a copy rather than a pointer swap; acceptable for the local simulation.

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

import logging
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
    ("summary", "sessions_daily_platform"),
]

# Per-DAG subsets. These are disjoint by design — see "Table ownership" above.
# Each DAG clones, builds, audits, and publishes exactly its own tables.
FINANCE_SUMMARY_TABLES: list[tuple[str, str]] = [
    ("finance", "fact_sales"),
    ("finance", "fact_inventory_snapshot"),
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
                "Run the clone task and dbt with wap_phase='pending' before publishing."
            )

        live_exists = _table_exists(conn, schema, table, dialect)
        live_fqn = f"{schema}.{table}"
        old_fqn = f"{schema}.{table}__wap_old"
        pending_fqn = f"{pending}.{table}"

        _begin(conn, dialect)
        try:
            _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)
            if live_exists:
                _exec(conn, f"ALTER TABLE {live_fqn} RENAME TO {table}__wap_old", dialect)
            if dialect == "duckdb":
                _exec(conn, f"CREATE TABLE {live_fqn} AS SELECT * FROM {pending_fqn}", dialect)
                _exec(conn, f"DROP TABLE {pending_fqn}", dialect)
            else:
                _exec(conn, f"ALTER TABLE {pending_fqn} SET SCHEMA {schema}", dialect)
            _commit(conn, dialect)
        except Exception:
            _rollback(conn, dialect)
            raise

        if live_exists:
            # Separate transaction: the swap is already durable, so failing to
            # drop the old copy must not roll the publish back.
            try:
                _exec(conn, f"DROP TABLE IF EXISTS {old_fqn}", dialect)
                _commit(conn, dialect)
            except Exception as exc:  # noqa: BLE001
                _rollback(conn, dialect)
                log.warning("WAP published %s but could not drop %s: %s", live_fqn, old_fqn, exc)

        published.append(live_fqn)
        log.info("WAP published %s (live existed: %s)", live_fqn, live_exists)

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
