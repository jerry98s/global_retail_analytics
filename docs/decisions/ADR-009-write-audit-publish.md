# ADR-009: Write-Audit-Publish for Gold marts

**Status:** Accepted

**Date:** 2026-08-09
**Author:** Data platform team

---

## Context

Before this change, the Gold layer was **write-then-audit**: dbt built marts
directly into the live `finance` / `marketing` / `summary` schemas, and only
then did dbt tests, row-count reconciliation, and Great Expectations run. A bad
build (silent data loss, a broken SCD2 close-out, a null consent flag) was
already visible to consumers — serving views, the Streamlit dashboard, and BI —
before any quality gate could stop it. A failing audit could not un-write the
live tables.

We needed a pattern where a failing run **never touches live**, so consumers
always read the last good publish.

A first implementation wrote into empty `*_pending` schemas and used
`wap_prior_state()` so incrementals could look back at live. That design is
incorrect for this warehouse:

- dbt's `is_incremental()` is false when the target relation does not exist, so
  an empty pending schema made every model take its **full-refresh** branch.
  `wap_prior_state()` never executed.
- `marketing.dim_product` is an SCD2 accumulator whose history exists only in
  the table. A full-refresh rebuilds it as current-only rows and **destroys
  version history on publish**.
- Two DAGs both published `dim_product`, so overlapping runs could clobber the
  same pending table.

## Decision

Adopt **Write-Audit-Publish (WAP)** for consumer-facing Gold marts, with a
**clone-first** write phase:

1. **Clone live → pending.** Each owning DAG copies its live Gold tables into
   the matching `*_pending` schema (`CREATE TABLE pending.x (LIKE live.x)` +
   `INSERT … SELECT *`, so DISTKEY/SORTKEY survive). A missing live table
   (first-ever run) is skipped and dbt performs its initial load.
2. **Write pending.** dbt builds Gold *tables* into `finance_pending` /
   `marketing_pending` / `summary_pending` when
   `--vars '{"wap_phase": "pending"}'` is set. `generate_schema_name` routes
   only those three schemas; `staging`, `intermediate`, `serving`, and `bronze`
   are never redirected. Gold *views* (`customer_360_view`) stay in the live
   schema — they are not published.
3. **Audit pending.** `dbt test` and row-count reconciliation run against an
   explicit `schema.table` list owned by that DAG (`pending_tables`). A blanket
   schema suffix would point at pending twins the calling DAG never cloned.
   Great Expectations does not audit Gold; it checks the Silver event
   contract only, so dbt tests are the single Gold gate.
4. **Publish atomically.** After every audit passes, preflight that every
   DAG-owned pending table exists and has the same columns as live, then
   promote the **entire set** in one transaction: `DELETE FROM live.x` +
   `INSERT INTO live.x (cols) SELECT cols FROM pending.x` (first publish:
   `CREATE TABLE live.x (LIKE pending.x)`). A missing table or a column
   mismatch aborts before any write. Redshift has no `ALTER TABLE … SET
   SCHEMA`, and `ALTER TABLE APPEND` cannot run inside a transaction block, so
   a pointer swap cannot publish the set atomically. Delete + insert keeps the
   live table's identity, grants, owner, and DIST/SORT keys; readers see the
   previous publish until commit.

Because pending already exists and holds prior state, `is_incremental()` is
true and `{{ this }}` is the correct incremental anchor. Cross-DAG Gold **model**
reads (e.g. `fact_sales` joining `dim_product`) use the `wap_live_ref()` macro so
they never write another DAG's pending table. Cross-DAG **relationship tests**
use `source('gold_marketing', 'dim_product')` instead: dbt cannot infer a nested
`ref()` inside generic test YAML, so `wap_live_ref` there fails `dbt compile`.

Dependent Redshift views are **late-binding** (`dbt_project.yml` `+bind: false`,
hand DDL `WITH NO SCHEMA BINDING`) so a deliberate live DDL migration can drop
and recreate a table without dropping its views. Gold models declare `dist` / `sort` matching
`the warehouse DDL on main` so a published table keeps its tuning.

## Correctness guarantees

- **Incremental and SCD2 state survive.** The clone is prior state. A failed
  audit is discarded on the next clone (`DROP TABLE pending` first), not
  carried forward.
- **One owner per Gold table.** Per-DAG subsets of `WAP_TABLES` are disjoint
  and cover the full list. `marketing.dim_product` belongs only to
  `catalog_bihourly_product_scd2_refresh`. Each of those DAGs sets
  `max_active_runs=1` so overlapping runs cannot drop/reclone the same
  `*_pending` tables.
- **Reference dims are not published.** `finance.dim_date` and
  `finance.dim_store` are stable seed/bootstrap data. They stay in live
  `finance` and are excluded from clone/publish lists.
- **Serving refresh after publish.** `marketing.customer_360_view` and
  `serving.customer_360_serving` are views over live Gold. The marketing DAG
  rebuilds them after publish. They are not published through WAP.
- **Transaction handling.** `redshift_connector` is not autocommit: publish
  preflights the full set, then commits every table together via
  `conn.commit()`. Dropping the pending copies is a follow-up transaction so a
  drop failure cannot roll the publish back.
- **Publish audit.** Each published table writes a `wap_publish` row to
  `meta.dq_check_result` (fail-open).

## Ownership boundaries

| DAG | Clones / writes pending / publishes |
|-----|--------------------------------------|
| `warehouse_daily_batch_pipeline` | finance facts + finance summaries |
| `marketing_hourly_customer_360_pipeline` | marketing marts + `sessions_daily_platform` |
| `catalog_bihourly_product_scd2_refresh` | `dim_product` only |

The hourly `quality_hourly_ge_checkpoint` DAG checks the canonical Silver
events (`silver_events_daily`). Earlier versions also ran Gold GE suites as a
second pending gate; they repeated the dbt tests on the same tables and were
removed.

## Alternatives considered

| Option | Why rejected |
|--------|--------------|
| Serving-gate-only (point serving at pending after audit) | Serving views would chase two schemas; rollback is a view re-point, not a clean table swap |
| Dual database (dev/prod clone) | Heavyweight; Spectrum external schemas and metadata DB already assume a single env database |
| Iceberg branches for Gold | Gold is Redshift native tables, not Iceberg — no branch primitive |
| Empty pending + `wap_prior_state()` | `is_incremental()` is false when pending does not exist, so the lookback never runs; SCD2 history is wiped on publish |
| View-only pending (no table copy) | Leaves two sources of truth; incrementals still have nowhere to write prior state |

## Consequences

- A failing audit leaves live on the last good publish; consumers never see a
  half-built mart.
- Each run copies Gold twice (clone, then publish), because Redshift has no
  zero-copy clone or cross-schema swap. Acceptable at this project's volumes;
  Snowflake/Iceberg would use zero-copy clones or branches instead.
- The canonical table list lives in
  `orchestration/airflow/plugins/wap_publish.py` (`WAP_TABLES` + per-DAG
  subsets) so cloud Airflow and the local DuckDB stack share one definition.
- A model that changes columns needs a deliberate live DDL migration before
  its next publish; WAP refuses to publish a mismatched shape.
