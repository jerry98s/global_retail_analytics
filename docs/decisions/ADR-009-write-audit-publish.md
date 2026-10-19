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
- Bound Redshift views follow a renamed table by OID, so the publish swap
  (`RENAME` + `DROP ... __wap_old`) would fail or CASCADE-drop
  `customer_360_view`.
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
3. **Audit pending.** `dbt test`, row-count reconciliation, and GE run against
   an explicit `schema.table` list owned by that DAG (`pending_tables` /
   `--pending-tables`). A blanket schema suffix would point at pending twins
   the calling DAG never cloned.
4. **Publish atomically.** After every audit passes, preflight that every
   DAG-owned pending table exists, then promote the **entire set** in one
   transaction: Redshift `ALTER TABLE pending.x SET SCHEMA live` (rename swap
   keeping the old live as `x__wap_old` until commit); DuckDB
   `CREATE TABLE live AS SELECT * FROM pending` in that same transaction. A
   missing later table aborts before any swap.

Because pending already exists and holds prior state, `is_incremental()` is
true and `{{ this }}` is the correct incremental anchor. Cross-DAG Gold **model**
reads (e.g. `fact_sales` joining `dim_product`) use the `wap_live_ref()` macro so
they never write another DAG's pending table. Cross-DAG **relationship tests**
use `source('gold_marketing', 'dim_product')` instead: dbt cannot infer a nested
`ref()` inside generic test YAML, so `wap_live_ref` there fails `dbt compile`.

Dependent Redshift views are **late-binding** (`dbt_project.yml` `+bind: false`,
hand DDL `WITH NO SCHEMA BINDING`) so the rename/drop swap does not follow
OIDs. Gold models declare `dist` / `sort` matching
`transformation/redshift/ddl/` so a published table keeps its tuning.

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
  rebuilds them after the swap. They are not renamed through WAP.
- **Transaction handling.** `redshift_connector` is not autocommit: publish
  preflights the full set, then commits every swap together via
  `conn.commit()`. Dropping `__wap_old` is a follow-up transaction so a drop
  failure cannot roll the swap back.

## Ownership boundaries

| DAG | Clones / writes pending / publishes |
|-----|--------------------------------------|
| `warehouse_daily_batch_pipeline` | finance facts + finance summaries |
| `marketing_hourly_customer_360_pipeline` | marketing marts + `sessions_daily_platform` |
| `catalog_bihourly_product_scd2_refresh` | `dim_product` only |

The hourly `quality_hourly_ge_checkpoint` DAG intentionally stays on **live** —
it monitors the published Gold that consumers actually read. The warehouse DAG's
GE task is the pending audit gate (`--pending-tables` = that DAG's owned list).

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
- Each run copies Gold once (Redshift has no zero-copy clone). Acceptable at
  this project's volumes; Snowflake/Iceberg would use zero-copy clones or
  branches instead.
- The canonical table list lives in
  `orchestration/airflow/plugins/wap_publish.py` (`WAP_TABLES` + per-DAG
  subsets) so cloud Airflow and the local DuckDB stack share one definition.
- Schema-level GRANTs (not table-level) are assumed: a `SET SCHEMA` swap
  creates a new object identity. This repo has no table-level GRANTs.
