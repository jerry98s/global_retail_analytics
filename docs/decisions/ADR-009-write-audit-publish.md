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

## Decision

Adopt **Write-Audit-Publish (WAP)** for consumer-facing Gold marts:

1. **Write pending.** dbt builds Gold marts into `finance_pending`,
   `marketing_pending`, `summary_pending` schemas when
   `--vars '{"wap_phase": "pending"}'` is set. `generate_schema_name` routes
   only `finance` / `marketing` / `summary`; `staging`, `intermediate`,
   `serving`, and `bronze` are never redirected.
2. **Audit pending.** `dbt test`, row-count reconciliation, and the GE
   `gold_layer_daily` checkpoint run against the `*_pending` relations
   (reconcile via `schema_suffix`, GE via `run_ge_checkpoint.py
   --schema-suffix _pending`).
3. **Publish atomically.** Only after every audit passes does a publish step
   promote pending to live: Redshift `ALTER TABLE pending.x SET SCHEMA live`
   (rename swap keeping the old live as `x__wap_old` until commit); DuckDB
   `CREATE TABLE live AS SELECT * FROM pending` in one transaction.

## Correctness guarantees

- **Incremental lookbacks read live, not empty pending.** Incremental models
  anchor on `max(...) from {{ this }}`. On a fresh pending schema `this` is
  empty, which would silently turn every incremental into a full rebuild. The
  `wap_prior_state()` macro returns the last committed **live** relation during
  a pending build (`this` when `wap_phase='live'`), preserving crash-resilient
  incremental behavior.
- **Reference dims are not published.** `finance.dim_date` and
  `finance.dim_store` are stable seed/bootstrap data, not dbt-built marts. They
  stay in live `finance`, are excluded from the publish list, and their GE /
  reconcile reads are never suffixed.
- **Serving refresh after publish.** `marketing.customer_360_view` and
  `serving.customer_360_serving` are views over Gold. The marketing DAG
  re-runs the serving view against **live** after the swap so it binds to the
  freshly published tables. They are not renamed through WAP.

## Ownership boundaries

| DAG | Writes pending | Publishes |
|-----|----------------|-----------|
| `warehouse_daily_batch_pipeline` | `dim_product`, finance facts, finance summaries | `FINANCE_SUMMARY_TABLES` |
| `marketing_hourly_customer_360_pipeline` | marketing marts, `sessions_daily_platform` | `MARKETING_TABLES` |
| `catalog_bihourly_product_scd2_refresh` | `dim_product` only | `dim_product` |

The hourly `quality_hourly_ge_checkpoint` DAG intentionally stays on **live** —
it monitors the published Gold that consumers actually read.

## Alternatives considered

| Option | Why rejected |
|--------|--------------|
| Serving-gate-only (point serving at pending after audit) | Serving views would chase two schemas; rollback is a view re-point, not a clean table swap |
| Dual database (dev/prod clone) | Heavyweight; Spectrum external schemas and metadata DB already assume a single env database |
| Iceberg branches for Gold | Gold is Redshift native tables, not Iceberg — no branch primitive |
| True incremental-on-pending (copy live→pending first) | Adds a copy step and doubles storage; the full-rebuild-from-lookback is correct and cheap at portfolio scale. Follow-up optimization. |

## Consequences

- A failing audit leaves live on the last good publish; consumers never see a
  half-built mart.
- Gold incrementals rebuild from their Bronze/Silver lookback each run because
  pending starts empty. Acceptable at this scale; seeding pending from live is
  a documented follow-up.
- The canonical publish table list lives in
  `orchestration/airflow/plugins/wap_publish.py` (`WAP_TABLES`) so cloud Airflow
  and the local DuckDB stack share one definition.
