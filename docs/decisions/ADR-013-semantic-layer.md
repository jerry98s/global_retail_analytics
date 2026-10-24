# ADR-013: dbt Semantic Layer (MetricFlow) Over the Summary Tables

**Status:** Accepted
**Date:** 2026-10-21
**Author:** Data platform team
**Related:** [ADR-008](ADR-008-metadata-database.md), [ADR-009](ADR-009-write-audit-publish.md), [ADR-012](ADR-012-inventory-lambda.md)

---

## Context

Metrics were defined in three places that did not agree:

- `metadata/catalog/metric_catalog.yml` described metrics as text. Nothing
  executed it.
- The dashboard wrote its own aggregates over the facts.
- `summary.*` tables held the daily rollups.

Inventory on-hand exposed the gap. The dashboard summed `quantity_on_hand` per
hour from `fact_inventory_snapshot`, then summed those hours again. On-hand is
semi-additive: it adds across products and stores but not across time.

The fact and `summary.inventory_daily_product_store` are also sparse. They only
have a row when a product/store moved. Locally about 5,900 of 10,000 pairs
appear on a given day. "Total on-hand on day X" from those tables misses every
pair that did not move, and a product whose `dim_product` version changed is
split across two `product_key` values.

## Decision

1. **Dense inventory position.** `summary.inventory_position_daily` has one row
   per `(snapshot_date_key, product_id, store_id)` for every pair with an
   opening balance or a movement, on every day from the first to the last fact
   day. Days without movement carry the previous balance; days before the first
   movement carry `opening_qty`. `stockout_hour_count` counts hours at or below
   zero, including hours with no movement. It reads only
   `fact_inventory_snapshot` (plus dims and the opening-balance seed), is owned
   by `warehouse_daily_batch_pipeline`, and goes through WAP like the other
   summary tables.
2. **MetricFlow semantic models** (legacy spec, dbt Core 1.11) in
   `transformation/dbt_project/models/semantic/` sit on
   `summary.sales_daily_store`, `summary.inventory_position_daily`,
   `summary.sessions_daily_platform`, and the `dim_store` seed. Never on facts.
   - On-hand and available are `sum` measures with
     `non_additive_dimension: position_date, window_choice: max`. Over a period
     they take the last day.
   - `metricflow_time_spine` is a view over `dim_date`.
   - Time dimensions parse the `YYYYMMDD` keys with `substring`/`cast`, which
     runs unchanged on DuckDB and Redshift.
3. **Compiled SQL, not a runtime engine.**
   `scripts/common/generate_semantic_queries.py` reads
   `target/semantic_manifest.json`, compiles each dashboard query to DuckDB
   and Redshift SQL with a compile-only SQL client, and writes
   `dashboard/semantic_queries.py`. Relations are rendered `"schema"."table"`
   so the same SQL runs on `local_retail.duckdb` and on dev/prod Redshift. CI
   runs it with `--check` after the live `dbt compile` and before the pending
   compiles.
4. **Dashboard.** The sales, inventory and session tabs run only the compiled
   SQL. A `duckdb` mode reads the local Gold DuckDB read-only.
5. **Metric catalog.** `metric_catalog.yml` holds only the operational `meta.*`
   metrics, which stay outside MetricFlow because they live in the metadata
   database. `metadata_observer.py seed-catalog` derives the `summary.*` rows of
   `meta.metric_catalog` (source table, aggregate, grain) from the semantic
   YAML, so analytics metrics are not described twice.

## Consequences

- A metric is defined once in YAML. Changing it means `dbt parse` and
  regenerating the dashboard SQL; CI fails if the two drift.
- The dashboard container needs neither MetricFlow nor a dbt profile.
- `dbt-metricflow` and `metricflow` are pinned exactly in the dev group and CI,
  because a different MetricFlow version can render different SQL and fail
  `--check`.
- `inventory_position_daily` costs pairs × days rows (500,000 locally for
  10,000 pairs over 50 days) and recomputes the whole grid each run, upserting
  only changed rows.
- `summary.inventory_daily_product_store` stays for its existing readers but is
  not in the semantic layer. Do not sum its on-hand across products for a day.
- No dbt Cloud Semantic Layer API: ad hoc questions go through `mf query`
  locally.
- MetricFlow's config loader renders every profile target, and on Windows the
  `mf validate-configs` spinner can fail on a dbt parse that succeeds. Run it
  with a profile that only has the local target.

## Alternatives considered

- **Run MetricFlow inside the dashboard.** Needs `dbt-metricflow`, a dbt
  adapter and a profile in the App Runner image, and imports MetricFlow
  internals at runtime. Rejected.
- **"Last value" on the sparse summary.** Cannot fix missing rows or split
  `product_key`s. Rejected.
- **Upgrade to dbt Core 1.12 for the newer YAML spec.** The repo pins 1.11.
  Deferred.
