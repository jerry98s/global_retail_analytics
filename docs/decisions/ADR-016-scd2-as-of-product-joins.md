# ADR-016: As-Of SCD2 Product Joins in Facts

**Status:** Accepted
**Date:** 2026-11-23
**Author:** Data platform team
**Related:** [ADR-009](ADR-009-write-audit-publish.md), [ADR-012](ADR-012-inventory-lambda.md), [ADR-015](ADR-015-pos-voids-and-returns.md)

---

## Context

`dim_product` is SCD Type 2 with half-open `[effective_from, effective_to)`
intervals, but `fact_sales`, `fact_returns` and `fact_inventory_snapshot`
joined only the `is_current` row. A sale therefore carried whichever version
was current when its day was loaded. Two loads of the same day could disagree,
and a category or cost change was attributed to sales made before it.

## Decision

1. **Join the version valid on the business date.** The macros
   `product_versions()` and `product_as_of()` read live `dim_product` through
   `wap_live_ref` and match `valid_from <= date < valid_to`. The business date
   is the sale date for `fact_sales`, the original sale date for
   `fact_returns` (the return date for an `UNKNOWN_SALE`), and the snapshot
   date for `fact_inventory_snapshot`. A refund therefore rolls up under the
   attributes the item was sold with.
2. **Dates before the first version take the first version.** The catalog
   records a product from the day it first sees it, which can be after older
   sales or stock. The earliest version's `valid_from` is widened to
   1900-01-01. The warn test `facts_dated_before_first_product_version`
   counts these rows.
3. **Facts keep `product_id`.** It is the lineage for `product_key`, the key
   for re-keying, and the join for current-attribute reporting.
4. **Re-key without a manual full refresh.** Outside an explicit
   restatement, `fact_sales` and `fact_returns` reprocess every stored day
   whose `product_key` differs from the as-of version, or that has no
   `product_id`. The day is replaced whole by delete+insert. A table from
   before `product_id` existed reprocesses every day once. The inventory fact
   keys delete+insert on `product_id` instead of `product_key` and rewrites
   hours whose as-of key changed. A pre-hook empties a table that lacks
   `product_id` once, and the fact rebuilds from `int_inventory_hourly`.
   `inventory_daily_product_store` deletes rows whose `product_key` the fact
   no longer has for that day.
5. **A blocking test proves it.** `facts_product_key_matches_as_of_version`
   fails the finance publish when any fact row carries a key other than the
   as-of version (`-1` only while the product is unpublished).

## Consequences

- Each run compares every stored row with the as-of version: one join of
  each fact against the small product dimension.
- A back-dated catalog correction re-keys old days on the next finance run;
  their attributes then follow the corrected history.
- Live tables gain `product_id` at publish: WAP adds pending-only columns to
  live (ADR-009). On `main` the Redshift DDL adds the column too.
- Current-attribute reporting joins `product_id` to the `is_current` row
  (`serving.dim_product_current` on `main`).

## Alternatives considered

- **Keep the current-version join and document it.** Simple, but it rewrites
  history silently and makes reloads non-deterministic. Rejected.
- **Require `--full-refresh` after a catalog change.** Operators would have to
  notice the change, and a full refresh of `fact_sales` re-reads all POS
  Bronze. Rejected in favour of decision 4.
- **Map pre-history rows to `-1`.** Correct but unhelpful: most early sales
  would lose every product attribute. Rejected.
