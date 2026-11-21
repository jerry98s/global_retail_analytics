# ADR-014: Product Catalog Refresh Frequency

**Status:** Accepted
**Date:** 2026-11-21
**Author:** Data platform team
**Related:** [ADR-009](ADR-009-write-audit-publish.md)

---

## Context

`marketing.dim_product` is the platform's only SCD Type 2 dimension. Its DAG
ran every two hours on the assumption that the merchandising team changes the
catalog during the day. Nothing requires that: product attributes are assumed
to change rarely, and POS finance reporting is daily.

The finance DAG reads the published `dim_product` through `wap_live_ref` at
00:15 UTC. The catalog's midnight run came 15 minutes earlier only by
schedule. Two independent schedules do not guarantee that the catalog has
published before finance reads it.

The catalog source is the generated `reference.product_master` seed, not a
delivered daily feed. A schedule change alone adds no product changes.

## Decision

1. **Daily refresh.** `catalog_daily_product_scd2_refresh` (renamed from
   `catalog_bihourly_product_scd2_refresh`) runs at 00:00 UTC and keeps dbt
   SCD2, WAP and its separate DAG for ownership and independent recovery.
2. **Explicit dependency.** `warehouse_daily_batch_pipeline` waits for the
   catalog's `wap_publish_dim_product` of the same day with an
   `ExternalTaskSensor` (`wait_for_catalog_publish`, two-hour timeout). The
   POS steps run during the wait; only the WAP clone and the marts wait for it.
   A manual run waits for the latest daily catalog run.
3. **Soft fail.** If the catalog publish fails or is not done in two hours,
   the sensor is skipped, `catalog_publish_check` sends an SNS warning, and
   finance builds on the last published `dim_product`.

## Consequences

- SCD2 keeps the attribute state observed at each daily refresh. A product
  that changes twice between refreshes records only the later state. That is
  acceptable while the contract is daily observed history.
- A late catalog run delays finance by up to two hours, then finance runs on
  the previous version and the on-call is warned. A sale of a product the
  catalog has not published yet gets the unknown `product_key` -1;
  `fact_sales` re-keys it on a later run inside its `pos_restatement_days`
  lookback.
- Renaming the DAG starts it with no run history on MWAA. The old
  `catalog_bihourly_product_scd2_refresh` entry remains in the Airflow UI until
  deleted there.
- Airflow 2.8.1 on MWAA has no combined time-and-dataset schedule, so the
  dependency is a sensor rather than a dataset trigger.

## Alternatives considered

- **Keep the two-hour schedule.** No demonstrated requirement for intraday
  attributes. Rejected.
- **Rely on the 15-minute gap.** Does not guarantee order. Rejected.
- **Fail finance when the catalog is late.** Blocks daily revenue reporting on
  a dimension that rarely changes. Rejected in favour of a warning.
- **Intraday change data capture.** Deferred until a stricter freshness or
  history requirement appears.
