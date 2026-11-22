{{ config(materialized='table') }}

/*
  Hourly inventory movements per (product, store): the input of
  fact_inventory_snapshot (ADR-012).

  Spark canonical silver.inventory_events only, for hours strictly before
  the published cutoff (int_inventory_cutoff.closed_hour_exclusive). Flink's
  silver.inventory_hourly is not an input: it feeds the serving view, which
  adds hours at or after the cutoff on top of published Gold. There is no
  provisional row in this table, so is_estimated is always false. The column
  stays because dropping a live fact column aborts the WAP publish.

  A table, not a view: one query reads the events export. export_run_id is
  carried for lineage. The audit that the events, the coverage and the cutoff
  are one run reads those sources directly, before this aggregation
  (tests/inventory_canonical_run_is_one.sql).
*/

with cutoff as (
    select max(closed_hour_exclusive) as closed_hour_exclusive
    from {{ ref('int_inventory_cutoff') }}
),

events as (
    select
        {{ date_key_from_date('event_time') }} as snapshot_date_key,
        cast(extract(hour from cast(event_time as timestamp)) as integer) as snapshot_hour,
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        qty_delta,
        export_run_id
    from {{ source('silver', 'inventory_events') }}
)

select
    e.snapshot_date_key,
    e.snapshot_hour,
    e.product_id,
    e.store_id,
    cast(sum(e.qty_delta) as numeric(38, 0)) as qty_delta_hour,
    cast(sum(case when e.qty_delta > 0 then e.qty_delta else 0 end) as numeric(38, 0))
        as qty_received_hour,
    false as is_estimated,
    min(e.export_run_id) as export_run_id
from events e
cross join cutoff c
where c.closed_hour_exclusive is not null
  and e.snapshot_date_key * 100 + e.snapshot_hour < c.closed_hour_exclusive
group by
    e.snapshot_date_key,
    e.snapshot_hour,
    e.product_id,
    e.store_id
