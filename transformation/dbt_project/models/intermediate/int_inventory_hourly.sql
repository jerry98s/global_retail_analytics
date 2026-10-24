{{ config(materialized='view') }}

/*
  Hourly inventory movements per (product, store): the input of
  fact_inventory_snapshot (ADR-012, lambda).

  - Batch hours (inside int_inventory_batch_coverage) are summed from Spark's
    canonical silver.inventory_events, rebuilt from the Bronze ledger with no
    watermark. Late scans are included and duplicates are removed across all
    history. is_estimated = false.
  - Every other hour comes from Flink silver.inventory_hourly, the speed
    layer. is_estimated = true: the hour is provisional until Spark covers it.

  The two ranges never overlap, so each (date, hour, product, store) has one
  row. Local Flink writes 1-minute windows that share the hour key; they are
  summed here.
*/

with coverage as (
    select batch_start_hour_key, batch_end_hour_key
    from {{ ref('int_inventory_batch_coverage') }}
),

batch_hourly as (
    select
        {{ date_key_from_date('event_time') }} as snapshot_date_key,
        cast(extract(hour from cast(event_time as timestamp)) as integer) as snapshot_hour,
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(sum(qty_delta) as numeric(38, 0)) as qty_delta_hour,
        cast(sum(case when qty_delta > 0 then qty_delta else 0 end) as numeric(38, 0))
            as qty_received_hour
    from {{ source('silver', 'inventory_events') }}
    group by
        {{ date_key_from_date('event_time') }},
        cast(extract(hour from cast(event_time as timestamp)) as integer),
        cast(product_id as varchar),
        cast(store_id as varchar)
),

speed_hourly as (
    select
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(sum(qty_delta_hour) as numeric(38, 0)) as qty_delta_hour,
        cast(sum(qty_received_hour) as numeric(38, 0)) as qty_received_hour
    from {{ source('silver', 'inventory_hourly') }}
    group by
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar),
        cast(store_id as varchar)
)

select
    b.snapshot_date_key,
    b.snapshot_hour,
    b.product_id,
    b.store_id,
    b.qty_delta_hour,
    b.qty_received_hour,
    false as is_estimated
from batch_hourly b
cross join coverage c
where b.snapshot_date_key * 100 + b.snapshot_hour >= c.batch_start_hour_key
  and b.snapshot_date_key * 100 + b.snapshot_hour < c.batch_end_hour_key

union all

select
    s.snapshot_date_key,
    s.snapshot_hour,
    s.product_id,
    s.store_id,
    s.qty_delta_hour,
    s.qty_received_hour,
    true as is_estimated
from speed_hourly s
cross join coverage c
where c.batch_start_hour_key is null
   or s.snapshot_date_key * 100 + s.snapshot_hour < c.batch_start_hour_key
   or s.snapshot_date_key * 100 + s.snapshot_hour >= c.batch_end_hour_key
