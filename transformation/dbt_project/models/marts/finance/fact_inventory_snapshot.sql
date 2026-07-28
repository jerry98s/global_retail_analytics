{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date_key', 'snapshot_hour', 'product_key', 'store_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Hourly inventory fact with running on-hand balance per product/store.

  Kappa architecture: reads from silver.inventory_hourly (Flink
  inventory_silver_job output) rather than re-aggregating from
  bronze.inventory_events. Silver is the authoritative stream-output layer;
  this mart only adds the running-balance window and surrogate-key joins to
  dim_product and dim_store. See ADR-002 (and the kappa-conversion note in
  docs/REDSHIFT.md).

  quantity_on_hand: cumulative sum of silver.qty_delta_hour across hours
                    within (product_id, store_id), ordered by
                    (snapshot_date_key, snapshot_hour).
  quantity_available: max(quantity_on_hand, 0) — floor at zero so a temporary
                    over-shipping delta doesn't read as a negative shelf count.

  Incremental strategy: the running-balance window depends on full per-partition
  history, so we scan all silver rows to recompute balances, then upsert only
  rows with (snapshot_date_key, snapshot_hour) greater than the max already in
  {{ this }}. Crash-resilient: if a run dies after a partial insert, the next
  run recomputes from the max committed snapshot, and `delete+insert` on the
  composite `unique_key` makes the partial rows idempotent.
*/

with silver_inventory as (
    -- Roll up to the declared hour grain. Cloud silver uses 1-hour TUMBLE
    -- windows (one row per grain). Local short sims use 1-minute windows that
    -- still emit snapshot_hour = EXTRACT(HOUR ...), so multiple minute buckets
    -- share the same hour key and must be summed here.
    select
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(sum(qty_delta_hour) as numeric(38, 0)) as qty_delta_hour,
        cast(sum(qty_received_hour) as numeric(38, 0)) as qty_received_hour,
        (max(case when is_estimated then 1 else 0 end) = 1) as is_estimated
    from {{ source('silver', 'inventory_hourly') }}
    group by
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar),
        cast(store_id as varchar)
),

with_running_balance as (
    select
        snapshot_date_key,
        snapshot_hour,
        product_id,
        store_id,
        qty_delta_hour,
        qty_received_hour,
        is_estimated,
        sum(qty_delta_hour) over (
            partition by product_id, store_id
            order by snapshot_date_key, snapshot_hour
            rows unbounded preceding
        ) as quantity_on_hand
    from silver_inventory
),

-- Single integer that sorts the same way (date_key, hour) does, so a partial
-- insert cannot cause the next run to skip hours between max(date_key) and
-- max(hour) on a previous date.
new_or_changed as (
    select *
    from with_running_balance
    {% if is_incremental() %}
      where (snapshot_date_key * 100 + snapshot_hour) > (
          select coalesce(max(snapshot_date_key * 100 + snapshot_hour), 0)
          from {{ this }}
      )
    {% endif %}
)

select
    f.snapshot_date_key,
    f.snapshot_hour,
    p.product_key,
    s.store_key,
    f.quantity_on_hand,
    greatest(f.quantity_on_hand, 0) as quantity_available,
    f.is_estimated
from new_or_changed f
left join {{ ref('dim_product') }} p
  on f.product_id = p.product_id
 and p.is_current = true
left join {{ source('gold_finance', 'dim_store') }} s
  on f.store_id = s.store_id
