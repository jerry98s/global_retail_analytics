{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date_key', 'product_key', 'store_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Daily product/store inventory rollup from finance.fact_inventory_snapshot only.

  Grain: (snapshot_date_key, product_key, store_key)
  ending_* taken from the latest hour of the day; stockout hours count hours
  where quantity_available <= 0.
*/

with hourly as (
    select
        snapshot_date_key,
        snapshot_hour,
        product_key,
        store_key,
        quantity_on_hand,
        quantity_available
    from {{ ref('fact_inventory_snapshot') }}
    {% if is_incremental() %}
      where snapshot_date_key >= (
          select coalesce(max(snapshot_date_key), 0)
          from {{ wap_prior_state() }}
      )
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date_key, product_key, store_key
            order by snapshot_hour desc
        ) as rn_end
    from hourly
)

select
    snapshot_date_key,
    product_key,
    store_key,
    cast(
        max(case when rn_end = 1 then quantity_on_hand end) as numeric(18, 4)
    ) as ending_on_hand_qty,
    cast(
        max(case when rn_end = 1 then quantity_available end) as numeric(18, 4)
    ) as ending_available_qty,
    cast(min(quantity_available) as numeric(18, 4)) as minimum_available_qty,
    cast(
        sum(case when quantity_available <= 0 then 1 else 0 end) as integer
    ) as stockout_hour_count
from ranked
group by snapshot_date_key, product_key, store_key
