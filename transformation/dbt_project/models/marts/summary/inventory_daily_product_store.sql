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

  Incremental: the fact rewrites past hours when Spark replaces a Flink hour
  (ADR-012), so any earlier day can change. Each run rolls up every day and
  upserts only rows that are new or differ from {{ this }}.
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
),

ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date_key, product_key, store_key
            order by snapshot_hour desc
        ) as rn_end
    from hourly
),

daily as (
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
)

select
    d.snapshot_date_key,
    d.product_key,
    d.store_key,
    d.ending_on_hand_qty,
    d.ending_available_qty,
    d.minimum_available_qty,
    d.stockout_hour_count
from daily d
{% if is_incremental() %}
left join {{ this }} t
  on d.snapshot_date_key = t.snapshot_date_key
 and d.product_key = t.product_key
 and d.store_key = t.store_key
where t.snapshot_date_key is null
   or t.ending_on_hand_qty <> d.ending_on_hand_qty
   or t.ending_available_qty <> d.ending_available_qty
   or t.minimum_available_qty <> d.minimum_available_qty
   or t.stockout_hour_count <> d.stockout_hour_count
{% endif %}
