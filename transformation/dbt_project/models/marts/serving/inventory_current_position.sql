{{
  config(
    materialized='view',
    schema='serving'
  )
}}

/*
  Current stock per (product_id, store_id): the speed-layer read (ADR-012).

  published_on_hand_qty is the latest balance in live
  finance.fact_inventory_snapshot (rebuilt by the warehouse DAG). Flink
  silver.inventory_hourly hours after the fact's newest hour are added on top as
  provisional_qty_delta, so this view moves as soon as a new Flink hour is
  exported, without waiting for the daily rebuild. The next rebuild replaces
  those hours with Spark-canonical ones where Spark covers them.

  A view, so every query recomputes from the current tables. Freshness is
  bounded by the Flink window (1 hour) plus the inventory_hourly export.

  is_provisional: the balance includes a Flink hour that Spark has not yet
  confirmed, either after the fact or as the fact's latest row.
  Pairs with no fact row start from inventory_opening_balance.
*/

with fact_hours as (
    select
        cast(p.product_id as varchar) as product_id,
        cast(s.store_id as varchar) as store_id,
        f.snapshot_date_key * 100 + f.snapshot_hour as hour_key,
        f.quantity_on_hand,
        f.is_estimated
    from {{ ref('fact_inventory_snapshot') }} f
    inner join {{ wap_live_ref('dim_product') }} p
      on f.product_key = p.product_key
    inner join {{ source('gold_finance', 'dim_store') }} s
      on f.store_key = s.store_key
),

published as (
    select product_id, store_id, hour_key, quantity_on_hand, is_estimated
    from (
        select
            *,
            row_number() over (
                partition by product_id, store_id
                order by hour_key desc
            ) as rn
        from fact_hours
    ) ranked
    where rn = 1
),

published_through as (
    select max(hour_key) as hour_key
    from fact_hours
),

provisional as (
    select
        cast(h.product_id as varchar) as product_id,
        cast(h.store_id as varchar) as store_id,
        cast(sum(h.qty_delta_hour) as numeric(38, 0)) as qty_delta,
        max(h.snapshot_date_key * 100 + h.snapshot_hour) as hour_key
    from {{ source('silver', 'inventory_hourly') }} h
    cross join published_through t
    where t.hour_key is null
       or h.snapshot_date_key * 100 + h.snapshot_hour > t.hour_key
    group by cast(h.product_id as varchar), cast(h.store_id as varchar)
),

opening as (
    select
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(opening_qty as numeric(38, 0)) as opening_qty
    from {{ ref('inventory_opening_balance') }}
),

pairs as (
    select product_id, store_id from published
    union
    select product_id, store_id from provisional
    union
    select product_id, store_id from opening
),

positioned as (
    select
        k.product_id,
        k.store_id,
        coalesce(pub.quantity_on_hand, o.opening_qty, cast(0 as numeric(38, 0)))
            as published_on_hand_qty,
        pub.hour_key as published_hour_key,
        coalesce(pr.qty_delta, cast(0 as numeric(38, 0))) as provisional_qty_delta,
        pr.hour_key as provisional_hour_key,
        coalesce(pub.is_estimated, false) or pr.qty_delta is not null as is_provisional
    from pairs k
    left join published pub
      on k.product_id = pub.product_id
     and k.store_id = pub.store_id
    left join provisional pr
      on k.product_id = pr.product_id
     and k.store_id = pr.store_id
    left join opening o
      on k.product_id = o.product_id
     and k.store_id = o.store_id
)

select
    p.product_id,
    p.store_id,
    s.store_key,
    cast(p.published_on_hand_qty as numeric(18, 4)) as published_on_hand_qty,
    cast(p.provisional_qty_delta as numeric(18, 4)) as provisional_qty_delta,
    cast(p.published_on_hand_qty + p.provisional_qty_delta as numeric(18, 4))
        as current_on_hand_qty,
    cast(greatest(p.published_on_hand_qty + p.provisional_qty_delta, 0) as numeric(18, 4))
        as current_available_qty,
    p.published_hour_key,
    coalesce(p.provisional_hour_key, p.published_hour_key) as as_of_hour_key,
    p.is_provisional
from positioned p
left join {{ source('gold_finance', 'dim_store') }} s
  on p.store_id = s.store_id
