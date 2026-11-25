{{
  config(
    materialized='view',
    schema='serving'
  )
}}

/*
  Current stock per (product_id, store_id): the speed-layer read (ADR-012).

  published_on_hand_qty is the latest balance in live
  finance.fact_inventory_snapshot, which holds only hours before the cutoff
  in live control.inventory_publication. Flink silver.inventory_hourly hours
  at or after that cutoff are added as provisional_qty_delta. The view never
  reads Spark's pending coverage, so a movement cannot disappear between a
  Spark run and the publish that records the cutoff.

  Before the first publication (no control row) every Flink hour is
  provisional on top of inventory_opening_balance.

  A view, so every query recomputes from the current tables. Freshness is
  bounded by the Flink window. A committed snapshot is visible on the next query.

  is_provisional: the balance includes a Flink hour at or after the cutoff.
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

cutoff as (
    select max(closed_hour_exclusive) as closed_hour_exclusive
    from {{ source('gold_control', 'inventory_publication') }}
),

speed_hours as (
    select
        cast(h.product_id as varchar) as product_id,
        cast(h.store_id as varchar) as store_id,
        h.snapshot_date_key * 100 + h.snapshot_hour as hour_key,
        h.qty_delta_hour
    from {{ source('silver', 'inventory_hourly') }} h
    cross join cutoff c
    where c.closed_hour_exclusive is null
       or h.snapshot_date_key * 100 + h.snapshot_hour >= c.closed_hour_exclusive
),

provisional as (
    select
        s.product_id,
        s.store_id,
        cast(sum(s.qty_delta_hour) as numeric(38, 0)) as qty_delta,
        max(s.hour_key) as hour_key
    from speed_hours s
    left join published pub
      on s.product_id = pub.product_id
     and s.store_id = pub.store_id
    cross join cutoff c
    where c.closed_hour_exclusive is null
       or pub.hour_key is null
       or s.hour_key > pub.hour_key
    group by s.product_id, s.store_id
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
        case
            when c.closed_hour_exclusive is null
                then coalesce(o.opening_qty, cast(0 as numeric(38, 0)))
            else coalesce(pub.quantity_on_hand, o.opening_qty, cast(0 as numeric(38, 0)))
        end as published_on_hand_qty,
        case when c.closed_hour_exclusive is null then null else pub.hour_key end
            as published_hour_key,
        coalesce(pr.qty_delta, cast(0 as numeric(38, 0))) as provisional_qty_delta,
        pr.hour_key as provisional_hour_key,
        c.closed_hour_exclusive is null
            or coalesce(pub.is_estimated, false)
            or pr.qty_delta is not null as is_provisional
    from pairs k
    cross join cutoff c
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
