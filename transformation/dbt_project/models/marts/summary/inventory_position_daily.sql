{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date_key', 'product_id', 'store_id'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Dense daily inventory position from finance.fact_inventory_snapshot only.

  Grain: (snapshot_date_key, product_id, store_id) for every product/store with
  an opening balance or any movement, on every day from the first to the last
  fact day. The fact only has rows for hours that moved, so a pair that did not
  move on a day still gets a row here carrying its previous balance (or its
  opening_qty before the first movement). This is the table to sum on-hand
  across products and stores for a given day.

  Keyed by product_id, not product_key: a product keeps one position across
  dim_product SCD2 versions.

  stockout_hour_count: hours of the day the balance was <= 0. A balance holds
  from its hour until the next movement hour, and the day starts with the
  previous day's ending balance.

  Incremental: the fact rewrites past hours when Spark replaces a Flink hour
  (ADR-012), and a changed hour moves every later day of that pair. Each run
  recomputes the full grid and upserts only rows that are new or differ from
  {{ this }}.
*/

with fact_hours as (
    select
        f.snapshot_date_key,
        f.snapshot_hour,
        cast(p.product_id as varchar) as product_id,
        cast(s.store_id as varchar) as store_id,
        f.quantity_on_hand
    from {{ ref('fact_inventory_snapshot') }} f
    inner join {{ wap_live_ref('dim_product') }} p
      on f.product_key = p.product_key
    inner join {{ source('gold_finance', 'dim_store') }} s
      on f.store_key = s.store_key
),

days as (
    select d.date_key as snapshot_date_key
    from {{ source('gold_finance', 'dim_date') }} d
    where d.date_key >= (select min(snapshot_date_key) from fact_hours)
      and d.date_key <= (select max(snapshot_date_key) from fact_hours)
),

opening as (
    select
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(opening_qty as numeric(38, 0)) as opening_qty
    from {{ ref('inventory_opening_balance') }}
),

pairs as (
    select
        coalesce(o.product_id, m.product_id) as product_id,
        coalesce(o.store_id, m.store_id) as store_id,
        coalesce(o.opening_qty, cast(0 as numeric(38, 0))) as opening_qty
    from (select distinct product_id, store_id from fact_hours) m
    full outer join opening o
      on m.product_id = o.product_id
     and m.store_id = o.store_id
),

day_end as (
    select snapshot_date_key, product_id, store_id, quantity_on_hand as day_end_qty
    from (
        select
            *,
            row_number() over (
                partition by snapshot_date_key, product_id, store_id
                order by snapshot_hour desc
            ) as rn_end
        from fact_hours
    ) ranked
    where rn_end = 1
),

grid as (
    select
        d.snapshot_date_key,
        p.product_id,
        p.store_id,
        p.opening_qty,
        e.day_end_qty,
        coalesce(
            last_value(e.day_end_qty ignore nulls) over (
                partition by p.product_id, p.store_id
                order by d.snapshot_date_key
                rows between unbounded preceding and current row
            ),
            p.opening_qty
        ) as ending_qty
    from days d
    cross join pairs p
    left join day_end e
      on d.snapshot_date_key = e.snapshot_date_key
     and p.product_id = e.product_id
     and p.store_id = e.store_id
),

carried as (
    select
        *,
        coalesce(
            lag(ending_qty) over (
                partition by product_id, store_id
                order by snapshot_date_key
            ),
            opening_qty
        ) as starting_qty
    from grid
),

segments as (
    select snapshot_date_key, product_id, store_id, 0 as segment_hour, 0 as segment_order, starting_qty as qty
    from carried
    union all
    select snapshot_date_key, product_id, store_id, snapshot_hour, 1, quantity_on_hand
    from fact_hours
),

held as (
    select
        snapshot_date_key,
        product_id,
        store_id,
        qty,
        coalesce(
            lead(segment_hour) over (
                partition by snapshot_date_key, product_id, store_id
                order by segment_hour, segment_order
            ),
            24
        ) - segment_hour as hours_held
    from segments
),

stockouts as (
    select
        snapshot_date_key,
        product_id,
        store_id,
        cast(sum(case when qty <= 0 then hours_held else 0 end) as integer) as stockout_hour_count
    from held
    group by snapshot_date_key, product_id, store_id
),

final as (
    select
        c.snapshot_date_key,
        c.product_id,
        c.store_id,
        s.store_key,
        cast(c.ending_qty as numeric(18, 4)) as ending_on_hand_qty,
        cast(greatest(c.ending_qty, 0) as numeric(18, 4)) as ending_available_qty,
        st.stockout_hour_count,
        (c.day_end_qty is not null) as had_movement
    from carried c
    inner join stockouts st
      on c.snapshot_date_key = st.snapshot_date_key
     and c.product_id = st.product_id
     and c.store_id = st.store_id
    left join {{ source('gold_finance', 'dim_store') }} s
      on c.store_id = s.store_id
)

select f.*
from final f
{% if is_incremental() %}
left join {{ this }} t
  on f.snapshot_date_key = t.snapshot_date_key
 and f.product_id = t.product_id
 and f.store_id = t.store_id
where t.snapshot_date_key is null
   or t.ending_on_hand_qty <> f.ending_on_hand_qty
   or t.stockout_hour_count <> f.stockout_hour_count
   or t.had_movement <> f.had_movement
{% endif %}
