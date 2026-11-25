-- Each stored balance equals the opening count plus every movement stored up
-- to and including its hour. A failure means a balance was rewritten without
-- its movement (or the reverse), so the fact no longer explains itself.
with recomputed as (
    select
        f.snapshot_date_key,
        f.snapshot_hour,
        f.product_id,
        f.store_key,
        f.qty_delta_hour,
        f.quantity_on_hand,
        coalesce(cast(o.opening_qty as numeric(38, 0)), 0)
        + sum(f.qty_delta_hour) over (
            partition by f.product_id, f.store_key
            order by f.snapshot_date_key, f.snapshot_hour
            rows unbounded preceding
        ) as expected_on_hand
    from {{ ref('fact_inventory_snapshot') }} f
    inner join {{ source('gold_finance', 'dim_store') }} s
      on f.store_key = s.store_key
    left join {{ ref('inventory_opening_balance') }} o
      on f.product_id = o.product_id
     and s.store_id = o.store_id
)

select *
from recomputed
where qty_delta_hour is null
   or quantity_on_hand <> expected_on_hand
