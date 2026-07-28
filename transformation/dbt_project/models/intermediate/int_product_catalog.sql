{{
  config(
    materialized='view'
  )
}}

/*
  Current product attribute snapshot from POS (primary) plus inventory-only SKUs.
  Feeds dim_product SCD2 change detection via record_hash.
*/

with pos_products as (
    select
        cast(product_id as varchar) as product_id,
        min(transaction_date) as effective_from,
        max(transaction_date) as last_seen_date,
        cast(
            sum(net_revenue) / nullif(sum(quantity_sold), 0) as decimal(18, 4)
        ) as unit_cost
    from {{ ref('stg_pos_transactions') }}
    where not is_voided
    group by 1
),

inventory_only as (
    select
        cast(i.product_id as varchar) as product_id,
        min(cast(cast(i.event_time as timestamp) as date)) as effective_from,
        max(cast(cast(i.event_time as timestamp) as date)) as last_seen_date,
        cast(0 as decimal(18, 4)) as unit_cost
    from {{ ref('stg_inventory_events') }} i
    where cast(i.product_id as varchar) not in (select product_id from pos_products)
    group by 1
),

combined as (
    select * from pos_products
    union all
    select * from inventory_only
),

prepared as (
    select
        product_id,
        product_id as sku,
        product_id as product_name,
        case when unit_cost > 0 then 'PRIVATE_LABEL' else 'RETAIL' end as brand,
        'GENERAL' as category_l1,
        'MERCHANDISE' as category_l2,
        coalesce(unit_cost, cast(0 as decimal(18, 4))) as unit_cost,
        'UNKNOWN' as supplier_id,
        effective_from,
        last_seen_date
    from combined
)

select
    product_id,
    sku,
    product_name,
    brand,
    category_l1,
    category_l2,
    unit_cost,
    supplier_id,
    effective_from,
    last_seen_date,
    sha2(
        coalesce(product_id, '')
        || '|' || coalesce(brand, '')
        || '|' || coalesce(cast(unit_cost as varchar), ''),
        256
    ) as record_hash
from prepared
