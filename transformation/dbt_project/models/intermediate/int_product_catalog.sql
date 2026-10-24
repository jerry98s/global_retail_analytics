{{
  config(
    materialized='view'
  )
}}

/*
  Current product attribute snapshot for dim_product SCD2 change detection.

  - Product set: every product_id observed in POS (non-voided) or inventory.
  - Attributes: reference.product_master (catalog source of truth). Products
    missing from the master keep their id and get UNKNOWN / NULL attributes
    rather than disappearing, so fact joins never lose a product_key.
  - record_hash covers catalog attributes only. Never hash values derived
    from sales (e.g. average selling price): they move with every new sale
    and would open a new SCD2 version for nearly every product each day.
*/

with pos_activity as (
    select
        cast(product_id as varchar) as product_id,
        min(transaction_date) as first_seen_date,
        max(transaction_date) as last_seen_date
    from {{ ref('stg_pos_transactions') }}
    where not is_voided
    group by 1
),

inventory_activity as (
    select
        cast(product_id as varchar) as product_id,
        min(cast(cast(event_time as timestamp) as date)) as first_seen_date,
        max(cast(cast(event_time as timestamp) as date)) as last_seen_date
    from {{ ref('stg_inventory_events') }}
    group by 1
),

activity as (
    select
        product_id,
        min(first_seen_date) as effective_from,
        max(last_seen_date) as last_seen_date
    from (
        select * from pos_activity
        union all
        select * from inventory_activity
    ) observed
    group by product_id
),

prepared as (
    select
        a.product_id,
        coalesce(m.sku, a.product_id) as sku,
        coalesce(m.product_name, a.product_id) as product_name,
        coalesce(m.brand, 'UNKNOWN') as brand,
        coalesce(m.category_l1, 'UNKNOWN') as category_l1,
        coalesce(m.category_l2, 'UNKNOWN') as category_l2,
        cast(m.unit_cost as decimal(18, 4)) as unit_cost,
        coalesce(m.supplier_id, 'UNKNOWN') as supplier_id,
        a.effective_from,
        a.last_seen_date
    from activity a
    left join {{ ref('product_master') }} m
      on a.product_id = m.product_id
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
    {{ record_hash_sha256(
        "coalesce(sku, '')"
        ~ " || '|' || coalesce(product_name, '')"
        ~ " || '|' || coalesce(brand, '')"
        ~ " || '|' || coalesce(category_l1, '')"
        ~ " || '|' || coalesce(category_l2, '')"
        ~ " || '|' || coalesce(cast(unit_cost as varchar), '')"
        ~ " || '|' || coalesce(supplier_id, '')"
    ) }} as record_hash
from prepared
