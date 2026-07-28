{{
  config(
    materialized='incremental',
    unique_key='product_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  SCD Type 2 product dimension.

  - Source attributes: int_product_catalog (POS-weighted)
  - Closes prior version (effective_to, is_current=false) when record_hash changes
  - Inserts new current row with effective_from = catalog effective_from on change
*/

with source_products as (
    select * from {{ ref('int_product_catalog') }}
),

{% if is_incremental() %}

existing as (
    select * from {{ this }}
),

changed_products as (
    select
        s.product_id,
        s.effective_from as new_effective_from,
        s.sku,
        s.product_name,
        s.brand,
        s.category_l1,
        s.category_l2,
        s.unit_cost,
        s.supplier_id,
        s.record_hash
    from source_products s
    inner join existing e
      on s.product_id = e.product_id
     and e.is_current = true
     and s.record_hash != e.record_hash
),

closed_versions as (
    select
        e.product_key,
        e.product_id,
        e.sku,
        e.product_name,
        e.brand,
        e.category_l1,
        e.category_l2,
        e.unit_cost,
        e.supplier_id,
        e.effective_from,
        cp.new_effective_from as effective_to,
        false as is_current,
        e.record_hash
    from existing e
    inner join changed_products cp
      on e.product_id = cp.product_id
     and e.is_current = true
),

unchanged_history as (
    select e.*
    from existing e
    left join changed_products cp
      on e.product_id = cp.product_id
     and e.is_current = true
    where not (e.is_current = true and cp.product_id is not null)
),

new_and_updated_current as (
    select
        {{ generate_product_key('s.product_id', 's.effective_from') }} as product_key,
        s.product_id,
        s.sku,
        s.product_name,
        s.brand,
        s.category_l1,
        s.category_l2,
        s.unit_cost,
        s.supplier_id,
        s.effective_from,
        cast(null as date) as effective_to,
        true as is_current,
        s.record_hash
    from source_products s
    left join existing e
      on s.product_id = e.product_id
     and e.is_current = true
     and s.record_hash = e.record_hash
    where e.product_key is null
)

select * from closed_versions
union all
select * from unchanged_history
union all
select * from new_and_updated_current

{% else %}

initial_load as (
    select
        {{ generate_product_key('s.product_id', 's.effective_from') }} as product_key,
        s.product_id,
        s.sku,
        s.product_name,
        s.brand,
        s.category_l1,
        s.category_l2,
        s.unit_cost,
        s.supplier_id,
        s.effective_from,
        cast(null as date) as effective_to,
        true as is_current,
        s.record_hash
    from source_products s
)

select * from initial_load

{% endif %}
