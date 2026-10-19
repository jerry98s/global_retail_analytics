{{
  config(
    materialized='incremental',
    unique_key=['date_key', 'store_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Daily store sales rollup from finance.fact_sales only (no fact-to-fact joins).

  Grain: (date_key, store_key)
  Measures exclude voided lines.
  Incremental: reprocess from max(date_key) already in {{ this }}.
*/

with source_sales as (
    select
        date_key,
        store_key,
        transaction_id,
        quantity_sold,
        gross_revenue,
        net_revenue,
        gross_margin,
        is_voided
    from {{ ref('fact_sales') }}
    {% if is_incremental() %}
      where date_key >= (
          select coalesce(max(date_key), 0)
          from {{ this }}
      )
    {% endif %}
),

non_voided as (
    select *
    from source_sales
    where coalesce(is_voided, false) = false
)

select
    date_key,
    store_key,
    count(distinct transaction_id) as transaction_count,
    cast(sum(quantity_sold) as numeric(18, 4)) as units_sold,
    cast(sum(gross_revenue) as numeric(18, 4)) as gross_revenue,
    cast(sum(net_revenue) as numeric(18, 4)) as net_revenue,
    cast(sum(gross_margin) as numeric(18, 4)) as gross_margin
from non_voided
group by date_key, store_key
