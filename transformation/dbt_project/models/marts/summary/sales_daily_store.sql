{{
  config(
    materialized='incremental',
    unique_key=['date_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Daily store sales rollup from finance.fact_sales only (no fact-to-fact joins).

  Grain: (date_key, store_key)
  Measures exclude voided lines.
  Incremental: reprocess the same pos_restatement_days window as fact_sales
  and replace each reprocessed day whole (delete+insert on date_key), so a
  store-day whose lines all became voided or were removed is deleted rather
  than left with its old totals. pos_restate_from / pos_restate_to switch
  the window to that explicit date range, matching fact_sales.
*/

{% set restate = pos_restate_range() %}

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
    {% if is_incremental() and restate %}
      where date_key between {{ restate.from_key }} and {{ restate.to_key }}
    {% elif is_incremental() %}
      where date_key >= (
          select coalesce({{ date_key_from_date(dateadd_unit(
              'day',
              -var('pos_restatement_days', 3),
              date_from_date_key('max(date_key)')
          )) }}, 0)
          from {{ this }}
      )
    {% endif %}
),

non_voided as (
    select *
    from source_sales
    where not is_voided
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
