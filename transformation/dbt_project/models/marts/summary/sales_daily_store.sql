{{
  config(
    materialized='incremental',
    unique_key=['date_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    pre_hook="{{ pos_clear_empty_days() }}",
    post_hook="delete from {{ this }} where transaction_count = 0"
  )
}}

/*
  Daily store sales rollup from finance.fact_sales only (no fact-to-fact joins).

  Grain: (date_key, store_key)
  Measures exclude voided lines.
  Incremental: reprocess the same pos_restatement_days window as fact_sales
  and replace each reprocessed day whole (delete+insert on date_key). Every
  reprocessed store-day is emitted, voided lines included, so a day whose
  lines all became voided still reaches delete+insert; it arrives with
  transaction_count = 0 and the post_hook removes it, leaving no row for a
  store-day without sales. pos_restate_from / pos_restate_to switch the
  window to that explicit date range, matching fact_sales, and days in
  pos_restate_empty_dates are deleted first.
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
)

select
    date_key,
    store_key,
    count(distinct case when not is_voided then transaction_id end) as transaction_count,
    cast(sum(case when not is_voided then quantity_sold end) as numeric(18, 4)) as units_sold,
    cast(sum(case when not is_voided then gross_revenue end) as numeric(18, 4)) as gross_revenue,
    cast(sum(case when not is_voided then net_revenue end) as numeric(18, 4)) as net_revenue,
    cast(sum(case when not is_voided then gross_margin end) as numeric(18, 4)) as gross_margin
from source_sales
group by date_key, store_key
