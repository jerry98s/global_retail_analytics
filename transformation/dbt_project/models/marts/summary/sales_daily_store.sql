{{
  config(
    materialized='incremental',
    unique_key=['date_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    pre_hook="{{ pos_clear_empty_days() }}",
    post_hook="delete from {{ this }} where transaction_count = 0 and return_transaction_count = 0"
  )
}}

/*
  Daily store sales rollup from finance.fact_sales and finance.fact_returns.

  Grain: (date_key, store_key)
  The one summary built from two facts (ADR-015): each fact is aggregated to
  (date_key, store_key) on its own and the two rollups are full-outer-joined.
  Sales and returns are never joined line to line.

  Sales measures exclude voided lines: a line void or a transaction void
  (fact_sales.is_voided, ADR-015). A fully voided transaction is not counted.
  transaction_count and net_revenue stay completed sales before returns.
  Return measures count only recognized refunds (completed, of a non-voided
  sale), on the return's processing date and store. A store-day with returns
  and no sales keeps a row with zero sales measures.
  net_sales_after_returns = net_revenue - return_amount.

  Incremental: reprocess the same pos_restatement_days window as fact_sales
  and replace each reprocessed day whole (delete+insert on date_key). Every
  reprocessed store-day is emitted, voided lines and unrecognized returns
  included, so a day whose lines all became voided still reaches
  delete+insert; it arrives with both counts 0 and the post_hook removes it,
  leaving no row for a store-day without sales or refunds.
  pos_restate_from / pos_restate_to switch the window to that explicit date
  range plus pos_return_window_days after it, where fact_returns may have
  re-judged refunds of the restated sales; both facts are re-aggregated for
  every day in that span. Days in pos_restate_empty_dates are deleted first.
  A table built before the return columns existed (no return_amount in
  {{ this }}) re-aggregates every day once, so no older row keeps NULL return
  measures after append_new_columns adds them.
*/

{% set restate = pos_restate_range() %}
{% set needs_return_backfill = is_incremental() and execute and 'return_amount' not in (
    adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list
) %}
{% set window_filter %}
    {% if needs_return_backfill %}
    {% elif is_incremental() and restate %}
      where date_key between {{ restate.from_key }} and {{ restate.returns_to_key }}
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
{% endset %}

with sales as (
    select
        date_key,
        store_key,
        count(distinct case when not is_voided then transaction_id end) as transaction_count,
        sum(case when not is_voided then quantity_sold end) as units_sold,
        sum(case when not is_voided then gross_revenue end) as gross_revenue,
        sum(case when not is_voided then net_revenue end) as net_revenue,
        sum(case when not is_voided then gross_margin end) as gross_margin
    from {{ ref('fact_sales') }}
    {{ window_filter }}
    group by date_key, store_key
),

returns as (
    select
        date_key,
        store_key,
        count(distinct case when is_refund_recognized then return_id end) as return_transaction_count,
        sum(case when is_refund_recognized then refund_amount end) as return_amount
    from {{ ref('fact_returns') }}
    {{ window_filter }}
    group by date_key, store_key
),

combined as (
    select
        coalesce(s.date_key, r.date_key) as date_key,
        coalesce(s.store_key, r.store_key) as store_key,
        coalesce(s.transaction_count, 0) as transaction_count,
        cast(coalesce(s.units_sold, 0) as numeric(18, 4)) as units_sold,
        cast(coalesce(s.gross_revenue, 0) as numeric(18, 4)) as gross_revenue,
        cast(coalesce(s.net_revenue, 0) as numeric(18, 4)) as net_revenue,
        cast(coalesce(s.gross_margin, 0) as numeric(18, 4)) as gross_margin,
        coalesce(r.return_transaction_count, 0) as return_transaction_count,
        cast(coalesce(r.return_amount, 0) as numeric(18, 4)) as return_amount
    from sales s
    full outer join returns r
      on s.date_key = r.date_key
     and s.store_key = r.store_key
)

select
    date_key,
    store_key,
    transaction_count,
    units_sold,
    gross_revenue,
    net_revenue,
    gross_margin,
    return_transaction_count,
    return_amount,
    cast(net_revenue - return_amount as numeric(18, 4)) as net_sales_after_returns
from combined
