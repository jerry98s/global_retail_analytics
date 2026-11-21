{{
  config(
    materialized='incremental',
    unique_key=['date_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='product_key',
    sort='date_key',
    pre_hook="{{ pos_clear_restated_days() }}"
  )
}}

/*
  Returns fact at return-line grain (ADR-015).

  Grain: one row per (return_id, return_line_number).
  date_key / store_key are the return processing date and store: a refund
  counts there, not on the original sale. The original sale is referenced by
  (original_transaction_id, original_line_item_number) and never changed.
  Joins: stg_pos_transactions for the original line (staging, not
         fact_sales: no fact-to-fact joins), dim_product CURRENT version via
         wap_live_ref (-1 until the catalog publishes it), dim_store.

  eligibility_status:
    ELIGIBLE      original line exists and was not voided
    VOIDED_SALE   original line was voided; kept for audit, no refund
    UNKNOWN_SALE  no original line sold within pos_return_window_days on or
                  before this return's own date (bounds are per return, so
                  a full build, a replay and a one-day rebuild agree)
    PRODUCT_MISMATCH  the original line sold a different product
  The last two block the publish via
  tests/pos_returns_stay_within_original_sale.sql.
  is_refund_recognized: COMPLETED and ELIGIBLE. Rejected returns keep their
  row with no financial impact.

  Incremental strategy: same lookback and explicit restatement vars as
  fact_sales, replacing each reprocessed return day whole. An explicit
  restatement clears its whole range first (pos_clear_restated_days), since a
  zero-row returns file means the day has no returns. It also rebuilds every
  later return day holding a return of a sale line in the range, because a
  corrected or voided sale changes that refund's eligibility; those days are
  reloaded whole so delete+insert keeps their other returns.
*/

{% set restate = pos_restate_range() %}
{% set window_days = var('pos_return_window_days', 90) %}
{% set lookback %}
          select cast({{ dateadd_unit(
              'day',
              -var('pos_restatement_days', 3),
              "coalesce(max(" ~ date_from_date_key('date_key') ~ "), cast('1970-01-04' as date))"
          ) }} as date)
          from {{ this }}
{% endset %}

with
{% if is_incremental() and restate %}
{% set restate_from = "cast('" ~ restate.from_date ~ "' as date)" %}
{% set restate_to = "cast('" ~ restate.to_date ~ "' as date)" %}
{% set returns_to = "cast('" ~ restate.returns_to_date ~ "' as date)" %}
-- Return days holding a refund of a restated sale line: matched in the
-- corrected source, or matched before to a line the correction removed.
rebuild_days as (
    select r.return_date
    from {{ ref('stg_pos_returns') }} r
    join {{ ref('stg_pos_transactions') }} p
      on p.transaction_id = r.original_transaction_id
     and p.line_item_number = r.original_line_item_number
    where p.transaction_date between {{ restate_from }} and {{ restate_to }}
      and p.dt between {{ restate_from }} and {{ restate_to }}
      and r.return_date between {{ restate_from }} and {{ returns_to }}
      and r.dt between {{ restate_from }} and {{ returns_to }}
    union
    select {{ date_from_date_key('date_key') }}
    from {{ this }}
    where original_date_key between {{ restate.from_key }} and {{ restate.to_key }}
),

{% endif %}
returns as (
    select *
    from {{ ref('stg_pos_returns') }}
    {% if is_incremental() and restate %}
      where (
              return_date between {{ restate_from }} and {{ restate_to }}
              and dt between {{ restate_from }} and {{ restate_to }}
            )
         or (
              return_date in (select return_date from rebuild_days)
              and dt between {{ restate_from }} and {{ returns_to }}
            )
    {% elif is_incremental() %}
      where return_date >= ( {{ lookback }} )
        and dt >= ( {{ lookback }} )
    {% endif %}
),

-- Prunes the sale scan for the whole batch; each return is then matched only
-- within its own window in the join below.
return_window as (
    select
        cast({{ dateadd_unit('day', -window_days, 'min(return_date)') }} as date) as first_sale_date,
        max(return_date) as last_sale_date
    from returns
),

originals as (
    select
        p.transaction_id,
        p.line_item_number,
        p.transaction_date,
        p.product_id,
        p.is_voided
    from {{ ref('stg_pos_transactions') }} p
    cross join return_window w
    where p.dt between w.first_sale_date and w.last_sale_date
      and p.transaction_date between w.first_sale_date and w.last_sale_date
),

checked as (
    select
        r.*,
        o.transaction_date as original_transaction_date,
        case
            when o.transaction_id is null then 'UNKNOWN_SALE'
            when o.product_id <> r.product_id then 'PRODUCT_MISMATCH'
            when o.is_voided then 'VOIDED_SALE'
            else 'ELIGIBLE'
        end as eligibility_status
    from returns r
    left join originals o
      on o.transaction_id = r.original_transaction_id
     and o.line_item_number = r.original_line_item_number
     and o.transaction_date between cast({{ dateadd_unit('day', -window_days, 'r.return_date') }} as date)
                                and r.return_date
)

select
    {{ date_key_from_date('c.return_date') }} as date_key,
    coalesce(p.product_key, -1) as product_key,
    s.store_key,
    {{ date_key_from_date('c.original_transaction_date') }} as original_date_key,
    c.return_id,
    c.return_line_number,
    c.original_transaction_id,
    c.original_line_item_number,
    c.quantity_returned,
    c.refund_amount,
    c.return_status,
    c.eligibility_status,
    (c.return_status = 'COMPLETED' and c.eligibility_status = 'ELIGIBLE') as is_refund_recognized,
    c.reason_code,
    c.processed_at
from checked c
left join {{ wap_live_ref('dim_product') }} p
  on c.product_id = p.product_id
 and p.is_current = true
left join {{ source('gold_finance', 'dim_store') }} s
  on c.return_store_id = s.store_id
