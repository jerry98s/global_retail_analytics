{{ config(materialized='view') }}

/*
  Online checkout observations, one row per order_id.

  A checkout is what the clickstream saw, not a confirmed sale. Confirmed POS
  transactions are the financial record (int_rfm_scoring, fact_sales). POS
  Bronze has no order_id today, so no observation can be matched and every
  order is 'unconfirmed'. The status becomes a match on order_id once POS
  carries it (a POS contract change).

  Retries of one checkout share an order_id and collapse to one row.
*/

with checkouts as (
    select
        {{ json_path_text('properties', 'order_id') }} as order_id,
        {{ session_key() }} as session_id,
        event_time,
        cast({{ json_path_text('properties', 'cart_value') }} as decimal(14, 2)) as cart_value
    from {{ ref('stg_clickstream_events') }}
    where event_type = 'checkout'
),

orders as (
    select
        order_id,
        max(session_id) as session_id,
        min(event_time) as checkout_at,
        max(cart_value) as cart_value
    from checkouts
    where order_id is not null
    group by order_id
)

select
    o.order_id,
    o.session_id,
    s.customer_key,
    s.attribution_status,
    o.checkout_at,
    o.cart_value,
    'unconfirmed' as reconciliation_status
from orders o
left join {{ ref('int_session_reconstruction') }} s
  on s.session_id = o.session_id
