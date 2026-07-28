{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  RFM scoring by customer_key.

  Monetary / frequency / recency combine:
    - POS: stg_pos_transactions via loyalty_id → int_identity_resolution
    - Clickstream: converted sessions from int_session_reconstruction
      (conversion_value = checkout cart_value, frequency = distinct order_id
      or session_id when order_id is null)

  Omnichannel note: POS net_revenue and online cart_value are summed. If the
  same physical sale appears in both sources, monetary_value can double-count.
  Prefer POS as the finance source of truth; clickstream conversion fills gaps
  for customers who convert online without a linked POS loyalty ticket.
*/

with pos_sales as (
    select
        i.customer_key,
        max(p.transaction_date) as last_purchase_date,
        count(distinct p.transaction_id) as frequency_orders,
        cast(sum(p.net_revenue) as decimal(18, 2)) as monetary_value
    from {{ ref('stg_pos_transactions') }} p
    join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = 'loyalty_id'
     and i.identifier_value = p.loyalty_id
    where not coalesce(p.is_voided, false)
    group by i.customer_key
),

online_conversions as (
    select
        s.customer_key,
        max(cast(s.session_start_time as date)) as last_purchase_date,
        count(distinct coalesce(nullif(s.order_id, ''), s.session_id)) as frequency_orders,
        cast(sum(coalesce(s.conversion_value, 0)) as decimal(18, 2)) as monetary_value
    from {{ ref('int_session_reconstruction') }} s
    where s.converted
      and s.customer_key is not null
    group by s.customer_key
),

sales as (
    select
        coalesce(p.customer_key, o.customer_key) as customer_key,
        case
            when p.last_purchase_date is null then o.last_purchase_date
            when o.last_purchase_date is null then p.last_purchase_date
            when p.last_purchase_date >= o.last_purchase_date then p.last_purchase_date
            else o.last_purchase_date
        end as last_purchase_date,
        coalesce(p.frequency_orders, 0) + coalesce(o.frequency_orders, 0) as frequency_orders,
        coalesce(p.monetary_value, 0) + coalesce(o.monetary_value, 0) as monetary_value
    from pos_sales p
    full outer join online_conversions o
      on p.customer_key = o.customer_key
),

scored as (
    select
        customer_key,
        {{ datediff_unit('day', 'last_purchase_date', 'current_date') }} as recency_days,
        frequency_orders,
        monetary_value,
        ntile(5) over (order by {{ datediff_unit('day', 'last_purchase_date', 'current_date') }} asc) as r_score,
        ntile(5) over (order by frequency_orders desc) as f_score,
        ntile(5) over (order by monetary_value desc) as m_score
    from sales
    where frequency_orders > 0
       or monetary_value > 0
)

select
    customer_key,
    recency_days,
    frequency_orders,
    monetary_value,
    r_score,
    f_score,
    m_score,
    cast(r_score as varchar) || cast(f_score as varchar) || cast(m_score as varchar) as rfm_code,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'champions'
        when r_score >= 3 and f_score >= 3 then 'loyal'
        when r_score <= 2 and f_score >= 3 then 'at_risk'
        else 'general'
    end as rfm_segment
from scored
