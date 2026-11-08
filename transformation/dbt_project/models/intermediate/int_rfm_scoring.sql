{{ config(materialized='table') }}

/*
  RFM scoring by customer_key from confirmed POS transactions only.

  Online checkout observations are not revenue until reconciled to a POS
  transaction (int_order_reconciliation), so they are not in monetary_value
  or frequency.

  Scores run 1 (worst) to 5 (best): most recent, most frequent and highest
  spend get 5. Recency is measured from the latest non-voided transaction
  date in the data, not the run date, so a replay scores the same.
  Rebuilt in full: scores are relative across all customers, and customer
  keys that leave the identity graph must leave this table.
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

as_of as (
    select max(transaction_date) as as_of_date
    from {{ ref('stg_pos_transactions') }}
    where not coalesce(is_voided, false)
),

scored as (
    select
        s.customer_key,
        {{ datediff_unit('day', 's.last_purchase_date', 'a.as_of_date') }} as recency_days,
        s.frequency_orders,
        s.monetary_value,
        ntile(5) over (order by s.last_purchase_date asc, s.customer_key) as r_score,
        ntile(5) over (order by s.frequency_orders asc, s.customer_key) as f_score,
        ntile(5) over (order by s.monetary_value asc, s.customer_key) as m_score
    from pos_sales s
    cross join as_of a
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
