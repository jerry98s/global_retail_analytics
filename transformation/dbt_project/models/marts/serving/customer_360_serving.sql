{{
  config(
    materialized='view',
    schema='serving'
  )
}}

/*
  Customer-grain serving view: one row per consented customer.
  marketing.customer_360_view is customer x session; this view rolls the
  sessions up so BI and the dashboard can count customers directly.
  Customers with no sessions appear with session_count = 0.
*/

select
    customer_key,
    loyalty_id,
    loyalty_tier,
    rfm_segment,
    churn_risk_score,
    total_lifetime_value,
    count(session_id) as session_count,
    sum(case when converted then 1 else 0 end) as converted_session_count,
    max(session_date_key) as last_session_date_key
from {{ ref('customer_360_view') }}
group by
    customer_key,
    loyalty_id,
    loyalty_tier,
    rfm_segment,
    churn_risk_score,
    total_lifetime_value
