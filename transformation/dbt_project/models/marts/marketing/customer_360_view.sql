{{ config(materialized='view') }}

/*
  Canonical Customer 360 view (consent-gated).
  serving.customer_360_serving selects from this view — keep columns aligned.
*/

select
    c.customer_key,
    c.loyalty_id,
    c.loyalty_tier,
    c.rfm_segment,
    c.churn_risk_score,
    c.total_lifetime_value,
    s.session_id,
    s.session_date_key,
    s.session_duration_seconds,
    s.page_view_count,
    s.product_view_count,
    s.add_to_cart_count,
    s.converted,
    s.order_id,
    s.platform
from {{ ref('dim_customer') }} c
left join {{ ref('fact_customer_session') }} s
  on c.customer_key = s.customer_key
where c.marketing_consent = true
