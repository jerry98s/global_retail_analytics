{{ config(materialized='table') }}

/*
  Sessions by attribution, from marketing.fact_customer_session only.

  Grain: (session_date_key, platform, attribution_status)
  Rebuilt in full each run because fact_customer_session restates sessions of
  any age. session_count includes anonymous and ambiguous sessions, so a
  conversion rate summed from this table keeps them in the denominator.
  The stage counts are sessions that reached that step, not event counts.
*/

select
    session_date_key,
    platform,
    attribution_status,
    count(*) as session_count,
    cast(sum(case when converted then 1 else 0 end) as integer) as converted_sessions,
    cast(sum(case when page_view_count > 0 then 1 else 0 end) as integer) as page_view_sessions,
    cast(sum(case when product_view_count > 0 then 1 else 0 end) as integer) as product_view_sessions,
    cast(sum(case when add_to_cart_count > 0 then 1 else 0 end) as integer) as cart_sessions
from {{ ref('fact_customer_session') }}
group by session_date_key, platform, attribution_status
