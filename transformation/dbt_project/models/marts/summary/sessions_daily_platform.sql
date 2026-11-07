{{ config(materialized='table') }}

/*
  Daily platform session rollup from marketing.fact_customer_session only.

  Grain: (session_date_key, platform)
  Preserves platform so digital mix analysis remains possible.
  Rebuilt in full each run: fact_customer_session restates sessions of any
  age (late events, identity changes), so no recent-days window is complete.
  Anonymous and ambiguous sessions count in session_count and the
  conversion_rate denominator; identified_customer_count counts only
  attributed customers.
*/

select
    session_date_key,
    platform,
    count(*) as session_count,
    cast(sum(case when attribution_status = 'anonymous' then 1 else 0 end) as integer)
        as anonymous_session_count,
    count(distinct customer_key) as identified_customer_count,
    cast(sum(case when converted then 1 else 0 end) as integer) as converted_sessions,
    cast(
        sum(case when converted then 1 else 0 end) as double precision
    ) / nullif(count(*), 0) as conversion_rate,
    cast(sum(product_view_count) as bigint) as product_view_count,
    cast(sum(add_to_cart_count) as bigint) as add_to_cart_count
from {{ ref('fact_customer_session') }}
group by session_date_key, platform
