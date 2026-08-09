{{
  config(
    materialized='incremental',
    unique_key=['session_date_key', 'platform'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Daily platform session rollup from marketing.fact_customer_session only.

  Grain: (session_date_key, platform)
  Preserves platform so digital mix analysis remains possible.
*/

with sessions as (
    select
        session_date_key,
        platform,
        session_id,
        customer_key,
        converted,
        product_view_count,
        add_to_cart_count
    from {{ ref('fact_customer_session') }}
    {% if is_incremental() %}
      where session_date_key >= (
          select coalesce(max(session_date_key), 0)
          from {{ wap_prior_state() }}
      )
    {% endif %}
)

select
    session_date_key,
    platform,
    count(*) as session_count,
    count(distinct customer_key) as identified_customer_count,
    cast(sum(case when converted then 1 else 0 end) as integer) as converted_sessions,
    cast(
        sum(case when converted then 1 else 0 end) as double precision
    ) / nullif(count(*), 0) as conversion_rate,
    cast(sum(product_view_count) as bigint) as product_view_count,
    cast(sum(add_to_cart_count) as bigint) as add_to_cart_count
from sessions
group by session_date_key, platform
