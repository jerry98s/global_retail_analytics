{{
  config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  Session-grain customer behavior fact.

  Grain: one row per session_id (from int_session_reconstruction).
  Source: int_session_reconstruction (which handles 30-min inactivity
          boundaries and producer session_id changes).

  Incremental strategy: crash-resilient lookback anchored on
  max(session_start_time) from {{ this }} (NOT current_timestamp).
  The 2h buffer mirrors int_session_reconstruction's late-event window,
  so sessions that received late events within the buffer are re-emitted
  and `delete+insert` on session_id overwrites them in place. If a run
  dies after a partial insert, the next run picks up from the max
  committed session_start_time minus the 2h buffer - no cliff, no
  skipped hours.

  Lookback cutoff is computed in a CTE (not a scalar subquery under WHERE)
  so DuckDB accepts the aggregate.
*/

{% if is_incremental() %}
with lookback as (
    select
        {{ dateadd_unit(
            'hour',
            -2,
            "coalesce(max(session_start_time), cast('1970-01-01' as timestamp))"
        ) }} as cutoff_ts
    from {{ this }}
),
sessions as (
    select s.*
    from {{ ref('int_session_reconstruction') }} s
    cross join lookback l
    where s.session_start_time >= l.cutoff_ts
)
{% else %}
with sessions as (
    select *
    from {{ ref('int_session_reconstruction') }}
)
{% endif %}
select
    s.session_id,
    s.session_date_key,
    s.session_start_time,
    s.customer_key,
    s.client_id,
    s.session_duration_seconds,
    s.page_view_count,
    s.product_view_count,
    s.add_to_cart_count,
    s.converted,
    s.order_id,
    s.funnel_depth,
    s.platform
from sessions s
