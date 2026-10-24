-- int_session_reconstruction.sql
-- Groups raw clickstream events into sessions.
-- Session boundary: 30 minutes of inactivity OR new producer session_id.
-- Grain: one row per session per client_id.
--
-- Incremental: events at or after late_cutoff (max session_end_time - 2h)
-- are new. A session that ended within 30 minutes of late_cutoff can still be
-- extended by them, so its client's events are re-read from that session's
-- start; rebuilding it from the new tail only would overwrite the stored
-- session with a truncated one. Events older than late_cutoff that arrive
-- later are not picked up (2h late-event window).

{{
  config(
    materialized = 'incremental',
    unique_key   = 'session_id',
    on_schema_change = 'append_new_columns'
  )
}}

{% if is_incremental() %}
with bounds as (
    select
        {{ dateadd_unit(
            'hour',
            -2,
            "coalesce(max(session_end_time), cast('1970-01-01' as timestamp))"
        ) }} as late_cutoff
    from {{ this }}
),

open_sessions as (
    select
        t.client_id,
        min(t.session_start_time) as restart_ts
    from {{ this }} t
    cross join bounds b
    where t.session_end_time >= {{ dateadd_unit('minute', -30, 'b.late_cutoff') }}
    group by t.client_id
),

events_with_lag as (
{% else %}
with events_with_lag as (
{% endif %}
    select
        e.event_id,
        e.client_id,
        i.customer_key,
        e.event_time,
        e.event_type,
        e.session_id,
        e.platform,
        e.properties,
        lag(event_time) over (
            partition by e.client_id
            order by e.event_time
        ) as prev_event_time,
        lag(session_id) over (
            partition by e.client_id
            order by e.event_time
        ) as prev_session_id
    from {{ ref('stg_clickstream_events') }} e
    left join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = case
            when e.customer_id is not null then 'customer_id'
            else 'client_id'
         end
     and i.identifier_value = coalesce(e.customer_id, e.client_id)
    {% if is_incremental() %}
    cross join bounds b
    left join open_sessions o
      on o.client_id = e.client_id
    where e.event_time >= case
            when o.restart_ts is not null and o.restart_ts < b.late_cutoff
            then o.restart_ts
            else b.late_cutoff
          end
    {% endif %}
),

session_boundaries as (
    select
        *,
        case
            when {{ datediff_unit('minute', 'prev_event_time', 'event_time') }} > 30
              or prev_session_id != session_id
              or prev_event_time is null
            then 1
            else 0
        end as is_session_start
    from events_with_lag
),

sessions_numbered as (
    select
        *,
        sum(is_session_start) over (
            partition by client_id
            order by event_time
            rows unbounded preceding
        ) as session_number
    from session_boundaries
),

session_summary as (
    select
        client_id,
        session_number,
        max(session_id)                                     as session_id,
        -- Prefer authenticated customer_key (non-null wins)
        max(customer_key)                                   as customer_key,
        min(event_time)                                     as session_start_time,
        max(event_time)                                     as session_end_time,
        {{ datediff_unit('second', 'min(event_time)', 'max(event_time)') }} as session_duration_seconds,
        count(*)                                            as event_count,
        sum(case when event_type = 'page_view'    then 1 else 0 end) as page_view_count,
        sum(case when event_type = 'product_view' then 1 else 0 end) as product_view_count,
        sum(case when event_type = 'add_to_cart'  then 1 else 0 end) as add_to_cart_count,
        sum(case when event_type = 'search'       then 1 else 0 end) as search_count,
        (sum(case when event_type = 'checkout' then 1 else 0 end) > 0) as converted,
        max(case
            when event_type = 'checkout'
            then {{ json_path_text('properties', 'order_id') }}
        end)                                                as order_id,
        max(case
            when event_type = 'checkout'
            then cast({{ json_path_text('properties', 'cart_value') }} as decimal(14, 2))
        end)                                                as conversion_value,
        max(case
            when event_type = 'page_view'       then 1
            when event_type = 'product_view'    then 2
            when event_type = 'add_to_cart'     then 3
            when event_type = 'checkout_start'  then 4
            when event_type = 'checkout'        then 5
            else 0
        end)                                                as funnel_depth,
        max(platform)                                       as platform,
        cast(min(event_time) as date)                       as session_date
    from sessions_numbered
    group by client_id, session_number
)

select
    s.*,
    d.date_key as session_date_key
from session_summary s
left join {{ source('gold_finance', 'dim_date') }} d
    on s.session_date = d.full_date
