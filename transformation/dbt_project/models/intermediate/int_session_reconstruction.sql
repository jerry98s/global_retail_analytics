{{ config(materialized='table') }}

/*
  One row per session, rebuilt in full from canonical Silver each run.

  Session key:
    - v1.3+ events: the producer-issued session_instance_id. The producer
      starts a new instance after 30 idle minutes, a logout or an account
      switch, so dbt does not derive boundaries. A late event joins its
      instance by id, so there is no late-event window.
    - Pre-1.3 events (no instance id): one legacy session per
      (client_id, producer session_id), keyed 'legacy:<client>:<session>'.
      Legacy sessions are always anonymous.

  Attribution (ADR-003): a session gets a customer_key only when
  silver.device_session_associations holds exactly one account for the
  instance, whether from a login or a remembered sign-in. Every event in the
  session counts for that customer, including events before the login.
  No account: anonymous. More than one: ambiguous. Both keep
  customer_key NULL and stay in conversion denominators.

  session_fingerprint changes whenever a column fact_customer_session stores
  changes (late event, rekeyed customer), which drives its restatement.
*/

with events as (
    select
        {{ session_key() }}                 as session_key,
        session_instance_id,
        session_id                          as producer_session_id,
        client_id,
        event_time,
        event_type,
        platform,
        properties
    from {{ ref('stg_clickstream_events') }}
),

accounts as (
    select
        session_instance_id,
        count(distinct customer_id) as account_count,
        max(customer_id)            as customer_id
    from {{ source('silver', 'device_session_associations') }}
    group by session_instance_id
),

session_summary as (
    select
        session_key                                         as session_id,
        max(session_instance_id)                            as session_instance_id,
        max(producer_session_id)                            as producer_session_id,
        max(client_id)                                      as client_id,
        max(session_instance_id) is null                    as is_legacy,
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
    from events
    group by session_key
),

attributed as (
    select
        s.*,
        case when a.account_count = 1 then i.customer_key end as customer_key,
        case
            when coalesce(a.account_count, 0) = 0 then 'anonymous'
            when a.account_count > 1 then 'ambiguous'
            else 'attributed'
        end                                                   as attribution_status
    from session_summary s
    left join accounts a
      on a.session_instance_id = s.session_instance_id
    left join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = 'customer_id'
     and i.identifier_value = a.customer_id
)

select
    s.*,
    d.date_key as session_date_key,
    {{ record_hash_sha256(
        "coalesce(cast(d.date_key as varchar), '') || '|' ||"
        ~ " cast(s.session_start_time as varchar) || '|' ||"
        ~ " cast(s.session_end_time as varchar) || '|' ||"
        ~ " cast(s.event_count as varchar) || '|' ||"
        ~ " coalesce(cast(s.customer_key as varchar), '') || '|' ||"
        ~ " s.attribution_status || '|' ||"
        ~ " cast(s.page_view_count as varchar) || '|' ||"
        ~ " cast(s.product_view_count as varchar) || '|' ||"
        ~ " cast(s.add_to_cart_count as varchar) || '|' ||"
        ~ " case when s.converted then '1' else '0' end || '|' ||"
        ~ " coalesce(s.order_id, '') || '|' ||"
        ~ " coalesce(cast(s.conversion_value as varchar), '') || '|' ||"
        ~ " cast(s.funnel_depth as varchar) || '|' ||"
        ~ " coalesce(s.platform, '')"
    ) }} as session_fingerprint
from attributed s
left join {{ source('gold_finance', 'dim_date') }} d
    on s.session_date = d.full_date
