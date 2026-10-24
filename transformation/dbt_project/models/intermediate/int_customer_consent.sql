{{
  config(
    materialized='view'
  )
}}

/*
  Latest explicit consent statement per customer_key.

  Source: marketing_consent / analytics_consent in clickstream event
  properties (login/checkout). Each flag takes the most recent event that
  states it, so a later opt-out overrides an earlier opt-in. NULL means the
  customer never stated that preference; dim_customer applies the defaults.
*/

-- One equi-join per identifier type. An OR across the two join keys forces
-- a nested-loop join (events x identities). Each identity row has exactly
-- one identifier_type, so UNION ALL returns the same rows.
with matched as (
    select i.customer_key, e.event_time, e.event_id, e.properties
    from {{ ref('stg_clickstream_events') }} e
    inner join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = 'customer_id'
     and i.identifier_value = e.customer_id
    union all
    select i.customer_key, e.event_time, e.event_id, e.properties
    from {{ ref('stg_clickstream_events') }} e
    inner join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = 'client_id'
     and i.identifier_value = e.client_id
),

statements as (
    select
        customer_key,
        event_time,
        event_id,
        lower({{ json_path_text('properties', 'marketing_consent') }}) as marketing_raw,
        lower({{ json_path_text('properties', 'analytics_consent') }}) as analytics_raw
    from matched
),

marketing as (
    select
        customer_key,
        marketing_raw in ('true', '1', 'yes') as explicit_marketing_consent,
        row_number() over (
            partition by customer_key order by event_time desc, event_id desc
        ) as rn
    from statements
    where marketing_raw in ('true', '1', 'yes', 'false', '0', 'no')
),

analytics as (
    select
        customer_key,
        analytics_raw in ('true', '1', 'yes') as explicit_analytics_consent,
        row_number() over (
            partition by customer_key order by event_time desc, event_id desc
        ) as rn
    from statements
    where analytics_raw in ('true', '1', 'yes', 'false', '0', 'no')
),

customers as (
    select distinct customer_key from statements
)

select
    c.customer_key,
    m.explicit_marketing_consent,
    a.explicit_analytics_consent
from customers c
left join marketing m
  on c.customer_key = m.customer_key
 and m.rn = 1
left join analytics a
  on c.customer_key = a.customer_key
 and a.rn = 1
