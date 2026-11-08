{{ config(materialized='view') }}

/*
  Current consent per customer_key and purpose: the latest explicit statement
  in int_consent_events. NULL means the customer never stated that purpose;
  dim_customer treats NULL as no consent.

  When accounts merge into one customer, the latest statement across them
  wins for each purpose.
*/

with ranked as (
    select
        customer_key,
        purpose,
        is_granted,
        event_time,
        consent_source,
        row_number() over (
            partition by customer_key, purpose
            order by event_time desc, event_id desc
        ) as rn
    from {{ ref('int_consent_events') }}
)

select
    customer_key,
    max(case when purpose = 'marketing' then case when is_granted then 1 else 0 end end) = 1
        as marketing_consent,
    max(case when purpose = 'marketing' then event_time end)     as marketing_consent_at,
    max(case when purpose = 'marketing' then consent_source end) as marketing_consent_source,
    max(case when purpose = 'analytics' then case when is_granted then 1 else 0 end end) = 1
        as analytics_consent,
    max(case when purpose = 'analytics' then event_time end)     as analytics_consent_at,
    max(case when purpose = 'analytics' then consent_source end) as analytics_consent_source
from ranked
where rn = 1
group by customer_key
