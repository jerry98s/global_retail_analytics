{{ config(materialized='view') }}

/*
  Explicit consent statements, one row per (event_id, purpose).

  Only signed-in envelope 1.3+ events count: the Spark contract quarantines
  any 1.3 event whose consent flag is not a boolean or lacks consent_source.
  Pre-1.3 flags carry no source and are ignored. Loyalty membership is never
  a statement.

  customer_key comes from the account's current identity, so statements
  follow the customer through merges.
*/

with statements as (
    select
        e.event_id,
        e.event_time,
        e.customer_id,
        i.customer_key,
        {{ json_path_text('e.properties', 'consent_source') }}    as consent_source,
        {{ json_path_text('e.properties', 'marketing_consent') }} as marketing_raw,
        {{ json_path_text('e.properties', 'analytics_consent') }} as analytics_raw
    from {{ ref('stg_clickstream_events') }} e
    inner join {{ ref('int_identity_resolution') }} i
      on i.identifier_type = 'customer_id'
     and i.identifier_value = e.customer_id
    where e.session_instance_id is not null
),

marketing as (
    select
        event_id, event_time, customer_id, customer_key,
        'marketing' as purpose,
        marketing_raw = 'true' as is_granted,
        consent_source
    from statements
    where marketing_raw in ('true', 'false')
      and consent_source is not null
),

analytics as (
    select
        event_id, event_time, customer_id, customer_key,
        'analytics' as purpose,
        analytics_raw = 'true' as is_granted,
        consent_source
    from statements
    where analytics_raw in ('true', 'false')
      and consent_source is not null
)

select * from marketing
union all
select * from analytics
