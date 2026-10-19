{{
  config(
    materialized='view'
  )
}}

/*
  Consent flags per customer_key.

  Sources:
    - Explicit marketing_consent in clickstream event properties (login/checkout)
    - Implicit analytics consent when any authenticated event exists
  Loyalty holders receive marketing consent in dim_customer (known customers).
*/

with explicit_consent as (
    select
        i.customer_key,
        max(
            case
                when lower(coalesce({{ json_path_text('e.properties', 'marketing_consent') }}, '')) in ('true', '1', 'yes')
                then true
                else false
            end
        ) as explicit_marketing_consent,
        max(
            case
                when lower(coalesce({{ json_path_text('e.properties', 'analytics_consent') }}, '')) in ('false', '0', 'no')
                then false
                else true
            end
        ) as explicit_analytics_consent
    from {{ ref('stg_clickstream_events') }} e
    inner join {{ ref('int_identity_resolution') }} i
      on (
            (i.identifier_type = 'customer_id' and i.identifier_value = e.customer_id)
         or (i.identifier_type = 'client_id' and i.identifier_value = e.client_id)
         )
    group by i.customer_key
)

select
    customer_key,
    coalesce(explicit_marketing_consent, false) as explicit_marketing_consent,
    coalesce(explicit_analytics_consent, true) as explicit_analytics_consent
from explicit_consent
