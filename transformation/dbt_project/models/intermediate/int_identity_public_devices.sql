{{
  config(
    materialized='table'
  )
}}

/*
  Public devices — client_ids linked to >= var('identity_public_device_threshold', 10)
  distinct customer_ids in clickstream (internet cafés, shared family tablets, store iPads).

  Excluded from identity_graph merging. Appear in int_identity_resolution with
  resolution_method='public_device_excluded' at confidence 0.3 and their own
  customer_key (device-only) so they remain queryable for audit but do not
  inflate Customer 360.
*/

{% set threshold = var('identity_public_device_threshold', 10) %}

with device_customer_counts as (
    select
        client_id,
        count(distinct customer_id) as distinct_customer_count,
        min(event_time)             as first_seen_at,
        max(event_time)             as last_seen_at
    from {{ ref('stg_clickstream_events') }}
    where client_id is not null
      and customer_id is not null
    group by client_id
)

select
    client_id,
    distinct_customer_count,
    first_seen_at,
    last_seen_at
from device_customer_counts
where distinct_customer_count >= {{ threshold }}
