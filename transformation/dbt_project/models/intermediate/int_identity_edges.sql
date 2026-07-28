{{
  config(
    materialized='table'
  )
}}

/*
  Identity edges extracted from bronze for connected-components resolution.

  Edge types:
    - session_link         : (client_id, customer_id) seen together in a clickstream event
    - loyalty_value_match  : (loyalty_id, customer_id) when clickstream customer_id
                             equals a POS loyalty_id value (cross-source equality)

  Public devices (client_id linked to >= var('identity_public_device_threshold', 10)
  distinct customer_ids) are EXCLUDED from edges here so they appear as isolated
  singletons in int_identity_components. The list itself is in
  int_identity_public_devices.

  Companion model: int_identity_components (bounded Union-Find closure).
*/

{% set threshold = var('identity_public_device_threshold', 10) %}

with clickstream_pairs as (
    select
        e.client_id,
        e.customer_id,
        e.event_time
    from {{ ref('stg_clickstream_events') }} e
    where e.client_id is not null
      and e.customer_id is not null
),

loyalty_from_pos as (
    select distinct cast(loyalty_id as varchar) as loyalty_id
    from {{ ref('stg_pos_transactions') }}
    where loyalty_id is not null
),

public_device_keys as (
    select 'client:' || client_id as node_a
    from (
        select
            client_id,
            count(distinct customer_id) as distinct_customers
        from clickstream_pairs
        group by client_id
    ) s
    where distinct_customers >= {{ threshold }}
),

edges_session as (
    select
        'client:' || client_id      as node_a,
        'customer:' || customer_id  as node_b,
        'session_link'              as edge_type,
        max(event_time)             as last_observed_at
    from clickstream_pairs
    group by 1, 2, 3
),

edges_session_filtered as (
    select es.*
    from edges_session es
    left join public_device_keys pd on es.node_a = pd.node_a
    where pd.node_a is null
),

customer_first_seen as (
    select
        customer_id,
        min(event_time) as first_seen_at,
        max(event_time) as last_seen_at
    from clickstream_pairs
    group by customer_id
),

edges_loyalty_match as (
    select
        'loyalty:' || l.loyalty_id    as node_a,
        'customer:' || c.customer_id  as node_b,
        'loyalty_value_match'         as edge_type,
        c.last_seen_at                as last_observed_at
    from loyalty_from_pos l
    join customer_first_seen c
      on l.loyalty_id = c.customer_id
)

select
    node_a as src,
    node_b as dst,
    edge_type,
    last_observed_at
from edges_session_filtered
union all
select
    node_a as src,
    node_b as dst,
    edge_type,
    last_observed_at
from edges_loyalty_match
