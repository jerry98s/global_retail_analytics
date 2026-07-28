{{
  config(
    materialized='incremental',
    unique_key='identity_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

/*
  OneID identity resolution — graph-based connected components.

  Consumes:
    - int_identity_components  (bounded Union-Find over int_identity_edges)
    - int_identity_public_devices (excluded client_ids)

  Output per (identifier_type, identifier_value):
    - identity_key        : surrogate key (deterministic from type+value)
    - customer_key        : surrogate key from component representative
    - confidence_score    : per ADR-003
    - resolution_method   : audit label
    - is_public_device    : true for excluded client_ids

  Public devices get their OWN customer_key at 0.3 confidence so they remain
  queryable for audit but are excluded from Customer 360 / identity_graph
  merging. The component merge is computed without them; they appear here as
  isolated singletons overridden to a device-only key.

  Incremental strategy: delete+insert on identity_key recomputes *all*
  mappings every run. Append-only would leave stale customer_key values when
  int_identity_components reassigns a component representative after a
  full-refresh or graph merge. Recomputing is cheap (identifier grain is
  small vs clickstream event volume).

  See:
    - docs/data-model/identity-resolution.md
    - docs/decisions/ADR-003-identity-graph.md
*/

with components as (
    select
        identifier_type,
        identifier_value,
        component_rep_type,
        component_rep_node,
        customer_key
    from {{ ref('int_identity_components') }}
),

public_devices as (
    select client_id from {{ ref('int_identity_public_devices') }}
),

resolved as (
    select
        c.identifier_type,
        c.identifier_value,
        c.component_rep_type,
        c.component_rep_node,
        c.customer_key,
        case
            when c.identifier_type = 'client_id'
                and c.identifier_value in (select client_id from public_devices)
                then true
            else false
        end as is_public_device
    from components c
)

select
    {{ generate_surrogate_key(['r.identifier_type', 'r.identifier_value']) }} as identity_key,
    r.identifier_type,
    r.identifier_value,
    case
        when r.is_public_device
            then {{ generate_customer_key("'client:' || r.identifier_value") }}
        else r.customer_key
    end as customer_key,
    case
        when r.is_public_device                                        then cast(0.3000 as decimal(5, 4))
        when r.identifier_type = 'loyalty_id'                          then cast(1.0000 as decimal(5, 4))
        when r.identifier_type = 'customer_id'
            and r.component_rep_type = 'loyalty_id'                    then cast(1.0000 as decimal(5, 4))
        when r.identifier_type = 'customer_id'                         then cast(0.9000 as decimal(5, 4))
        when r.identifier_type = 'client_id'
            and r.component_rep_type in ('loyalty_id', 'customer_id')  then cast(0.8500 as decimal(5, 4))
        when r.identifier_type = 'client_id'                           then cast(0.5000 as decimal(5, 4))
        else cast(0.5000 as decimal(5, 4))
    end as confidence_score,
    case
        when r.is_public_device                                        then 'public_device_excluded'
        when r.identifier_type = 'loyalty_id'
            and r.component_rep_type = 'loyalty_id'
            and r.identifier_value = split_part(r.component_rep_node, ':', 2)
                                                                        then 'component_anchor'
        when r.identifier_type = 'loyalty_id'                          then 'loyalty_member'
        when r.identifier_type = 'customer_id'
            and r.component_rep_type = 'loyalty_id'                    then 'loyalty_match'
        when r.identifier_type = 'customer_id'                         then 'customer_id_standalone'
        when r.identifier_type = 'client_id'
            and r.component_rep_type in ('loyalty_id', 'customer_id')  then 'session_linked'
        when r.identifier_type = 'client_id'                           then 'device_only'
        else 'unknown'
    end as resolution_method,
    r.is_public_device
from resolved r
