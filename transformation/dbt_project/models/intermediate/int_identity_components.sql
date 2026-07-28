{{
  config(
    materialized='table'
  )
}}

/*
  Connected components over int_identity_edges — bounded multi-hop closure
  (SQL approximation of Union-Find).

  Hop depth is configurable via dbt var `identity_component_hops` (default 6).
  Default raised from 4 → 6 so the practical chain
  device → customer → loyalty → sibling_customer → sibling_device still merges.
  For deeper chains or >1M identifiers, migrate to a Python Union-Find job
  (documented in ADR-003).

  Component representative selection (priority order, then alphabetical):
    1. loyalty:*   (anchor preference — loyalty IDs are the stablest)
    2. customer:*  (authenticated clickstream ID)
    3. client:*    (device cookie — weakest)

  Isolated nodes (no edges — loyalty IDs never seen in clickstream, anonymous
  client_ids, standalone customer_ids) appear as singletons with themselves
  as the representative.
*/

{% set hops = var('identity_component_hops', 6) | int %}
{% if hops < 1 or hops > 12 %}
  {{ exceptions.raise_compiler_error(
      "identity_component_hops must be between 1 and 12, got " ~ hops
  ) }}
{% endif %}

with raw_edges as (
    select src, dst from {{ ref('int_identity_edges') }}
),

-- Symmetric closure: (a,b) AND (b,a) so traversal works in both directions
sym_edges as (
    select src, dst from raw_edges
    union
    select dst as src, src as dst from raw_edges
),

-- All known identifier nodes (singletons included so they get a component)
all_loyalty as (
    select distinct 'loyalty:' || cast(loyalty_id as varchar) as node
    from {{ ref('stg_pos_transactions') }}
    where loyalty_id is not null
),
all_customer as (
    select distinct 'customer:' || customer_id as node
    from {{ ref('stg_clickstream_events') }}
    where customer_id is not null
),
all_client as (
    select distinct 'client:' || client_id as node
    from {{ ref('stg_clickstream_events') }}
    where client_id is not null
),
all_nodes as (
    select node from all_loyalty
    union
    select node from all_customer
    union
    select node from all_client
),

-- Pairs (src, dst) where dst is reachable from src in 1 hop, plus reflexive
pairs_1 as (
    select src, dst from sym_edges
    union
    select node as src, node as dst from all_nodes
),

{% for n in range(2, hops + 1) %}
pairs_{{ n }} as (
    select distinct p.src, e.dst
    from pairs_{{ n - 1 }} p
    join sym_edges e on p.dst = e.src
),
{% endfor %}

all_pairs as (
    {% for n in range(1, hops + 1) %}
    select src, dst from pairs_{{ n }}
    {% if not loop.last %}union{% endif %}
    {% endfor %}
),

-- Priority sort key: 1=loyalty (preferred rep), 2=customer, 3=client
node_priority as (
    select
        distinct src as node,
        case
            when src like 'loyalty:%'  then '1:' || src
            when src like 'customer:%' then '2:' || src
            when src like 'client:%'   then '3:' || src
            else '9:' || src
        end as sort_key
    from all_pairs
),

-- For each node, the rep = min(sort_key) over all reachable nodes
component_reps as (
    select
        np.node,
        min(p.sort_key) as rep_sort_key
    from node_priority np
    join all_pairs ap on np.node = ap.src
    join node_priority p on ap.dst = p.node
    group by np.node
)

select
    cr.node,
    case
        when cr.node like 'loyalty:%'  then 'loyalty_id'
        when cr.node like 'customer:%' then 'customer_id'
        when cr.node like 'client:%'   then 'client_id'
    end as identifier_type,
    split_part(cr.node, ':', 2)                 as identifier_value,
    substring(cr.rep_sort_key, 3)               as component_rep_node,
    case
        when cr.rep_sort_key like '1:%' then 'loyalty_id'
        when cr.rep_sort_key like '2:%' then 'customer_id'
        when cr.rep_sort_key like '3:%' then 'client_id'
    end as component_rep_type,
    {{ generate_customer_key("substring(cr.rep_sort_key, 3)") }} as customer_key
from component_reps cr
