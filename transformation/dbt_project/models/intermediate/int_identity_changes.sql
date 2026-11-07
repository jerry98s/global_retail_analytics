{{ config(materialized='table') }}

/*
  Identifier mappings that differ from the last PUBLISHED identity graph.

  Grain: one row per (identifier_type, identifier_value) whose customer_key
  changed: added (new identifier), removed (no longer in the mapping) or
  rekeyed (merged into, or split from, another customer).

  Compares Spark's current mapping (int_identity_resolution) with the live
  marketing.identity_graph, read through source() so it is the published copy
  even during a pending build. The marketing DAG builds this with its live
  models, before the pending marts, so restatement can use it before publish.
  Before the first publish every identifier is 'added'.
*/

{%- set published = source('gold_marketing', 'identity_graph') %}

with current_mapping as (
    select identifier_type, identifier_value, customer_key
    from {{ ref('int_identity_resolution') }}
),

published_mapping as (
{% if relation_exists(published) %}
    select identifier_type, identifier_value, customer_key
    from {{ published }}
{% else %}
    select identifier_type, identifier_value, customer_key
    from current_mapping
    where 1 = 0
{% endif %}
)

select
    coalesce(c.identifier_type, p.identifier_type)   as identifier_type,
    coalesce(c.identifier_value, p.identifier_value) as identifier_value,
    case
        when p.customer_key is null then 'added'
        when c.customer_key is null then 'removed'
        else 'rekeyed'
    end                                              as change_type,
    p.customer_key                                   as published_customer_key,
    c.customer_key                                   as current_customer_key
from current_mapping c
full outer join published_mapping p
  on c.identifier_type = p.identifier_type
 and c.identifier_value = p.identifier_value
where p.customer_key is null
   or c.customer_key is null
   or c.customer_key <> p.customer_key
