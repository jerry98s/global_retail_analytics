{{
  config(
    materialized='table',
    dist='customer_key',
    sort=['identifier_type', 'identifier_value']
  )
}}

/*
  Physical identity graph (marketing.identity_graph DDL): one row per
  customer_id / loyalty_id with its resolved customer_key.

  Rebuilt in full each run so an identifier that leaves the mapping, or a key
  that changes after a merge, never lingers in the published table.
  int_identity_changes compares this table's live copy with the next mapping.
*/

select
    i.identifier_type,
    i.identifier_value,
    i.customer_key,
    i.confidence_score,
    i.resolution_method,
    cast(true as boolean) as is_active
from {{ ref('int_identity_resolution') }} i
inner join {{ ref('dim_customer') }} c
  on i.customer_key = c.customer_key
