{{
  config(
    materialized='incremental',
    unique_key=['identifier_type', 'identifier_value'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='customer_key',
    sort=['identifier_type', 'identifier_value']
  )
}}

/*
  Physical identity graph (marketing.identity_graph DDL).
  Populated from int_identity_resolution after dim_customer exists.

  Public devices (is_public_device = true) are EXCLUDED from this mart — they
  should not appear in Customer 360 joins. They remain in int_identity_resolution
  for audit.
*/

select
    i.identifier_type,
    i.identifier_value,
    i.customer_key,
    i.confidence_score,
    i.resolution_method,
    i.is_public_device,
    cast(true as boolean) as is_active
from {{ ref('int_identity_resolution') }} i
inner join {{ ref('dim_customer') }} c
  on i.customer_key = c.customer_key
where not coalesce(i.is_public_device, false)
