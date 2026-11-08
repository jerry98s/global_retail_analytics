{{ config(materialized='view') }}

/*
  Devices used by more than one signed-in account (family tablets, store
  kiosks). Audit and dashboard input only: devices never merge customers, so
  this changes no customer_key.

  Grain: one row per client_id with two or more accounts.
*/

select
    client_id,
    count(distinct customer_id)         as account_count,
    count(distinct session_instance_id) as session_instance_count,
    min(valid_from)                     as first_signed_in_at,
    max(valid_to)                       as last_signed_in_at,
    max(case when is_ambiguous then 1 else 0 end) = 1 as has_ambiguous_instance
from {{ source('silver', 'device_session_associations') }}
group by client_id
having count(distinct customer_id) > 1
