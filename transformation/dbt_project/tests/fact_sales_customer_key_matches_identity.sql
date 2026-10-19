{{ config(severity='warn') }}

-- fact_sales keeps the customer_key assigned at load time, while
-- silver.identity_resolution is recomputed on every marketing run. A merge of
-- identity components re-keys loyalty IDs, and old fact rows are not restated.
-- Compare against the current identity graph rather than dim_customer: the
-- incremental dim keeps superseded customer_keys, so a relationships test there
-- would pass on stale rows. Restate with
-- `dbt run --select fact_sales --full-refresh` (see backfill-verification.md).
select
    f.transaction_id,
    f.line_item_number,
    f.loyalty_id,
    f.customer_key as fact_customer_key,
    i.customer_key as current_customer_key
from {{ ref('fact_sales') }} f
left join {{ ref('int_identity_resolution') }} i
  on i.identifier_type = 'loyalty_id'
 and i.identifier_value = f.loyalty_id
where f.loyalty_id is not null
  and (i.customer_key is null or i.customer_key <> f.customer_key)
