{{ config(materialized='table') }}

/*
  Verified customer profiles by RFM segment, from marketing.dim_customer only.

  Grain: rfm_segment
  dim_customer holds one row per resolved customer key, so anonymous sessions
  are not in this table. marketing_eligible_count is an explicit marketing
  consent; everyone else is excluded. Rebuilt in full each run.
*/

select
    rfm_segment,
    count(*) as customer_count,
    cast(sum(case when marketing_consent then 1 else 0 end) as integer) as marketing_eligible_count,
    cast(sum(case when not marketing_consent then 1 else 0 end) as integer) as excluded_count,
    cast(sum(total_lifetime_value) as decimal(18, 2)) as lifetime_value
from {{ ref('dim_customer') }}
group by rfm_segment
