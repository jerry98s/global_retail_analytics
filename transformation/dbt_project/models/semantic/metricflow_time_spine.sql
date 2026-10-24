/*
  MetricFlow time spine: one row per calendar day from the dim_date seed.
*/

select cast(full_date as date) as date_day
from {{ source('gold_finance', 'dim_date') }}
