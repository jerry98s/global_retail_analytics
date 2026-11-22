{{ config(severity='warn') }}

/*
  The daily run could not prove every partition through its target hour.
  closed_hour_exclusive holds at the last proven hour and Gold still publishes.
  POS finance is not blocked. Check Bronze capture for the incomplete partitions.
*/

select
    inventory_run_id,
    closed_hour_exclusive,
    target_hour_exclusive,
    incomplete_partitions
from {{ ref('int_inventory_cutoff') }}
where coverage_status = 'INCOMPLETE'
