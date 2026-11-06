{{ config(severity='warn') }}

/*
  Bronze is missing Kafka offsets. The affected hours stay on Flink
  (is_estimated) instead of Spark; investigate Bronze capture or Kafka
  retention. Warn-only: Gold stays correct-as-provisional for those hours.
*/

select *
from {{ ref('int_inventory_coverage_gaps') }}
