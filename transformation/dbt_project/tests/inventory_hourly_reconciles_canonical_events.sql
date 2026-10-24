{{ config(severity='warn') }}

-- Flink silver.inventory_hourly and Spark canonical silver.inventory_events
-- must agree once predicted late drops are removed. Late drops themselves are
-- expected (watermark) and reported in late_dropped_events, not failed here.
-- Only hours Spark has covered are compared; the open hour is incomplete on
-- the Spark side.
select *
from {{ ref('int_inventory_reconciliation_hourly') }}
where is_batch_covered
  and unexplained_qty <> 0
