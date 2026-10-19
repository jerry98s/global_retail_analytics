{{ config(severity='warn') }}

-- Flink silver.inventory_hourly and Spark canonical silver.inventory_events
-- must agree once predicted late drops are removed. Late drops themselves are
-- expected (watermark) and reported in late_dropped_events, not failed here.
select *
from {{ ref('int_inventory_reconciliation_hourly') }}
where unexplained_qty <> 0
