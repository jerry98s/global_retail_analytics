/*
  The Flink hours read by one dbt build come from one committed export.
  The hourly export's run id is not the daily canonical run id; this test
  only checks the hourly source against itself, before any aggregation.
*/

select export_run_id
from {{ source('silver', 'inventory_hourly') }}
where (
    select count(distinct export_run_id)
    from {{ source('silver', 'inventory_hourly') }}
) > 1
