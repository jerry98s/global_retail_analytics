/*
  The inventory fact must be built from one Spark run's exports.

  Spark publishes inventory_events and inventory_hourly through separate
  manifests; a dbt build that read them while a Spark run was publishing would
  mix runs. int_inventory_hourly is one query over both and keeps each row's
  export_run_id, so any second run id here means the fact saw mixed inputs.
  Returns one row per run id when more than one is present.
*/

with runs as (
    select export_run_id, count(*) as hourly_rows
    from {{ ref('int_inventory_hourly') }}
    group by export_run_id
)

select export_run_id, hourly_rows
from runs
where (select count(*) from runs) > 1
