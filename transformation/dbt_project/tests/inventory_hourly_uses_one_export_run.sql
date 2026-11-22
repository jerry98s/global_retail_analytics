/*
  The inventory fact must be built from one Spark run's exports.

  Spark publishes inventory_events, inventory_hourly and
  inventory_partition_coverage through separate manifests; a dbt build that
  read them while a Spark run was publishing would mix runs. A newer coverage
  over older events would mark hours covered that those events do not hold.
  int_inventory_hourly keeps each row's export_run_id and
  int_inventory_batch_coverage keeps the coverage's, so any second run id here
  means the fact saw mixed inputs. Returns one row per run id when more than
  one is present.
*/

with runs as (
    select export_run_id
    from {{ ref('int_inventory_hourly') }}
    union
    select export_run_id
    from {{ ref('int_inventory_batch_coverage') }}
    where export_run_id is not null
)

select export_run_id
from runs
where (select count(*) from runs) > 1
