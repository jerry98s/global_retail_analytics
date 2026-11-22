/*
  Canonical inventory inputs of one dbt build are one Spark run, inspected
  before any aggregation. A newer coverage export over older events would mark
  hours covered that those events do not hold.

  Coverage must have one row per expected partition, so a run with no inventory
  events still cannot pass on a missing coverage export.
*/

with runs as (
    select export_run_id as inventory_run_id
    from {{ source('silver', 'inventory_events') }}
    union
    select inventory_run_id
    from {{ source('silver', 'inventory_partition_coverage') }}
    union
    select inventory_run_id
    from {{ source('silver', 'inventory_cutoff') }}
),

mixed as (
    select inventory_run_id
    from runs
    where (select count(*) from runs) > 1
),

coverage_gap as (
    select 'coverage_row_count' as inventory_run_id
    where (
        select count(*) from {{ source('silver', 'inventory_partition_coverage') }}
    ) <> {{ var('inventory_kafka_partitions') }}
)

select inventory_run_id from mixed
union all
select inventory_run_id from coverage_gap
