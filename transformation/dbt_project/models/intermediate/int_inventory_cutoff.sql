{{ config(materialized='view') }}

/*
  The daily inventory run's cutoff (ADR-012).

  One row, written by Spark after it checks every inventory partition against
  Kafka. closed_hour_exclusive (YYYYMMDDHH) is the first hour Gold does not
  include. Hours before it are canonical. coverage_status is COMPLETE only
  when every expected partition was fully captured through the target hour;
  otherwise the cutoff holds at the last hour Spark could prove, and the
  warn test inventory_coverage_incomplete fires. export_run_id is the same
  Spark run as inventory_run_id, appended by the export writer.
*/

select
    inventory_run_id,
    bronze_snapshot_id,
    target_hour_exclusive,
    closed_hour_exclusive,
    coverage_status,
    incomplete_partitions,
    export_run_id
from {{ source('silver', 'inventory_cutoff') }}
