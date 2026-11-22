{{
  config(
    materialized='table',
    schema='control'
  )
}}

/*
  One row: the inventory cutoff published with this finance run (ADR-012).

  Built from int_inventory_cutoff and published in the same WAP transaction
  as fact_inventory_snapshot, so the cutoff advances even when no stock
  moved, and it cannot disagree with the balances. Serving reads the live
  row. published_at is the build time; the publish commits it with the fact.
  Incomplete coverage does not block this row: closed_hour_exclusive holds
  at the last proven hour and coverage_status stays INCOMPLETE.
*/

select
    inventory_run_id,
    bronze_snapshot_id,
    closed_hour_exclusive,
    coverage_status,
    incomplete_partitions,
    cast(current_timestamp as timestamp) as published_at
from {{ ref('int_inventory_cutoff') }}
