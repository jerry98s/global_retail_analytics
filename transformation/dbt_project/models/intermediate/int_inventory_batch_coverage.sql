{{ config(materialized='view') }}

{%- set watermark_seconds = var('inventory_silver_watermark_delay_seconds', 60) -%}
{%- set expected_partitions = var('inventory_kafka_partitions') -%}

/*
  Hours that Spark's canonical silver.inventory_events fully covers (ADR-012).

  One row, from silver.inventory_partition_coverage: the first and last Kafka
  offset and time Bronze captured per partition in the snapshot Spark
  promoted. Bounds are YYYYMMDDHH integers (date_key * 100 + hour):
  - batch_start_hour_key: hour of the first record Bronze captured. Earlier
    hours stay on Flink, because Bronze holds nothing for them. A partition
    whose first captured offset is above 0 lost earlier records, which reached
    Kafka before its first captured one, so coverage starts the hour after.
  - batch_end_hour_key (exclusive): an hour is covered once every partition
    has records past its end plus the Flink watermark delay, the same rule
    that closes a Flink window. The slowest partition sets the bound, so an
    hour a lagging partition has not fully reached stays on Flink. An idle
    partition holds coverage back the same way.

  Both bounds are null until Bronze holds exactly inventory_kafka_partitions
  partitions, so every hour stays on Flink. Internal offset gaps are handled
  separately by int_inventory_coverage_gaps. export_run_id is the Spark run
  the coverage came from (tests/inventory_hourly_uses_one_export_run.sql).
*/

with partitions as (
    select
        kafka_partition,
        first_offset,
        cast(first_kafka_ts as timestamp) as first_ts,
        cast(last_kafka_ts as timestamp) as last_ts,
        export_run_id
    from {{ source('silver', 'inventory_partition_coverage') }}
),

bounds as (
    select
        count(distinct kafka_partition) as captured_partitions,
        date_trunc('hour', min(first_ts)) as first_hour,
        max(
            case when first_offset > 0
                then date_trunc('hour', {{ dateadd_unit('hour', 1, 'first_ts') }})
            end
        ) as head_gap_end_hour,
        date_trunc(
            'hour',
            {{ dateadd_unit('second', -watermark_seconds, 'min(last_ts)') }}
        ) as open_hour,
        max(export_run_id) as export_run_id
    from partitions
),

complete as (
    select
        case when captured_partitions = {{ expected_partitions }}
            then coalesce(
                case when head_gap_end_hour > first_hour then head_gap_end_hour end,
                first_hour
            )
        end as start_hour,
        case when captured_partitions = {{ expected_partitions }} then open_hour end as open_hour,
        export_run_id
    from bounds
)

select
    {{ date_key_from_date('start_hour') }} * 100
        + cast(extract(hour from start_hour) as integer) as batch_start_hour_key,
    {{ date_key_from_date('open_hour') }} * 100
        + cast(extract(hour from open_hour) as integer) as batch_end_hour_key,
    export_run_id
from complete
