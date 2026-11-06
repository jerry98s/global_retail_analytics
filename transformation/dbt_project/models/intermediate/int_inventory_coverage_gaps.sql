{{ config(materialized='view') }}

{%- set lateness_seconds = var('inventory_max_event_lateness_seconds', 3600) -%}

/*
  Event-time hours Spark cannot vouch for because Bronze is missing Kafka
  offsets (silver.inventory_offset_gaps, written by Spark from the pinned
  Bronze snapshot).

  A missing record reached Kafka between gap_start_ts and gap_end_ts, and its
  event_time is at most inventory_max_event_lateness_seconds before that. Every
  hour key (date_key * 100 + hour) in that range stays on Flink, which read
  Kafka independently of Bronze, and remains is_estimated.
*/

select
    source_system,
    kafka_topic,
    kafka_partition,
    missing_offsets,
    {{ date_key_from_date("date_trunc('hour', " ~ dateadd_unit('second', -lateness_seconds, 'cast(gap_start_ts as timestamp)') ~ ")") }} * 100
        + cast(extract(hour from {{ dateadd_unit('second', -lateness_seconds, 'cast(gap_start_ts as timestamp)') }}) as integer)
        as gap_start_hour_key,
    {{ date_key_from_date("date_trunc('hour', cast(gap_end_ts as timestamp))") }} * 100
        + cast(extract(hour from cast(gap_end_ts as timestamp)) as integer)
        as gap_end_hour_key
from {{ source('silver', 'inventory_offset_gaps') }}
