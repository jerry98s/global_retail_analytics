{{ config(materialized='view') }}

{%- set watermark_seconds = var('inventory_silver_watermark_delay_seconds', 60) -%}

/*
  Hours that Spark's canonical silver.inventory_events fully covers (ADR-012).

  One row. Bounds are YYYYMMDDHH integers (date_key * 100 + hour):
  - batch_start_hour_key: hour of the first record Bronze captured. Earlier
    hours stay on Flink, because Bronze holds nothing for them.
  - batch_end_hour_key (exclusive): the hour still open when the Spark
    snapshot was taken. An hour is covered once Kafka has received records
    past its end plus the Flink watermark delay, the same rule that closes a
    Flink window.

  Both bounds are null when Spark has not produced any events yet, so every
  hour stays on Flink.
*/

with bounds as (
    select
        date_trunc('hour', min(cast(kafka_timestamp as timestamp))) as first_hour,
        date_trunc(
            'hour',
            {{ dateadd_unit('second', -watermark_seconds, 'max(cast(kafka_timestamp as timestamp))') }}
        ) as open_hour
    from {{ source('silver', 'inventory_events') }}
)

select
    {{ date_key_from_date('first_hour') }} * 100
        + cast(extract(hour from first_hour) as integer) as batch_start_hour_key,
    {{ date_key_from_date('open_hour') }} * 100
        + cast(extract(hour from open_hour) as integer) as batch_end_hour_key
from bounds
