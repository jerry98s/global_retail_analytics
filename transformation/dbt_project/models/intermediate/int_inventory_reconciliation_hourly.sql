{{ config(materialized='view') }}

{#- Must match inventory_silver_job: 1-hour TUMBLE windows on EMR, 1-minute
    (INVENTORY_SILVER_WINDOW) in local compose, 60s watermark delay. -#}
{%- set window_unit = var('inventory_silver_window_unit', 'minute' if target.type == 'duckdb' else 'hour') -%}
{%- set watermark_seconds = var('inventory_silver_watermark_delay_seconds', 60) -%}

/*
  Reconciles the two inventory Silver outputs per (hour, product, store):
  - silver.inventory_events: Spark canonical events rebuilt from the lossless
    Bronze ledger (ADR-011), deduplicated, no watermark.
  - silver.inventory_hourly: Flink windowed sums read directly from Kafka; the
    kappa input of fact_inventory_snapshot.

  An event is predicted late-dropped when it reached Kafka after its window
  closed (window_end + watermark delay). unexplained_qty is what remains after
  removing predicted drops; non-zero values mean the two paths disagree for a
  reason other than lateness (consumer gaps, restarts from latest-offset,
  dedup differences). The prediction uses Kafka append time as a proxy for
  the job watermark, so expect occasional boundary noise.
*/

with canonical as (
    select
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(qty_delta as numeric(38, 0)) as qty_delta,
        date_trunc('{{ window_unit }}', cast(event_time as timestamp)) as window_start,
        cast(kafka_timestamp as timestamp) as kafka_timestamp
    from {{ source('silver', 'inventory_events') }}
),

classified as (
    select
        product_id,
        store_id,
        qty_delta,
        window_start,
        kafka_timestamp >= {{ dateadd_unit('second', watermark_seconds, dateadd_unit(window_unit, 1, 'window_start')) }}
            as is_late_dropped
    from canonical
),

canonical_hourly as (
    select
        {{ date_key_from_date('window_start') }} as snapshot_date_key,
        cast(extract(hour from window_start) as integer) as snapshot_hour,
        product_id,
        store_id,
        count(*) as canonical_events,
        sum(qty_delta) as canonical_qty,
        sum(case when is_late_dropped then 1 else 0 end) as late_dropped_events,
        sum(case when is_late_dropped then qty_delta else 0 end) as late_dropped_qty
    from classified
    group by
        {{ date_key_from_date('window_start') }},
        cast(extract(hour from window_start) as integer),
        product_id,
        store_id
),

silver_hourly as (
    select
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        cast(sum(qty_delta_hour) as numeric(38, 0)) as silver_qty
    from {{ source('silver', 'inventory_hourly') }}
    group by
        snapshot_date_key,
        snapshot_hour,
        cast(product_id as varchar),
        cast(store_id as varchar)
)

select
    coalesce(c.snapshot_date_key, s.snapshot_date_key) as snapshot_date_key,
    coalesce(c.snapshot_hour, s.snapshot_hour) as snapshot_hour,
    coalesce(c.product_id, s.product_id) as product_id,
    coalesce(c.store_id, s.store_id) as store_id,
    coalesce(c.canonical_events, 0) as canonical_events,
    coalesce(c.canonical_qty, 0) as canonical_qty,
    coalesce(c.late_dropped_events, 0) as late_dropped_events,
    coalesce(c.late_dropped_qty, 0) as late_dropped_qty,
    coalesce(c.canonical_qty, 0) - coalesce(c.late_dropped_qty, 0) as expected_silver_qty,
    coalesce(s.silver_qty, 0) as silver_qty,
    coalesce(s.silver_qty, 0)
        - (coalesce(c.canonical_qty, 0) - coalesce(c.late_dropped_qty, 0)) as unexplained_qty
from canonical_hourly c
full outer join silver_hourly s
  on c.snapshot_date_key = s.snapshot_date_key
 and c.snapshot_hour = s.snapshot_hour
 and c.product_id = s.product_id
 and c.store_id = s.store_id
