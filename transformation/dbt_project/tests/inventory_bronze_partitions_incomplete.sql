{{ config(severity='warn') }}

{%- set expected_partitions = var('inventory_kafka_partitions') -%}

/*
  An inventory partition is missing from Bronze, or its newest captured record
  is more than an hour behind the newest partition's. Batch coverage stops at
  the slowest partition, so those hours stay on Flink (is_estimated). A missing
  partition keeps every hour on Flink. Warn-only: Gold stays
  correct-as-provisional; check the Bronze capture job, or an idle partition.
*/

with expected as (
    {% for partition in range(expected_partitions) -%}
    select {{ partition }} as kafka_partition
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

captured as (
    select kafka_partition, cast(last_kafka_ts as timestamp) as last_ts
    from {{ source('silver', 'inventory_partition_coverage') }}
),

newest as (
    select max(last_ts) as newest_ts from captured
)

select
    e.kafka_partition,
    c.last_ts,
    n.newest_ts,
    cov.batch_end_hour_key
from expected e
left join captured c on e.kafka_partition = c.kafka_partition
cross join newest n
cross join {{ ref('int_inventory_batch_coverage') }} cov
where c.kafka_partition is null
   or c.last_ts < {{ dateadd_unit('hour', -1, 'n.newest_ts') }}
