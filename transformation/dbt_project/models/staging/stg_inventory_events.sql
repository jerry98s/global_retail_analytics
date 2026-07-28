{{ config(materialized='view') }}

select
    cast(event_id as varchar) as event_id,
    cast(event_time as timestamp) as event_time,
    cast(store_id as varchar) as store_id,
    cast(product_id as varchar) as product_id,
    cast(qty_delta as numeric(38,0)) as qty_delta,
    cast(event_type as varchar) as event_type,
    cast(scanner_id as varchar) as scanner_id,
    coalesce(cast(is_late as boolean), false) as is_late,
    cast(schema_version as varchar) as schema_version
from {{ source('bronze', 'inventory_events') }}
