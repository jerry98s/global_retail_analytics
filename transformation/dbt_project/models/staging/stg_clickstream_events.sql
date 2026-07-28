{{ config(materialized='view') }}

select
    cast(event_id as varchar) as event_id,
    cast(event_type as varchar) as event_type,
    cast(event_time as timestamp) as event_time,
    cast(session_id as varchar) as session_id,
    cast(client_id as varchar) as client_id,
    nullif(cast(customer_id as varchar), '') as customer_id,
    cast(platform as varchar) as platform,
    cast(app_version as varchar) as app_version,
    cast(properties as varchar(65535)) as properties,
    cast(schema_version as varchar) as schema_version,
    cast(event_date as date) as event_date
from {{ source('bronze', 'clickstream_events') }}
