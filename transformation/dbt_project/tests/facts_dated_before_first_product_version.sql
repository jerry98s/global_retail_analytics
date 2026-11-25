{{ config(severity='warn') }}

-- Fact rows dated before the product's first dim_product version take that
-- first version (ADR-016), so their attributes are the earliest the catalog
-- knows, not what was true on the day. Usually a catalog history backfill gap.
with first_version as (
    select product_id, min(effective_from) as first_effective_from
    from {{ wap_live_ref('dim_product') }}
    group by product_id
),

fact_dates as (
    select 'fact_sales' as fact_name, product_id,
        {{ date_from_date_key('date_key') }} as business_date
    from {{ ref('fact_sales') }}
    union all
    select 'fact_returns', product_id,
        {{ date_from_date_key('coalesce(original_date_key, date_key)') }}
    from {{ ref('fact_returns') }}
    union all
    select 'fact_inventory_snapshot', product_id,
        {{ date_from_date_key('snapshot_date_key') }}
    from {{ ref('fact_inventory_snapshot') }}
)

select
    d.fact_name,
    d.product_id,
    min(d.business_date) as earliest_business_date,
    v.first_effective_from,
    count(*) as row_count
from fact_dates d
join first_version v on v.product_id = d.product_id
where d.business_date < cast(v.first_effective_from as date)
group by d.fact_name, d.product_id, v.first_effective_from
