{{ config(materialized='view') }}

/*
  Typed POS bronze. Gold fact_sales and identity/catalog both read this view
  so casts live in one place.

  `dt` is the Spectrum Hive partition column (PARTITIONED BY dt). Filtering
  on it is what prunes S3 scans. Local DuckDB has no Hive partition catalog,
  so dt is aliased from transaction_date (the generator writes one date per
  dt= directory, so they match).

  Void semantics (ADR-015): is_voided is derived here as line_status or
  transaction_status VOIDED, not read from Bronze. Partitions written before
  the status columns existed have NULL statuses; their is_voided flag was a
  line void, so it maps to line_status with transaction_status COMPLETED.
  A NULL flag stays NULL, which fact_sales' not_null test blocks.
*/

with typed as (
    select
        cast(transaction_id as varchar) as transaction_id,
        cast(line_item_number as integer) as line_item_number,
        cast(transaction_date as date) as transaction_date,
        cast(store_id as varchar) as store_id,
        cast(product_id as varchar) as product_id,
        nullif(cast(loyalty_id as varchar), '') as loyalty_id,
        cast(quantity_sold as integer) as quantity_sold,
        cast(gross_revenue as decimal(18,2)) as gross_revenue,
        cast(net_revenue as decimal(18,2)) as net_revenue,
        cast(gross_margin as decimal(18,2)) as gross_margin,
        coalesce(
            cast(line_status as varchar),
            case cast(is_voided as boolean) when true then 'VOIDED' when false then 'ACTIVE' end
        ) as line_status,
        coalesce(
            cast(transaction_status as varchar),
            case when is_voided is not null then 'COMPLETED' end
        ) as transaction_status,
        cast(voided_at as timestamp) as voided_at,
        nullif(cast(void_reason as varchar), '') as void_reason,
        cast(source_updated_at as timestamp) as source_updated_at,
        {% if target.type == 'duckdb' %}
        cast(transaction_date as date) as dt
        {% else %}
        dt
        {% endif %}
    from {{ source('bronze', 'pos_transactions') }}
)

select
    transaction_id,
    line_item_number,
    transaction_date,
    store_id,
    product_id,
    loyalty_id,
    quantity_sold,
    gross_revenue,
    net_revenue,
    gross_margin,
    case
        when line_status = 'VOIDED' or transaction_status = 'VOIDED' then true
        when line_status is not null and transaction_status is not null then false
    end as is_voided,
    line_status,
    transaction_status,
    voided_at,
    void_reason,
    source_updated_at,
    dt
from typed
