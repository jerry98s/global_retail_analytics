{{ config(materialized='view') }}

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
    coalesce(cast(is_voided as boolean), false) as is_voided
from {{ source('bronze', 'pos_transactions') }}
