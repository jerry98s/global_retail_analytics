{{ config(materialized='view') }}

/*
  Typed POS returns bronze (ADR-015), one row per (return_id,
  return_line_number). A return is its own transaction; it references the
  original sale line and never changes it.

  An exact duplicate delivery (the same row in two files) collapses to one
  row, so its refund counts once. Two different rows with the same key stay,
  and fact_returns' uniqueness test blocks the publish.

  `dt` is the Spectrum Hive partition column; DuckDB aliases it from
  return_date (the generator writes one return date per dt= directory).
*/

select distinct
    cast(return_id as varchar) as return_id,
    cast(return_line_number as integer) as return_line_number,
    cast(return_date as date) as return_date,
    cast(original_transaction_id as varchar) as original_transaction_id,
    cast(original_line_item_number as integer) as original_line_item_number,
    cast(return_store_id as varchar) as return_store_id,
    cast(product_id as varchar) as product_id,
    cast(quantity_returned as integer) as quantity_returned,
    cast(refund_amount as decimal(18,2)) as refund_amount,
    cast(return_status as varchar) as return_status,
    nullif(cast(reason_code as varchar), '') as reason_code,
    cast(processed_at as timestamp) as processed_at,
    {% if target.type == 'duckdb' %}
    cast(return_date as date) as dt
    {% else %}
    dt
    {% endif %}
from {{ source('bronze', 'pos_returns') }}
