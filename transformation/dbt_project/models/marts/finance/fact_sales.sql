{{
  config(
    materialized='incremental',
    unique_key=['transaction_id', 'line_item_number'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='product_key',
    sort='date_key'
  )
}}

/*
  Sales fact at transaction line-item grain.

  Grain: one row per (transaction_id, line_item_number).
  Source: bronze.pos_transactions (Spectrum external over S3 Parquet
          written by generate_pos_parquet.py).
  Joins: dim_product (SCD2 — filtered to is_current=true),
         dim_store (Type 0/1, no SCD2 filter needed),
         int_identity_resolution (loyalty_id -> customer_key; LEFT JOIN
         yields NULL customer_key for anonymous transactions — see
         not_null-with-where test on customer_key).

  Incremental strategy: crash-resilient lookback anchored on
  max(date_key) from {{ this }} (NOT current_timestamp). If a run dies
  after a partial insert, the next run picks up from the max committed
  date_key — no cliff, no skipped days.
*/

with base as (
    select *
    from {{ source('bronze', 'pos_transactions') }}
    {% if is_incremental() %}
      where cast(transaction_date as date) >= (
          select coalesce(
              max({{ date_from_date_key('date_key') }}),
              cast('1970-01-01' as date)
          )
          from {{ this }}
      )
    {% endif %}
)
select
    {{ date_key_from_date('b.transaction_date') }} as date_key,
    p.product_key,
    s.store_key,
    i.customer_key,
    nullif(cast(b.loyalty_id as varchar), '') as loyalty_id,
    b.transaction_id,
    b.line_item_number,
    b.quantity_sold,
    b.gross_revenue,
    b.net_revenue,
    b.gross_margin,
    b.is_voided
from base b
-- dim_product is owned by catalog_bihourly_product_scd2_refresh (ADR-009), so
-- read the last published live version rather than this DAG's pending schema.
left join {{ wap_live_ref('dim_product') }} p
  on b.product_id = p.product_id
 and p.is_current = true
left join {{ source('gold_finance', 'dim_store') }} s
  on cast(b.store_id as varchar) = s.store_id
left join {{ ref('int_identity_resolution') }} i
  on i.identifier_type = 'loyalty_id'
 and i.identifier_value = nullif(cast(b.loyalty_id as varchar), '')
