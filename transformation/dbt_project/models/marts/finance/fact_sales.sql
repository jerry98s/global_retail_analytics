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
  Source: stg_pos_transactions (typed view over bronze.pos_transactions
          Parquet written by generate_pos_parquet.py).
  Joins: dim_product CURRENT version only (is_current=true) — this is not an
         as-of SCD2 join. Line items pick up the product_key that is current
         at load time; already-loaded history is not restated when merchandising
         changes price/category. Catalog DAG owns dim_product; this model reads
         the last published live table via wap_live_ref. A product the catalog
         DAG has not published yet gets the unknown key -1; the restatement
         window below re-keys it on a later run.
         dim_store (Type 0/1 seed, never WAP'd).
         int_identity_resolution (loyalty_id -> customer_key; LEFT JOIN
         yields NULL customer_key for anonymous transactions).

  Incremental strategy: lookback from max(date_key) in {{ this }} (the WAP
  pending clone of live) minus var pos_restatement_days (default 3), with
  delete+insert on the grain key. That rewrites a partial last day, reloads
  restated or late POS partitions inside the window, and re-keys unknown
  products once dim_product publishes them.
  `dt` is the Spectrum partition column (aliased from transaction_date on
  DuckDB); filtering it is what prunes Hive dt= prefixes on S3.
*/

{% set lookback %}
          select cast({{ dateadd_unit(
              'day',
              -var('pos_restatement_days', 3),
              "coalesce(max(" ~ date_from_date_key('date_key') ~ "), cast('1970-01-04' as date))"
          ) }} as date)
          from {{ this }}
{% endset %}

with base as (
    select *
    from {{ ref('stg_pos_transactions') }}
    {% if is_incremental() %}
      where transaction_date >= ( {{ lookback }} )
        and dt >= ( {{ lookback }} )
    {% endif %}
)
select
    {{ date_key_from_date('b.transaction_date') }} as date_key,
    coalesce(p.product_key, -1) as product_key,
    s.store_key,
    i.customer_key,
    b.loyalty_id,
    b.transaction_id,
    b.line_item_number,
    b.quantity_sold,
    b.gross_revenue,
    b.net_revenue,
    b.gross_margin,
    b.is_voided
from base b
left join {{ wap_live_ref('dim_product') }} p
  on b.product_id = p.product_id
 and p.is_current = true
left join {{ source('gold_finance', 'dim_store') }} s
  on b.store_id = s.store_id
left join {{ ref('int_identity_resolution') }} i
  on i.identifier_type = 'loyalty_id'
 and i.identifier_value = b.loyalty_id
