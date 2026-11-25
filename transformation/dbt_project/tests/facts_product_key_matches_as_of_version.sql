-- Every fact row carries the dim_product version valid on its business date
-- (ADR-016): the sale date, the original sale date for a return (return date
-- for an UNKNOWN_SALE), the snapshot date for inventory. -1 is expected only
-- while the catalog DAG has not published the product. The incremental facts
-- re-key a day whose stored key drifts, so a row here means the re-key failed.
with sales as (
    select
        'fact_sales' as fact_name,
        f.date_key as business_date_key,
        f.product_id,
        f.product_key,
        coalesce(p.product_key, -1) as as_of_product_key
    from {{ ref('fact_sales') }} f
    left join {{ product_versions() }} p
      on p.product_id = f.product_id
     and {{ product_as_of('p', date_from_date_key('f.date_key')) }}
),

returns_ as (
    select
        'fact_returns' as fact_name,
        coalesce(f.original_date_key, f.date_key) as business_date_key,
        f.product_id,
        f.product_key,
        coalesce(p.product_key, -1) as as_of_product_key
    from {{ ref('fact_returns') }} f
    left join {{ product_versions() }} p
      on p.product_id = f.product_id
     and {{ product_as_of('p', date_from_date_key('coalesce(f.original_date_key, f.date_key)')) }}
),

inventory as (
    select
        'fact_inventory_snapshot' as fact_name,
        f.snapshot_date_key as business_date_key,
        f.product_id,
        f.product_key,
        coalesce(p.product_key, -1) as as_of_product_key
    from {{ ref('fact_inventory_snapshot') }} f
    left join {{ product_versions() }} p
      on p.product_id = f.product_id
     and {{ product_as_of('p', date_from_date_key('f.snapshot_date_key')) }}
)

select * from sales where product_key <> as_of_product_key
union all
select * from returns_ where product_key <> as_of_product_key
union all
select * from inventory where product_key <> as_of_product_key
