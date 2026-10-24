{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date_key', 'snapshot_hour', 'product_key', 'store_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='product_key',
    sort='snapshot_date_key'
  )
}}

/*
  Hourly inventory fact with running on-hand balance per product/store.

  Lambda (ADR-012): reads int_inventory_hourly, which takes hours Spark has
  covered from canonical silver.inventory_events and later hours from Flink
  silver.inventory_hourly (is_estimated = true). This mart adds the running
  balance and the surrogate-key joins to dim_product and dim_store.

  quantity_on_hand: reference.inventory_opening_balance.opening_qty (0 when the
                    product/store has no opening count) plus the cumulative
                    sum of qty_delta_hour across hours within
                    (product_id, store_id), ordered by
                    (snapshot_date_key, snapshot_hour).
  quantity_available: max(quantity_on_hand, 0) — floor at zero so a temporary
                    over-shipping delta doesn't read as a negative shelf count.

  Incremental strategy: past hours change when Spark replaces a Flink hour or
  picks up a late scan, and every later balance of that product/store moves
  with it. Each run recomputes all balances from full history and upserts
  only rows that are new or whose balance or is_estimated changed.
  - A rewritten hour keeps the product_key already stored for it, so
    delete+insert replaces that row instead of adding a second version.
  - A stored hour that no longer has movement is re-emitted with delta 0 and
    the carried balance, so it cannot keep a stale number.
  Crash-resilient: a partial insert is just more rows that already match.
*/

with hourly as (
    select
        snapshot_date_key,
        snapshot_hour,
        product_id,
        store_id,
        qty_delta_hour,
        is_estimated
    from {{ ref('int_inventory_hourly') }}
),

{% if is_incremental() %}
existing as (
    select
        t.snapshot_date_key,
        t.snapshot_hour,
        t.product_key,
        t.store_key,
        cast(p.product_id as varchar) as product_id,
        cast(s.store_id as varchar) as store_id,
        t.quantity_on_hand,
        t.is_estimated
    from {{ this }} t
    inner join {{ wap_live_ref('dim_product') }} p
      on t.product_key = p.product_key
    inner join {{ source('gold_finance', 'dim_store') }} s
      on t.store_key = s.store_key
),
{% endif %}

movements as (
    select
        snapshot_date_key,
        snapshot_hour,
        product_id,
        store_id,
        cast(sum(qty_delta_hour) as numeric(38, 0)) as qty_delta_hour,
        (max(case when is_estimated then 1 else 0 end) = 1) as is_estimated
    from (
        select
            snapshot_date_key,
            snapshot_hour,
            product_id,
            store_id,
            qty_delta_hour,
            is_estimated
        from hourly
        {% if is_incremental() %}
        union all
        select
            snapshot_date_key,
            snapshot_hour,
            product_id,
            store_id,
            cast(0 as numeric(38, 0)) as qty_delta_hour,
            false as is_estimated
        from existing
        {% endif %}
    ) all_hours
    group by
        snapshot_date_key,
        snapshot_hour,
        product_id,
        store_id
),

with_running_balance as (
    select
        si.snapshot_date_key,
        si.snapshot_hour,
        si.product_id,
        si.store_id,
        si.is_estimated,
        coalesce(cast(o.opening_qty as numeric(38, 0)), 0)
        + sum(si.qty_delta_hour) over (
            partition by si.product_id, si.store_id
            order by si.snapshot_date_key, si.snapshot_hour
            rows unbounded preceding
        ) as quantity_on_hand
    from movements si
    left join {{ ref('inventory_opening_balance') }} o
      on si.product_id = o.product_id
     and si.store_id = o.store_id
)

select
    f.snapshot_date_key,
    f.snapshot_hour,
    {% if is_incremental() %}coalesce(e.product_key, p.product_key){% else %}p.product_key{% endif %} as product_key,
    {% if is_incremental() %}coalesce(e.store_key, s.store_key){% else %}s.store_key{% endif %} as store_key,
    f.quantity_on_hand,
    greatest(f.quantity_on_hand, 0) as quantity_available,
    f.is_estimated
from with_running_balance f
-- dim_product is owned by catalog_bihourly_product_scd2_refresh (ADR-009), so
-- read the last published live version rather than this DAG's pending schema.
left join {{ wap_live_ref('dim_product') }} p
  on f.product_id = p.product_id
 and p.is_current = true
left join {{ source('gold_finance', 'dim_store') }} s
  on f.store_id = s.store_id
{% if is_incremental() %}
left join existing e
  on f.snapshot_date_key = e.snapshot_date_key
 and f.snapshot_hour = e.snapshot_hour
 and f.product_id = e.product_id
 and f.store_id = e.store_id
where e.product_key is null
   or e.quantity_on_hand <> f.quantity_on_hand
   or e.is_estimated <> f.is_estimated
{% endif %}
