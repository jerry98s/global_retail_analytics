{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date_key', 'snapshot_hour', 'product_id', 'store_key'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='product_key',
    sort='snapshot_date_key',
    pre_hook="{% if is_incremental() and not column_exists(this, 'product_id') %}delete from {{ this }}{% endif %}"
  )
}}

/*
  Hourly observed-position fact: one row per product/store hour with a
  movement (or a stored hour whose movement went away), carrying the hour's
  net movement and the balance after it. Hours without a row hold the
  previous balance; summary.inventory_position_daily fills them in.

  qty_delta_hour: net units moved in the hour (additive across hours,
                  products and stores). 0 on a stored hour that lost its
                  movement.

  Lambda (ADR-012): reads int_inventory_hourly, which is Spark canonical
  events for hours before the published cutoff. is_estimated stays on the
  table (dropping a live column aborts WAP) and is always false. This mart
  adds the running balance and the surrogate-key joins to dim_product and
  dim_store. product_key is the dim_product version valid on the snapshot
  date (as-of SCD2, ADR-016); the balance runs per product_id, so it carries
  across versions.

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
  only rows that are new or whose balance, movement, is_estimated or
  product_key changed. A row stored before qty_delta_hour existed has it
  NULL and is rewritten once.
  - delete+insert keys on product_id, not product_key, so an hour whose
    as-of version changed (a corrected dimension history) replaces its row.
  - A stored hour that no longer has movement is re-emitted with delta 0 and
    the carried balance, so it cannot keep a stale number.
  - Hours of a product the catalog DAG has not published yet are held back
    rather than stored under the unknown key. Balances are recomputed from
    full history, so held-back hours land on the first run after the product
    publishes.
  - A table from before product_id existed is emptied by the pre-hook and
    rebuilt from int_inventory_hourly once.
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
    {% if column_exists(this, 'product_id') %}
    select
        t.snapshot_date_key,
        t.snapshot_hour,
        t.product_key,
        t.store_key,
        cast(t.product_id as varchar) as product_id,
        cast(s.store_id as varchar) as store_id,
        t.quantity_on_hand,
        {% if column_exists(this, 'qty_delta_hour') -%}
        t.qty_delta_hour,
        {%- else -%}
        cast(null as numeric(38, 0)) as qty_delta_hour,
        {%- endif %}
        t.is_estimated
    from {{ this }} t
    inner join {{ source('gold_finance', 'dim_store') }} s
      on t.store_key = s.store_key
    {% else %}
    select
        cast(null as integer) as snapshot_date_key,
        cast(null as integer) as snapshot_hour,
        cast(null as bigint) as product_key,
        cast(null as bigint) as store_key,
        cast(null as varchar) as product_id,
        cast(null as varchar) as store_id,
        cast(null as numeric(38, 0)) as quantity_on_hand,
        cast(null as numeric(38, 0)) as qty_delta_hour,
        cast(null as boolean) as is_estimated
    where 1 = 0
    {% endif %}
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
        si.qty_delta_hour,
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
    p.product_key,
    f.product_id,
    s.store_key,
    f.qty_delta_hour,
    f.quantity_on_hand,
    greatest(f.quantity_on_hand, 0) as quantity_available,
    f.is_estimated
from with_running_balance f
left join {{ product_versions() }} p
  on f.product_id = p.product_id
 and {{ product_as_of('p', date_from_date_key('f.snapshot_date_key')) }}
left join {{ source('gold_finance', 'dim_store') }} s
  on f.store_id = s.store_id
{% if is_incremental() %}
left join existing e
  on f.snapshot_date_key = e.snapshot_date_key
 and f.snapshot_hour = e.snapshot_hour
 and f.product_id = e.product_id
 and f.store_id = e.store_id
where p.product_key is not null
  and (
      e.product_key is null
      or e.product_key <> p.product_key
      or e.quantity_on_hand <> f.quantity_on_hand
      or e.qty_delta_hour is null
      or e.qty_delta_hour <> f.qty_delta_hour
      or e.is_estimated <> f.is_estimated
  )
{% else %}
where p.product_key is not null
{% endif %}
