-- Blocking money reconciliation for the POS restatement window: every day's
-- line count and net revenue in stg_pos_transactions must equal fact_sales,
-- and every store-day of non-voided fact_sales must equal sales_daily_store.
-- Catches lines left behind by a corrected file and stale store-days.
-- The window matches the models' pos_restatement_days lookback, so the
-- Spectrum side scans only the partitions the run just reprocessed.

{% set window_start %}
    (
        select cast({{ dateadd_unit(
            'day',
            -var('pos_restatement_days', 3),
            "max(" ~ date_from_date_key('date_key') ~ ")"
        ) }} as date)
        from {{ ref('fact_sales') }}
    )
{% endset %}

with source_daily as (
    select
        {{ date_key_from_date('transaction_date') }} as date_key,
        count(*) as line_count,
        sum(net_revenue) as net_revenue
    from {{ ref('stg_pos_transactions') }}
    where transaction_date >= {{ window_start }}
      and dt >= {{ window_start }}
    group by 1
),

fact_daily as (
    select
        date_key,
        count(*) as line_count,
        sum(net_revenue) as net_revenue
    from {{ ref('fact_sales') }}
    where date_key >= {{ date_key_from_date(window_start) }}
    group by 1
),

fact_store_daily as (
    select
        date_key,
        store_key,
        count(distinct transaction_id) as transaction_count,
        sum(quantity_sold) as units_sold,
        sum(net_revenue) as net_revenue
    from {{ ref('fact_sales') }}
    where date_key >= {{ date_key_from_date(window_start) }}
      and not is_voided
    group by 1, 2
),

summary_store_daily as (
    select date_key, store_key, transaction_count, units_sold, net_revenue
    from {{ ref('sales_daily_store') }}
    where date_key >= {{ date_key_from_date(window_start) }}
),

source_vs_fact as (
    select
        'source_vs_fact_sales' as check_name,
        coalesce(s.date_key, f.date_key) as date_key,
        cast(null as integer) as store_key,
        s.line_count as expected_count,
        f.line_count as actual_count,
        s.net_revenue as expected_net_revenue,
        f.net_revenue as actual_net_revenue
    from source_daily s
    full outer join fact_daily f
      on s.date_key = f.date_key
),

fact_vs_summary as (
    select
        'fact_sales_vs_sales_daily_store' as check_name,
        coalesce(f.date_key, m.date_key) as date_key,
        coalesce(f.store_key, m.store_key) as store_key,
        f.transaction_count as expected_count,
        m.transaction_count as actual_count,
        f.net_revenue as expected_net_revenue,
        m.net_revenue as actual_net_revenue
    from fact_store_daily f
    full outer join summary_store_daily m
      on f.date_key = m.date_key
     and f.store_key = m.store_key
    where f.date_key is null
       or m.date_key is null
       or f.units_sold <> m.units_sold
       or f.transaction_count <> m.transaction_count
       or f.net_revenue <> m.net_revenue
)

select *
from source_vs_fact
where expected_count is null
   or actual_count is null
   or expected_count <> actual_count
   or expected_net_revenue <> actual_net_revenue
union all
select *
from fact_vs_summary
