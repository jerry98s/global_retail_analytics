-- Blocking money reconciliation for the POS restatement window, at exact
-- decimal amounts:
--   * every day's line count and net revenue in stg_pos_transactions equals
--     fact_sales, and every store-day of non-voided fact_sales equals
--     sales_daily_store;
--   * every return day's line count and refund amount in stg_pos_returns
--     equals fact_returns, and every store-day of recognized refunds equals
--     sales_daily_store's return measures;
--   * net_sales_after_returns equals non-voided net revenue minus recognized
--     refunds for every store-day (ADR-015).
-- Catches lines left behind by a corrected file and stale store-days.
-- The window matches the models' pos_restatement_days lookback, or the
-- pos_restate_from / pos_restate_to range when set, so the Spectrum side
-- scans only the partitions the run just reprocessed. A restatement also
-- re-judges refunds on dependent return days. Summary checks cover the full
-- extended range rebuilt by sales_daily_store.

{% set restate = pos_restate_range() %}
{% if restate %}
    {% set window_start = "cast('" ~ restate.from_date ~ "' as date)" %}
    {% set source_end = "and transaction_date <= cast('" ~ restate.to_date ~ "' as date)" %}
    {% set key_end = "and date_key <= " ~ restate.to_key %}
    {% set summary_key_end = "and date_key <= " ~ restate.returns_to_key %}
{% else %}
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
    {% set source_end = "" %}
    {% set key_end = "" %}
    {% set summary_key_end = "" %}
{% endif %}

with
{% if restate %}
    {# Recover deleted sale links from the same live state cloned before the build. #}
    {% set returns_fact = ref('fact_returns') %}
    {% set prior = adapter.get_relation(
        database=returns_fact.database, schema='finance', identifier=returns_fact.identifier
    ) if execute else none %}
dependent_days as (
    {{ pos_dependent_return_days(restate, prior if prior is not none else returns_fact) }}
),
{% endif %}
source_daily as (
    select
        {{ date_key_from_date('transaction_date') }} as date_key,
        count(*) as line_count,
        sum(net_revenue) as net_revenue
    from {{ ref('stg_pos_transactions') }}
    where transaction_date >= {{ window_start }}
      and dt >= {{ window_start }}
      {{ source_end }}
    group by 1
),

fact_daily as (
    select
        date_key,
        count(*) as line_count,
        sum(net_revenue) as net_revenue
    from {{ ref('fact_sales') }}
    where date_key >= {{ date_key_from_date(window_start) }}
      {{ key_end }}
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
      {{ summary_key_end }}
      and not is_voided
    group by 1, 2
),

return_source_daily as (
    select
        {{ date_key_from_date('return_date') }} as date_key,
        count(*) as line_count,
        sum(refund_amount) as refund_amount
    from {{ ref('stg_pos_returns') }}
    where return_date >= {{ window_start }}
      and dt >= {{ window_start }}
      {% if restate %}
      and dt <= cast('{{ restate.returns_to_date }}' as date)
      and (return_date <= cast('{{ restate.to_date }}' as date)
           or return_date in (select return_date from dependent_days))
      {% endif %}
    group by 1
),

return_fact_daily as (
    select
        date_key,
        count(*) as line_count,
        sum(refund_amount) as refund_amount
    from {{ ref('fact_returns') }}
    where date_key >= {{ date_key_from_date(window_start) }}
      {% if restate %}
      and (date_key <= {{ restate.to_key }}
           or date_key in (select {{ date_key_from_date('return_date') }} from dependent_days))
      {% endif %}
    group by 1
),

return_fact_store_daily as (
    select
        date_key,
        store_key,
        count(distinct return_id) as return_transaction_count,
        sum(refund_amount) as return_amount
    from {{ ref('fact_returns') }}
    where date_key >= {{ date_key_from_date(window_start) }}
      {{ summary_key_end }}
      and is_refund_recognized
    group by 1, 2
),

summary_store_daily as (
    select
        date_key,
        store_key,
        transaction_count,
        units_sold,
        net_revenue,
        return_transaction_count,
        return_amount,
        net_sales_after_returns
    from {{ ref('sales_daily_store') }}
    where date_key >= {{ date_key_from_date(window_start) }}
      {{ summary_key_end }}
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
    full outer join (
        select * from summary_store_daily where transaction_count > 0
    ) m
      on f.date_key = m.date_key
     and f.store_key = m.store_key
    where f.date_key is null
       or m.date_key is null
       or f.units_sold <> m.units_sold
       or f.transaction_count <> m.transaction_count
       or f.net_revenue <> m.net_revenue
),

return_source_vs_fact as (
    select
        'source_vs_fact_returns' as check_name,
        coalesce(s.date_key, f.date_key) as date_key,
        cast(null as integer) as store_key,
        s.line_count as expected_count,
        f.line_count as actual_count,
        s.refund_amount as expected_net_revenue,
        f.refund_amount as actual_net_revenue
    from return_source_daily s
    full outer join return_fact_daily f
      on s.date_key = f.date_key
),

return_fact_vs_summary as (
    select
        'fact_returns_vs_sales_daily_store' as check_name,
        coalesce(f.date_key, m.date_key) as date_key,
        coalesce(f.store_key, m.store_key) as store_key,
        f.return_transaction_count as expected_count,
        m.return_transaction_count as actual_count,
        f.return_amount as expected_net_revenue,
        m.return_amount as actual_net_revenue
    from return_fact_store_daily f
    full outer join (
        select * from summary_store_daily where return_transaction_count > 0
    ) m
      on f.date_key = m.date_key
     and f.store_key = m.store_key
    where f.date_key is null
       or m.date_key is null
       or f.return_transaction_count <> m.return_transaction_count
       or f.return_amount <> m.return_amount
),

expected_net_after_returns as (
    select
        coalesce(s.date_key, r.date_key) as date_key,
        coalesce(s.store_key, r.store_key) as store_key,
        coalesce(s.net_revenue, 0) - coalesce(r.return_amount, 0) as net_sales_after_returns
    from fact_store_daily s
    full outer join return_fact_store_daily r
      on s.date_key = r.date_key
     and s.store_key = r.store_key
),

net_after_returns_vs_summary as (
    select
        'net_sales_after_returns' as check_name,
        coalesce(e.date_key, m.date_key) as date_key,
        coalesce(e.store_key, m.store_key) as store_key,
        cast(null as bigint) as expected_count,
        cast(null as bigint) as actual_count,
        e.net_sales_after_returns as expected_net_revenue,
        m.net_sales_after_returns as actual_net_revenue
    from expected_net_after_returns e
    full outer join summary_store_daily m
      on e.date_key = m.date_key
     and e.store_key = m.store_key
    where e.date_key is null
       or m.date_key is null
       or e.net_sales_after_returns <> m.net_sales_after_returns
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
union all
select *
from return_source_vs_fact
where expected_count is null
   or actual_count is null
   or expected_count <> actual_count
   or expected_net_revenue <> actual_net_revenue
union all
select *
from return_fact_vs_summary
union all
select *
from net_after_returns_vs_summary
