-- Blocking void-semantics check on fact_sales (ADR-015):
--   * every line of a transaction carries the same transaction_status;
--   * a voided line has voided_at and void_reason, and a recognized line has
--     neither. Bronze written before ADR-015 has no audit fields, so the
--     audit check covers only rows with source_updated_at.

with mixed_transactions as (
    select
        'transaction_status_differs_across_lines' as check_name,
        transaction_id,
        cast(null as integer) as line_item_number
    from {{ ref('fact_sales') }}
    group by transaction_id
    having count(distinct transaction_status) > 1
),

void_audit as (
    select
        case
            when is_voided then 'voided_line_missing_voided_at_or_reason'
            else 'recognized_line_has_void_fields'
        end as check_name,
        transaction_id,
        line_item_number
    from {{ ref('fact_sales') }}
    where source_updated_at is not null
      and (
          (is_voided and (voided_at is null or void_reason is null))
          or (not is_voided and (voided_at is not null or void_reason is not null))
      )
)

select * from mixed_transactions
union all
select * from void_audit
