-- Blocking return-validity check (ADR-015). A failing row blocks the finance
-- publish; nothing is silently dropped from reconciliation:
--   * a return must reference a known sale line of the same product
--     (fact_returns marks UNKNOWN_SALE / PRODUCT_MISMATCH);
--   * every recognized refund must still point at a non-voided line in
--     fact_sales, so a sale voided or removed by a restatement cannot keep a
--     refund that fact_returns failed to re-judge;
--   * across every committed return, recognized refunds of a line must not
--     exceed its quantity_sold or net_revenue (partial returns on several
--     days are summed, not checked per batch).
-- Returns of voided sales are not failures when fact_returns keeps them as
-- VOIDED_SALE with no recognized refund.

with invalid_reference as (
    select
        lower(eligibility_status) as check_name,
        original_transaction_id,
        original_line_item_number,
        return_id
    from {{ ref('fact_returns') }}
    where eligibility_status in ('UNKNOWN_SALE', 'PRODUCT_MISMATCH')
),

recognized as (
    select
        original_transaction_id,
        original_line_item_number,
        sum(quantity_returned) as quantity_returned,
        sum(refund_amount) as refund_amount
    from {{ ref('fact_returns') }}
    where is_refund_recognized
    group by original_transaction_id, original_line_item_number
),

against_sale as (
    select
        case
            when s.transaction_id is null then 'refund_of_missing_sale'
            when s.is_voided then 'refund_of_voided_sale'
            when r.quantity_returned > s.quantity_sold then 'returned_more_than_sold'
            else 'refunded_more_than_net_revenue'
        end as check_name,
        r.original_transaction_id,
        r.original_line_item_number,
        cast(null as varchar) as return_id
    from recognized r
    left join {{ ref('fact_sales') }} s
      on s.transaction_id = r.original_transaction_id
     and s.line_item_number = r.original_line_item_number
    where s.transaction_id is null
       or s.is_voided
       or r.quantity_returned > s.quantity_sold
       or r.refund_amount > s.net_revenue
)

select * from invalid_reference
union all
select * from against_sale
