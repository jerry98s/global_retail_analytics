# Dimensional Model Specification

This project uses a three-source Kimball model with shared conformed dimensions.

## Diagrams

- [Mermaid ERD](./erd.md) - view directly in Cursor/GitHub.
- [Naming conventions](./naming-conventions.md) - Kafka → Iceberg → Spectrum → dbt object names.
- [Identity resolution](./identity-resolution.md) - Customer 360 identifier mapping.

## Grain Definitions

- `fact_sales`: one row per `(transaction_id, line_item_number)`
- `fact_returns`: one row per `(return_id, return_line_number)`, dated and
  stored at the return; references the original sale line (ADR-015)
- `fact_inventory_snapshot`: hourly observed-position fact, one row per
  `(snapshot_date_key, snapshot_hour, product_id, store_key)` for hours with a
  movement. `qty_delta_hour` is the hour's net movement (additive);
  `quantity_on_hand` is the balance after it (semi-additive), and equals the
  opening count plus the cumulative movement. Hours without a row hold the
  previous balance; `summary.inventory_position_daily` is the dense daily
  view. `product_key` is the version valid on that date (ADR-016)
- `fact_customer_session`: one row per `session_id` (the producer's
  `session_instance_id`; one legacy session per client and producer session
  for pre-1.3 events). `customer_key` is NULL unless exactly one account
  signed in during the session.

## Dimensions

- `dim_date` (Type 0): canonical calendar attributes keyed by `date_key`
- `dim_store` (Type 1): current store attributes keyed by `store_key`
- `dim_product` (SCD Type 2): historical product attributes keyed by `product_key`
- `dim_customer` (Type 1): customer profile and consent state keyed by `customer_key`

## Identity Resolution Layer

`identity_graph` maps raw identifiers to `customer_key` and is resolved before
joining clickstream/POS activity into Customer 360 marts.

## SCD Rules

- Only `dim_product` uses SCD Type 2.
- `effective_from` is inclusive; `effective_to` is exclusive.
- Exactly one `is_current = true` row per natural `product_id`.
- `record_hash` tracks attribute-level changes for merge decisions.
- Facts join `dim_product` as of their business date (ADR-016): the sale
  date, a return's original sale date (the return date when the sale is
  unknown), the snapshot date. A date before the product's first version
  takes that first version. Facts also store `product_id`; join it to the
  `is_current` row to report on current attributes.

## Referential Integrity

- `fact_sales.product_key -> dim_product.product_key`
- `fact_sales.store_key -> dim_store.store_key`
- `fact_sales.customer_key -> dim_customer.customer_key`
- `fact_returns.date_key -> dim_date.date_key`
- `fact_returns.product_key -> dim_product.product_key`
- `fact_returns.store_key -> dim_store.store_key`
- `fact_inventory_snapshot.product_key -> dim_product.product_key`
- `fact_inventory_snapshot.store_key -> dim_store.store_key`
- `fact_customer_session.customer_key -> dim_customer.customer_key`
- `fact_customer_session.session_date_key -> dim_date.date_key`
