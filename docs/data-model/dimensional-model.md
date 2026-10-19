# Dimensional Model Specification

This project uses a three-source Kimball model with shared conformed dimensions.

## Diagrams

- [Mermaid ERD](./erd.md) - view directly in Cursor/GitHub.
- [DBML ERD](./erd.dbml) - paste into dbdiagram.io or import into visual diagramming tools.
- [Naming conventions](./naming-conventions.md) - Kafka → Iceberg → Spectrum → dbt object names.
- [Identity resolution](./identity-resolution.md) - Customer 360 identifier mapping.

## Grain Definitions

- `fact_sales`: one row per `(transaction_id, line_item_number)`
- `fact_inventory_snapshot`: one row per `(snapshot_date_key, snapshot_hour, product_key, store_key)`
- `fact_customer_session`: one row per `session_id`

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

## Referential Integrity

- `fact_sales.product_key -> dim_product.product_key`
- `fact_sales.store_key -> dim_store.store_key`
- `fact_sales.customer_key -> dim_customer.customer_key`
- `fact_inventory_snapshot.product_key -> dim_product.product_key`
- `fact_inventory_snapshot.store_key -> dim_store.store_key`
- `fact_customer_session.customer_key -> dim_customer.customer_key`
- `fact_customer_session.session_date_key -> dim_date.date_key`
