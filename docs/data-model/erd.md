# Data Model ERD

This diagram is the repo-native ERD for the Global Retail Analytics dimensional
model. It can be viewed directly in Cursor/GitHub as Mermaid, while
`erd.dbml` can be pasted into dbdiagram.io for a Lucidchart-style canvas.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_SALES : "date_key"
    DIM_DATE ||--o{ FACT_INVENTORY_SNAPSHOT : "snapshot_date_key"
    DIM_DATE ||--o{ FACT_CUSTOMER_SESSION : "session_date_key"

    DIM_STORE ||--o{ FACT_SALES : "store_key"
    DIM_STORE ||--o{ FACT_INVENTORY_SNAPSHOT : "store_key"

    DIM_PRODUCT ||--o{ FACT_SALES : "product_key"
    DIM_PRODUCT ||--o{ FACT_INVENTORY_SNAPSHOT : "product_key"

    DIM_CUSTOMER ||--o{ FACT_SALES : "customer_key"
    DIM_CUSTOMER ||--o{ FACT_CUSTOMER_SESSION : "customer_key"
    DIM_CUSTOMER ||--o{ IDENTITY_GRAPH : "customer_key"

    DIM_DATE {
      int date_key PK
      date full_date
      tinyint day_of_week
      tinyint month_number
      tinyint quarter
      smallint fiscal_year
      boolean is_weekend
      boolean is_public_holiday
    }

    DIM_STORE {
      int store_key PK
      varchar store_id UK
      varchar store_name
      varchar store_type
      varchar city
      char country_code
      varchar region
      boolean is_active
    }

    DIM_PRODUCT {
      int product_key PK
      varchar product_id
      varchar sku
      varchar product_name
      varchar brand
      varchar category_l1
      varchar category_l2
      decimal unit_cost
      varchar supplier_id
      date effective_from
      date effective_to
      boolean is_current
      char record_hash
    }

    DIM_CUSTOMER {
      int customer_key PK
      varchar loyalty_id
      char email_hashed
      varchar loyalty_tier
      varchar rfm_segment
      decimal churn_risk_score
      decimal total_lifetime_value
      boolean marketing_consent
      boolean analytics_consent
      timestamp last_updated_at
    }

    IDENTITY_GRAPH {
      varchar identifier_type PK
      varchar identifier_value PK
      int customer_key FK
      decimal confidence_score
      varchar resolution_method
    }

    FACT_SALES {
      int date_key FK
      int product_key FK
      int store_key FK
      int customer_key FK
      varchar transaction_id PK
      tinyint line_item_number PK
      int quantity_sold
      decimal gross_revenue
      decimal net_revenue
      decimal gross_margin
      boolean is_voided
    }

    FACT_INVENTORY_SNAPSHOT {
      int snapshot_date_key PK,FK
      tinyint snapshot_hour PK
      int product_key PK,FK
      int store_key PK,FK
      int quantity_on_hand
      int quantity_available
      boolean is_estimated
    }

    FACT_CUSTOMER_SESSION {
      varchar session_id PK
      int session_date_key FK
      int customer_key FK
      varchar client_id
      int session_duration_seconds
      int page_view_count
      int product_view_count
      int add_to_cart_count
      boolean converted
      varchar order_id
      tinyint funnel_depth
      varchar platform
    }
```

## Diagram Files

- `erd.md`: Mermaid ERD for Cursor/GitHub rendering.
- `erd.dbml`: DBML version for dbdiagram.io or Lucidchart-style editing.
