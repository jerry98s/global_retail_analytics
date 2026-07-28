# Platform layers

Authoritative vocabulary for the retail analytics platform. Do not rename
Bronze/Silver/Gold to alternate branding.

## Layer stack

```text
Kafka / POS batch
        │
        ▼
   Bronze (Iceberg) ──► Silver (Iceberg windowed)
        │                      │
        └──────────┬───────────┘
                   ▼
         Spectrum / staging / intermediate
                   ▼
              Gold (Kimball)
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
      Summary             Serving
   (reusable rollups)   (consumer views)
         │
         └──────► metadata DB (ops / DQ / catalog)
```

| Platform layer | Schemas / systems | Role |
|----------------|-------------------|------|
| Bronze | `bronze.*` (Iceberg + Spectrum) | Raw landed events / POS |
| Silver | `silver.*` | Stream aggregates (e.g. inventory hourly) |
| Detail transform | `staging.*`, `intermediate.*` | dbt detail prep |
| Gold | `finance.*`, `marketing.*` | Atomic Kimball facts/dims |
| Summary | `summary.*` | Reusable aggregates from **one** Gold fact |
| Serving | `serving.*` | Consumer-facing views (`customer_360_serving` is dbt-managed; `dim_product_current` remains Redshift DDL) |
| Metadata | `metadata.meta.*` | Catalog + pipeline/DQ/freshness history |

## Analytics database vs metadata database

| Database | Cloud | Local | Contents |
|----------|-------|-------|----------|
| Environment analytics | `dev` or `prod` | `local_retail.duckdb` | staging → Gold → Summary → Serving |
| Metadata | `metadata` (same workgroup) | `local_metadata.duckdb` | `meta.*` only |

Glue remains the Iceberg/Spectrum catalog. The `metadata` database is observability
and governance history, not a metastore replacement.

See [ADR-008](../decisions/ADR-008-metadata-database.md).

## Summary grain rules

| Relation | Grain | Source fact |
|----------|-------|-------------|
| `summary.sales_daily_store` | `(date_key, store_key)` | `finance.fact_sales` |
| `summary.inventory_daily_product_store` | `(snapshot_date_key, product_key, store_key)` | `finance.fact_inventory_snapshot` |
| `summary.sessions_daily_platform` | `(session_date_key, platform)` | `marketing.fact_customer_session` |

No fact-to-fact joins. Conformed dimensions only when needed for attributes
already present on the fact (keys).

## Metadata tables

| Table | Purpose |
|-------|---------|
| `meta.layer_catalog` | Object registry (YAML-seeded) |
| `meta.metric_catalog` | Metric definitions (YAML-seeded) |
| `meta.pipeline_run` | One row per run/attempt |
| `meta.table_freshness` | Append-only freshness / row counts |
| `meta.dq_check_result` | Append-only dbt / GE / reconciliation outcomes |

Writes are fail-open via `scripts/common/metadata_observer.py`.
