# Lake & warehouse naming conventions

Single naming rule: **the same logical object uses the same name from Kafka through
Spectrum/dbt**, except where the layer is already implied by schema (`staging.stg_*`).

## Layer overview

| Layer | Redshift | Iceberg namespace | Example table |
|-------|----------|-------------------|---------------|
| Bronze (lake) | `bronze.*` (Spectrum external) | `bronze` | `bronze.clickstream_events` |
| Silver (lake) | `silver.*` (Spectrum external) | `silver` | `silver.inventory_hourly` |
| Staging (dbt) | `staging.*` | — | `staging.stg_clickstream_events` |
| Intermediate | `intermediate.*` | — | `intermediate.int_session_reconstruction` |
| Gold | `finance.*`, `marketing.*` | — | `finance.fact_sales` |
| Summary | `summary.*` | — | `summary.sales_daily_store` |
| Serving | `serving.*` | — | `serving.customer_360_serving` |
| Metadata (ops DB) | `metadata.meta.*` | — | `meta.pipeline_run` |

Redshift **analytics database** is per environment: `dev` or `prod`
(`redshift_database_name` in tfvars). Operational catalog/DQ history lives in a
separate database `metadata` (`redshift_metadata_database_name`) on the same
workgroup — see [platform-layers.md](platform-layers.md) and
[ADR-008](../decisions/ADR-008-metadata-database.md).

## Bronze tables

| Kafka topic | Iceberg table | S3 path suffix | Spectrum / dbt source |
|-------------|---------------|----------------|------------------------|
| `clickstream.events.v1` | `bronze.clickstream_events` | `iceberg/bronze/clickstream_events/data/` | `bronze.clickstream_events` |
| `inventory.events.v1` | `bronze.inventory_events` | `iceberg/bronze/inventory_events/data/` | `bronze.inventory_events` |
| *(batch POS)* | *(Parquet only)* | `iceberg/bronze/pos_transactions/data/` | `bronze.pos_transactions` |

POS is a **batch exception**: grain is line items, not events, so the name stays
`pos_transactions`.

## Silver tables

| Kafka topic | Iceberg table | S3 path suffix |
|-------------|---------------|----------------|
| `inventory.events.v1` | `silver.inventory_hourly` | `iceberg/silver/inventory_hourly/data/` |

`finance.fact_inventory_snapshot` reads from **silver** `inventory_hourly`
(kappa architecture — see [ADR-007](../decisions/ADR-007-inventory-kappa.md)).
Bronze `inventory_events` is the audit/replay layer; no Gold model reads it
directly. Silver is exposed in Redshift via Spectrum
(`transformation/redshift/spectrum/silver_external_tables.sql`).

## S3 layout

```
s3://{project}-{env}-bronze/iceberg/{namespace}/{table}/data/
s3://{project}-{env}-silver/iceberg/{namespace}/{table}/data/
```

Iceberg warehouse URI is the bucket prefix ending in `/iceberg` (not `/iceberg/bronze`).
The namespace (`bronze`, `silver`) is the Iceberg database name, not an extra S3 folder
under the warehouse root.

Local Docker uses the same layout under `/tmp/iceberg/`.

## Glue Data Catalog

Terraform creates one Glue database per env for Spectrum:

```
{project}_{env}_bronze   →  e.g. retail_platform_dev_bronze
```

Redshift external schema **`bronze`** maps to that Glue database via
`transformation/redshift/spectrum/bronze_external_tables.sql`.

## Code constants

Python constants live in `streaming/flink_jobs/lake_names.py` and are imported by Flink
jobs. When adding a new bronze/silver table, update that module and this document together.

## Gold / serving (unchanged)

Kimball names follow `docs/data-model/dimensional-model.md`:

- Facts: `fact_{entity}` (`fact_sales`, `fact_inventory_snapshot`, `fact_customer_session`)
- Dims: `dim_{entity}` (`dim_product`, `dim_customer`, …)
- Serving views: `serving.{use_case}` (`customer_360_serving`, `dim_product_current`)
