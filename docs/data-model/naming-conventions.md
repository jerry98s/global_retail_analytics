# Lake & warehouse naming conventions

Single naming rule: **the same logical object uses the same name from Kafka through
Spectrum/dbt**, except where the layer is already implied by schema (`staging.stg_*`).

## Layer overview

| Layer | Redshift | Iceberg namespace | Example table |
|-------|----------|-------------------|---------------|
| Bronze (lake) | POS Spectrum; raw ledgers via Spark/Iceberg | `bronze` | `bronze.clickstream_records` |
| Silver (lake) | `silver.*` (Spectrum external) | `silver` | `silver.clickstream_events` |
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

Bronze keeps every Kafka delivery as raw bytes (ADR-011), so the tables are
named `*_records`, not `*_events`: a record is one delivery, and transport
repeats are kept.

| Kafka topic | Iceberg table | S3 path suffix | Read by |
|-------------|---------------|----------------|---------|
| `clickstream.events.v1` | `bronze.clickstream_records` | Iceberg-managed; read snapshots | Spark promotion only |
| `inventory.events.v1` | `bronze.inventory_records` | Iceberg-managed; read snapshots | Spark promotion only |
| *(batch POS)* | *(Parquet only)* | `iceberg/bronze/pos_transactions/data/` | `bronze.pos_transactions` (Spectrum / dbt source) |

POS is a **batch exception**: grain is line items, not events, so the name stays
`pos_transactions`.

`bronze.clickstream_events` / `bronze.inventory_events` are legacy filtered
tables from before ADR-011. Spectrum still registers them, but no current
model reads them.

## Silver tables

| Source | Iceberg table | S3 path suffix |
|--------|---------------|----------------|
| Spark promotion of `bronze.clickstream_records` | `silver.clickstream_events` | `iceberg/consumer_current/clickstream_events/` |
| Spark promotion of `bronze.inventory_records` | `silver.inventory_events` | `iceberg/consumer_current/inventory_events/` |
| `inventory.events.v1` (Flink window) | `silver.inventory_hourly` | `iceberg/silver/inventory_hourly/data/` |
| Spark GraphFrames job | `silver.identity_resolution`, `silver.identity_edges` | `iceberg/consumer_current/identity_*/` |

Spark-written Silver tables also have a replace-only `consumer_current/{table}/`
Parquet export. Spectrum reads that export, never the Iceberg `data/` folders.

`finance.fact_inventory_snapshot` reads `int_inventory_hourly`: **silver**
`inventory_events` for hours Spark has covered, **silver** `inventory_hourly`
after that (see [ADR-012](../decisions/ADR-012-inventory-lambda.md)).
Bronze `*_records` is the audit/replay layer (ADR-011); legacy `bronze.*_events`
is filtered historical data. Canonical Silver event exports replace on each
successful Spark run; quarantine and duplicates use sibling `*_quarantine` and
`*_duplicates` Iceberg tables and exports. Silver events are exposed via Spectrum
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

## Semantic layer and metadata catalogs

- Semantic measures and metrics use the summary column they aggregate
  (`ending_on_hand_qty`, `session_count`, `conversion_rate`). Labels carry the
  business wording ("Quantity on hand").
- Time dimensions drop `_key` from the date key (`snapshot_date`,
  `session_date`); the generic `date_key` on sales is `sales_date`.
- Primary entities name the grain: `store_day`, `product_store_day`,
  `platform_day`.
- `layer_catalog.yml` grain is the unique-key tuple when the object has one,
  e.g. `(date_key, store_key)`; prose only for keyless objects.
- Operational `meta.*` metrics are `{subject}_{measure}` with the unit last
  (`table_freshness_breach_count`, `pipeline_success_rate`).
