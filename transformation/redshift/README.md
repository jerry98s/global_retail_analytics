# Redshift serving-layer DDL

Hand-written DDL for the Gold/serving layer on **Amazon Redshift** (Serverless).
dbt builds the marts in dev; these scripts are the canonical schema definition and
can also be used to pre-create tables or stand up a fresh cluster.

## What changed from Snowflake

| Snowflake | Redshift |
|---|---|
| `NUMBER(p,s)` | `DECIMAL/NUMERIC(p,s)`, `INTEGER`, `BIGINT`, `SMALLINT` |
| `AUTOINCREMENT` | `IDENTITY(seed, step)` |
| `ADD SEARCH OPTIMIZATION` | `DISTKEY` / `SORTKEY` / `DISTSTYLE ALL` |
| `TIMESTAMP_NTZ` | `TIMESTAMP` |
| Virtual warehouses (`02_warehouses.sql`) | Serverless RPU base capacity (Terraform) |
| Resource monitors (`03_resource_monitors.sql`) | Serverless **usage limits** (Terraform) |

Cost guardrails are no longer SQL objects — they are Redshift Serverless RPU base
capacity plus usage limits managed in `infra/terraform/modules/redshift`.

> Note: Redshift declares `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` for the query
> planner but does **not** enforce them. Uniqueness is enforced by the dbt tests.

## Run order

```
spectrum/bronze_external_tables.sql   # once per env — bronze over S3
spectrum/silver_external_tables.sql   # optional — silver inventory_hourly
ddl/01_schemas.sql
ddl/02_dim_date.sql
ddl/03_dim_store.sql
seeds/dim_date.sql          # calendar 2020–2030 (after 02)
seeds/dim_store.sql         # STORE-001..020 (after 03)
ddl/04_dim_product.sql
ddl/05_dim_customer.sql
ddl/06_identity_graph.sql
ddl/07_fact_sales.sql
ddl/08_fact_inventory_snapshot.sql
ddl/09_fact_customer_session.sql
views/dim_product_current.sql
views/customer_360_serving.sql
```

Generate a filled bootstrap script (terraform outputs substituted):

```powershell
.\scripts\cloud\bootstrap_redshift.ps1 -Env dev -IncludeSilver
# → target/redshift_bootstrap_dev.sql
```

Bronze is a Spectrum **external schema** over S3 — see `transformation/redshift/spectrum/bronze_external_tables.sql`.

## Two `seeds/` directories

| Path | Purpose | Loaded by |
|---|---|---|
| `transformation/redshift/seeds/` | Canonical SQL inserts for conformed dimensions (`dim_date`, `dim_store`) on a fresh Redshift cluster | `scripts/cloud/bootstrap_redshift.ps1` |
| `transformation/dbt_project/seeds/bronze/` | Small CSV seed data for **local DuckDB simulation** of bronze sources — no Redshift needed | `dbt seed --target local` |

The Redshift seeds are part of the cloud bootstrap path; the dbt seeds exist only to
exercise the identity-graph chain in CI without cloud credentials.
