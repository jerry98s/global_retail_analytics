-- ===========================================================================
-- Spectrum external schema + silver inventory_hourly (Flink silver layer)
-- ===========================================================================
-- Prereqs: platform stack + inventory_hourly Flink job writing Parquet.
--
-- Column semantics (NOT the same as gold finance.fact_inventory_snapshot):
--   qty_delta_hour    = SUM(qty_delta) within the hourly TUMBLE window
--                       (positive = net receipts, negative = net sales/adjustments)
--   qty_received_hour = SUM(qty_delta) for receipts only (qty_delta > 0)
--                       — gross inbound in the hour, not a running balance
--
-- For the running on-hand balance (cumulative across windows), query
-- finance.fact_inventory_snapshot in Redshift — dbt computes it there.
--
-- Placeholders from terraform output:
--   <REDSHIFT_IAM_ROLE_ARN>  -> redshift_iam_role_arn
--   <SILVER_BUCKET>          -> silver_bucket_name
--   <GLUE_SILVER_DB>         -> optional; use same glue DB or create silver DB in TF
-- ===========================================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS silver
FROM DATA CATALOG
DATABASE '<GLUE_SILVER_DB>'
IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

DROP TABLE IF EXISTS silver.inventory_hourly;
CREATE EXTERNAL TABLE silver.inventory_hourly (
    snapshot_date_key   integer,
    snapshot_hour       integer,
    product_id          varchar(32),
    store_id            varchar(32),
    qty_delta_hour      bigint,
    qty_received_hour   bigint,
    is_estimated        boolean
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/silver/inventory_hourly/data/';

SELECT COUNT(*) AS inventory_hourly_rows FROM silver.inventory_hourly;

-- ---------------------------------------------------------------------------
-- silver.identity_resolution + silver.identity_edges (ADR-010)
-- Written by the Spark GraphFrames job (spark/identity_resolution/) as
-- Iceberg tables plus dedicated current-run Parquet exports. Spectrum must not
-- scan an Iceberg table's data/ directory: superseded snapshot files may remain
-- there after replace/compaction and would be returned as duplicate stale rows.
-- dbt reads identity_resolution via source('silver', 'identity_resolution')
-- (int_identity_resolution view). Re-run this DDL only if the schema changes;
-- plain data refreshes need no Spectrum action (unpartitioned table).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.identity_resolution;
CREATE EXTERNAL TABLE silver.identity_resolution (
    identifier_type     varchar(50),
    identifier_value    varchar(256),
    customer_key        bigint,
    confidence_score    decimal(5,4),
    resolution_method   varchar(100),
    is_public_device    boolean,
    component_rep_node  varchar(320),
    component_rep_type  varchar(50),
    computed_at         timestamp
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/identity_resolution/';

DROP TABLE IF EXISTS silver.identity_edges;
CREATE EXTERNAL TABLE silver.identity_edges (
    src                 varchar(320),
    dst                 varchar(320),
    edge_type           varchar(50),
    last_observed_at    timestamp
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/identity_edges/';

SELECT COUNT(*) AS identity_resolution_rows FROM silver.identity_resolution;
