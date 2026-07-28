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
