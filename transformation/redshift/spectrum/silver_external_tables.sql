-- ===========================================================================
-- Spectrum external schema + Silver consumer exports
-- ===========================================================================
-- Spectrum must never scan an Iceberg table's data/ directory: superseded
-- and uncommitted snapshot files live there and would be returned as rows.
-- The hourly Spark step (spark/identity_resolution) reads each table through
-- the Iceberg catalog and publishes a snapshot-safe export: a run directory
-- plus a manifest object that is replaced atomically
-- (spark/identity_resolution/consumer_exports.py). Every table below points
-- at <export>/manifest, so a query sees one complete run.
-- Re-run this DDL only if a schema changes; data refreshes need no action.
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
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/inventory_hourly/manifest';

SELECT COUNT(*) AS inventory_hourly_rows FROM silver.inventory_hourly;

-- ---------------------------------------------------------------------------
-- silver.identity_resolution + silver.identity_edges (ADR-010)
-- Written by the Spark GraphFrames job as Iceberg tables plus exports.
-- dbt reads identity_resolution via source('silver', 'identity_resolution')
-- (int_identity_resolution view).
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
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/identity_resolution/manifest';

DROP TABLE IF EXISTS silver.identity_edges;
CREATE EXTERNAL TABLE silver.identity_edges (
    src                 varchar(320),
    dst                 varchar(320),
    edge_type           varchar(50),
    last_observed_at    timestamp
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/identity_edges/manifest';

SELECT COUNT(*) AS identity_resolution_rows FROM silver.identity_resolution;

-- Canonical replay outputs (ADR-011).
DROP TABLE IF EXISTS silver.clickstream_events;
CREATE EXTERNAL TABLE silver.clickstream_events (
    event_id varchar(256),
    event_type varchar(64),
    event_time timestamp,
    session_id varchar(256),
    client_id varchar(256),
    customer_id varchar(256),
    platform varchar(16),
    app_version varchar(32),
    properties varchar(65535),
    schema_version varchar(32),
    event_date date,
    source_system varchar(256),
    kafka_topic varchar(256),
    kafka_partition integer,
    kafka_offset bigint,
    kafka_timestamp timestamp,
    ingested_at timestamp
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/clickstream_events/manifest';

DROP TABLE IF EXISTS silver.inventory_events;
CREATE EXTERNAL TABLE silver.inventory_events (
    event_id varchar(256),
    event_time timestamp,
    store_id varchar(256),
    product_id varchar(256),
    qty_delta integer,
    event_type varchar(64),
    scanner_id varchar(256),
    is_late boolean,
    schema_version varchar(32),
    event_date date,
    source_system varchar(256),
    kafka_topic varchar(256),
    kafka_partition integer,
    kafka_offset bigint,
    kafka_timestamp timestamp,
    ingested_at timestamp
)
STORED AS PARQUET
LOCATION 's3://<SILVER_BUCKET>/iceberg/consumer_current/inventory_events/manifest';
