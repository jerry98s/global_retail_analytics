-- ===========================================================================
-- Register Spectrum external schema + bronze tables over S3 Iceberg parquet
-- ===========================================================================
-- Existing deployments: re-run after rename (clickstream_bronze → clickstream_events).
-- Old S3 prefixes are not migrated automatically; copy data or backfill from Kafka.
-- Prereqs:
--   1. Platform stack applied: .\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply
--   2. Flink + POS batch jobs landed parquet: .\scripts\cloud\deploy_platform.ps1 -Env dev
--
-- Fill placeholders from tf output:
--   <REDSHIFT_IAM_ROLE_ARN>  -> redshift_iam_role_arn
--   <BRONZE_BUCKET>          -> bronze_bucket_name
--   <GLUE_BRONZE_DB>         -> redshift_glue_bronze_database
-- ===========================================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze
FROM DATA CATALOG
DATABASE '<GLUE_BRONZE_DB>'
IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- clickstream_events (Flink Iceberg bronze.clickstream_events)
DROP TABLE IF EXISTS bronze.clickstream_events;
CREATE EXTERNAL TABLE bronze.clickstream_events (
    event_id        varchar(64),
    event_type      varchar(64),
    event_time      timestamp,
    session_id      varchar(64),
    client_id       varchar(64),
    customer_id     varchar(64),
    platform        varchar(32),
    app_version     varchar(32),
    properties      varchar(65535),
    schema_version  varchar(16),
    event_date      date
)
STORED AS PARQUET
LOCATION 's3://<BRONZE_BUCKET>/iceberg/bronze/clickstream_events/data/';

-- inventory_events (Flink inventory_bronze job — raw stream for dbt)
DROP TABLE IF EXISTS bronze.inventory_events;
CREATE EXTERNAL TABLE bronze.inventory_events (
    event_id        varchar(64),
    event_time      timestamp,
    store_id        varchar(32),
    product_id      varchar(32),
    qty_delta       integer,
    event_type      varchar(32),
    scanner_id      varchar(32),
    is_late         boolean,
    schema_version  varchar(16),
    event_date      date
)
STORED AS PARQUET
LOCATION 's3://<BRONZE_BUCKET>/iceberg/bronze/inventory_events/data/';

-- pos_transactions (daily POS Parquet batch job — generate_pos_parquet.py)
-- POS data lands at s3://.../iceberg/bronze/pos_transactions/data/dt=<YYYY-MM-DD>/
-- so we declare `dt` as a Hive-style partition key. Redshift Spectrum does not
-- support MSCK REPAIR; warehouse_daily_batch_pipeline registers each day with
--   ALTER TABLE bronze.pos_transactions ADD IF NOT EXISTS PARTITION (dt='{{ ds }}')
--   LOCATION '<pos_bronze_s3_path>data/dt={{ ds }}/'
-- Without that, the new directory is invisible. Filter Gold on `dt` (not only
-- transaction_date) so Spectrum can prune. See docs/runbooks/iceberg-maintenance.md.
DROP TABLE IF EXISTS bronze.pos_transactions;
CREATE EXTERNAL TABLE bronze.pos_transactions (
    transaction_id    varchar(64),
    line_item_number  integer,
    transaction_date  date,
    store_id          varchar(32),
    product_id        varchar(32),
    loyalty_id        varchar(32),
    quantity_sold     integer,
    gross_revenue     decimal(18,2),
    net_revenue       decimal(18,2),
    gross_margin      decimal(18,2),
    is_voided         boolean
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://<BRONZE_BUCKET>/iceberg/bronze/pos_transactions/data/';
-- After first deploy + after every daily POS batch (the warehouse DAG does this):
--   ALTER TABLE bronze.pos_transactions ADD IF NOT EXISTS PARTITION (dt='YYYY-MM-DD')
--   LOCATION 's3://<BRONZE_BUCKET>/iceberg/bronze/pos_transactions/data/dt=YYYY-MM-DD/';

SELECT 'clickstream' AS tbl, COUNT(*) AS rows FROM bronze.clickstream_events
UNION ALL
SELECT 'inventory', COUNT(*) FROM bronze.inventory_events
UNION ALL
SELECT 'pos', COUNT(*) FROM bronze.pos_transactions;
