-- Existing-cluster changes for the Customer 360 contract.
-- CREATE TABLE IF NOT EXISTS does not alter a table that already exists.
-- Run once on a cluster created from the previous scripts. A fresh deploy
-- uses 06 and 09 and does not need this file.
-- Redshift has no transactional DDL; run each statement and skip any that
-- report the column is already gone or already present.

ALTER TABLE marketing.identity_graph DROP COLUMN is_public_device;

ALTER TABLE marketing.fact_customer_session ALTER COLUMN session_id TYPE VARCHAR(640);
ALTER TABLE marketing.fact_customer_session ALTER COLUMN client_id TYPE VARCHAR(256);
ALTER TABLE marketing.fact_customer_session ADD COLUMN session_start_time TIMESTAMP;
ALTER TABLE marketing.fact_customer_session ADD COLUMN producer_session_id VARCHAR(256);
ALTER TABLE marketing.fact_customer_session ADD COLUMN attribution_status VARCHAR(16);
ALTER TABLE marketing.fact_customer_session ADD COLUMN is_legacy BOOLEAN;
ALTER TABLE marketing.fact_customer_session ADD COLUMN session_fingerprint VARCHAR(64);

-- Skip when summary.sessions_daily_platform does not exist yet. A later dbt
-- rebuild creates it with this column. When the table already exists, the
-- next publish inserts the column and fails without it.
ALTER TABLE summary.sessions_daily_platform ADD COLUMN anonymous_session_count INTEGER;
