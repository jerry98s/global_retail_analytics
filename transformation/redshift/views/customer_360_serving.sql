-- Bootstrap / fallback serving view for Customer 360.
-- Canonical ownership: dbt model marts/serving/customer_360_serving.sql
-- (marketing_hourly_customer_360_pipeline). Safe to re-run; dbt CREATE OR
-- REPLACE will refresh the same relation after the hourly marketing job.

CREATE OR REPLACE VIEW serving.customer_360_serving AS
SELECT *
FROM marketing.customer_360_view;
