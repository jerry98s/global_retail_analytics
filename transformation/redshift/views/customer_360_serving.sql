-- Bootstrap / fallback serving view for Customer 360.
-- Canonical ownership: dbt model marts/serving/customer_360_serving.sql
-- (marketing_hourly_customer_360_pipeline). Safe to re-run.
--
-- WITH NO SCHEMA BINDING is required, not cosmetic: the underlying
-- marketing.dim_customer / fact_customer_session tables are WAP-published
-- (ADR-009) by renaming the live table aside and dropping the old copy. A bound
-- view chain would follow the renamed tables by OID and block that drop.
-- dbt emits the same clause via `+bind: false` in dbt_project.yml.
--
-- Redshift cannot convert a bound view to late-binding in place
-- ("Cannot replace a normal view with a late binding view"), so drop first.

DROP VIEW IF EXISTS serving.customer_360_serving CASCADE;

CREATE VIEW serving.customer_360_serving AS
SELECT *
FROM marketing.customer_360_view
WITH NO SCHEMA BINDING;
