-- Schema bootstrap for the Redshift Gold/serving layers.
-- Bronze is a Spectrum EXTERNAL SCHEMA over S3 (see transformation/redshift/spectrum/bronze_external_tables.sql);
-- it is NOT created here.
--
-- Cost guardrails that lived in Snowflake warehouses + resource monitors are now
-- enforced by Redshift Serverless RPU base capacity and usage limits, which are
-- Redshift Serverless namespace/workgroup is provisioned by the platform stack
-- (infra/terraform/modules/redshift).

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS finance;
CREATE SCHEMA IF NOT EXISTS marketing;
CREATE SCHEMA IF NOT EXISTS summary;
CREATE SCHEMA IF NOT EXISTS serving;
