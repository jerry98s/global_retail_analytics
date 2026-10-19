-- Create the operational metadata database on the existing Redshift Serverless
-- namespace/workgroup. Run this while connected to the environment analytics
-- database (dev or prod), NOT while already connected to metadata.
--
-- Does NOT change Terraform redshift_database_name / namespace db_name.
-- Generate via: .\scripts\cloud\bootstrap_redshift.ps1 -Env <env> -MetadataOnly

CREATE DATABASE metadata;
