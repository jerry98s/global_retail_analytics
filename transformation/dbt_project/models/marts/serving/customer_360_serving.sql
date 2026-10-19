{{
  config(
    materialized='view',
    schema='serving'
  )
}}

/*
  Thin serving wrapper over marketing.customer_360_view (consent-gated).
  Dashboard and BI read this relation. Redshift DDL under
  transformation/redshift/views/customer_360_serving.sql is bootstrap /
  CREATE OR REPLACE fallback when dbt has not yet run.
*/

select *
from {{ ref('customer_360_view') }}
