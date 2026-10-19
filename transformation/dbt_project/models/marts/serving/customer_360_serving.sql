{{
  config(
    materialized='view',
    schema='serving'
  )
}}

/*
  Thin serving wrapper over marketing.customer_360_view (consent-gated).
  Dashboard and BI read this relation. Redshift DDL under
  the serving view on main is bootstrap /
  CREATE OR REPLACE fallback when dbt has not yet run.
*/

select *
from {{ ref('customer_360_view') }}
