{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    dist='customer_key',
    sort='loyalty_id'
  )
}}

/*
  Type 1 customer dimension enriched with RFM + consent attributes.

  Grain: one row per customer_key (resolved by int_identity_resolution).
  Source: int_identity_resolution (loyalty_id anchor) + int_rfm_scoring
          (omnichannel RFM: POS net_revenue + converted clickstream sessions)
          + int_customer_consent (explicit consent flags).
  Notes:
    - email_hashed is NULL today — placeholder for future PII ingestion
      behind the marketing_consent gate.
    - Consent flags take the customer's latest explicit statement, so an
      opt-out revokes on the next run. Without a statement, loyalty members
      default to marketing consent and authenticated or loyalty customers to
      analytics consent. marketing_consent is the PII gate. See
      docs/data-model/identity-resolution.md.
    - last_updated_at is current_timestamp — every dbt run on an affected
      customer_key overwrites the row via delete+insert.
*/

with identity_base as (
    select
        customer_key,
        max(case when identifier_type = 'loyalty_id' then identifier_value end) as loyalty_id
    from {{ ref('int_identity_resolution') }}
    group by customer_key
),
rfm as (
    select *
    from {{ ref('int_rfm_scoring') }}
),
consent as (
    select *
    from {{ ref('int_customer_consent') }}
)
select
    i.customer_key,
    i.loyalty_id,
    cast(null as varchar) as email_hashed,
    case
        when r.monetary_value >= 5000 then 'platinum'
        when r.monetary_value >= 1000 then 'gold'
        else 'standard'
    end as loyalty_tier,
    coalesce(r.rfm_segment, 'general') as rfm_segment,
    cast(case
        when r.recency_days > 90 then 0.75
        when r.recency_days > 30 then 0.40
        else 0.10
    end as decimal(10, 4)) as churn_risk_score,
    cast(coalesce(r.monetary_value, 0) as decimal(18, 2)) as total_lifetime_value,
    -- An explicit statement always wins; loyalty enrolment is only the default.
    coalesce(
        c.explicit_marketing_consent,
        i.loyalty_id is not null
    ) as marketing_consent,
    coalesce(
        c.explicit_analytics_consent,
        c.customer_key is not null or i.loyalty_id is not null
    ) as analytics_consent,
    current_timestamp as last_updated_at
from identity_base i
left join rfm r
  on i.customer_key = r.customer_key
left join consent c
  on i.customer_key = c.customer_key
