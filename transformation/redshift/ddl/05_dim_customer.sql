-- Customer dimension (Type 1) enriched with consent + lifecycle attributes.
-- Distributed on customer_key to colocate with fact joins.
-- customer_key is computed deterministically by dbt (md5-based), so no IDENTITY.

CREATE TABLE IF NOT EXISTS marketing.dim_customer (
  customer_key           BIGINT         NOT NULL,
  loyalty_id             VARCHAR(64),
  email_hashed           CHAR(64),
  loyalty_tier           VARCHAR(50),
  rfm_segment            VARCHAR(50),
  churn_risk_score       DECIMAL(10,4),
  total_lifetime_value   DECIMAL(18,2),
  marketing_consent      BOOLEAN,
  analytics_consent      BOOLEAN,
  last_updated_at        TIMESTAMP,
  PRIMARY KEY (customer_key)
)
DISTKEY (customer_key)
SORTKEY (loyalty_id);

COMMENT ON TABLE marketing.dim_customer IS 'Customer dimension (Type 1) enriched with consent and lifecycle attributes.';
COMMENT ON COLUMN marketing.dim_customer.email_hashed IS 'SHA-256 hash of customer email, never raw PII.';
