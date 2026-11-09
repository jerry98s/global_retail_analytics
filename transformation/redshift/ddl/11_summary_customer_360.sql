-- Customer 360 summary tables. Each is rebuilt from one marketing Gold table.
-- A fresh deploy creates them here so the marketing clone has a live table
-- to copy. dbt still owns the rows. An existing cluster that already published
-- summary.sessions_daily_platform also needs the column added in
-- ddl/10_customer_360_columns.sql before the next publish.

CREATE TABLE IF NOT EXISTS summary.consent_identity_daily (
  session_date_key        INTEGER        NOT NULL,
  platform                VARCHAR(32)    NOT NULL,
  attribution_status      VARCHAR(16)    NOT NULL,
  session_count           BIGINT         NOT NULL,
  converted_sessions      INTEGER        NOT NULL,
  page_view_sessions      INTEGER        NOT NULL,
  product_view_sessions   INTEGER        NOT NULL,
  cart_sessions           INTEGER        NOT NULL,
  PRIMARY KEY (session_date_key, platform, attribution_status)
)
DISTKEY (session_date_key)
SORTKEY (session_date_key, platform);

COMMENT ON TABLE summary.consent_identity_daily IS 'Sessions by attribution from fact_customer_session. Anonymous and ambiguous sessions stay in session_count.';

CREATE TABLE IF NOT EXISTS summary.customer_segments (
  rfm_segment                 VARCHAR(50)    NOT NULL,
  customer_count              BIGINT         NOT NULL,
  marketing_eligible_count    INTEGER        NOT NULL,
  excluded_count              INTEGER        NOT NULL,
  lifetime_value              DECIMAL(18,2)  NOT NULL,
  PRIMARY KEY (rfm_segment)
)
DISTSTYLE ALL
SORTKEY (rfm_segment);

COMMENT ON TABLE summary.customer_segments IS 'Verified profiles by RFM segment from dim_customer. Anonymous sessions are not rows.';
