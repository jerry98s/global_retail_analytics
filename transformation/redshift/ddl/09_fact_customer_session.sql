-- Session-grain customer behavior fact used for Customer 360 and funnel analytics.
-- Grain is session_id: the producer session_instance_id, or
-- 'legacy:<client>:<session>' for envelope versions before 1.3.
-- customer_key is null when attribution_status is anonymous or ambiguous.

CREATE TABLE IF NOT EXISTS marketing.fact_customer_session (
  session_id                 VARCHAR(640)   NOT NULL,
  session_date_key           INTEGER        NOT NULL,
  session_start_time         TIMESTAMP,
  customer_key               BIGINT,
  client_id                  VARCHAR(256)   NOT NULL,
  session_duration_seconds   BIGINT,
  page_view_count            BIGINT,
  product_view_count         BIGINT,
  add_to_cart_count          BIGINT,
  converted                  BOOLEAN,
  order_id                   VARCHAR(128),
  funnel_depth               SMALLINT,
  platform                   VARCHAR(32),
  producer_session_id        VARCHAR(256),
  attribution_status         VARCHAR(16),
  is_legacy                  BOOLEAN,
  session_fingerprint        VARCHAR(64),
  PRIMARY KEY (session_id),
  FOREIGN KEY (session_date_key) REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (customer_key)     REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (customer_key)
SORTKEY (session_date_key);

COMMENT ON TABLE marketing.fact_customer_session IS 'One row per session instance. Anonymous and ambiguous sessions stay in the conversion denominator with a null customer_key.';
COMMENT ON COLUMN marketing.fact_customer_session.attribution_status IS 'attributed, anonymous, or ambiguous. A session is attributed only when exactly one verified account is on it.';
COMMENT ON COLUMN marketing.fact_customer_session.session_fingerprint IS 'Hash of the stored session columns. A change restates the row.';
