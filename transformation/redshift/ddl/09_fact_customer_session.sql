-- Session-grain customer behavior fact used for Customer 360 and funnel analytics.

CREATE TABLE IF NOT EXISTS marketing.fact_customer_session (
  session_id                 VARCHAR(128)   NOT NULL,
  session_date_key           INTEGER        NOT NULL,
  customer_key               BIGINT,
  client_id                  VARCHAR(128)   NOT NULL,
  session_duration_seconds   BIGINT,
  page_view_count            BIGINT,
  product_view_count         BIGINT,
  add_to_cart_count          BIGINT,
  converted                  BOOLEAN,
  order_id                   VARCHAR(128),
  funnel_depth               SMALLINT,
  platform                   VARCHAR(32),
  PRIMARY KEY (session_id),
  FOREIGN KEY (session_date_key) REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (customer_key)     REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (customer_key)
SORTKEY (session_date_key);

COMMENT ON TABLE marketing.fact_customer_session IS 'Session-grain customer behavior fact used for Customer 360 and funnel analytics.';
