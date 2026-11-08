-- Identity mapping from raw identifiers to customer_key for Customer 360 joins.
-- Only verified account-to-loyalty logins create a shared customer_key.
-- Devices are never rows here.

CREATE TABLE IF NOT EXISTS marketing.identity_graph (
  identifier_type    VARCHAR(50)    NOT NULL,
  identifier_value   VARCHAR(256)   NOT NULL,
  customer_key       BIGINT         NOT NULL,
  confidence_score   DECIMAL(5,4),
  resolution_method  VARCHAR(100),
  is_active          BOOLEAN        NOT NULL DEFAULT TRUE,
  PRIMARY KEY (identifier_type, identifier_value),
  FOREIGN KEY (customer_key) REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (customer_key)
SORTKEY (identifier_type, identifier_value);

COMMENT ON TABLE marketing.identity_graph IS 'Identity mapping from raw identifiers to customer_key. A loyalty card shares a key only when a verified login linked it.';
COMMENT ON COLUMN marketing.identity_graph.confidence_score IS 'Resolution confidence from the verified-login connected component.';
