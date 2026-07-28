-- Identity mapping from raw identifiers to customer_key for Customer 360 joins.

CREATE TABLE IF NOT EXISTS marketing.identity_graph (
  identifier_type    VARCHAR(50)    NOT NULL,
  identifier_value   VARCHAR(256)   NOT NULL,
  customer_key       BIGINT         NOT NULL,
  confidence_score   DECIMAL(5,4),
  resolution_method  VARCHAR(100),
  is_public_device   BOOLEAN        NOT NULL DEFAULT FALSE,
  is_active          BOOLEAN        NOT NULL DEFAULT TRUE,
  PRIMARY KEY (identifier_type, identifier_value),
  FOREIGN KEY (customer_key) REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (customer_key)
SORTKEY (identifier_type, identifier_value);

COMMENT ON TABLE marketing.identity_graph IS 'Identity mapping table from raw identifiers to customer_key for Customer 360 joins (public devices excluded).';
COMMENT ON COLUMN marketing.identity_graph.confidence_score IS 'Resolution confidence from deterministic + graph-based connected-components logic.';
COMMENT ON COLUMN marketing.identity_graph.is_public_device IS 'True if the identifier is a flagged public device (excluded from merges). Always false in identity_graph mart — public devices stay in int_identity_resolution only.';
