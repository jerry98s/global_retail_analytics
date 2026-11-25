-- Sales fact at transaction line-item grain.
-- Distributed on product_key to colocate with dim_product; sorted on date_key
-- for time-range pruning.
--
-- loyalty_id is denormalized so anonymous vs loyalty tests do not need a
-- round-trip to staging. It is not a dimension key.
--
-- Void columns (ADR-015): voided lines stay with their recorded amounts;
-- recognized sales are the rows where is_voided is false. A cluster created
-- before these columns gets them at the next WAP publish (ADD COLUMN), with
-- NULL statuses on rows loaded before then.
--
-- product_key is the dim_product version valid on the sale date (ADR-016);
-- product_id is its natural key. A cluster created before product_id gets it
-- at the next WAP publish, and dbt reloads every day once to fill it.

CREATE TABLE IF NOT EXISTS finance.fact_sales (
  date_key           INTEGER       NOT NULL,
  product_key        BIGINT        NOT NULL,
  product_id         VARCHAR(64),
  store_key          BIGINT        NOT NULL,
  customer_key       BIGINT,
  loyalty_id         VARCHAR(64),
  transaction_id     VARCHAR(128)  NOT NULL,
  line_item_number   INTEGER       NOT NULL,
  quantity_sold      INTEGER,
  gross_revenue      DECIMAL(18,2),
  net_revenue        DECIMAL(18,2),
  gross_margin       DECIMAL(18,2),
  is_voided          BOOLEAN,
  line_status        VARCHAR(16),
  transaction_status VARCHAR(16),
  voided_at          TIMESTAMP,
  void_reason        VARCHAR(64),
  source_updated_at  TIMESTAMP,
  PRIMARY KEY (transaction_id, line_item_number),
  FOREIGN KEY (date_key)     REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (product_key)  REFERENCES marketing.dim_product(product_key),
  FOREIGN KEY (store_key)    REFERENCES finance.dim_store(store_key),
  FOREIGN KEY (customer_key) REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (product_key)
SORTKEY (date_key);

COMMENT ON TABLE finance.fact_sales IS 'Sales fact at transaction line-item grain.';
COMMENT ON COLUMN finance.fact_sales.loyalty_id IS 'POS loyalty identifier; NULL for anonymous transactions.';
