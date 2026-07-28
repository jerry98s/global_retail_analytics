-- Sales fact at transaction line-item grain.
-- Distributed on product_key to colocate with dim_product; sorted on date_key
-- for time-range pruning.

CREATE TABLE IF NOT EXISTS finance.fact_sales (
  date_key           INTEGER       NOT NULL,
  product_key        BIGINT        NOT NULL,
  store_key          BIGINT        NOT NULL,
  customer_key       BIGINT,
  transaction_id     VARCHAR(128)  NOT NULL,
  line_item_number   INTEGER       NOT NULL,
  quantity_sold      BIGINT,
  gross_revenue      DECIMAL(18,2),
  net_revenue        DECIMAL(18,2),
  gross_margin       DECIMAL(18,2),
  is_voided          BOOLEAN,
  PRIMARY KEY (transaction_id, line_item_number),
  FOREIGN KEY (date_key)     REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (product_key)  REFERENCES marketing.dim_product(product_key),
  FOREIGN KEY (store_key)    REFERENCES finance.dim_store(store_key),
  FOREIGN KEY (customer_key) REFERENCES marketing.dim_customer(customer_key)
)
DISTKEY (product_key)
SORTKEY (date_key);

COMMENT ON TABLE finance.fact_sales IS 'Sales fact at transaction line-item grain.';
