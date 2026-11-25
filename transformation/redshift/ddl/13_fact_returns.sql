-- Returns fact at return-line grain (ADR-015).
-- date_key / store_key are the return processing date and store. The original
-- sale is referenced by (original_transaction_id, original_line_item_number);
-- finance.fact_sales is never changed by a return.
-- Distributed and sorted like fact_sales so drill-across by product and date
-- stays colocated.
-- product_key is the dim_product version valid on the original sale date
-- (the return date for an UNKNOWN_SALE), ADR-016.

CREATE TABLE IF NOT EXISTS finance.fact_returns (
  date_key                   INTEGER       NOT NULL,
  product_key                BIGINT        NOT NULL,
  product_id                 VARCHAR(64),
  store_key                  BIGINT        NOT NULL,
  original_date_key          INTEGER,
  return_id                  VARCHAR(64)   NOT NULL,
  return_line_number         INTEGER       NOT NULL,
  original_transaction_id    VARCHAR(64)   NOT NULL,
  original_line_item_number  INTEGER       NOT NULL,
  quantity_returned          INTEGER       NOT NULL,
  refund_amount              DECIMAL(18,2) NOT NULL,
  return_status              VARCHAR(16)   NOT NULL,
  eligibility_status         VARCHAR(16)   NOT NULL,
  is_refund_recognized       BOOLEAN       NOT NULL,
  reason_code                VARCHAR(64),
  processed_at               TIMESTAMP     NOT NULL,
  PRIMARY KEY (return_id, return_line_number),
  FOREIGN KEY (date_key)     REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (product_key)  REFERENCES marketing.dim_product(product_key),
  FOREIGN KEY (store_key)    REFERENCES finance.dim_store(store_key)
)
DISTKEY (product_key)
SORTKEY (date_key);

COMMENT ON TABLE finance.fact_returns IS 'Returns fact at return-line grain. Only is_refund_recognized rows reduce net sales.';
