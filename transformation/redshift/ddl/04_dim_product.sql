-- Product dimension (SCD2). Distributed on product_key to colocate with the
-- fact joins; sorted on the natural key + current flag for SCD2 lookups.
-- product_key is computed deterministically by dbt (md5-based), so no IDENTITY.

CREATE TABLE IF NOT EXISTS marketing.dim_product (
  product_key     BIGINT        NOT NULL,
  product_id      VARCHAR(64)   NOT NULL,
  sku             VARCHAR(64),
  product_name    VARCHAR(200),
  brand           VARCHAR(100),
  category_l1     VARCHAR(100),
  category_l2     VARCHAR(100),
  unit_cost       DECIMAL(18,4),
  supplier_id     VARCHAR(64),
  effective_from  DATE,
  effective_to    DATE,
  is_current      BOOLEAN,
  record_hash     VARCHAR(64),
  PRIMARY KEY (product_key)
)
DISTKEY (product_key)
SORTKEY (product_id, is_current);

COMMENT ON TABLE marketing.dim_product IS 'Product dimension (Type 2 SCD) with effective dating.';
COMMENT ON COLUMN marketing.dim_product.record_hash IS 'SHA-256 hash of tracked attributes for change detection.';
