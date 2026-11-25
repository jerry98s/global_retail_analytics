-- Hourly observed-position fact: one row per product/store hour with a
-- movement. qty_delta_hour is the hour's net movement (additive);
-- quantity_on_hand / quantity_available are the balance after it
-- (semi-additive). A cluster created before qty_delta_hour gets it at the
-- next WAP publish, and dbt rewrites every row once to fill it.
--
-- All measure columns are NOT NULL — dbt's fact_inventory_snapshot.sql
-- always populates them via greatest(quantity_on_hand, 0) and a literal
-- FALSE for is_estimated. Marking NOT NULL here lets Redshift reject
-- any future regression that would otherwise surface as NULL downstream
-- (e.g. a LEFT JOIN that drops rows, an upstream schema change).
-- Closes P3.1 from docs/runbooks/dw-checklist-audit.md.
--
-- The grain is per product_id (ADR-016): product_key is the dim_product
-- version valid on the snapshot date, and the balance carries across
-- versions. A cluster created before product_id gets it at the next WAP
-- publish; dbt empties and rebuilds the table once to fill it.

CREATE TABLE IF NOT EXISTS finance.fact_inventory_snapshot (
  snapshot_date_key    INTEGER      NOT NULL,
  snapshot_hour        SMALLINT     NOT NULL,
  product_key          BIGINT       NOT NULL,
  product_id           VARCHAR(64)  NOT NULL,
  store_key            BIGINT       NOT NULL,
  qty_delta_hour       BIGINT       NOT NULL,
  quantity_on_hand     BIGINT       NOT NULL,
  quantity_available   BIGINT       NOT NULL,
  is_estimated         BOOLEAN      NOT NULL,
  PRIMARY KEY (snapshot_date_key, snapshot_hour, product_id, store_key),
  FOREIGN KEY (snapshot_date_key) REFERENCES finance.dim_date(date_key),
  FOREIGN KEY (product_key)       REFERENCES marketing.dim_product(product_key),
  FOREIGN KEY (store_key)         REFERENCES finance.dim_store(store_key)
)
DISTKEY (product_key)
SORTKEY (snapshot_date_key);

COMMENT ON TABLE finance.fact_inventory_snapshot IS 'Hourly observed-position fact: net movement (additive) and balance after it (semi-additive).';
