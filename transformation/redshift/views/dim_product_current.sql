-- Current-version convenience view over the SCD2 product dimension.
--
-- WITH NO SCHEMA BINDING: a bound view would block (or be CASCADE-dropped by)
-- a deliberate DDL migration that drops and recreates marketing.dim_product.
--
-- Redshift cannot convert a bound view to late-binding in place
-- ("Cannot replace a normal view with a late binding view"), so drop first.
-- CASCADE is safe here: nothing else selects from this serving view.

DROP VIEW IF EXISTS serving.dim_product_current CASCADE;

CREATE VIEW serving.dim_product_current AS
SELECT
  product_key,
  product_id,
  sku,
  product_name,
  brand,
  category_l1,
  category_l2,
  unit_cost,
  supplier_id,
  effective_from,
  effective_to,
  is_current,
  record_hash
FROM marketing.dim_product
WHERE is_current = TRUE
WITH NO SCHEMA BINDING;
