CREATE OR REPLACE VIEW serving.dim_product_current AS
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
WHERE is_current = TRUE;
