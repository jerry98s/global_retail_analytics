-- Seed finance.dim_store (Type 1) — idempotent.
-- Natural keys match simulators: generate_pos_parquet.py, inventory_producer.py (STORE-001..020).
-- Run after ddl/03_dim_store.sql.

INSERT INTO finance.dim_store (
  store_id,
  store_name,
  store_type,
  city,
  country_code,
  region,
  is_active
)
SELECT
  s.store_id,
  s.store_name,
  'RETAIL' AS store_type,
  s.city,
  'MY' AS country_code,
  s.region,
  true AS is_active
FROM (
  SELECT 'STORE-001' AS store_id, 'Kuala Lumpur Central' AS store_name, 'Kuala Lumpur' AS city, 'Central' AS region
  UNION ALL SELECT 'STORE-002', 'Petaling Jaya Main', 'Petaling Jaya', 'Central'
  UNION ALL SELECT 'STORE-003', 'Shah Alam Plaza', 'Shah Alam', 'Central'
  UNION ALL SELECT 'STORE-004', 'Subang Parade', 'Subang Jaya', 'Central'
  UNION ALL SELECT 'STORE-005', 'Penang Gurney', 'George Town', 'North'
  UNION ALL SELECT 'STORE-006', 'Penang Queensbay', 'Bayan Lepas', 'North'
  UNION ALL SELECT 'STORE-007', 'Ipoh Parade', 'Ipoh', 'North'
  UNION ALL SELECT 'STORE-008', 'Johor Bahru City', 'Johor Bahru', 'South'
  UNION ALL SELECT 'STORE-009', 'Melaka Mall', 'Melaka', 'South'
  UNION ALL SELECT 'STORE-010', 'Kuching Riverside', 'Kuching', 'East'
  UNION ALL SELECT 'STORE-011', 'Kota Kinabalu Suria', 'Kota Kinabalu', 'East'
  UNION ALL SELECT 'STORE-012', 'Seremban Gateway', 'Seremban', 'Central'
  UNION ALL SELECT 'STORE-013', 'Klang Sentral', 'Klang', 'Central'
  UNION ALL SELECT 'STORE-014', 'Cyberjaya Hub', 'Cyberjaya', 'Central'
  UNION ALL SELECT 'STORE-015', 'Putrajaya Presint', 'Putrajaya', 'Central'
  UNION ALL SELECT 'STORE-016', 'Alor Setar Mall', 'Alor Setar', 'North'
  UNION ALL SELECT 'STORE-017', 'Kuantan City', 'Kuantan', 'East'
  UNION ALL SELECT 'STORE-018', 'Miri Boulevard', 'Miri', 'East'
  UNION ALL SELECT 'STORE-019', 'Sandakan Plaza', 'Sandakan', 'East'
  UNION ALL SELECT 'STORE-020', 'Klang Valley Outlet', 'Puchong', 'Central'
) s
WHERE NOT EXISTS (
  SELECT 1 FROM finance.dim_store existing WHERE existing.store_id = s.store_id
);

SELECT COUNT(*) AS dim_store_rows FROM finance.dim_store;
