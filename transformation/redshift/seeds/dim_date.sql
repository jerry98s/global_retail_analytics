-- Seed finance.dim_date (Type 0 calendar) — idempotent.
-- Range: 2020-01-01 through 2030-12-31 (matches typical demo / batch dates).
-- Run after ddl/02_dim_date.sql.

INSERT INTO finance.dim_date (
  date_key,
  full_date,
  day_of_week,
  month_number,
  quarter,
  fiscal_year,
  is_weekend,
  is_public_holiday
)
SELECT
  cast(to_char(d.full_date, 'YYYYMMDD') as integer) AS date_key,
  d.full_date,
  cast(extract(dow from d.full_date) as smallint) AS day_of_week,
  cast(extract(month from d.full_date) as smallint) AS month_number,
  cast(extract(quarter from d.full_date) as smallint) AS quarter,
  cast(extract(year from d.full_date) as smallint) AS fiscal_year,
  (extract(dow from d.full_date) IN (0, 6)) AS is_weekend,
  false AS is_public_holiday
FROM (
  SELECT dateadd(day, seq.n, date '2020-01-01') AS full_date
  FROM (
    SELECT
      (a.n + b.n * 10 + c.n * 100 + d.n * 1000) AS n
    FROM (
      SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) a
    CROSS JOIN (
      SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) b
    CROSS JOIN (
      SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) c
    CROSS JOIN (
      SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) d
  ) seq
  WHERE seq.n <= datediff(day, date '2020-01-01', date '2030-12-31')
) d
WHERE NOT EXISTS (SELECT 1 FROM finance.dim_date LIMIT 1);

SELECT COUNT(*) AS dim_date_rows FROM finance.dim_date;
