-- Date dimension (Type 0). Small + joined everywhere -> DISTSTYLE ALL.
-- Constraints are declared for the planner; Redshift does not enforce them.

CREATE TABLE IF NOT EXISTS finance.dim_date (
  date_key           INTEGER     NOT NULL,
  full_date          DATE        NOT NULL,
  day_of_week        SMALLINT,
  month_number       SMALLINT,
  quarter            SMALLINT,
  fiscal_year        SMALLINT,
  is_weekend         BOOLEAN,
  is_public_holiday  BOOLEAN,
  PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);

COMMENT ON TABLE finance.dim_date IS 'Date dimension (Type 0) used across all marts.';
COMMENT ON COLUMN finance.dim_date.date_key IS 'Surrogate YYYYMMDD integer key.';
COMMENT ON COLUMN finance.dim_date.full_date IS 'Calendar date.';
