-- Store dimension (Type 1). Small reference table -> DISTSTYLE ALL.

CREATE TABLE IF NOT EXISTS finance.dim_store (
  store_key      BIGINT IDENTITY(1, 1),
  store_id       VARCHAR(64)   NOT NULL,
  store_name     VARCHAR(200),
  store_type     VARCHAR(100),
  city           VARCHAR(100),
  country_code   CHAR(2),
  region         VARCHAR(100),
  is_active      BOOLEAN,
  PRIMARY KEY (store_key),
  UNIQUE (store_id)
)
DISTSTYLE ALL
SORTKEY (store_id);

COMMENT ON TABLE finance.dim_store IS 'Store dimension (Type 1) for retail locations.';
COMMENT ON COLUMN finance.dim_store.store_id IS 'Natural store identifier from source systems.';
