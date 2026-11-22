-- One-row inventory cutoff published in the same transaction as
-- finance.fact_inventory_snapshot (ADR-012). Not a Kimball mart.
-- is_estimated on the fact stays and is always false; this row is the cutoff.

CREATE TABLE IF NOT EXISTS control.inventory_publication (
  inventory_run_id       VARCHAR(64)  NOT NULL,
  bronze_snapshot_id     BIGINT,
  closed_hour_exclusive  INTEGER,
  coverage_status        VARCHAR(16)  NOT NULL,
  incomplete_partitions  INTEGER      NOT NULL,
  published_at           TIMESTAMP    NOT NULL
);

COMMENT ON TABLE control.inventory_publication IS
  'Published inventory cutoff. Hours before closed_hour_exclusive are canonical in finance.fact_inventory_snapshot.';
