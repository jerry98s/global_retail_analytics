"""Entity ID ranges for the producer simulators and POS batch generator.

Single source of truth for STORE / PRODUCT / LOYALTY ID ranges so the
inventory producer, POS producer, POS Parquet batch generator, and any
future sim all draw from the same population.

Importing this module from a producer sim:
    from ingestion.kafka.sim_entities import STORES, PRODUCTS, LOYALTY_IDS

Importing the platform enum (which is schema-bound, not simulation-bound):
    from streaming.flink_jobs.event_types import PLATFORMS
"""

from __future__ import annotations

# 20 stores — matches `transformation/redshift/seeds/dim_store.sql` seed rows.
# Update both files together if the store count changes.
STORES: list[str] = [f"STORE-{i:03d}" for i in range(1, 21)]

# 500 products — matches `transformation/dbt_project/seeds/` product seed
# (when present). Update together.
PRODUCTS: list[str] = [f"PROD-{i:04d}" for i in range(1, 501)]

# 10,000 loyalty IDs — synthetic customer anchor space for the identity graph.
# Real deployments replace this with the loyalty member dimension.
LOYALTY_IDS: list[str] = [f"LOYAL-{i:06d}" for i in range(1, 10_001)]
