# Fixture seeds (CI / `-DbtSource seeds`)

Curated CSV for identity-graph scenarios (`verify_local_identity.py`):

- `clickstream_events.csv` — hand-crafted sessions encoding loyalty match,
  session link, multi-hop closure, public device (10 distinct customers), and
  singleton client scenarios.
- `pos_transactions.csv` — POS rows that produce `loyalty_value_match` edges.

The pipeline producers generate **random** identifiers and co-occurrences, so
they cannot deterministically reproduce these scenarios — the CSVs stay as CI
fixtures.

`inventory_events` (bronze) and `silver.inventory_hourly` were removed: the
Flink bronze/silver jobs generate those, and no CI test reads them.

Default local platform demos use **Iceberg Parquet** from Flink +
`generate_pos_parquet --output-dir` instead — see
`scripts/local/run_local_stack.ps1 -DbtSource iceberg`. Reference dims
(`finance/dim_date.csv`, `finance/dim_store.csv`) stay seeded in both modes
because the pipeline cannot generate calendar/store-master data.

Do not treat the fixture IDs (`L1001`, `c-aaa`, `ST-001`) as matching
`ingestion/kafka/sim_entities.py` (`LOYAL-*`, `STORE-*`).
