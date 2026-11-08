# Fixture seeds (CI / `-DbtSource seeds`)

Curated CSV for identity scenarios (`verify_local_identity.py`):

- `clickstream_events.csv` — hand-crafted events (envelope 1.3.0 unless noted):
  - `c-aaa`: anonymous view, then a login verified to `L1001`, checkout, logout.
  - `c-bbb` / `c-ccc`: two accounts (`C2002`, `C2003`) each verified to `L2002`.
  - `c-tab`: a shared tablet; `C3001` signs out, then `C3002` signs in.
  - `c-ddd`: `C4001` claims `L1001` without verifying it.
  - `c-eee`: a remembered sign-in that continues after 60 idle minutes.
  - `c-fff`: two accounts inside one session instance (ambiguous).
  - `c-solo`: anonymous only.
  - `c-legacy`: envelope 1.2.0 with `customer_id = L1001`; legacy events carry
    no verification evidence and must not merge.
- `pos_transactions.csv` — POS rows that create the `L1001` / `L2002` loyalty
  nodes.

The pipeline producer's `--scenario acceptance` mode sends comparable journeys
through Kafka; these CSVs keep the same rules testable without Docker.

`inventory_events` (bronze) and `silver.inventory_hourly` were removed: the
Flink bronze/silver jobs generate those, and no CI test reads them.

Default local platform demos use **Iceberg Parquet** from Flink +
`generate_pos_parquet --output-dir` instead — see
`scripts/local/run_local_stack.ps1 -DbtSource iceberg`. Reference dims
(`finance/dim_date.csv`, `finance/dim_store.csv`) stay seeded in both modes
because the pipeline cannot generate calendar/store-master data.

Do not treat the fixture IDs (`L1001`, `c-aaa`, `ST-001`) as matching
`ingestion/kafka/sim_entities.py` (`LOYAL-*`, `CUST-*`, `STORE-*`).
