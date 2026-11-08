# Spark jobs

Spark runs **one** workload on the existing EMR cluster (ADR-010): identity
resolution with GraphFrames. Flink still owns all streaming; dbt still owns
all Gold marts. Do not add Spark Streaming or Spark SQL marts without
revisiting ADR-006.

## identity_resolution/

`identity_resolution_job.py` — PySpark + GraphFrames batch job:

- Reads bronze `clickstream_events` (Iceberg, Hadoop catalog) and POS loyalty
  IDs (batch Parquet prefix).
- Builds one edge type, `verified_loyalty_link`, from v1.3 logins verified to
  a loyalty card (ADR-003), and runs GraphFrames `connectedComponents` (no hop
  bound). Devices are never nodes.
- Writes `silver.identity_resolution` (consumed by dbt as a source),
  `silver.identity_edges`, `silver.loyalty_link_claims` (every claim, verified
  or not) and `silver.device_session_associations` (signed-in intervals per
  device and session instance) to the silver Iceberg warehouse. Full
  overwrite per run — recompute is cheap at identifier grain.
- Publishes `consumer_current/<name>/` exports (`consumer_exports.py`):
  the four identity outputs, the canonical event/quarantine/duplicate
  sets, and `silver.inventory_hourly` read from its committed Iceberg snapshot.
  Each run writes `run=<utc ts>/` and then atomically replaces `manifest`;
  Spectrum's `LOCATION` and the local DuckDB loader both read only the files
  it lists, so a reader sees one complete run. The current and previous runs
  are kept; older ones are deleted. Consumers never glob Iceberg `data/`
  directories, where superseded snapshot files may remain.

`graph_logic.py` — engine-independent source of truth for the rules (identifier
normalization, the
merge edge, device-session associations, representative priority,
confidence/method mapping, deterministic `customer_key` formula matching dbt's
`generate_customer_key`). Covered by `tests/unit/test_spark_identity_resolution.py`;
CI also executes the real DataFrame/GraphFrames path through
`tests/integration/verify_spark_identity.py`.

`generate_fixture.py` — regenerates
`transformation/dbt_project/seeds/silver/identity_resolution.csv` and
`device_session_associations.csv` from the bronze fixture CSVs so the DuckDB CI chain can run without Spark. CI runs
`--check` to fail on drift.

## Cloud

`marketing_hourly_customer_360_pipeline` submits the job on EMR before the
WAP clone. Packages: Iceberg runtime 1.4.3 and GraphFrames
0.8.3-spark3.4-s_2.12. A one-off run is `deploy_platform.ps1 -Action spark`.

## Local (laptop)

The same job runs under local PySpark against `.local/iceberg`. The Docker
wrapper for that command lives on `local-testing-version`. Fixture mode
(`-DbtSource seeds`) needs no Spark — it reads the generated seed.
