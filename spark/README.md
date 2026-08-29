# Spark jobs

Spark runs **one** workload on the existing EMR cluster (ADR-010): identity
resolution with GraphFrames. Flink still owns all streaming; dbt still owns
all Gold marts. Do not add Spark Streaming or Spark SQL marts without
revisiting ADR-006.

## identity_resolution/

`identity_resolution_job.py` — PySpark + GraphFrames batch job:

- Reads bronze `clickstream_events` (Iceberg, Hadoop catalog) and POS loyalty
  IDs (batch Parquet prefix).
- Builds identity edges (`session_link`, `loyalty_value_match`), excludes
  public devices, runs GraphFrames `connectedComponents` (no hop bound).
- Writes `silver.identity_resolution` (consumed by dbt as a source) and
  `silver.identity_edges` (audit) to the silver Iceberg warehouse. Full
  overwrite per run — recompute is cheap at identifier grain.

`graph_logic.py` — engine-independent source of truth for the rules (edge
types, public-device threshold, representative priority, confidence/method
mapping, deterministic `customer_key` formula matching dbt's
`generate_customer_key`). Covered by `tests/unit/test_spark_identity_resolution.py`.

`generate_fixture.py` — regenerates
`transformation/dbt_project/seeds/silver/identity_resolution.csv` from the
bronze fixture CSVs so the DuckDB CI chain can run without Spark. CI runs
`--check` to fail on drift.

## Cloud (EMR 6.15, Spark 3.4)

The `marketing_hourly_customer_360_pipeline` DAG submits the step hourly and
waits for COMPLETED before the WAP clone. One-off (no-MWAA dev runs):

```powershell
.\scripts\cloud\deploy_platform.ps1 -Env dev -Action spark
```

Packages resolved at submit time: `iceberg-spark-runtime-3.4_2.12:1.4.3`
(matches Flink's Iceberg 1.4.3) + `graphframes:graphframes:0.8.3-spark3.4-s_2.12`.

## Local (laptop)

On `local-testing-version`, the same job runs under local PySpark against
`.local/iceberg`:

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,graphframes:graphframes:0.8.3-spark3.4-s_2.12 \
  spark/identity_resolution/identity_resolution_job.py --local \
  --bronze-warehouse .local/iceberg \
  --silver-warehouse .local/iceberg \
  --pos-parquet-path .local/pos_parquet/
```

Requires `pyspark==3.4.*` and a JDK 8/11/17 on PATH. Fixture mode
(`-DbtSource seeds`) needs no Spark at all — it reads the generated seed.
