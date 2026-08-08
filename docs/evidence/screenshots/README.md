# Screenshot assets

Captured 2026-08-01 from the local end-to-end run (see the parent
[evidence index](../README.md) for the run's numbers):

- `01-dashboard-overview.png` — Streamlit local mode over 90,000 streamed
  clickstream events.
- `02-flink-checkpoints.png` — `clickstream_bronze_job` RUNNING with completed
  checkpoint history.
- `03-iceberg-query.png` — DuckDB queries over Iceberg Bronze/Silver Parquet
  (landed vs unique counts, event-type breakdown, silver aggregates).
- `04-dbt-lineage.png` — dbt docs DAG with `identity_graph` highlighted.
- `05-ci-green.png` — GitHub Actions green on `main` and
  `local-testing-version`.
