# ADR-017: Read Hourly Inventory from Its Iceberg Snapshot

**Status:** Accepted
**Date:** 2026-11-25
**Author:** Data platform team
**Related:** [ADR-012](ADR-012-inventory-lambda.md)

---

## Context

`serving.inventory_current_position` is a view over published Gold plus
Flink's `silver.inventory_hourly`. Spark copied that Iceberg table to a
Parquet export on every hourly run because Spectrum cannot scan an Iceberg
`data/` directory safely, and the Flink catalog was Hadoop, which Glue and
Redshift do not see. The copy made current stock as old as the Spark job.

## Decision

1. **Read the committed snapshot.** Local DuckDB loads
   `silver.inventory_hourly` from the current snapshot's manifest list.
   A file left in `data/` that the snapshot does not list is not a row.
2. **Cloud commits that table through Glue.** The hourly Flink job uses
   `GlueCatalog` when `ICEBERG_SILVER_CATALOG_IMPL` is set. Bronze and the
   other Silver tables stay on the Hadoop catalog and their Parquet exports.
   The Glue database name is `ICEBERG_SILVER_NAMESPACE`, the same database
   Redshift's `silver` external schema already uses. Local leaves the
   variable empty and keeps namespace `silver`.
3. **Redshift does not get a Parquet external table for this one table.**
   The Glue Iceberg table is the relation. The old manifest `LOCATION` is
   removed from the Spectrum DDL. Drop that external table once, before
   Flink's first Glue commit, so the name is free.
4. **The hourly Spark job no longer exports it.** Customer 360 Spark still
   runs for clickstream and identity. Gold inventory is still the daily
   canonical Spark build.

## Consequences

- Current stock is as fresh as Flink's last checkpoint, not the next Spark
  hour. dbt does not need an hourly run for the view to see a new commit.
- The EMR instance role must be allowed to create and update the Glue table.
  That role is supplied to Terraform; this repo does not own it.
- `export_run_id` is gone from `silver.inventory_hourly`. The one-export
  test goes with it.
- The Redshift query that proves a commit is visible was not run from this
  workspace. The local snapshot read is the check that stands in for it.

## Alternatives considered

- **Keep the export.** Valid, and it was the path until this decision. It
  adds a copy and ties freshness to Spark. Rejected.
- **Register the Hadoop metadata file in Glue after each commit.** The file
  path changes every checkpoint, so the registration goes stale. Rejected.
