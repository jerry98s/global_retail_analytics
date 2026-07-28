# ADR-001: Table Format Selection

**Status:** Accepted  
**Date:** 2024-01-15  
**Author:** Data platform team  

---

## Context

Bronze and Silver layers require ACID transactions on S3 object storage.
Three mature table formats are evaluated: Apache Iceberg, Delta Lake, Apache Hudi.

## Decision

**Apache Iceberg** for all Bronze and Silver layers.  
**Exception:** Silver inventory (high-frequency upserts) evaluated for Hudi MoR.

## Rationale

### Why Iceberg

1. **Multi-engine reads required.** Silver tables are consumed by Flink 
   (streaming), dbt/Spark (batch transforms), and Amazon Redshift Spectrum
   (finance queries over S3). Iceberg is the only format with first-class support
   across all three without bespoke connectors or format conversion.

2. **Schema and partition evolution.** Adding `store_region` column or changing
   partition strategy from daily → hourly requires no data rewrite. Delta Lake
   supports column adds but not partition evolution without full rewrite.

3. **Open specification.** No vendor lock-in. If the team migrates from EMR to
   Databricks or adds Trino, Iceberg tables remain readable without migration.

### Why Not Delta Lake

- Engine lock-in: best performance requires Databricks/Spark
- Reading Delta from Redshift Spectrum requires manifest files and extra tooling
- Partition evolution requires full table rewrite

### Why Not Hudi (for most tables)

- Weakest multi-engine support of the three
- `.hoodie/` metadata and compaction schedules add operational complexity
- MoR advantage only matters for sub-minute upsert latency

### Hudi Exception: Silver Inventory

Inventory Silver requires high-frequency upserts (stock changes every few
seconds per store+product). If sub-60s Silver freshness is required in future,
migrate this one table to Hudi MoR. Current 5-minute micro-batch is sufficient
and Iceberg handles it adequately.

## Consequences

- All pipeline engineers must use Iceberg-compatible Flink/Spark connectors
- Schedule daily `rewrite_data_files` compaction job on all streaming tables
- Monitor small file accumulation: alert if avg file size < 64MB

## Alternatives Considered

| Format | Rejected Because |
|---|---|
| Delta Lake | Engine lock-in, Redshift Spectrum connector friction |
| Hudi | Operational complexity, weaker multi-engine support |
| Raw Parquet | No ACID, no schema evolution, no time travel |
