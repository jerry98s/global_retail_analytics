# ADR-006: Flink for streaming, defer Spark until Iceberg maintenance is needed

**Status:** Accepted — partially revisited by ADR-010 (2026-08-29): Spark +
GraphFrames now runs **one batch job** (identity resolution) on the existing
EMR cluster. Flink still owns all streaming; dbt still owns all Gold marts.
The deferral rationale below still applies to every workload except the
identity graph.
**Date:** 2026-07-05
**Author:** Data platform team

---

## Context

The platform runs three streaming paths today (clickstream bronze, inventory bronze,
inventory hourly silver) on PyFlink 1.17.1 over EMR 6.15. Gold marts are built by dbt
on Redshift. Ad-hoc lake queries go through Redshift Spectrum over S3 Iceberg.

The question: should we also introduce Spark on the same EMR cluster?

This is an engine-choice question, separate from ADR-002 (which decides batch vs.
stream **per pipeline**). ADR-002 says *whether* a path streams; this ADR says *which
engine* the streaming paths use, and whether a second engine is worth running.

## Options considered

| Workload | Flink | Spark | dbt/Redshift | Spectrum |
|---|---|---|---|---|
| Bronze ingest (Kafka → Iceberg) | Sub-second, exactly-once, native | Micro-batch ≥ seconds, weaker DLQ | n/a | n/a |
| Silver hourly aggregation | Native TUMBLE windows, watermarks | Doable, more boilerplate | n/a | n/a |
| DLQ side-output | Native via StatementSet | Ugly — needs foreach sink or join | n/a | n/a |
| Gold marts (Kimball) | Possible but reinvents dbt | Possible but reinvents dbt | **Right tool** | n/a |
| Ad-hoc lake queries | n/a | Spark SQL/Databricks nicer | n/a | **Already serves this** |
| Iceberg maintenance (compact, expire, rewrite manifests) | Partial via Iceberg `Maintenance` API | **Industry-standard procedures** | n/a | n/a |
| Historical backfills (replay N months of Kafka) | Flink batch mode or `earliest-offset` reset | Micro-batch with date predicates | n/a | n/a |
| ML feature engineering | Possible | Right tool, when needed | n/a | n/a |

## Decision

**Flink is the streaming engine. Spark is deferred.**

- Bronze + silver streaming paths: **Flink only.** No Spark Streaming.
- Gold marts: **dbt on Redshift** — no Spark SQL alternative.
- Ad-hoc lake queries: **Redshift Spectrum** — no Spark SQL notebooks.
- Iceberg maintenance: defer until the lake exhibits small-file proliferation or
  snapshot-metadata bloat (rough threshold: >10k data files per partition, or
  Iceberg snapshot count >500). At that point, add a Spark EMR step on the
  existing cluster (no new infra) for `rewrite_data_files` / `expire_snapshots`.
- Historical backfills: use Flink batch mode (same job code, `latest-offset` →
  `earliest-offset` via consumer-group reset). Add Spark only if backfill windows
  exceed ~30 days and Flink batch is measurably slower.

## Rationale

1. **No current workload is Spark-shaped.** Every transformation we run today is
   either a Kafka-to-Iceberg stream (Flink's sweet spot) or a Kimball SQL
   transformation (dbt's sweet spot). Adding Spark would create a second engine
   that doesn't displace either.

2. **Spark on EMR is cheap to add later.** The EMR cluster already exists for
   Flink. Running a Spark step is an additional `yarn` application, not a new
   cluster. Deferring costs us nothing in infra agility.

3. **Two-streaming-engine teams are expensive.** Flink and Spark have different
   mental models (true streaming vs. micro-batch), different connector
   versioning, different watermark semantics, different exactly-once guarantees.
   Running both doubles the cognitive surface for on-call and code review.

4. **The dashboard's operational silver latency (<60 min) is well within Flink's
   comfort zone.** Spark Structured Streaming's sub-second latency claims don't
   help us — our fastest dashboard refresh is hourly.

5. **dbt + Spectrum already covers the ad-hoc case.** A data scientist who wants
   to query Iceberg can use Spectrum from Redshift Query Editor v2 — no notebook
   infra needed. If a notebook workflow emerges, revisit this ADR.

## Consequences

- **Three Flink jobs** to maintain (clickstream bronze, inventory bronze,
  inventory hourly silver). No Spark jobs to maintain.
- **Same EMR cluster** runs all Flink jobs; no Spark step today.
- **`infra/emr-bootstrap/install_flink_connectors.sh`** pins Flink + Iceberg +
  Kafka connector versions. When Spark is added, the bootstrap script will need
  a parallel Spark section pinning `iceberg-spark-runtime` and Spark version.
- **Iceberg maintenance is currently manual** via the Iceberg `Maintenance` API
  callable from Flink jobs. If small-file problems emerge before Spark is added,
  this ADR is revisited.
- **Backfills** are done by resetting the Flink consumer group to
  `earliest-offset`. Documented in `docs/runbooks/`.

## Revisit triggers

Introduce Spark when **any** of these become true:

- Iceberg bronze or silver table exceeds **10,000 data files per partition**
  (small-file problem hurts Spectrum scan performance).
- Iceberg snapshot count for any table exceeds **500** (metadata bloat).
- A backfill window of **>30 days** is needed and Flink batch is measurably
  slower than a Spark equivalent.
- An ML feature engineering pipeline is approved (Spark is the standard tool).
- A notebook-based data science workflow is requested and Spectrum SQL is
  insufficient.

Until then, **Flink is enough.**
