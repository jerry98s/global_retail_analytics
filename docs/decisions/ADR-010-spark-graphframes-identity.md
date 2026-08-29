# ADR-010: Spark GraphFrames for identity resolution

**Status:** Accepted
**Date:** 2026-08-29
**Author:** Data platform team
**Supersedes:** the identity-resolution portion of ADR-003's dbt implementation
note and partially revisits ADR-006 (Spark deferral)

---

## Context

Identity resolution (ADR-003) was implemented as dbt SQL on Redshift:
`int_identity_edges` → `int_identity_components` → `int_identity_resolution`.
The connected-components step was a **bounded N-hop SQL closure**
(`identity_component_hops`, default 6, hard cap 12) — an approximation of
Union-Find that materializes the full reachability set per hop.

Limits of the SQL approach:

- **Hop bound is a correctness ceiling.** Chains deeper than N hops silently
  fail to merge (two components where there should be one). Raising N costs a
  quadratic-ish join per extra hop.
- **Cost shape.** Each hop joins the accumulated pairs against all edges; on
  Redshift the pairs relation grows combinatorially for large components
  (shared devices, loyalty programs with many linked accounts).
- **Not a graph engine.** We hand-rolled representative selection, singleton
  handling, and symmetrization in SQL — readable, but re-implementing what
  graph libraries already provide.

ADR-003 already named the escape hatch ("migrate to a Python Union-Find job");
ADR-006 deferred Spark until a Spark-shaped workload appeared. Identity
resolution at scale **is** that workload: connected components is a graph
algorithm, not a relational one.

## Decision

**Identity edge construction and connected components move to a Spark
GraphFrames batch job. Everything downstream stays in dbt.**

- New job: `spark/identity_resolution/identity_resolution_job.py`
  (PySpark + GraphFrames `connectedComponents`), running on the existing EMR
  cluster (Spark 3.4 on emr-6.15.0 — no new infra, per ADR-006's "cheap to
  add later" rationale).
- Inputs: bronze `clickstream_events` (Iceberg, Hadoop catalog) + POS loyalty
  IDs (batch Parquet prefix).
- Outputs (Iceberg, silver warehouse):
  - `silver.identity_resolution` — one row per identifier with
    `customer_key`, `confidence_score`, `resolution_method`,
    `is_public_device`, component rep audit columns. **Full overwrite each
    run** (recompute is cheap at identifier grain; matches the old
    `delete+insert` semantics).
  - `silver.identity_edges` — audit copy of the graph edges.
- dbt handoff: `int_identity_resolution` becomes a **thin view** over
  `source('silver', 'identity_resolution')` that only adds the surrogate
  `identity_key`. `int_identity_edges` and `int_identity_components` are
  retired. All consumers (sessions, RFM, consent, dim_customer, fact_sales,
  identity_graph mart) are unchanged; WAP (ADR-009) is untouched because the
  published Gold marts remain dbt-built.
- Orchestration: `marketing_hourly_customer_360_pipeline` submits the Spark
  EMR step and waits for COMPLETED before the WAP clone. One-off submission
  (no-MWAA dev runs): `deploy_platform.ps1 -Action spark`.
- Rules live once: `spark/identity_resolution/graph_logic.py` holds the edge
  types, public-device threshold, rep priority, confidence/method mapping,
  and the `customer_key` formula (byte-identical to dbt's
  `generate_customer_key` — md5, first 8 hex chars, base 16, mod 1e8, +1 —
  so keys are stable across the cutover). The Spark job mirrors it in
  DataFrame ops; the dbt seed fixture is *generated* from it
  (`generate_fixture.py`), and CI fails on drift (`--check`).
- `int_identity_public_devices` stays in dbt as a staging-derived audit
  model; Spark remains authoritative for edge exclusion.

## What GraphFrames gives us over the SQL closure

- **True connected components** — no hop bound, no `identity_component_hops`
  var, no silent merge failures on deep chains.
- **Linear-ish scaling** in edges instead of per-hop pair explosions.
- A path to richer graph analytics (PageRank for identifier trust, label
  propagation for probabilistic matching) without another engine.

## Consequences

- **Spark is now a second engine in the platform** (the ADR-006 trade-off we
  accepted). Scope is deliberately narrow: one batch job, no Spark Streaming,
  no Spark SQL marts. Flink still owns all streaming; dbt still owns all Gold.
- **EMR bootstrap**: Spark steps resolve
  `iceberg-spark-runtime-3.4_2.12:1.4.3` (matches Flink's Iceberg 1.4.3) and
  `graphframes:graphframes:0.8.3-spark3.4-s_2.12` via `--packages` at submit
  time (same Maven reachability the Flink bootstrap already requires).
- **dbt compile no longer covers edge/component logic** — that correctness
  signal moved to `tests/unit/test_spark_identity_resolution.py` (fixture
  scenarios) plus the CI fixture-drift check. The DuckDB identity chain in CI
  now verifies the *handoff* (seed → view → mart), not the graph algorithm.
- **Redshift Spectrum** registers `silver.identity_resolution` /
  `silver.identity_edges` as external Parquet tables
  (`transformation/redshift/spectrum/silver_external_tables.sql`); unpartitioned,
  so hourly overwrites need no partition maintenance.
- **Local DuckDB sim** reads the generated seed fixture in fixture mode; on
  `local-testing-version` the same job runs under local PySpark against
  `.local/iceberg`.
- **Revisit triggers from ADR-006** for *other* Spark uses (Iceberg
  maintenance, ML features) are unchanged — this ADR does not open the door
  to Spark-everything.

## Alternatives considered

- **Keep dbt, raise hops**: preserves the single-engine story but keeps the
  correctness ceiling and the quadratic cost. Rejected.
- **Python Union-Find job** (ADR-003's original escape hatch): correct and
  simple, but single-machine; needs a rewrite the day identifiers outgrow
  driver memory. GraphFrames is the same algorithm on the cluster we already
  pay for. Rejected as a stopgap.
- **Full C360 in Spark**: rejected — sessions/RFM/dim_customer are Kimball
  SQL, dbt's sweet spot (ADR-006 rationale still holds for them), and moving
  them would forfeit dbt tests + WAP for marketing Gold.
