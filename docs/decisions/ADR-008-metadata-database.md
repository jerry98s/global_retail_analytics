# ADR-008: Operational metadata database (separate from analytics Gold)

**Status:** Accepted  
**Date:** 2026-07-18  
**Author:** Data platform team  

---

## Context

Pipeline run history, table freshness, and data-quality outcomes are operational
concerns. Mixing them into Kimball facts/dimensions pollutes the analytics model
and couples observability to mart refreshes. We also need reusable daily rollups
that are not consumer-specific serving views.

Glue already catalogs Iceberg/Spectrum tables. We need an observability and
governance database, not a second metastore.

## Decision

1. Keep platform vocabulary **Bronze → Silver → Gold → Summary → Serving**.
2. Add analytics schema **`summary`** inside the existing environment database
   (`dev` / `prod` locally mirrored in DuckDB).
3. Add a separate Redshift database **`metadata`** on the **same** Serverless
   namespace/workgroup. Local equivalent:
   `transformation/dbt_project/local_metadata.duckdb`.
4. Create `metadata` via versioned SQL under `transformation/redshift/metadata/`,
   not by changing Terraform `redshift_database_name` / namespace `db_name`.
5. Write metadata with a shared Python collector
   (`scripts/common/metadata_observer.py`) using a second DB connection.
   **Do not** have dbt write cross-database metadata models.
6. Metadata writes are **fail-open**: collector errors are logged; they must not
   flip a successful business pipeline to failed.

## Rationale

| Option | Why rejected / accepted |
|--------|-------------------------|
| Tables in `finance` / `marketing` | Confuses Kimball grain and ownership |
| dbt models into `metadata` DB | Current profiles target one database; MWAA/local paths would need fragile cross-DB refs |
| Replace Glue | Glue remains the Iceberg/Spectrum catalog |
| Separate workgroup | Extra cost and ops; shared RPUs are acceptable for low-volume ops writes |

Shared RPUs mean metadata queries compete with analytics on the same workgroup.
Volume is small (append-only run/DQ/freshness rows); accept the trade-off and
keep queries lightweight.

## Consequences

- Bootstrap produces **two** labelled scripts: create database (connected to
  `dev`/`prod`), then schema DDL (connected to `metadata`).
- Airflow exposes `redshift_metadata_database` as a variable/output only.
- Catalog seeds live in `metadata/catalog/*.yml` and are upserted by the
  collector.
- Freshness and DQ rows are append-only, keyed by `execution_id`.
- Summary models read **one** Gold fact each (no fact-to-fact joins).

## Alternatives considered

- OpenLineage / DataHub as the sole store — deferred; too heavy for local + MWAA
  parity in this phase.
- Copying Flink/CloudWatch metrics into `metadata` — out of scope; those systems
  remain the source for streaming telemetry.

## Rollout (branch-safe)

1. Land shared contracts + local wiring on `local-testing-version`
   (DDL, YAML catalogs, summary dbt models, `metadata_observer`, local stack).
2. Verify with local E2E: non-empty `summary.*`, completed `meta.pipeline_run`,
   freshness + dbt/GE DQ rows in `local_metadata.duckdb`.
3. Cherry-pick cloud-safe pieces to `main`: metadata DDL, catalogs, summary
   models, observer, Airflow/Redshift wiring, docs/ADR — **exclude** local-only
   compose/bind-mount/venv shortcuts.
4. On each env: `bootstrap_redshift.ps1 -MetadataOnly`, set Airflow Variable
   `redshift_metadata_database`, redeploy MWAA assets.
