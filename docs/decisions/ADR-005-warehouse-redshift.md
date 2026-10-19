# ADR-005: Data Warehouse — Amazon Redshift Serverless

**Status:** Accepted (supersedes the Snowflake choice in ADR-004)  
**Date:** 2026-06-07  
**Author:** Data platform team  

---

## Context

The Gold/serving layer was originally specified on Snowflake (ADR-004). The
platform is otherwise entirely on AWS (S3, MSK, EMR, Glue, IAM). Operating a
second cloud vendor for the warehouse added account/billing/identity overhead,
a separate storage integration to broker S3 access, and a separate cost-control
surface (resource monitors) — all for a workload whose source data already lives
in S3.

## Decision

Use **Amazon Redshift Serverless** as the single data warehouse for all
environments (dev and prod). Bronze is read **in place** from S3 via **Redshift
Spectrum** (Glue Data Catalog external schema); Silver/Gold marts are built by
dbt (`dbt-redshift`) as native Redshift tables.

Snowflake is removed from the repository entirely.

## Rationale

1. **All-AWS.** One account, one IAM trust model, one bill. Redshift assumes an
   IAM role for S3 + Glue — no cross-cloud storage integration.
2. **Spectrum reads S3 in place.** Bronze parquet is queried where it lands; no
   `COPY`/load step or duplicate storage for the raw layer.
3. **Serverless economics.** Pay per RPU-hour while active; the workgroup
   auto-pauses when idle (the analogue of Snowflake auto-suspend). A monthly
   RPU-hour **usage limit** is the hard cost cap, managed in Terraform.
4. **dbt parity.** `dbt-redshift` supports the same staging → intermediate →
   marts layering, incremental models, and tests. Models use native Redshift SQL.

## Consequences

- **dbt:** single `redshift` adapter; marts use `delete+insert` incremental
  strategy with a `unique_key`. Snowflake-only SQL (`QUALIFY`, `to_varchar`,
  variadic `concat`, `MERGE`-by-default, `:variant` access) was rewritten to
  Redshift equivalents (`row_number()` subqueries, `cast`, `||`,
  `json_extract_path_text`, etc.).
- **Keys:** surrogate keys are computed deterministically in SQL
  (`abs(mod(strtol(substring(md5(...),1,8),16), N)) + 1`) instead of relying on
  `IDENTITY`, so incremental rebuilds stay stable.
- **Constraints:** Redshift declares `PRIMARY KEY`/`FOREIGN KEY`/`UNIQUE` for the
  planner but does not enforce them — uniqueness is enforced by dbt tests.
- **Performance:** marts carry explicit `DISTKEY`/`SORTKEY` (and `DISTSTYLE ALL`
  for small dims) — see `transformation/redshift/`.
- **Cost control:** Snowflake resource monitors → Redshift Serverless usage
  limits (`infra/terraform/modules/redshift`).
- **Infra:** the `snowflake` Terraform module is deleted; the **platform** stack
  (`infra/terraform/`) provisions MSK + EMR + Redshift for dev and prod via per-env tfvars.

## Alternatives Considered

| Option | Rejected Because |
|---|---|
| Snowflake (status quo) | Second cloud vendor; cross-cloud storage integration overhead |
| BigQuery | Not AWS-native; egress + a third identity plane |
| Redshift provisioned (RA3) | Always-on cost; Serverless auto-pause fits bursty dev/batch better |
