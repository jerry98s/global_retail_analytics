# Data Warehouse Checklist Audit — status index

**Date opened:** 2026-07-04
**Date closed:** 2026-07-05
**Scope:** All Gold marts + dimensions + Flink jobs + Airflow DAGs + batch producer
**Checklists applied:**
1. **Data Model Review Checklist (10 must-check items)** — per new table
2. **Idempotent Design Checklist (6 items)** — per ETL task

Source: industry best practice for data warehouse development. See references at end.

---

## TL;DR

All 21 identified gaps (6 P1, 9 P2, 6 P3) have been closed across 8 PRs.
The detailed per-model scoring matrices and gap-to-fix tables from the
original audit have been superseded by the implementation itself — the
current source of truth is the code (dbt tests, GE suites, Flink jobs,
Airflow DAGs, runbooks). This page is now a status index that points at
where each closed gap actually lives.

## Tier status (final)

| Tier | Total | Closed |
|------|-------|--------|
| P1   | 6     | 6      |
| P2   | 9     | 9      |
| P3   | 6     | 6      |

## Progress log

| PR | Commit (`local` / `main`) | Gaps closed | Notes |
|----|---------------------------|-------------|-------|
| PR1 | `1977248` / `8b4c97a` | P1.1, P1.2, P2.6, P2.7, P2.9 | session/C360 cluster: crash-resilient incremental lookback, `customer_360_view` grain, accepted_values, column docs, NULL `customer_key` |
| PR2 | `f... / e...` | P1.3 | dim_date/dim_store dbt + GE tests (source-level disable caused a follow-up regression fixed in PR5) |
| PR3 | `080a0c0` / `e8488ab` | P1.4, P1.6, P2.4 | Airflow governance: Flink idempotency guard, bronze/silver dependency doc, deleted redundant `nightly_dbt_run` |
| PR4 | `f9e2774` / `2497d9f` | P1.5 | DLQ `WHERE NOT (valid_predicate)` — null-event_id rows now routed to DLQ, not dropped |
| PR5 | `d11e0e6` / `d7a7b66` | P2.1, P2.5 | dbt idempotency test (`tests/integration`); Gold row-count reconciliation Airflow task + plugin; also fixed the PR2 source-disable regression in `_sources.yml` |
| PR6 | `ea86e01` / `382a6a4` | P2.2, P2.3 | `docs/runbooks/backfill-verification.md` (per-model 7-day backfill procedure) + `docs/runbooks/upstream-incident-response.md` (upstream missing + volume spike, per-model) |
| PR7 | `7b5770d` / `2bbda83` | P2.8 | GE expectation suites for `fact_inventory_snapshot`, `dim_customer`, `customer_360_view`, `inventory_bronze`; wired into `gold_layer_daily` checkpoint |
| PR8 | `383ca1a` / `5e40b4f` | P3.1, P3.2, P3.5, P3.6 (P3.3 closed in PR1; P3.4 closed in PR3) | DDL NOT NULL on `fact_inventory_snapshot` measures; watermark asymmetry documented in both inventory Flink jobs; `doc_md` on `catalog_bihourly_product_scd2_refresh` + `quality_hourly_ge_checkpoint`; date-derived `random` seeding + `uuid5` transaction_ids + `--seed` override in `generate_pos_parquet.py`; determinism unit tests |
| PR8b | `ddbec94` / `a9905c8` | DL-A through DL-M (Part 6) | Iceberg `PARTITIONED BY` on all 3 bronze/silver tables + POS Spectrum; `iceberg_maintenance` Airflow DAG + Flink batch job; `redshift_analyze` task in `warehouse_daily_batch_pipeline`; `on_schema_change` unit test; `docs/runbooks/iceberg-maintenance.md` runbook |
| PR9 | (this commit) | DAG-1 through DAG-7 (Part 7) | All 6 DAGs renamed to `{domain}_{frequency}_{description}` template; `email_on_failure=True` added to `catalog_bihourly_product_scd2_refresh` + `quality_hourly_ge_checkpoint`; `warehouse_daily_batch_pipeline` dbt tasks refactored via `_dbt_step` factory; `tests/unit/test_dag_contract.py` static lint (61 tests); `docs/runbooks/dag-review-checklist.md` runbook |
| PR10 | (this commit) | K-PROD through K-RUNBOOK (Part 8) | Producer reliability + performance defaults in `build_producer_config()` (acks=all, idempotence, retries, batch.size, linger.ms, lz4); explicit `enable.auto.commit=false` + `isolation.level=read_committed` + `auto.offset.reset=earliest` on all 3 Flink Kafka sources; orphaned `aws_msk_configuration` removed; per-(consumer_group, topic) CloudWatch alarm on `PCTConsumerLag` + shared alert SNS topic; `tests/unit/test_msk_config.py` (12 tests) + `tests/unit/test_flink_kafka_source.py` (12 tests); `docs/runbooks/kafka-operations.md` runbook |
| PR11 | (this commit) | F-STATE through F-RUNBOOK (Part 9) | `streaming/config/state.yaml` (rocksdb backend, incremental checkpoints, 7d TTL, 1min source idle timeout) + `apply_state_config()` helper in `_config.py`; each streaming Flink job's `run()` now calls `apply_state_config`; `min_pause_between_checkpoints_ms` bumped from 5s to 30s; `properties.partition.discovery.interval.ms=300000` added to all 3 Kafka source DDLs; per-topic parallelism config (`inventory_parallelism`, `clickstream_parallelism`) in `flink_conf.yaml`; `tests/unit/test_flink_state_config.py` (25 tests); `docs/runbooks/flink-operations.md` runbook |
| PR12 | (this commit) | Repo consolidation (Part 10) | `docker-compose*.yml` moved to `infra/docker/compose/`; `run_local_stack.ps1` moved from repo root into `scripts/local/`; `scripts/` split into `scripts/cloud/` (5 PowerShell) + `scripts/local/` (Python helpers + `run_local_stack.ps1`); `infra/docker/flink/versions.env` as single source of truth for connector JAR versions; `tests/unit/test_flink_connector_versions.py` (8 tests) asserts no drift between Dockerfile, `install_flink_connectors.sh`, and `versions.env`; `infra/docker/README.md` + rewritten `scripts/README.md`; all references in README/AGENTS/ARCHITECTURE/docs/cursor rules/CI/orchestration/transformation/infra-terraform/notebooks/dashboard updated |
| PR13 | (this commit) | Notebooks + scripts reclassification (Part 11) | Established the principle "`scripts/` is do-work only (apply, deploy, sync, submit, generate, bootstrap, start/stop); anything that verifies/peeks/analyses belongs in `notebooks/` (or `tests/integration/` for CI test gates)". Moved `scripts/local/query_local_iceberg.py` → `notebooks/query_local_iceberg.py` (added `--code <python>` ad-hoc passthrough flag); `scripts/local/verify_local_identity.py` → `tests/integration/verify_local_identity.py` (CI test gate, now sits next to `test_dbt_idempotency.py` + `conftest.py`); `scripts/cloud/verify_platform.ps1` → `notebooks/cloud_verification/verify_platform.ps1` (operator smoke check); converted `notebooks/01_architecture_walkthrough.ipynb` → `docs/architecture-walkthrough.md` (documentation, not analysis); refactored `notebooks/02_data_model_exploration.ipynb` to shell out to `notebooks/query_local_iceberg.py --code` instead of duplicating the `run_in_flink_container` helper; new `notebooks/README.md`; rewrote `scripts/README.md` to drop verify rows + add see-also pointers; updated all references in README/AGENTS/docs/case-studies/docs/runbooks/CI/deploy_platform.ps1. Final tally: `scripts/` is 4 cloud PowerShell + 1 local PowerShell = "automation only"; `notebooks/` is 3 analysis notebooks + 1 peek CLI + 1 cloud smoke check. |
| PR14 | (this commit) | Notebooks consistency (Part 12) | Refined PR13's principle so that `notebooks/` is **`.ipynb`-only** — no `.py` or `.ps1` files mixed in. Converted `notebooks/query_local_iceberg.py` → `notebooks/05_local_iceberg_queries.ipynb` (22 cells: setup + overview + 4 clickstream + 4 inventory + notes, using the inline `run_in_flink_container` helper); converted `notebooks/cloud_verification/verify_platform.ps1` → `notebooks/06_cloud_platform_verification.ipynb` (14 cells: helper + read outputs + EMR + S3 + Redshift + dashboard + notes, invoking PowerShell via `subprocess.run(["powershell", ...])`); refactored `notebooks/02_data_model_exploration.ipynb` back to inline `run_in_flink_container` (since `query_local_iceberg.py` is gone); deleted the `.py` and `.ps1` files and the `cloud_verification/` directory; rewrote `docs/runbooks/local-data-queries.md` so each canned query is a paste-able `docker compose -f infra/docker/compose/docker-compose.yml exec -T flink-taskmanager python3 -c "..."` one-liner (10 commands); updated `notebooks/README.md` + `scripts/README.md` + `README.md` + `AGENTS.md` + `scripts/cloud/deploy_platform.ps1` + `.cursor/rules/{project-context,flink}.mdc` to drop the `.py`/`.ps1` references and point at the new notebooks. Final tally: `notebooks/` is `02_data_model_exploration`, `03_rfm_analysis`, `04_cost_model`, `05_local_iceberg_queries`, `06_cloud_platform_verification` + `README.md` — all `.ipynb` (and the directory README). |
| PR15 | (this commit) | Clickstream gap fixes (Part 13) | Wired `CLICKSTREAM_PARALLELISM=24` / `INVENTORY_PARALLELISM=12` into EMR submit (`deploy_platform.ps1` + `streaming_manual_flink_jobs`); checkout business validation + `dlq.clickstream.business_violations` StatementSet branch; Spectrum + staging `event_date`; `clickstream_bronze` GE wired into `gold_layer_daily`; DLQ runbook corrected (Kafka-only); `int_identity_resolution` switched to `delete+insert` (no stale customer_key); seed `schema_version=1.2.0`; RFM GE typo `cant_loose` removed; contract marks `ingest_time`/`geo` optional-not-enforced |
| PR16 | (this commit) | Clickstream dbt gap fixes (Part 14) | DuckDB-portable `json_path_text` + `dateadd_unit` in sessions/consent; RFM omnichannel (POS + converted clickstream sessions); identity hop depth var `identity_component_hops` default 6; warehouse daily owns `marts.finance` only (marketing hourly owns C360); local Flink-vs-seed fidelity documented |
| PR17 | (this commit) | C360 flow polish (Part 15) | Marketing hourly select tightened to `stg_clickstream_events` + `stg_pos_transactions` + intermediate/marketing/summary/serving with `--exclude int_product_catalog dim_product` (no hourly SCD2 collision); dbt-managed `serving.customer_360_serving`; `int_rfm_scoring` tests moved to `intermediate.yml`; identity-resolution RFM doc + GE DAG docstring synced; `verify_metadata_e2e.py` under `tests/integration/` |
| PR18 | (this commit) | Declutter (Part 16) | dbt test DRY: 3 reusable singular macros under `macros/tests/` (`not_null_unique`, `identifier_type_accepted_values`, `non_negative`) replace repeated `[not_null, unique]` / `accepted_values` / `dbt_utils.accepted_range min:0` blocks across `dim_customer.yml`, `dim_product.yml`, `intermediate.yml`, `summary.yml`; unique-format tests (SCD2 macros, `unique_combination_of_columns`, bounded `accepted_range`, per-model `accepted_values`) stay singular. Docs/notebooks declutter: removed `docs/architecture-walkthrough.md` (redundant with root `ARCHITECTURE.md` + `docs/runbooks/local-data-queries.md`) and `notebooks/03_rfm_analysis.ipynb` (synthetic demo superseded by dbt `int_rfm_scoring`); `notebooks/README.md` updated. Seeds declutter: removed `seeds/bronze/inventory_events.csv` + `seeds/silver/inventory_hourly.csv` (Flink-generated, CI-unused); kept `seeds/bronze/clickstream_events.csv` + `pos_transactions.csv` (curated identity-scenario fixtures the random producers cannot deterministically reproduce) and `seeds/finance/dim_date.csv` + `dim_store.csv` (reference dims the pipeline cannot generate); `dbt_project.yml` + `seeds/bronze/README.md` + `_sources.yml` + `README.md` updated |
| PR19 | (this commit) | Notebook consolidation (Part 17) | Removed `notebooks/02_data_model_exploration.ipynb` — its cells (overview, clickstream-by-event-type/platform, inventory hourly totals) were a strict subset of `05_local_iceberg_queries.ipynb` using the same `run_in_flink_container` helper (02 even documented that "Notebook 05 uses the same helper for a wider set of canned queries"). Consolidated into 05. Kept `05_local_iceberg_queries.ipynb` (canonical local-Iceberg canned queries) and `06_cloud_platform_verification.ipynb` (canonical cloud post-deploy smoke check — `scripts/cloud/deploy_platform.ps1` line 236 delegates smoke checks to it, and it adds Redshift row-counts + dashboard HTTP beyond the deploy script's `status` action). `notebooks/README.md` + `scripts/README.md` updated. Final tally: `notebooks/` is `04_cost_model`, `05_local_iceberg_queries`, `06_cloud_platform_verification` + `README.md` |
| PR20 | (this commit) | Single walkthrough notebook (Part 18) | Collapsed to **one** end-to-end notebook + the cost model. New `notebooks/01_data_walkthrough.ipynb` (25 cells) walks every table in pipeline order Bronze (clickstream_events, inventory_events, pos_transactions) → Silver (inventory_hourly) → Gold dims/facts/identity → Summary → Serving (`customer_360_serving`) via the local DuckDB catalog; replaces `05_local_iceberg_queries.ipynb` (Bronze/Silver only) and `02_data_model_exploration.ipynb` (subset). Folded `06_cloud_platform_verification.ipynb` into `scripts/cloud/deploy_platform.ps1 -Action verify` (new `Invoke-PlatformVerify` function: EMR state, S3 bronze prefixes, Redshift row-counts via Data API, dashboard HTTP) so cloud verification is a first-class do-work task, not a notebook; updated the `-Action all` next-steps pointer. Renamed `04_cost_model.ipynb` → `02_cost_model.ipynb` (git mv). Fixed a latent UTF-16-LE→UTF-8 re-encoding issue in `deploy_platform.ps1` exposed by the edit (em-dashes/arrow → ASCII). `notebooks/README.md` + `scripts/README.md` + `README.md` + `docs/runbooks/local-data-queries.md` updated. Final tally: `notebooks/` is `01_data_walkthrough`, `02_cost_model` + `README.md` |
| PR21 | (this commit) | Scripts wrapper + docs (Part 19) | Audited `scripts/` (8 functional scripts, all referenced - none dead, none redundant). Asymmetry: local side had a single `-Task` wrapper (`run_local_stack.ps1`) but cloud side had 4 standalone scripts run as a manual sequence, and the 3 Python sub-scripts (`load_iceberg_to_duckdb.py`, `run_ge_local.py`, `metadata_observer.py`) were undocumented in `scripts/README.md`. Added `scripts/cloud/run_cloud_stack.ps1` — symmetric wrapper with `-Task apply|bootstrap|deploy|producers|verify|status|all` (default `-Env dev`; `-Task all` = apply -> bootstrap -> deploy -> producers -> verify, auto-approves terraform). Wrapper holds no logic of its own - it only orchestrates the 4 sibling scripts via splatting + `$LASTEXITCODE` checks. Rewrote `scripts/README.md` to document the wrapper + the 3 Python sub-scripts (invoked-by + purpose); updated root `README.md` + `AGENTS.md` cloud sections + Key References to surface `run_cloud_stack.ps1` as the primary cloud entry point with the manual sequence as the explicit alternative. ASCII-only (no em-dash/arrow re-encoding risk). |

**All P1, P2, and P3 gaps from the audit are now closed.**

## Where each closed gap now lives

The implementation is the source of truth. For every closed gap, the
artefact that enforces / documents the fix:

- **P1.1 / P1.2 — session/C360 incremental + grain:** `transformation/dbt_project/models/marts/marketing/fact_customer_session.sql` (crash-resilient lookback), `int_session_reconstruction.sql`, `customer_360_view.sql` header comment + composite unique dbt test
- **P1.3 — dim_date / dim_store tests:** `transformation/dbt_project/models/staging/_sources.yml` + `quality/great_expectations/expectations/dim_date.json` / `dim_store.json`
- **P1.4 — Flink re-submission idempotency:** `orchestration/airflow/dags/streaming_manual_flink_jobs.py` (ShortCircuitOperator)
- **P1.5 — DLQ null-event_id:** `streaming/flink_jobs/inventory_bronze_job.py` + `clickstream_bronze_job.py`; regression test in `tests/unit/test_flink_config.py::TestDlqSqlContract`
- **P1.6 — bronze → silver dependency:** documented in `streaming_manual_flink_jobs.py` (parallel submit is safe because silver reads its own Kafka consumer group; bronze is audit-only)
- **P2.1 — dbt idempotency:** `tests/integration/test_dbt_idempotency.py`
- **P2.2 — backfill verification:** `docs/runbooks/backfill-verification.md`
- **P2.3 — upstream incident response:** `docs/runbooks/upstream-incident-response.md`
- **P2.4 — `nightly_dbt_run` overlap:** DAG deleted in PR3 (redundant with `warehouse_daily_batch_pipeline`)
- **P2.5 — Gold row-count reconciliation:** `orchestration/airflow/plugins/row_count_reconciliation.py` + task in `warehouse_daily_batch_pipeline.py`; unit tests in `tests/unit/test_row_count_reconciliation.py`
- **P2.6 / P2.7 — accepted_values + column docs:** dbt YAML under `transformation/dbt_project/tests/` and `models/`
- **P2.8 — GE expectation suites:** `quality/great_expectations/expectations/*.json` + `checkpoints/gold_layer_daily.yml`
- **P2.9 — NULL customer_key in fact_sales:** `fact_sales.sql` header comment + dbt not_null-with-where test
- **P3.1 — DDL NOT NULL:** `transformation/redshift/ddl/08_fact_inventory_snapshot.sql` (`quantity_on_hand`, `quantity_available`, `is_estimated`)
- **P3.2 — watermark asymmetry:** in-SQL comments in `streaming/flink_jobs/inventory_bronze_job.py` + `inventory_silver_job.py`
- **P3.3 — SQL header comments:** closed in PR1 on `fact_sales.sql`, `fact_customer_session.sql`, `dim_customer.sql`
- **P3.4 — `nightly_dbt_run` doc_md:** closed in PR3 (DAG deleted, so the gap is moot)
- **P3.5 — DAG doc_md:** `orchestration/airflow/dags/catalog_bihourly_product_scd2_refresh.py` + `quality_hourly_ge_checkpoint.py`
- **P3.6 — POS Parquet determinism:** `ingestion/batch/generate_pos_parquet.py` (date-derived `random` seed + `uuid5` transaction_ids + `--seed` override) + `tests/unit/test_generate_pos_parquet.py`

---

## Part 6 — Data Lake Checklist (applied 2026-07-05)

A second, separate checklist covering Iceberg layout, file management,
concurrency, schema evolution, and query tuning — applied after PR1–PR8
closed the data warehouse checklist. 13 items considered; 10 applicable
to this stack, 3 not applicable (Delta/Hudi-specific or Spark-only).

| ID | Checklist item | Status | Artefact |
|---|---|---|---|
| DL-A | Time-based low-cardinality partitions; no high-cardinality partition keys | closed | `PARTITIONED BY (event_date)` on bronze inventory/clickstream (identity column derived from CAST(event_time AS DATE) — Flink SQL Hadoop catalog does not accept `days()` transform); `PARTITIONED BY (snapshot_date_key)` on silver inventory_hourly; `PARTITIONED BY (dt date)` on POS Spectrum external table. Unit test: `tests/unit/test_iceberg_partitions.py` |
| DL-B | Z-Ordering / clustering on high-cardinality filters | deferred | Flink Iceberg DDL doesn't expose sort-order; revisit when a query pattern benefits (documented in `docs/runbooks/iceberg-maintenance.md`) |
| DL-C | Data-skip rate monitoring | documented | `docs/runbooks/iceberg-maintenance.md` — `svl_s3query_summary` query for verifying Spectrum pushdown |
| DL-D | Compaction + snapshot expiration on a schedule | closed | `lakehouse_daily_iceberg_maintenance` Airflow DAG (03:00 UTC daily) + `streaming/flink_jobs/iceberg_maintenance.py` (Flink batch job calling `rewrite_data_files` + `expire_snapshots`) |
| DL-E | File-size / partition-count CloudWatch alerting | deferred | ADR-006 defers CloudWatch module; row-count reconciliation (P2.5) acts as a coarse proxy. Thresholds documented in `docs/runbooks/iceberg-maintenance.md` |
| DL-F | Concurrent write conflict avoidance (OCC) | documented | `docs/runbooks/iceberg-maintenance.md` — single-writer-per-table invariant; future-OCC-risk note for backfill jobs |
| DL-G | `MERGE INTO` over `INSERT OVERWRITE` | already in use | dbt `delete+insert` + `MERGE` for SCD2 (`dim_product`); documented in `docs/REDSHIFT.md` |
| DL-H | Idempotent writes for exactly-once | already in use | Flink `EXACTLY_ONCE` checkpoints; POS batch now deterministic (P3.6) |
| DL-I | New columns nullable (`on_schema_change='append_new_columns'`) | closed | 8/9 incremental models use `append_new_columns`; `dim_customer` uses `sync_all_columns` (documented exception for Type 1). Unit test: `tests/unit/test_dbt_on_schema_change.py` |
| DL-J | Avoid renaming / dropping columns directly | already in use | Data contract version bumps (`ingestion/schemas/`) + ADR-001 schema-evolution policy |
| DL-K | `ANALYZE TABLE` statistics after major imports | closed | `redshift_analyze` task in `warehouse_daily_batch_pipeline` (after `dbt_marts`, before `dbt_tests`) |
| DL-L | Predicate pushdown verification | documented | `docs/runbooks/iceberg-maintenance.md` — `svl_s3query_summary` + `sys_scan_s3_history` queries |
| DL-M | Tiered alerting on file size / partition count / transaction log | documented | Threshold table in `docs/runbooks/iceberg-maintenance.md` — wired as CloudWatch alarms deferred per ADR-006 |

**Tier status (Data Lake Checklist):**

| Status | Count |
|---|---:|
| Closed (implementation) | 5 (DL-A, DL-D, DL-I, DL-K, + DL-G/DL-H/J already in use) |
| Documented (runbook) | 5 (DL-C, DL-F, DL-L, DL-M, + DL-B with revisit trigger) |
| Deferred (ADR-006) | 2 (DL-B, DL-E) |
| N/A (Delta/Hudi/Spark-specific) | 0 — all 13 items had an Iceberg/Flink/Redshift answer |

The full data-lake operational contract — layout, maintenance schedule,
tuning knobs, monitoring thresholds, OCC policy, schema-evolution rules,
predicate-pushdown verification — lives in
[`docs/runbooks/iceberg-maintenance.md`](./iceberg-maintenance.md).

---

## Part 7 — DAG Review Checklist (applied 2026-07-05, PR9)

A third checklist covering DAG design + code review: naming,
granularity, parameterization, idempotency, dependencies, retries /
alerts, and template reuse. Applied to all 6 Airflow DAGs after the
data warehouse (Part 1–5) and data lake (Part 6) audits closed.

7 items considered; all 7 applicable.

| ID | Checklist item | Status | Artefact |
|---|---|---|---|
| DAG-1 | Naming standardized: `{domain}_{frequency}_{description}` template; file name == dag_id | closed | All 6 DAGs renamed: `warehouse_daily_batch_pipeline`, `marketing_hourly_customer_360_pipeline`, `catalog_bihourly_product_scd2_refresh`, `quality_hourly_ge_checkpoint`, `lakehouse_daily_iceberg_maintenance`, `streaming_manual_flink_jobs`. Unit test: `tests/unit/test_dag_contract.py::test_dag_id_follows_naming_template` + `test_dag_file_name_matches_dag_id`. Convention codified in `docs/runbooks/dag-review-checklist.md`. |
| DAG-2 | Task granularity 1–50 per DAG | closed | Current DAGs have 1–9 tasks each — well within band. Enforced by `test_dag_task_count_within_bounds`. |
| DAG-3 | Parameterization (no hardcoding) | closed | All DAGs use `{{ ds }}` and `{{ var.value.X }}`. No `datetime(...)` calls other than `start_date`. Enforced by `test_dag_no_hardcoded_datetime`. |
| DAG-4 | Idempotent writes | already in use | dbt `delete+insert` + `MERGE` for SCD2; POS writes to `dt={{ ds }}/` partitions; Flink `EXACTLY_ONCE` checkpoints. No bare `INSERT INTO` in DAG source — enforced by `test_dag_no_bare_insert_into`. |
| DAG-5 | Dependencies correct (no cycles) | closed | Airflow detects cycles at parse time; static test asserts at least one `>>` chain. Enforced by `test_dag_has_dependency_chain`. |
| DAG-6 | Retries + `email_on_failure=True` + `catchup=False` + `doc_md` | closed | Added `email_on_failure=True` + `email` to `catalog_bihourly_product_scd2_refresh` and `quality_hourly_ge_checkpoint` DEFAULT_ARGS (Airflow's default is False — silently disables alerts). Enforced by `test_dag_has_retries_configured` + `test_dag_has_email_on_failure` + `test_dag_has_catchup_false` + `test_dag_has_doc_md`. |
| DAG-7 | Templates reused (DRY for similar tasks) | closed | `warehouse_daily_batch_pipeline.py` refactored: 4 dbt BashOperator blocks → single `_dbt_step(...)` factory. `streaming_manual_flink_jobs.py` already used `_flink_step(...)` factory. `lakehouse_daily_iceberg_maintenance.py` uses `_iceberg_maintenance_step(...)`. Not statically enforceable — left to code review per `docs/runbooks/dag-review-checklist.md` § 7. |

**Tier status (DAG Review Checklist):**

| Status | Count |
|---|---:|
| Closed (implementation + test) | 6 (DAG-1, DAG-2, DAG-3, DAG-5, DAG-6, DAG-7) |
| Already in use (test guards regression) | 1 (DAG-4) |
| Documented (runbook) | 7/7 — `docs/runbooks/dag-review-checklist.md` |

The DAG review runbook (naming template, granularity, parameterization,
idempotency, dependencies, retries/alerts, template reuse, new-DAG
skeleton, rename procedure) lives in
[`docs/runbooks/dag-review-checklist.md`](./dag-review-checklist.md).

The DAG contract is enforced statically by
`tests/unit/test_dag_contract.py` (10 test functions × 6 DAGs + 1 count
test = 61 tests; 2 skipped for single-task DAGs).

---

## Part 8 — Kafka Master Checklist (applied 2026-07-05, PR10)

A fourth checklist covering the three-tier reliability defense (producer
/ broker / consumer), performance tuning (batching, threads, JVM, OS),
troubleshooting & operations, and advanced architecture (KRaft,
exactly-once, CDC, DR, tiered storage, Burrow + Schema Registry). Applied
after the data warehouse (Parts 1–5), data lake (Part 6), and DAG review
(Part 7) audits closed.

18 items considered; 6 applicable + implementable, 4 already in use, 5
documented as N/A for MSK Serverless or out of current scope, 3 deferred
to a future ADR.

| ID | Checklist item | Status | Artefact |
|---|---|---|---|
| K-PROD | Producer `acks=all` + `retries=MAX` + `enable.idempotence=true` + `max.in.flight.requests=5` + `delivery.timeout.ms` | closed | `ingestion/kafka/msk_config.py:_PRODUCER_DEFAULTS`; unit tests in `tests/unit/test_msk_config.py::TestProducerReliabilityDefaults` (5 tests) |
| K-PROD-PERF | Producer `batch.size` + `linger.ms` + `compression.type=lz4` | closed | same defaults block; unit tests in `tests/unit/test_msk_config.py::TestProducerPerformanceDefaults` (3 tests) + override test (2 tests) |
| K-BROKER | Broker `min.insync.replicas=2` + `unclean.leader.election.enable=false` | N/A for MSK Serverless | AWS-managed. The previous orphaned `aws_msk_configuration` resource (which set these but was never attached to the serverless cluster) was removed in PR10. |
| K-CONS | Consumer `enable.auto.commit=false` + manual offset commit + idempotent downstream | closed | Explicit `'properties.enable.auto.commit' = 'false'` + `'properties.isolation.level' = 'read_committed'` + `'properties.auto.offset.reset' = 'earliest'` on all 3 Flink Kafka source DDLs. Flink checkpoint committer handles manual commit under EXACTLY_ONCE. Unit tests in `tests/unit/test_flink_kafka_source.py` (12 tests). |
| K-BATCH | Broker thread tuning (`num.network.threads`, `num.io.threads`) | N/A for MSK Serverless | AWS-managed. Documented in `docs/runbooks/kafka-operations.md` § 2.2. |
| K-JVM | JVM heap 6-8GB + `vm.dirty_ratio` 60-80 | N/A for MSK Serverless | AWS-managed. Documented in runbook § 2.2. |
| K-PULL | Consumer `fetch.min.bytes` + `max.poll.records` | N/A for Flink | Flink Kafka source bypasses the poll loop. Throughput is tuned via `parallelism` in `streaming/config/flink_conf.yaml`. Documented in runbook § 2.3. |
| K-LAG | High consumer lag → increase partitions + consumer parallelism + custom Partitioner | documented | 12-24 partitions per topic (`ingestion/kafka/topics.py`); Flink parallelism 4 (configurable). Custom Partitioner not yet needed — current hash-by-`store_id:product_id` key distribution is balanced. Runbook § 3.1. |
| K-REBAL | Frequent rebalancing → `session.timeout.ms` + `CooperativeSticky` + Static Membership | N/A for Flink | Flink manages its own consumer group membership; bypasses the poll-driven heartbeat protocol. Documented in runbook § 3.2. |
| K-GC | Broker full GC → JVM heap <6GB | N/A for MSK Serverless | AWS-managed. Documented in runbook § 3.3. |
| K-LEADER | Leader imbalance → preferred leader election | N/A for MSK Serverless | AWS-managed. Documented in runbook § 3.4. |
| K-RETENTION | Disks filling up → `log.retention.{bytes,hours}` | documented | MSK Serverless manages retention automatically. Per-topic override possible via MSK API (not yet wired). Documented in runbook § 3.5. |
| K-KRAFT | KRaft Mode (Kafka 3.3+) | in use (managed) | MSK Serverless uses KRaft internally — transparent to the platform. |
| K-EOS | Exactly-Once via Kafka transactions + Flink Checkpoints | already in use | Flink `EXACTLY_ONCE` checkpoints + producer `enable.idempotence=true` + consumer `isolation.level=read_committed` — the standard Flink-Kafka EOS stack. |
| K-CDC | CDC via Kafka Connect + Debezium | N/A for current scope | POS source is daily Parquet batch, not a transactional DB. Deferred. Runbook § 4.3. |
| K-DR | Disaster Recovery via MirrorMaker 2 | deferred | Single-region deployment. Multi-region active-active would add a second MSK cluster + MirrorMaker 2 / MSK Replicator. Future ADR. Runbook § 4.4. |
| K-TIER | Tiered Storage (Kafka 3.6+) | in use (managed) | MSK Serverless automatically tiers older log segments to S3-backed storage. |
| K-MON | Burrow + Confluent Schema Registry | closed (CloudWatch) / documented (Schema Registry) | Consumer lag monitoring wired via CloudWatch alarm on `PCTConsumerLag` (replaces Burrow — simpler for MSK Serverless). Schema Registry deferred — current JSON Schemas in `ingestion/schemas/` act as data contracts; runbook § 4.6 documents the upgrade path. |

**Tier status (Kafka Master Checklist):**

| Status | Count |
|---|---:|
| Closed (implementation + test) | 5 (K-PROD, K-PROD-PERF, K-CONS, K-MON-consumer-lag, K-EOS already in use + now test-guarded) |
| In use (managed by MSK Serverless) | 3 (K-KRAFT, K-TIER, K-EOS) |
| N/A for MSK Serverless or current scope | 6 (K-BROKER, K-BATCH, K-JVM, K-PULL, K-REBAL, K-CDC) |
| Documented (runbook) | 2 (K-LAG, K-RETENTION) |
| Deferred (future ADR) | 1 (K-DR) |
| Documented upgrade path | 1 (K-MON Schema Registry portion) |

The Kafka operations runbook (three-tier defense, performance tuning,
troubleshooting playbook, advanced-architecture notes, operations
cheatsheet, codebase map) lives in
[`docs/runbooks/kafka-operations.md`](./kafka-operations.md).

The Kafka reliability contract is enforced statically by
`tests/unit/test_msk_config.py` (12 tests) and
`tests/unit/test_flink_kafka_source.py` (12 tests).

---

## Part 9 — Flink Production Checklist (applied 2026-07-05, PR11)

A fifth checklist covering the seven-section Flink production manual:
architecture/deployment, state management & checkpoints, sinks &
end-to-end exactly-once, sources (Kafka best practices), time/watermarks
/windows, resource tuning & data skew, and operations & fault tolerance
SOPs. Applied after the data warehouse (Parts 1–5), data lake (Part 6),
DAG review (Part 7), and Kafka (Part 8) audits closed.

~30 items considered across 7 sections; 6 applicable + implementable,
6 already in use, 4 documented as deferred (allowed lateness, side
outputs, salting, advanced memory tuning), 5 documented as N/A for this
stack (Kafka transactions, DataStream API patterns, K8s deployment).

| ID | Checklist item | Status | Artefact |
|---|---|---|---|
| F-DEPLOY | YARN Per-Job or K8s Native | in use | EMR YARN Per-Job via `streaming_manual_flink_jobs.py`. Documented in `docs/runbooks/flink-operations.md` § 1.1. |
| F-STATE | RocksDB state backend + incremental checkpoints + State TTL + source idle timeout | closed | `streaming/config/state.yaml` (rocksdb + incremental + 7d TTL + 1min idle); `apply_state_config()` helper in `streaming/flink_jobs/_config.py`; each streaming Flink job's `run()` calls it. Unit tests: `tests/unit/test_flink_state_config.py::TestStateConfigFile` (4 tests) + `TestApplyStateConfigHelper` (5 tests) + `TestJobsCallApplyStateConfig` (10 tests). |
| F-CHK | `minPauseBetweenCheckpoints` 30s + `RETAIN_ON_CANCELLATION` | closed (RETAIN_ON_CANCELLATION already set; min_pause bumped) | `streaming/config/checkpoints.yaml:min_pause_between_checkpoints_ms` bumped 5000 → 30000. Unit test: `TestCheckpointsMinPause`. |
| F-SINK | Batch sink writes | already in use | Iceberg connector batches per checkpoint (natural batching). Documented in runbook § 3.1. |
| F-KAFKA-TX | Kafka transaction timeout alignment | N/A | Iceberg sink, not Kafka transactions. Documented in runbook § 3.2. |
| F-IDEMPOTENT | Idempotent writes over 2PC | already in use | Iceberg checkpoint-committed (idempotent given checkpoint ID); POS batch `dt=YYYY-MM-DD` overwrite. Documented in runbook § 3.3. |
| F-SRC-PAR | Match parallelism to partitions | closed | Per-topic parallelism config (`inventory_parallelism=12`, `clickstream_parallelism=24` for cloud) in `streaming/config/flink_conf.yaml`. Unit test: `TestFlinkConfPerTopicParallelism`. |
| F-SRC-IDLE | `withIdleness()` for idle partitions | closed | `table.exec.source.idle-timeout = 1 min` in state.yaml, applied via `apply_state_config()`. |
| F-SRC-DCOMMIT | Double-offset management (auto.commit=true) | documented (decision: false) | PR10 chose `enable.auto.commit=false` for exactly-once; lag monitored via CloudWatch `PCTConsumerLag` instead. Trade-off documented in runbook § 4.3. |
| F-SRC-DISC | Dynamic partition discovery | closed | `'properties.partition.discovery.interval.ms' = '300000'` added to all 3 Kafka source DDLs. Unit test: `TestKafkaSourcePartitionDiscovery` (3 tests). |
| F-TIME | Use event time | already in use | All 3 jobs declare `WATERMARK FOR event_ts AS ...` with event-time semantics. |
| F-LATE-1 | Watermark delay (1st layer) | already in use | 30s for bronze, 60s for silver — wide enough that <1% of events are dropped. |
| F-LATE-2 | Allowed lateness (2nd layer) | deferred | Flink SQL `EMIT` clauses would require restructuring the hourly aggregation. Watermark is wide enough — see runbook § 5.2 for the "widen before adding lateness" decision rule. |
| F-LATE-3 | Side outputs for late data (3rd layer) | deferred | DLQ topics serve the same operational purpose (offline correction). Side outputs would require DataStream API migration — see runbook § 5.2. |
| F-WIN-1 | Avoid sliding windows | already in use | inventory_silver_job uses 1-hour tumbling window. |
| F-WIN-2 | AggregateFunction + ProcessWindowFunction | N/A | DataStream API pattern. SQL jobs use `GROUP BY TUMBLE(...)` which is the optimised SQL equivalent. |
| F-SLOT | Task slot config = CPU cores | documented | Deploy-time config in EMR bootstrap's `flink-config` classification. Runbook § 6.1. |
| F-MEM | Managed memory 50%+ for RocksDB; network 15-20% for high parallelism | documented | EMR bootstrap `flink-config` classification. Runbook § 6.2. |
| F-SKEW | Two-Stage Aggregation (Salting) | deferred (not needed) | Current data distribution is uniform by design. Runbook § 6.3 documents the technique for when a skew hotspot is measured. |
| F-UPGRADE | Golden Upgrade SOP (savepoint → stop → resume) | documented | Runbook § 7.1. `streaming_manual_flink_jobs.py` DAG already takes a savepoint path parameter. |
| F-PARSE | Fault-tolerant parsing (`json.ignore-parse-errors=true`) | already in use | Set on every Kafka source DDL. Schema-violating rows routed to DLQ topics. |
| F-METRIC-LAG | Kafka lag alerting | already in use (PR10) | CloudWatch alarm on `PCTConsumerLag`. |
| F-METRIC-CP | Checkpoint failures + duration alerting | documented | Next CloudWatch wiring target — requires EMR `flink-config` classification to expose Flink metrics. Runbook § 7.3. |
| F-METRIC-DQ | Data quality NULL-rate alerting | documented | Future Airflow daily check via Spectrum query on Iceberg bronze. Runbook § 7.3. |

**Tier status (Flink Production Checklist):**

| Status | Count |
|---|---:|
| Closed (implementation + test) | 6 (F-STATE, F-CHK min_pause bump, F-SRC-PAR, F-SRC-IDLE, F-SRC-DISC, F-METRIC-LAG already wired) |
| Already in use | 6 (F-DEPLOY, F-SINK, F-IDEMPOTENT, F-TIME, F-LATE-1, F-WIN-1, F-PARSE) |
| Documented (runbook + decision) | 4 (F-SRC-DCOMMIT trade-off, F-SLOT, F-MEM, F-METRIC-CP, F-METRIC-DQ) |
| Deferred | 3 (F-LATE-2, F-LATE-3, F-SKEW) |
| N/A | 2 (F-KAFKA-TX, F-WIN-2) |

The Flink operations runbook (deployment mode, state backend contract,
checkpoint safeguards, sink/source best practices, late-data 3-layer
defense, resource tuning, upgrade SOP, metric alerting plan,
operations cheatsheet, codebase map) lives in
[`docs/runbooks/flink-operations.md`](./flink-operations.md).

The Flink state + source contract is enforced statically by
`tests/unit/test_flink_state_config.py` (25 tests across 5 test classes)
plus the existing `tests/unit/test_flink_kafka_source.py` (12 tests from
Part 8).

## References

- Industry best practice for data warehouse development, applied to this project's Kimball + Iceberg + Redshift + Flink + dbt stack.
- Project source of truth: `ARCHITECTURE.md`, `docs/data-model/dimensional-model.md`, `docs/decisions/ADR-*.md`, `docs/ENVIRONMENTS.md`.
- Prior review context: `docs/decisions/ADR-007-inventory-kappa.md` (kappa conversion that closed 3 P1s on the inventory mart).
