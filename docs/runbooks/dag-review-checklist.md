# Runbook: DAG review checklist

Codifies the 7-item DAG design + code review checklist applied
2026-07-05 (PR9) to every DAG under `orchestration/airflow/dags/`.
The contract is enforced statically by
`tests/unit/test_dag_contract.py` — that test fails if a new DAG
violates any of items 1, 2, 3, 4, 5, or 6 below. Item 7 (templates
reused) is left to code review because static detection of
duplicate-task-block patterns is unreliable.

## 1. Naming standardized

### DAG IDs

Template: `{domain}_{frequency}_{description}`

| Component | Allowed values |
|---|---|
| `domain` | Lowercase letters / digits. Pick the business domain the DAG serves: `warehouse`, `streaming`, `marketing`, `catalog`, `quality`, `lakehouse`. |
| `frequency` | One of: `daily`, `hourly`, `bihourly`, `weekly`, `monthly`, `continuous`, `manual`. `manual` = `schedule_interval=None` (manual trigger). |
| `description` | Lowercase letters / digits / underscores. Short, action-oriented. |

Examples (current DAGs):

| DAG ID | Domain | Frequency | Description |
|---|---|---|---|
| `warehouse_daily_batch_pipeline` | warehouse | daily | batch_pipeline |
| `marketing_hourly_customer_360_pipeline` | marketing | hourly | customer_360_pipeline |
| `catalog_bihourly_product_scd2_refresh` | catalog | bihourly | product_scd2_refresh |
| `quality_hourly_ge_checkpoint` | quality | hourly | ge_checkpoint |
| `lakehouse_daily_iceberg_maintenance` | lakehouse | daily | iceberg_maintenance |
| `streaming_manual_flink_jobs` | streaming | manual | flink_jobs |

### Task IDs

Template: `{layer}_{action}_{object}` — e.g. `dbt_run_staging`,
`redshift_analyze_statistics`, `ge_gold_checkpoint`,
`row_count_reconciliation`. Task IDs are within-DAG and don't need
to follow the DAG rename.

### File names

DAG file name MUST equal the `dag_id` (without `.py`). Enforced by
`test_dag_file_name_matches_dag_id`.

## 2. Task granularity appropriate

- One task = one logical operation. Don't bundle multiple dbt runs
  into a single BashOperator; don't split a single SQL statement
  into multiple tasks.
- Target 10–50 tasks per DAG. The current 6 DAGs have 1–9 tasks each
  (well within the band). If a DAG grows past 50 tasks, split into
  TaskGroups or sub-DAGs.
- Enforced by `test_dag_task_count_within_bounds` (1 ≤ count ≤ 50).

## 3. Parameterization (no hardcoding)

- Use Airflow macros for execution dates: `{{ ds }}` (logical date,
  YYYY-MM-DD), `{{ ds_nodash }}` (YYYYMMDD), `{{ run_id }}`.
- Use Airflow Variables for infra endpoints: `{{ var.value.X }}`.
  Never hardcode S3 bucket names, Redshift endpoints, EMR cluster IDs,
  or Kafka brokers in DAG code.
- The only allowed `datetime(...)` call is `start_date=datetime(...)`.
  Any other `datetime(...)` is a parameterization smell — replace with
  a Jinja macro.
- Enforced by `test_dag_no_hardcoded_datetime` (≤ 1 `datetime()` call).

## 4. Idempotent

- Re-running a task for the same `{{ ds }}` must yield the same result
  without duplicating data. Strategies:
  - dbt incremental: `incremental_strategy='delete+insert'` with
    `unique_key`. See `transformation/dbt_project/models/`.
  - Raw SQL: `INSERT OVERWRITE` (Hive-style) or `MERGE INTO`. Never
    bare `INSERT INTO`.
  - File writes: write to `dt={{ ds }}/` partition paths so the same
    logical date overwrites the same partition.
  - Streaming sinks: Flink `EXACTLY_ONCE` checkpoints handle
    idempotency at the connector level.
- Enforced by `test_dag_no_bare_insert_into` (no `INSERT INTO` without
  `OVERWRITE` in DAG source). Streaming DAGs that wrap Flink jobs
  are exempt — their INSERT INTO statements live in the Flink job
  files, not the DAGs.

## 5. Dependencies correct

- Every task (except single-task DAGs) must be wired to at least one
  upstream or downstream task via `>>` or `<<`.
- Airflow detects circular dependencies at parse time, so the static
  test only checks that SOME chain exists.
- Cross-DAG dependencies: use `ExternalTaskSensor` if you need to wait
  for another DAG's task. None of the current 6 DAGs use this — they
  are independent.
- Enforced by `test_dag_has_dependency_chain`.

## 6. Retries and alerts configured

- `DEFAULT_ARGS` must declare `retries >= 1`. Transient faults (S3
  throttles, Redshift brief unavailability) should not fail the DAG
  on first attempt.
- `DEFAULT_ARGS` must declare `email_on_failure=True`. Airflow's
  default for this is **False** — omitting it silently disables
  alerting. This was the actual finding that triggered PR9
  (`catalog_bihourly_product_scd2_refresh` and
  `quality_hourly_ge_checkpoint` were missing it).
- `DEFAULT_ARGS.email` should point to the data-platform AlertEmail
  SNS subscription (in MWAA) or a real email address (local Airflow).
- `catchup=False` is required unless the DAG is explicitly a backfill
  DAG that should replay missed intervals.
- `doc_md` is required — set `doc_md=__doc__` (module docstring) or
  `doc_md=DAG_DOC_MD` for richer markdown. The Airflow UI renders this
  in the 'Details' tab; include purpose, idempotency notes, and
  recovery steps.
- Enforced by `test_dag_has_retries_configured`,
  `test_dag_has_email_on_failure`, `test_dag_has_catchup_false`,
  `test_dag_has_doc_md`.

## 7. Templates reused

- If a DAG has 3+ BashOperator tasks with near-identical bash_command
  patterns, extract a factory function (e.g. `_dbt_step(...)` in
  `warehouse_daily_batch_pipeline.py`).
- If a DAG submits multiple similar EMR steps, extract a step factory
  (e.g. `_flink_step(...)` in `streaming_manual_flink_jobs.py`).
- Not statically enforceable — left to code review. Reviewers should
  flag any new BashOperator block that's >80% identical to an existing
  one in the same DAG.

## Adding a new DAG — checklist

1. Pick the dag_id following the `{domain}_{frequency}_{description}`
   template. Confirm the frequency matches `schedule_interval`.
2. Create `orchestration/airflow/dags/<dag_id>.py`.
3. Start from this skeleton:

   ```python
   """
   <One-line purpose>.

   Schedule: <cron or None>.
   Required Airflow Variables:
     - <var_name> : <description>
   """

   from datetime import datetime, timedelta
   from airflow import DAG
   from airflow.operators.bash import BashOperator

   DEFAULT_ARGS = {
       "owner":            "data-platform",
       "depends_on_past":  False,
       "start_date":       datetime(2024, 1, 1),
       "retries":          2,
       "retry_delay":      timedelta(minutes=5),
       "email_on_failure": True,
       "email":            ["data-platform@company.com"],
   }

   with DAG(
       dag_id="<dag_id>",
       default_args=DEFAULT_ARGS,
       schedule_interval="<cron>",
       catchup=False,
       tags=["<domain>", "<frequency>"],
       doc_md=__doc__,
   ) as dag:
       # tasks here
       ...
   ```

4. Use `{{ ds }}` and `{{ var.value.X }}` for any date / infra-specific
   value.
5. If the DAG has 3+ similar BashOperator tasks, extract a factory.
6. Run `python -m pytest tests/unit/test_dag_contract.py -v` — all
   tests must pass for the new DAG.
7. Run `python -m compileall orchestration/airflow/dags/<dag_id>.py`
   to verify syntax.
8. Update `docs/runbooks/dw-checklist-audit.md` if the new DAG closes
   a checklist gap.
9. Update `AGENTS.md`, `README.md`, `.cursor/rules/airflow.mdc` to
   list the new DAG.

## Renaming a DAG

Renaming a DAG ID loses Airflow run history (the new dag_id has no
prior task instances). The 2026-07-05 PR9 rename of all 6 DAGs to
the `{domain}_{frequency}_{description}` template was a one-time
event to establish the convention; future renames should be rare and
require:

1. `git mv orchestration/airflow/dags/<old>.py <new>.py`
2. Update `dag_id="..."` inside the file.
3. Update all references in:
   - `AGENTS.md`
   - `README.md`
   - `ARCHITECTURE.md`
   - `.cursor/rules/project-context.mdc`
   - `.cursor/rules/airflow.mdc`
   - `docs/ENVIRONMENTS.md`
   - `docs/REDSHIFT.md`
   - `docs/decisions/ADR-*.md`
   - `docs/runbooks/*.md`
   - `docs/case-studies/global-retail-analytics-platform.md`
   - Any `orchestration/airflow/plugins/*.py` or
     `ingestion/batch/*.py` docstring that mentions the DAG.
4. Run `python -m pytest tests/unit/test_dag_contract.py -v` — the
   new dag_id must pass the naming template test.

## References

- Source checklist: applied 2026-07-05; closure recorded in
  `docs/runbooks/dw-checklist-audit.md` Part 7.
- Static enforcement: `tests/unit/test_dag_contract.py`
- DAG skeleton + factory examples: `warehouse_daily_batch_pipeline.py`
  (`_dbt_step`), `streaming_manual_flink_jobs.py` (`_flink_step`),
  `lakehouse_daily_iceberg_maintenance.py` (`_iceberg_maintenance_step`).
