---
name: verify-data-pipeline
description: Use when validating changes to this repository's ingestion, Flink, Spark, dbt, quality checks, orchestration, or deployment wiring, including requests for evidence before calling a pipeline fix complete. Not a request to deploy or repair failures.
---

# Verify data-pipeline changes

Match the evidence to the changed layer and state what remains unverified.
Resolve paths below from the repository root and read its `AGENTS.md`.

## Choose checks

Record branch, commit, working-tree state and the requested acceptance criteria.
Inspect the changed producers/consumers and tests. Use the check-selection table
and runtime precautions in `docs/runbooks/codex-workflow.md`, then confirm
commands and dependency versions in this branch's `.github/workflows/ci.yml`.
Do not assume main has the local branch's scripts or targets.

| Affected layer | Verification route |
|---|---|
| Python contracts | Unit regression plus full offline unit suite; Ruff for touched code |
| Graph/reference fixtures | Relevant generator `--check`; actual GraphFrames parity for runtime changes |
| Canonical replay | `tests/integration/verify_bronze_replay.py --iceberg` under Spark; see bronze replay runbook |
| dbt / Gold WAP | Applicable live/pending compilation and isolated materialization/tests; GE where affected |
| Deployment wiring | Safe local checks plus explicitly authorized cloud smoke/E2E; main's cloud runbook |
| Only docs/skills | Frontmatter/reference/command checks and diff whitespace checks; no pipeline launch |

## Execute within scope

Inspect test setup and outputs before execution. Preserve the user's profile,
database, Bronze snapshots and unrelated files. The dbt idempotency test deletes
its checkout's `local_retail.duckdb` and WAL: use a disposable checkout with a
consistent isolated profile/database, not an environment override alone.

Missing Docker does not block the standalone Spark replay regression if Java,
Spark and its runtime packages are available. Missing prerequisites must be
reported; do not silently install a different runtime or replace real-path
checks with seeds. Never use existing credentials as deployment authorization.
Do not run provisioning, production replay/publish or destructive cleanup as
an implicit verification step. When required evidence cannot be obtained safely,
complete independent safe checks and report the blocked check and next step.

For a verify-only request, report failures without changing implementation or
tests. In an authorized fix task, regression failures may drive in-scope fixes;
keep assertions meaningful and rerun affected checks after changes.

## Evidence handoff

For each check record command, working directory, target/data mode, exit code,
counts and relevant warnings. For skipped/blocked checks record the reason and
unverified claim, without inventing a result. Distinguish current execution from
previous logs. A skipped test is not a pass; compilation is not a data audit;
fixture replay is not live capture/checkpoint recovery or AWS acceptance.

Example request: `Use $verify-data-pipeline for the canonical replay diff on
main. Run safe available checks; no cloud deployment or existing-data writes.`

Common mistakes: declaring cloud ready from unit tests; using seed mode to hide
missing Silver; treating directory existence as successful replay; executing a
destructive test in the demo checkout. Use the routed runbooks to resolve these.
