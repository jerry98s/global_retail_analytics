# Codex workflow for this repository

Keep durable rules in [AGENTS.md](../../AGENTS.md), design in the existing
architecture/ADRs, and operational procedures in their runbooks. These skills
route work through those sources; they do not replace them or enable hooks.

## Start a focused task

State the branch, requested outcome, constraints, and acceptable evidence.
Keep unrelated questions in separate chats. Before a handoff, ask for a short
summary of the checkout, changes, decisions, verification and remaining work.
Use Git for recovery; a conversation summary is not a file backup.

Repo-local skills live under `.agents/skills`. If they are not listed in the
current session, start a new repo chat or explicitly ask Codex to read the named
`SKILL.md`. Discovery and invocation are described in the
[official skill documentation](https://learn.chatgpt.com/docs/build-skills).

## Copy-paste prompts

### Plan an uncertain change

> Plan only; do not edit. Inspect the current branch and relevant architecture.
> Ask me about decisions that change the scope. Propose the smallest change,
> regression tests, verification commands, and operational risks. Preserve
> replayable Bronze, Gold WAP, and the cloud/local branch separation.

### Fix a known issue

> Fix the described issue on local-testing-version. Preserve unrelated work.
> Reproduce it with a regression test, implement the fix, and use
> $verify-data-pipeline for relevant checks. Report exact results and anything
> unverified. Do not deploy, push, or delete existing data.

### Review a change

> Use $review-data-pipeline to review my uncommitted changes, including relevant
> untracked files. Focus on correctness and regression risks. Do not edit.
> Report actionable findings with locations, failure scenarios, and evidence;
> distinguish missing verification from demonstrated defects.

For a branch review, name the intended base branch or commit instead of asking
for uncommitted changes. To review both branches, request separate scopes and
reports; the cloud/local differences are intentional.

### Port shared logic to cloud

> Port the specified shared change to main in an isolated worktree. Inspect
> the source commit and main first. Preserve cloud deployment wiring and exclude
> local-only compose, short-window and DuckDB settings. Use
> $verify-data-pipeline, then request a read-only subagent correctness review.
> Do not deploy AWS resources, push, or delete data. Report unverified cloud paths.

## Select evidence, not just commands

Run from the repository root unless stated otherwise. Check the current
branch's `.github/workflows/ci.yml` for maintained dependencies and selectors.
The following examples describe the local-testing-version checkout; when
porting, inspect main's own files rather than assuming parity.

| Change | Evidence to select | Limit |
|---|---|---|
| Python logic | Unit suite and Ruff | No deployed-service coverage |
| Reference seeds / graph rules | Generator `--check` commands | Drift check, not runtime proof |
| Canonical Bronze replay | Spark regression with `--iceberg` | No live Kafka/Flink capture proof |
| Identity DataFrames | Real GraphFrames parity test in CI | Distinct from fixture generation |
| dbt SQL / WAP | Compile relevant live/pending selections; build/test in isolated target | Compile is not data validation |
| Local Gold quality | dbt tests and local GE on prepared data | Fixtures do not prove the Iceberg bridge |
| Cloud deployment | Authorized smoke/E2E using main's runbooks | Local success does not prove AWS wiring |

### Fast checks

These checks do not require cloud credentials. Run each separately and retain
its exit code (`$LASTEXITCODE` immediately after native commands in PowerShell).

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/ -q
.\.venv\Scripts\python.exe -m ruff check ingestion streaming scripts quality orchestration spark tests
.\.venv\Scripts\python.exe spark/identity_resolution/generate_fixture.py --check
.\.venv\Scripts\python.exe -m ingestion.batch.generate_product_master --check
```

### Runtime checks

- Bronze/Silver: follow the Tests section of [bronze replay](bronze-replay.md).
  Use Spark 3.4.1 and the pinned Iceberg runtime from CI, with repo `PYTHONPATH`.
  This regression uses temporary fixtures, needs Java/Spark, and may download
  Maven packages. Docker is not required for this test; it is required for the
  normal local stack. Missing prerequisites mean blocked, not passed.
- Identity: use the `spark-identity` job's GraphFrames command in CI.
- dbt: inspect `profiles.yml.example`, CI selectors and macro targets first.
  Use a disposable checkout/profile/database for materializing checks. Never
  overwrite an existing profile. In particular,
  `tests/integration/test_dbt_idempotency.py` deletes its checkout's
  `transformation/dbt_project/local_retail.duckdb` and WAL before rebuilding.
  Merely choosing another profile does not isolate that hardcoded deletion.
- Local GE: the prepared local stack's `-Task quality` runs dbt tests, GE and
  units. Inspect the wrapper's options before use; do not run stack `all` merely
  to verify a small change. Fixture mode must be explicitly identified.
- Cloud: use [dev cloud test run](the dev cloud test run on main) on main. The wrapper's
  `-Task all` is infrastructure/streaming smoke, not Gold E2E. Provisioning,
  publishing and destructive cleanup need authorization; credentials alone
  are not permission. Never run raw Terraform from a stack directory.

## Evidence handoff

Report the branch, commit and whether the working tree has uncommitted edits.
For each check record: command, working directory, target/data mode, exit code,
result counts and relevant warnings. For a check not run, record the reason and
the claim it leaves unverified. Do not fabricate an exit code for a skipped check.
Summarize logs without credentials or customer payloads.

For documentation/skill-only edits, validate skill frontmatter, referenced paths
and commands, and `git diff --check`. Explain why runtime tests were not needed;
do not launch a pipeline simply because the docs contain its commands.
