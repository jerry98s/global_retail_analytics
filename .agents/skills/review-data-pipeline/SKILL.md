---
name: review-data-pipeline
description: Use when reviewing correctness or regression risk in this repository's ingestion, Flink, Spark, dbt, data-quality, orchestration, or deployment changes. Not for implementing fixes or a dedicated security audit.
---

# Review data-pipeline changes

Produce a scoped correctness review, not a deployment-readiness verdict.
Use the repository root for paths below; start with its `AGENTS.md`.

## Establish the comparison

Inspect branch, status, diff and relevant untracked files without changing them.
For uncommitted work, cover staged and unstaged edits; for branch work, resolve
the user's intended base and record the compared commits. If the base is
ambiguous and changes the review scope, ask. Do not silently compare the local
demo branch with cloud main as though all their differences were defects.

Trace changed code to its producers, consumers and tests. Read only applicable
architecture, data-model docs and ADRs. When time is limited, prioritize data
loss, replay/correctness, publish boundaries and deployment wiring; report
unreviewed paths. A review request does not authorize fixes, commits or deployment.

## Review lenses

| Affected path | Evidence to inspect |
|---|---|
| Bronze / canonical Silver | ADR-011 and `docs/runbooks/bronze-replay.md`: retained bytes/coordinates, source incarnation, snapshot reads, deterministic winners, quarantine/duplicates and count conservation |
| Replay consumers | Replacement exports, stale-file exclusion, reader/writer coordination, downstream rebuild limits |
| Identity | ADR-010, graph rules, fixture parity and actual DataFrame/GraphFrames tests |
| Gold / orchestration | ADR-009, pending/live references, owning DAG, audits before publish, incremental and SCD2 preservation |
| Cloud port | Main's own packaging/configuration, helper dependencies and source locations; intentional local-only wiring stays local |

## Evidence and output

Use `docs/runbooks/codex-workflow.md` for safe check selection and evidence
levels. Non-mutating diagnostics and isolated tests can support a review;
inspect test side effects first. Earlier assistant claims or passing fixture
tests are not proof of current deployed behavior.

Return findings first, ordered by impact. Each finding needs a location, a
concrete trigger/input, resulting failure and supporting code/test evidence.
Separate suspected issues and missing verification from demonstrated defects.
Then state compared revisions, coverage, commands/results and remaining risks.
If no actionable findings were found, say so within the inspected scope.

Example request: `Use $review-data-pipeline on my uncommitted Bronze changes;
review only, prioritize data loss and replay regressions.`

Common mistakes: changing code during a review; inventing a base branch;
reporting intentional branch differences as bugs; treating unit success or a
previous summary as cloud acceptance. Cite the relevant contract instead.
