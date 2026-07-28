# Verification and benchmark evidence

This directory is the evidence index for portfolio claims. It separates what
has been reproduced from what is a configurable target or design assumption.

## Reproduced checks

Environment: Windows, Python 3.11, local project virtual environment.

| Date | Check | Result |
|---|---|---|
| 2026-07-08 | `python -m pytest tests/unit -q -p no:cacheprovider` | 211 passed, 2 skipped in 1.36s |
| 2026-07-08 | `python -m ruff check ingestion streaming scripts quality orchestration tests` | Passed |

Re-run these commands after any material change and update the table only from
captured output.

## End-to-end acceptance checklist

Before labelling the local platform “verified end to end,” capture evidence for:

- Kafka topics created and producers completing without delivery errors.
- All submitted Flink jobs in `RUNNING` state.
- At least two successful Flink checkpoints per streaming job.
- Iceberg Bronze and Silver tables containing rows.
- dbt local run and tests completing successfully.
- Dashboard loading clickstream and inventory data.
- One deliberately invalid event appearing in the correct DLQ.

## Throughput benchmark protocol

The clickstream producer's 10,000 events/second setting is a **target**, not a
measured platform result. Record a benchmark only after a repeatable run.

Suggested protocol:

1. Fix the machine, Docker resource limits, partition count, event payload, and
   test duration.
2. Warm the stack for two minutes.
3. Run at 1k, 3k, 5k, and 10k events/second for at least five minutes each.
4. Record produced, consumed, DLQ, and committed Iceberg row counts.
5. Record Kafka lag, Flink backpressure, checkpoint duration/failures, CPU,
   memory, and end-to-end p50/p95 latency.
6. Accept a rate only when no records are lost, lag returns to baseline, and
   checkpoints remain healthy.

| Requested rate | Sustained rate | Loss | p95 latency | Checkpoint health | Status |
|---:|---:|---:|---:|---|---|
| 1,000 eps | — | — | — | — | Not measured |
| 3,000 eps | — | — | — | — | Not measured |
| 5,000 eps | — | — | — | — | Not measured |
| 10,000 eps | — | — | — | — | Not measured |

## Visual-proof slots

Store final captures under `docs/evidence/screenshots/` using these names:

- `01-dashboard-overview.png` — populated Streamlit sales/inventory/customer view.
- `02-flink-checkpoints.png` — running jobs with successful checkpoints.
- `03-iceberg-query.png` — Bronze/Silver query output and row counts.
- `04-dbt-lineage.png` — dbt DAG for the identity or finance chain.
- `05-ci-green.png` — successful CI jobs for the reviewed commit.

Do not publish empty UI captures or screenshots containing credentials,
account IDs, private endpoints, customer data, or other secrets.
