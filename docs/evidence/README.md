# Verification and benchmark evidence

This directory is the evidence index for portfolio claims. It separates what
has been reproduced from what is a configurable target or design assumption.

## Reproduced checks

Environment: Windows, Python 3.11, local project virtual environment.

| Date | Check | Result |
|---|---|---|
| 2026-07-08 | `python -m pytest tests/unit -q -p no:cacheprovider` | 211 passed, 2 skipped in 1.36s |
| 2026-07-08 | `python -m ruff check ingestion streaming scripts quality orchestration tests` | Passed |
| 2026-08-01 | `python -m pytest tests/unit -q` | 264 passed in 3.15s |
| 2026-08-01 | Full local E2E (`run_local_stack.ps1 -Task all`, fresh volumes) | 90,000 clickstream events emitted → 90,000 rows / 90,000 unique `event_id` in Iceberg Bronze; 9,000 inventory events → 9,012 rows (injected retry-duplicates only); all 3 DLQs empty; 1,480 POS lines → 1,480 `fact_sales` |
| 2026-08-01 | `dbt run --target local` | 22/22 PASS |
| 2026-08-01 | `dbt test --target local` | 134/134 PASS |
| 2026-08-01 | Great Expectations `gold_layer_local` | 11/11 suites successful |
| 2026-08-01 | Metadata audit (`local_metadata.duckdb`) | `pipeline_run` SUCCESS for dbt + quality; all DQ checks `pass` |
| 2026-08-16 | `main`: `python -m pytest tests/unit -q` | 291 passed in 4.85s |
| 2026-08-16 | `main`: Ruff, Python bytecode compilation, Docker Compose config, dbt full compile | Passed |
| 2026-08-16 | `local-testing-version`: `python -m pytest tests/unit -q` | 306 passed in 3.21s (isolated branch snapshot) |
| 2026-08-16 | `local-testing-version`: Ruff, Python bytecode compilation, Docker Compose config, dbt full compile | Passed (isolated branch snapshot) |
| 2026-08-30 | `local-testing-version`: `python -m pytest tests/unit -q` | 324 passed in 3.34s |
| 2026-08-30 | `local-testing-version`: `python -m pytest tests/unit -q -m unit` | 301 passed, 23 deselected in 3.33s |
| 2026-08-30 | `local-testing-version`: Ruff, Python bytecode compilation, Docker Compose config, PowerShell parse, dbt full + WAP pending compiles, identity fixture drift | Passed |
| 2026-08-31 | `local-testing-version`: full local E2E (`run_local_stack.ps1 -Task all`, fully wiped Docker volumes + DuckDB + `.local/iceberg`) | `local_e2e` pipeline_run SUCCESS (~33 min): 40,990 clickstream + 9,001 inventory events → Iceberg Bronze (0 DLQ); 1,453 POS lines → 1,453 `finance.fact_sales` (exact 1:1); Spark GraphFrames identity job (Docker) → 19,633 rows in `silver.identity_resolution` (device_only 7,795 / session_linked 6,246 / customer_id_standalone 4,196 / component_anchor 954 / loyalty_match 442); dbt 18/18 models built via WAP pending → audit → publish; 122/122 dbt tests pass; 11/11 GE checks pass; 153/153 recorded DQ results `pass`; consent-gated `customer_360_view` / `customer_360_serving` = 3,431 rows; `marketing.dim_product` 500/500 current, 0 duplicate keys |
| 2026-08-31 | `local-testing-version`: `python -m pytest tests/unit -q` | 325 passed in 5.40s |

Re-run these commands after any material change and update the table only from
captured output.

These rows are not interchangeable: the 2026-08-01 entry is a full local E2E
run that predates ADR-010's Spark/GraphFrames cutover, while the 2026-08-31
entry is the first full local E2E that includes the Spark identity step end to
end (Kafka/Flink Bronze → Spark GraphFrames Silver → dbt/DuckDB Gold with WAP
→ GE). The remaining dated entries are offline regression checks on each
branch.

## End-to-end acceptance checklist

Status from the 2026-08-01 run:

- [x] Kafka topics created and producers completing without delivery errors.
- [x] All submitted Flink jobs in `RUNNING` state.
- [x] At least two successful Flink checkpoints per streaming job.
- [x] Iceberg Bronze and Silver tables containing rows.
- [x] dbt local run and tests completing successfully.
- [x] Dashboard loading clickstream and inventory data.
- [ ] One deliberately invalid event appearing in the correct DLQ — the
      2026-08-01 run produced no poison events, so DLQs were legitimately
      empty; DLQ routing is covered offline by unit tests instead.

## Cloud execution evidence

The AWS implementation is present on `main`, but this evidence ledger does not
yet contain a completed cloud run. Until it does, describe the project as
**cloud-deployable and locally verified**, not as production-deployed or
AWS-benchmarked. Follow [docs/runbooks/dev-cloud-test-run.md](../runbooks/dev-cloud-test-run.md)
for a lowest-cost cloud session and capture results here afterwards.

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

Captured 2026-08-01 from the local E2E run above, under
`docs/evidence/screenshots/`:

- `01-dashboard-overview.png` — Streamlit local mode: 90,000 clickstream
  events loaded from Iceberg Bronze Parquet.
- `02-flink-checkpoints.png` — `clickstream_bronze_job` RUNNING with completed
  checkpoint history.
- `03-iceberg-query.png` — DuckDB queries over Bronze/Silver Parquet: landed
  vs unique counts, event-type breakdown, silver aggregates.
- `04-dbt-lineage.png` — dbt docs DAG with `identity_graph` highlighted.
- `05-ci-green.png` — GitHub Actions runs green on both branches.

Do not publish empty UI captures or screenshots containing credentials,
account IDs, private endpoints, customer data, or other secrets.
