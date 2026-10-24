---
name: local-stack-loop
description: >-
  Runs the local Docker/Flink/DuckDB platform loop on branch
  local-testing-version via scripts/local/run_local_stack.ps1. Use when the
  user asks to start, reproduce, or demo the local stack, -Task all, or the
  README local test loop. Do not use it to verify a single model, test, or
  quality change.
---

# Local stack loop

Branch `local-testing-version` only. The wrapper owns the order. Do not run the steps by hand and do not reorder them.

## When

- The user wants the local platform running or a full local reproduction.
- A one-model dbt, GE, or unit change is not this loop. Use the narrow check for that layer.

## Run

From the repo root, after `uv sync --group dev`, with Docker Desktop up:

```powershell
.\scripts\local\run_local_stack.ps1 -Task all
```

`-DbtSource iceberg` is the default. `-Task all` then does: `up`, `topics`, `flink`, `simulate`, wait for Parquet and silver windows, `pos-parquet`, `spark`, dbt into `*_pending`, pending dbt tests, pending GE, WAP publish, live GE, unit pytest.

`-DbtSource seeds` skips POS Parquet and Spark and loads the identity CSV fixtures instead.

## Traps

- Flink jobs use `group-offsets` with `auto.offset.reset=earliest`: a new consumer group reads the topic from the start, a restart resumes from committed offsets. Simulating before Flink no longer drops events.
- Kafka from the host is `127.0.0.1:9092`. Flink UI is `http://localhost:8082`.
- The loop writes `.local/iceberg` and `transformation/dbt_project/local_retail.duckdb`. Do not point it at a checkout whose DuckDB file must be kept.
- `main` has no this wrapper's local compose path. Cloud `-Task all` is infra and streaming smoke, not this loop.
- Do not publish or destroy cloud resources from this skill.
