# Notebooks

This directory is **`.ipynb` only** — interactive analysis notebooks, one per
topic. No `.py` or `.ps1` files. Do-work automation lives in `scripts/`, and
CI test gates live in `tests/integration/`.

## Notebooks

| Notebook | Purpose | When to use |
|---|---|---|
| `01_data_walkthrough.ipynb` | End-to-end table walkthrough in pipeline order — Bronze (clickstream_events, inventory_events, pos_transactions) → Silver (inventory_hourly) → Gold dims/facts/identity → Summary → Serving (`customer_360_serving`). Reads from the local DuckDB catalog after `run_local_stack.ps1 -Task all`. | When you want to see every table from start to finish in one place. |
| `02_cost_model.ipynb` | Cost-driver model (EMR, S3, Redshift, MSK) with dev/prod scenarios | When estimating cloud run-rate; edit assumptions inline. |

## Removed notebooks

- `01_architecture_walkthrough.ipynb` — a documentation-style notebook (Mermaid
  diagram, PowerShell smoke-test commands, one file-existence check) with no
  analysis. Its content already lives in the root [`ARCHITECTURE.md`](../ARCHITECTURE.md)
  and [`docs/runbooks/local-data-queries.md`](../docs/runbooks/local-data-queries.md),
  so the standalone walkthrough was retired to avoid duplication.
- `02_data_model_exploration.ipynb` — a thin exploration notebook whose cells
  were a strict subset of the local-Iceberg canned queries. Consolidated into
  the end-to-end walkthrough notebook (`01_data_walkthrough.ipynb`).
- `03_rfm_analysis.ipynb` — a synthetic pandas RFM demo. The canonical RFM model
  is now the dbt model `intermediate.int_rfm_scoring` (consumed by
  `dim_customer`), so the standalone demo was redundant.
- `05_local_iceberg_queries.ipynb` — canned local-Iceberg queries (Bronze +
  Silver only). Folded into `01_data_walkthrough.ipynb`, which now covers all
  layers Bronze → Serving in one notebook.
- `06_cloud_platform_verification.ipynb` — post-deploy cloud smoke checks
  (EMR state, S3 bronze prefixes, Redshift row counts, dashboard HTTP). Folded
  into `scripts/cloud/deploy_platform.ps1 -Action verify` so cloud verification
  is a first-class do-work task instead of a notebook.

## See also

- [`scripts/README.md`](../scripts/README.md) — for the **do-work** automation
  scripts (Terraform wrapper, platform deploy + `-Action verify`, MSK producers,
  local stack start/stop).
- [`tests/integration/`](../tests/integration/) — for CI-guarded test gates
  (dbt idempotency, local identity-graph verification).
- [`docs/runbooks/local-data-queries.md`](../docs/runbooks/local-data-queries.md)
  — operator runbook with paste-able `docker compose exec -T flink-taskmanager
  python3 -c "..."` commands that do the same Bronze/Silver peeks without a
  Jupyter runtime.
