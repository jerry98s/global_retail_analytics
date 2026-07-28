# ADR-002: Batch vs. Stream per Pipeline

**Status:** Accepted  
**Date:** 2024-01-15  
**Author:** Data platform team  

---

## Context

Three pipelines with different latency requirements and different consumers.
Streaming adds cost and complexity. The decision must be made per-pipeline,
not as a platform-wide choice.

## Decision Matrix

| Pipeline | Approach | Latency | Justification |
|---|---|---|---|
| POS → Bronze | Batch (daily Parquet via Airflow) | T+4hr | Source exports daily snapshots — lands directly to S3 bronze |
| POS → Gold (fact_sales) | Batch (`warehouse_daily_batch_pipeline` dbt) | T+8hr | Finance runs at 9am. Sub-hour has zero business value |
| Inventory → Bronze | Stream (Flink) | <30s | Bronze raw for dbt + audit |
| Inventory → Silver | Micro-batch (hourly Flink job) | <60min | Hourly aggregates for silver.inventory_hourly |
| Inventory → Dashboard | Batch Gold (Redshift) | Daily/hourly | App Runner reads fact_inventory_snapshot |
| Clickstream → Bronze | Stream (Flink) | <30s | 10k/sec makes batch files impractical |
| Clickstream → Silver | *(not implemented)* | — | Bronze feeds dbt directly |
| Silver → Gold (Customer 360) | Batch (hourly dbt) | <75min | `marketing_hourly_customer_360_pipeline` |

## Implementation status (2026)

| Original decision | Status |
|---|---|
| Inventory → Redis (<5s) | **Deferred** — no ElastiCache/Flink sink; dashboard uses Gold |
| Clickstream silver (15min) | **Not built** — bronze → dbt is sufficient for hourly C360 |
| Customer 360 hourly dbt | **Implemented** — `marketing_hourly_customer_360_pipeline` DAG |
| POS batch (not Kafka) | **Implemented** — `generate_pos_parquet.py` → S3 bronze |

## Cost Comparison: Everything Streaming vs. Hybrid

| Architecture | Monthly Cost | Added Business Value |
|---|---|---|
| Hybrid (chosen) | $4,748 | Baseline |
| Everything streaming | $14,800 | Zero — finance/marketing don't act on sub-minute data |
| Delta: **$10,052/month wasted** | | |

## Principle

> Latency investment is justified only when a specific user action changes
> based on fresher data AND that action has a measurable business outcome.

Finance reports run at 9am regardless of when data arrives.
Marketing campaigns are planned days ahead.
Only the inventory dashboard has a real-time action tied to it.

## Consequences

- Three Flink jobs maintained (clickstream bronze, inventory bronze, inventory hourly)
- Inventory warehouse path: Iceberg bronze + silver → Spectrum → dbt Gold
- On-call runbooks must distinguish streaming vs. batch failures
- Redis path documented as future work when sub-5s SLA is required

## Related

- [ADR-007 — Inventory kappa conversion](ADR-007-inventory-kappa.md): `fact_inventory_snapshot`
  reads from **silver** rather than re-aggregating from bronze, so the inventory
  flow is a single-path kappa pipeline rather than split-brain lambda.
