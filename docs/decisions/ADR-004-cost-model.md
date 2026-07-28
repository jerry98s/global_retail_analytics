# ADR-004: Cost Model and Optimization

**Status:** Accepted (warehouse choice superseded by [ADR-005](./ADR-005-warehouse-redshift.md))  
**Date:** 2024-01-15  
**Author:** Data platform team  

---

> **Note (superseded in part):** The platform now runs on **Amazon Redshift
> Serverless** (see ADR-005), not Snowflake. The compute/storage line items below
> are retained as the original estimate for historical context. Redshift
> Serverless is billed per **RPU-hour** while active (auto-pauses when idle), with
> a monthly RPU-hour **usage limit** acting as the hard cap. The optimization
> levers and total order-of-magnitude still hold.

## Context

Platform cost should be estimated before design review. This ADR records an
illustrative planning scenario, not a measured AWS bill or a current price
quote. Recalculate it with the AWS Pricing Calculator before deployment.

## Bottom-Up Planning Scenario (100 GB/day ingestion, ap-southeast-1)

The storage figures model accumulated retained data after lifecycle policies;
they are not a claim that the platform has processed this volume. Compute and
MSK figures are planning assumptions. The companion notebook labels placeholder
inputs explicitly so estimated and observed costs are not confused.

### Storage

| Component | Calculation | Monthly Cost |
|---|---|---|
| S3 Standard (Bronze 90d) | 5TB × $0.025/GB | $125 |
| S3 Standard-IA (Bronze 90d–1yr) | 15TB × $0.0138/GB | $207 |
| S3 Glacier (Bronze 1yr+) | Growing, ~$0.005/GB | $98 |
| Redshift managed storage | 3.5TB × ~$24/TB | $100 |
| **Storage Total** | | **$530** |

### Compute

| Component | Naïve | Optimized | Saving |
|---|---|---|---|
| EMR / Flink | $1,244 | $330 (Spot + Savings Plan) | $914 |
| Redshift transform workload | $888 | $444 (right-sized base RPUs) | $444 |
| Redshift finance workload | $2,664 | $888 (scheduled windows) | $1,776 |
| Redshift dashboard workload | $2,664 | $888 (auto-pause when idle) | $1,776 |
| Redshift ad-hoc workload | $888 | $888 | $0 |
| **Compute Total** | **$8,348** | **$3,438** | **$4,910** |

### Other

| Component | Monthly Cost |
|---|---|
| MSK (Serverless) | $400 |
| Redis (ElastiCache r6g.large) | $80 |
| Data transfer | $200 |
| Monitoring/misc | $100 |
| **Other Total** | **$780** |

### Summary

| Scenario | Monthly | Annual |
|---|---|---|
| Naïve (no optimization) | $9,951 | $119,412 |
| Optimized (chosen) | $4,748 | $56,976 |
| **Saving** | **$5,203** | **$62,436** |

## The Five Optimization Levers (by impact)

1. **Redshift Serverless auto-pause** — idle workgroup stops billing RPUs, saves ~$2,200/month
2. **dbt incremental models** — avoid full-refresh, saves ~$655/month per heavy model
3. **EMR Spot instances** — 70% compute discount, saves ~$914/month
4. **S3 Intelligent-Tiering** — automatic tiering, saves ~48% on aging Bronze data
5. **Kafka retention tuning** — clickstream 7d → 24h, saves ~$39/month per topic

## Scale Evolution

| Scale | Monthly Cost | Primary Driver | Action Required |
|---|---|---|---|
| 100 GB/day (modeled) | $4,748 | Redshift base-RPU floor | Optimize auto-pause |
| 500GB | ~$8,200 | EMR scaling + storage growth | Spot instances + S3-IT |
| 2TB | ~$14,500 | Redshift sort/dist tuning needed | Workload isolation |
| 10TB | ~$35,000 | Storage tiering + Time Travel tax | Hot/warm/cold split |

The modeled cost grows about 7× for a 100× volume increase because of
incremental models, S3 tiering, and workload isolation. This is a planning
projection and must be validated with load tests and observed cloud billing.

## Enforcement

- Redshift Serverless usage limit: monthly RPU-hour cap, deactivate at 100% (Terraform `modules/redshift`)
- AWS Budget: $5,000/month hard ceiling, forecast alert at 100%
- AWS Cost Anomaly Detection: alert on >20% daily spend increase (min $200)
- All resources tagged: Project=retail-platform, Environment, Layer, Team
