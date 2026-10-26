# Streaming Job Health Monitoring

**Audience:** Data platform on-call, SREs  
**Related:** [ADR-006](../decisions/ADR-006-flink-vs-spark.md), [ADR-007](../decisions/ADR-007-inventory-kappa.md), [Streaming manual Flink jobs DAG](../../orchestration/airflow/dags/streaming_manual_flink_jobs.py)

---

## Context

The platform runs three long-running Flink streaming jobs on EMR:
- `clickstream_bronze_job` — Kafka → `bronze.clickstream_records`
- `inventory_bronze_job` — Kafka → `bronze.inventory_records`
- `inventory_silver_job` — Kafka → `silver.inventory_hourly`

Airflow's `streaming_manual_flink_jobs` DAG records successful job **submission**, but submission success does not prove the job remains healthy. A Flink job can fail hours later due to checkpoint timeouts, consumer lag accumulation, or upstream Kafka issues.

## Monitoring Approach

**Do not build a parallel monitoring platform.** Query existing Flink and CloudWatch metrics via their native interfaces.

### 1. Table Freshness SLA (Already Implemented)

The `quality_hourly_ge_checkpoint` DAG measures freshness of Bronze and Silver tables hourly (`:45` schedule). It queries `MAX(ingested_at)` or `MAX(snapshot_date_key, snapshot_hour)` and compares against SLA thresholds in `metadata/catalog/layer_catalog.yml`.

**Coverage:**
- `bronze.clickstream_records` — 60-minute SLA
- `bronze.inventory_records` — 60-minute SLA  
- `silver.inventory_hourly` — 120-minute SLA (hourly grain with date key + hour)

**How it works:**
- Metadata observer (`scripts/common/metadata_observer.py`) queries the analytics database/file
- For hourly tables with `*_date_key` timestamp columns, it infers the companion `*_hour` column
- Computes lag as `measured_at - MAX(event_timestamp)` in minutes
- Writes `meta.table_freshness` with `sla_status` = `ok` | `warn` | `breach` | `unknown`
- Breaches trigger SNS alerts via `notify_task_failure`

**Action:** No changes required. This catches output staleness.

### 2. Consumer Lag (Kafka/MSK)

**Flink jobs use Kafka consumer groups with committed offsets:**
- Group IDs: `flink-{job_name}` (e.g., `flink-clickstream-bronze`)
- Offset commit strategy: `group-offsets` (Flink checkpoints commit offsets back to Kafka)

**Query consumer lag via AWS CLI:**

```bash
# List consumer groups
aws kafka list-consumer-groups --cluster-arn <msk-cluster-arn>

# Describe lag for a specific group
aws kafka describe-consumer-group \
  --cluster-arn <msk-cluster-arn> \
  --group flink-clickstream-bronze
```

**CloudWatch Metrics (MSK):**
- Namespace: `AWS/Kafka`
- Metrics:
  - `SumOffsetLag` — total lag across all partitions for a consumer group
  - `MaxOffsetLag` — lag of the most-behind partition
- Dimension: `Consumer Group`, `Topic`, `Cluster Name`

**Recommended Alert Threshold:**
- **Warning:** `SumOffsetLag > 10,000` messages for >15 minutes
- **Critical:** `SumOffsetLag > 100,000` messages for >5 minutes

### 3. Checkpoint Health (Flink)

**Flink UI (EMR master node):**
- URL: `http://<emr-master-public-dns>:8088/proxy/application_<yarn-app-id>/`
- Navigate to **Checkpoints** tab for each running job
- Key metrics:
  - **Last completed checkpoint time** — should be ≤ checkpoint interval (default: 5 minutes)
  - **Checkpoint duration** — should be < 50% of checkpoint interval
  - **Checkpoint failures** — should be 0 (or rare transient blips)

**CloudWatch Metrics (EMR/YARN):**
- Namespace: `AWS/ElasticMapReduce`
- Metrics:
  - `ContainerAllocated` — should match job parallelism (not declining)
  - `AppsRunning` — should stay at expected count (3 Flink apps)

**Flink REST API (programmatic):**

```bash
# Get running jobs
curl http://<flink-jobmanager>:8081/jobs

# Get checkpoint stats for a job
curl http://<flink-jobmanager>:8081/jobs/<job-id>/checkpoints
```

**Recommended Alert Threshold:**
- **Warning:** No successful checkpoint in last 15 minutes
- **Critical:** No successful checkpoint in last 30 minutes OR checkpoint duration > checkpoint interval

### 4. Dead Letter Queue (DLQ) Volume

Flink jobs write validation failures and duplicate events to side outputs:
- `bronze.clickstream_records` → quarantine records logged (not written to separate table)
- `bronze.inventory_records` → quarantine records logged

**Current implementation:** DLQ events are logged to Flink job logs, not persisted to a separate table.

**Action to improve (future enhancement):**
- Add Iceberg side-output tables: `bronze.clickstream_dlq`, `bronze.inventory_dlq`
- Track `COUNT(*)` growth rate in metadata freshness checks
- Alert on: DLQ row count growth >100 rows/hour (indicates upstream schema drift or producer bugs)

**Workaround until implemented:**
- Search EMR YARN logs for `ValidationException` or `Quarantine` keywords
- CloudWatch Logs Insights query:

```
fields @timestamp, @message
| filter @message like /ValidationException|Quarantine/
| stats count() by bin(5m)
```

### 5. Job Restart Detection

**Current behavior:** `streaming/bin/submit_flink_job.sh` skips submission if YARN already runs the job.

**Gap:** No alert when a job restarts unexpectedly (e.g., due to OOM, task manager failure).

**Action to improve:**
- Track `application_<id>` from last successful submission in Airflow XCom or metadata
- On next DAG trigger, compare current YARN app ID to last known
- If different and age <24h, alert: "Flink job {name} restarted unexpectedly"

**Workaround:**
- Query EMR cluster's application history:

```bash
yarn application -list -appStates RUNNING
yarn application -status application_<id>
```

- Check `startedTime` — if recent and not aligned with a deploy, investigate

## Recommended Monitoring Dashboard

**Use CloudWatch Dashboard with:**

1. **Freshness Breaches** (metric: `meta.table_freshness_breach_count`)
   - Alarm: any breach in last hour
2. **Consumer Lag** (MSK `SumOffsetLag`)
   - Per consumer group, last 4 hours
3. **Checkpoint Success Rate** (Flink REST API → custom CloudWatch metric via Lambda)
   - Per job, last 1 hour
4. **EMR Cluster Health** (`AppsRunning`, `ContainerAllocated`)
   - Should match expected Flink job count and parallelism

## Incident Response

### Symptom: Freshness SLA breach for Bronze or Silver table

**Root cause candidates:**
1. **Kafka producer stopped** → Check ingestion producer logs
2. **Flink job failed** → Check Flink UI for exceptions, EMR logs
3. **Consumer lag accumulated** → Check MSK consumer lag
4. **Checkpoint timeout** → Check Flink checkpoint stats

**Action:**
- Restart Flink job if crashed: `airflow dags trigger streaming_manual_flink_jobs`
- If lag is high but job is healthy, consider scaling parallelism (redeploy with higher `INVENTORY_PARALLELISM` / `CLICKSTREAM_PARALLELISM`)

### Symptom: Consumer lag growing but table freshness OK

**Root cause:** Output is batched/buffered; lag accumulating but not yet visible in table timestamps.

**Action:**
- Check Flink job throughput in UI (records/sec)
- If throughput dropped, check for backpressure or slow sinks
- Verify Iceberg commit rate (CloudWatch or S3 metadata directory timestamp)

### Symptom: No successful checkpoint in >30 minutes

**Root cause candidates:**
1. **State size too large** → Checkpoint duration exceeds timeout
2. **S3 throttling** → Checkpoint writes to `s3://<checkpoints>/flink/checkpoints/<name>/` are slow
3. **Task manager OOM** → Check EMR container logs

**Action:**
- Increase checkpoint timeout (`execution.checkpointing.timeout` in Flink job config)
- Reduce checkpoint frequency if state is large
- Scale task manager memory if OOM

## Metrics Summary

| Metric | Source | Threshold | Alert |
|--------|--------|-----------|-------|
| Table freshness lag | `meta.table_freshness` | 60 min (Bronze), 120 min (Silver) | Critical |
| Consumer lag (sum) | MSK CloudWatch | >100k messages for >5 min | Critical |
| Checkpoint age | Flink UI / REST API | >30 min | Critical |
| DLQ row count | (Future) Bronze DLQ tables | >100 rows/hour | Warning |
| Job restart | EMR YARN app history | Unexpected restart within 24h | Warning |

---

**Next steps:**
- Set up CloudWatch alarms for MSK consumer lag (P0)
- Add Flink checkpoint age to CloudWatch via Lambda poller (P1)
- Implement DLQ Iceberg tables (P2)
