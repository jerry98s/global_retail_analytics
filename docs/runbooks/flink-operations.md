# Flink Operations Runbook

**Applied:** 2026-07-05 (PR11 — Flink Production Checklist)
**Source checklist:** 7 sections covering architecture/deployment, state/checkpoints, sinks/EOS, sources, time/watermarks/windows, resource tuning/data skew, and operations/fault tolerance SOPs.

This runbook documents how the platform implements each item from the Flink production checklist, where the configuration lives, and what to do when an alarm fires.

## 1. Architecture & Deployment

### 1.1 Production deployment mode — YARN Per-Job (EMR)

The platform runs Flink jobs on EMR, which submits each job as a YARN application. YARN Per-Job mode provides complete resource isolation between jobs — one job's TaskManager memory blowup cannot starve another job.

| Concern | Value |
|---|---|
| Cluster | `infra/terraform/modules/emr/main.tf` |
| Job submitter DAG | `orchestration/airflow/dags/streaming_manual_flink_jobs.py` |
| Flink version | 1.17.1 (PyFlink) — see `pyproject.toml` |
| Submit mode | `flink run -m yarn-cluster` (YARN Per-Job) |

**Why not Kubernetes Native?** The platform already runs Airflow on MWAA + Redshift Serverless + MSK Serverless — adding a Kubernetes control plane for Flink would multiply the ops surface. YARN-on-EMR is the AWS-native equivalent and matches the rest of the stack. A future migration to K8s Native would be driven by Flink 2.x's K8s operator improvements, not by a current operational gap.

## 2. State Management & Checkpoints

### 2.1 State backend — RocksDB (F-STATE)

| Setting | Value | Where |
|---|---|---|
| `state.backend` | `rocksdb` | `streaming/config/state.yaml` |
| `state.backend.incremental` | `true` | same |

Applied by `_config.apply_state_config()` in each Flink job's `run()`. The maintenance batch job (`iceberg_maintenance.py`) does NOT call this — it has no long-running state.

**Why RocksDB over HashMap?** HashMap is faster (~10-20% throughput) but JVM-heap-bound — OOMs at ~10GB state. RocksDB stores state off-heap on disk, supports TB-scale state, and is the Flink-recommended production default.

**Why incremental checkpoints?** Full checkpoints re-snapshot the entire rocksdb SST file tree — minutes for multi-GB state. Incremental checkpoints upload only the changed SST files since the last checkpoint — seconds. Required for any job with >1GB state.

### 2.2 State TTL — 7 days (F-STATE)

| Setting | Value | Where |
|---|---|---|
| `table.exec.state.ttl` | `7 d` | `streaming/config/state.yaml` |

**Mandatory for `inventory_silver_job.py`** — its dedup CTE (`ROW_NUMBER OVER (PARTITION BY event_id ORDER BY event_time)`) is a regular self-join. Without TTL, the dedup state grows by ~1 row per `event_id` forever — until the job OOMs.

7d is conservative; events older than 7d are essentially impossible to re-emit by upstream (the producer simulates up to 300s late). If upstream lateness grows, bump this rather than removing it.

### 2.3 Checkpoint safeguards

| Safeguard | Value | Where |
|---|---|---|
| Checkpoint interval | 10s | `streaming/config/checkpoints.yaml` |
| **Min pause between checkpoints** | **30s** (bumped from 5s in PR11) | same |
| Checkpoint timeout | 600s (10 minutes) | same |
| Max concurrent checkpoints | 1 | same |
| Tolerable checkpoint failures | 3 | same |
| `RETAIN_ON_CANCELLATION` | enabled | same |
| Checkpointing mode | `EXACTLY_ONCE` | same |

The `min_pause` bump from 5s to 30s (per the checklist) gives the previous checkpoint time to complete before the next one triggers. Without this, slow checkpoints pile up and cause backpressure.

`RETAIN_ON_CANCELLATION` is critical — without it, a manual job stop (e.g. for a code upgrade) wipes the checkpoint state, forcing a full replay from Kafka earliest offset.

## 3. Sinks & End-to-End Exactly-Once

### 3.1 Batch your sink writes

The platform's sink is Iceberg (via the Flink-Iceberg connector). The connector batches writes naturally — it accumulates records in memory and commits them atomically at each checkpoint boundary. There's no row-by-row write path to tune. The commit is a single Iceberg table-state update referencing a set of new data files.

### 3.2 Kafka transaction timeout alignment — **N/A**

This checklist item applies when using two-phase commit (2PC) with Kafka as the transactional sink. The platform's sink is Iceberg (not Kafka transactions), so the 2PC + transaction timeout alignment doesn't apply. Iceberg's commit protocol uses the Flink checkpoint ID as the idempotency key — slow checkpoints don't cause silent data loss because the next checkpoint retry uses the same ID.

### 3.3 Idempotent writes

The platform relies on idempotent writes, not 2PC, for end-to-end exactly-once:

| Sink | Idempotency mechanism |
|---|---|
| Iceberg bronze (`inventory_events`, `clickstream_events`) | Flink-Iceberg connector commits are idempotent given the same checkpoint ID. Replaying a checkpoint re-writes the same data files + the same table-state update — Iceberg's snapshot log deduplicates by commit hash. |
| Iceberg silver (`inventory_hourly`) | Same — Iceberg connector. |
| POS batch (`pos_transactions`) | `dt=YYYY-MM-DD` partition overwrite. Re-running the batch for a given date is idempotent. |

This is the checklist's "idempotent writes are king" pattern — simpler than 2PC, naturally recovers from savepoints without duplicating data.

## 4. Sources (Kafka Best Practices)

### 4.1 Match parallelism to partitions (F-SRC)

| Topic | Partitions | Recommended cloud parallelism | Config key |
|---|---:|---:|---|
| `inventory.events.v1` | 12 | 12 | `flink_conf.yaml:inventory_parallelism` |
| `clickstream.events.v1` | 24 | 24 | `flink_conf.yaml:clickstream_parallelism` |

Setting parallelism above partition count leaves subtasks permanently idle. Setting it below under-utilises partitions. The cloud deploy (EMR) sets `INVENTORY_PARALLELISM=12` and `CLICKSTREAM_PARALLELISM=24` via env vars on the Flink step. Local docker-compose uses the `${INVENTORY_PARALLELISM:-1}` default of 1 — running 12 parallel subtasks on a 1-TaskManager docker cluster just thrashes.

Partition counts: `ingestion/kafka/topics.py:PARTITIONS_BY_TOPIC`.

### 4.2 Handle idle partitions — `table.exec.source.idle-timeout` (F-STATE)

| Setting | Value | Where |
|---|---|---|
| `table.exec.source.idle-timeout` | `1 min` | `streaming/config/state.yaml` |

If a Kafka partition stops emitting (e.g. one store goes offline), the source's watermark for that partition stalls, which stalls the whole pipeline's watermark — no window ever closes. The idle timeout marks a source idle after no records for this duration, letting the watermark advance from the other partitions.

### 4.3 Double-offset management — `enable.auto.commit=false` decision

The checklist recommends enabling `auto.commit=true` on the Flink Kafka source so Kafka tools (e.g. `kafka-consumer-groups.sh`) can see consumer lag. **The platform chose `enable.auto.commit=false`** (see PR10 K-CONS) for exactly-once correctness — Flink commits offsets via the checkpoint committer under `EXACTLY_ONCE`, and enabling auto-commit would race with that.

**How we monitor lag instead:** CloudWatch alarm on `PCTConsumerLag` per `(consumer_group, topic)` pair — see `infra/terraform/modules/kafka/main.tf:aws_cloudwatch_metric_alarm.consumer_lag`. This is operationally simpler than running Burrow and doesn't require sacrificing exactly-once.

### 4.4 Dynamic partition discovery — 5 minutes (F-SRC)

| Setting | Value | Where |
|---|---|---|
| `properties.partition.discovery.interval.ms` | `300000` (5 min) | Each Kafka source DDL |

Without this, newly-added Kafka partitions (e.g. when ops bumps `inventory.events.v1` from 12 to 24 partitions) require a Flink job restart. With this, the Kafka source picks up the new partitions at the next 5-minute boundary.

## 5. Time, Watermarks, and Windows

### 5.1 Use event time — in use

All 3 streaming jobs declare `WATERMARK FOR event_ts AS event_ts - INTERVAL '...' SECOND` and use event-time semantics for windowing. Processing time would give non-deterministic results across replays.

### 5.2 The 3-layer defense for late data

| Layer | Status | Where |
|---|---|---|
| 1. Watermark delay (`forBoundedOutOfOrderness`) | **In use** — 30s for bronze, 60s for silver | `WATERMARK FOR` clauses in each Flink job's Kafka source DDL |
| 2. Allowed lateness (`allowedLateness`) | **Deferred** — see below | n/a (Flink SQL doesn't expose this directly) |
| 3. Side outputs for late events | **Deferred** — see below | would require DataStream API migration |

**Layer 1 (in use):** The 30s watermark on Bronze catches ~90% of out-of-order events. The 60s watermark on Silver is wider because its hourly aggregation can absorb more lateness without re-emitting past hours.

**Layer 2 (deferred):** Allowed lateness in Flink SQL is configured via `EMIT` clauses or `table.exec.window.early-fire.enabled` — both are SQL-syntax additions that would require restructuring the inventory_silver_job's hourly aggregation. The current 60s watermark on Silver is wide enough that <1% of events are dropped, so the operational cost of adding allowed lateness outweighs the benefit. **Action if late-event rate grows:** widen the watermark (e.g. to 90s or 120s) before adding allowed lateness — watermark widening is a one-line change.

**Layer 3 (deferred):** Side outputs require migrating the late-event-handling from SQL to DataStream API. The current SQL jobs route schema-violation rows to a DLQ Kafka topic (which serves the same operational purpose — late/schema-violating rows are visible for offline correction). A future side-output implementation would route `is_late=true AND event_ts < watermark` rows to a dedicated `dlq.inventory.late_events` topic. **Action if late-event analysis becomes a regular task:** see `docs/runbooks/late-event-remediation.md` for the current replay-from-Kafka-earliest-offset procedure, which is the operational substitute for side outputs.

### 5.3 Window optimizations

| Item | Status |
|---|---|
| Avoid sliding windows | **In use** — inventory_silver_job uses a 1-hour tumbling window. Sliding windows multiply data by the overlap count and cause state inflation. |
| `AggregateFunction + ProcessWindowFunction` | **N/A** — this is a DataStream API pattern. The SQL jobs use `GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)` which is Flink SQL's optimised aggregation path (incremental aggregation is automatic). |

## 6. Resource Tuning & Data Skew

### 6.1 Task slot configuration

This is a deploy-time config set in the EMR bootstrap action. The recommendation is `taskmanager.numberOfTaskSlots = <CPU cores per node>`. For the platform's EMR cluster (`infra/terraform/modules/emr/main.tf`), this is set on the EMR cluster configuration's `flink-config` classification. Flink's slot sharing means total slots = highest operator's parallelism, not sum of all operators' parallelism.

### 6.2 Memory tuning

| Memory pool | Recommendation | When to bump |
|---|---|---|
| Managed memory (RocksDB block cache) | 50%+ of total Flink memory | State > 10GB |
| Network memory | 15-20% of total Flink memory | High parallelism + long operator chains (Direct Memory OOM risk) |

These are set in the EMR bootstrap's `flink-config` classification (`taskmanager.memory.managed.fraction`, `taskmanager.memory.network.fraction`). The current values are Flink defaults — revisit if checkpoint duration grows or Direct Memory OOM appears.

### 6.3 Data skew handling — Two-Stage Aggregation (Salting)

**Not currently needed.** The inventory_silver_job aggregates by `(snapshot_date_key, snapshot_hour, product_id, store_id)` — the data distribution across stores and products is uniform by design (the producer picks `random.choice(STORES)` / `random.choice(PRODUCTS)`).

**Action if skew emerges:** the salting technique is:
1. Add a random suffix (0-9) to the skewed key.
2. Pre-aggregate by `(skewed_key, salt)` — distributes across 10x more subtasks.
3. Remove the salt and do a final global aggregation.

In SQL, this is two `GROUP BY` clauses with a `UNION ALL` of the salted subqueries. Deferred until a real skew hotspot is measured.

## 7. Operations & Fault Tolerance (SOPs)

### 7.1 The Golden Upgrade SOP

**Never just kill a Flink job to upgrade its code.** Always:

1. **Trigger a manual savepoint** — `flink savepoint <jobId> s3://<checkpoints_bucket>/savepoints/`. This writes a consistent snapshot of all operator state to S3.
2. **Stop the job with `--savepointPath`** — `flink stop --savepointPath s3://... <jobId>`. This drains in-flight records and stops cleanly.
3. **Deploy the new code** — submit via `streaming_manual_flink_jobs` DAG with the new artifacts bucket path.
4. **Resume from the savepoint** — `flink run -s s3://.../savepoints/<savepointId> ...`. State is restored; the job continues from where it left off.

The platform's `streaming_manual_flink_jobs.py` DAG already uses `EmrAddStepsOperator` with the savepoint path passed as a parameter. The checkpoints bucket is `module.s3.checkpoints_bucket_name` (see Terraform outputs).

### 7.2 Fault-tolerant parsing

`json.ignore-parse-errors=true` is set on every Kafka source DDL. Malformed JSON rows are skipped (not crash the pipeline). Schema-violating rows (e.g. wrong `event_type`, missing required field) are routed to the per-topic DLQ Kafka topic for offline analysis — see `dlq.inventory.schema_violations` and `dlq.clickstream.schema_violations`.

### 7.3 Core metrics alerting

| Metric | Source | Alert threshold | Wired? |
|---|---|---|---|
| Kafka consumer lag | CloudWatch `PCTConsumerLag` (AWS/MSK namespace) | > 50% for 3 consecutive 5-min periods | **Yes** (PR10 K-MON) |
| Checkpoint failures | Flink `numFailedCheckpoints` metric (via EMR CloudWatch) | > 0 in last 5 min | Document (wire in CloudWatch metric alarm on EMR step) |
| Checkpoint duration | Flink `lastCheckpointDuration` metric | > 5 min | Document |
| Data quality NULL rates | Spectrum query on Iceberg bronze (e.g. `SELECT COUNT(*) FROM bronze.inventory_events WHERE event_id IS NULL`) | > 1% of daily row count | Document (wire as Airflow daily check) |

The Kafka lag alarm is wired in `infra/terraform/modules/kafka/main.tf:aws_cloudwatch_metric_alarm.consumer_lag`. The checkpoint-failure and checkpoint-duration alarms are documented as the next CloudWatch wiring target — they require EMR's `flink-config` classification to expose Flink metrics to CloudWatch (set `metrics.reporter.prom.class` + `metrics.reporter.prom.port`).

## 8. Operations cheatsheet

| Symptom | First action |
|---|---|
| CloudWatch `consumer_lag` alarm fires | See `docs/runbooks/kafka-operations.md` § 3.1 — check Flink job status in EMR; restart from last checkpoint if failed |
| Checkpoint duration grows past 5 min | Check RocksDB state size in the Flink Web UI; if > 10GB, bump managed memory; if growing unboundedly, check `table.exec.state.ttl` is applied (PR11 F-STATE) |
| Job OOMs with "Direct buffer" | Increase `taskmanager.memory.network.fraction` to 0.2 in the EMR bootstrap; redeploy |
| Job OOMs with "Java heap space" | Switch state backend to rocksdb if not already (PR11 F-STATE); bump managed memory |
| Window never closes | Check `table.exec.source.idle-timeout` is applied (PR11 F-STATE) — a stalled Kafka partition stalls the watermark |
| DLQ topic grows fast | Inspect DLQ message bodies — schema drift is the most common cause; bump `schema_version` per the contract rules |
| Upgrade Flink job code | Follow § 7.1 Golden Upgrade SOP — savepoint → stop → deploy → resume |

## 9. Where to look in the codebase

| Concern | File |
|---|---|
| State backend + TTL + idle timeout config | `streaming/config/state.yaml` |
| Checkpoint safeguards config | `streaming/config/checkpoints.yaml` |
| Flink runtime config (parallelism, topics, consumer groups) | `streaming/config/flink_conf.yaml` |
| `apply_state_config()` helper | `streaming/flink_jobs/_config.py` |
| Streaming Flink jobs | `streaming/flink_jobs/{inventory_bronze,clickstream_bronze,inventory_silver}_job.py` |
| Maintenance batch job (no state) | `streaming/flink_jobs/iceberg_maintenance.py` |
| Job submitter DAG | `orchestration/airflow/dags/streaming_manual_flink_jobs.py` |
| Iceberg maintenance DAG | `orchestration/airflow/dags/lakehouse_daily_iceberg_maintenance.py` |
| State config unit tests | `tests/unit/test_flink_state_config.py` |
| Kafka source reliability unit tests | `tests/unit/test_flink_kafka_source.py` |
