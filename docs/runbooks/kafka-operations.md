# Kafka Operations Runbook

**Applied:** 2026-07-05 (PR10 — Kafka Master Checklist)
**Source checklist:** Three-tier reliability defense + performance tuning + troubleshooting + advanced architecture (Kafka interview manual).

This runbook documents how the platform implements each item from the consolidated Kafka master checklist, where the configuration lives, and what to do when an alarm fires.

## 1. Reliability Configuration (Three-Tier Defense)

### 1.1 Producer side

| Property | Value | Where set |
|---|---|---|
| `acks` | `all` | `ingestion/kafka/msk_config.py:_PRODUCER_DEFAULTS` |
| `enable.idempotence` | `true` | same |
| `retries` | `2**31 - 1` (effectively forever) | same |
| `max.in.flight.requests.per.connection` | `5` (max safe with idempotence on) | same |
| `delivery.timeout.ms` | `120000` (hard ceiling so retries can't hang) | same |

All producers (`ingestion/kafka/producer_sim/*._producer.py`) call `build_producer_config()`, which applies these defaults. A producer that needs to override (e.g. disable idempotence for a local debug run) passes `**extra` — see `tests/unit/test_msk_config.py::TestProducerOverride`.

**Unit-test guard:** `tests/unit/test_msk_config.py::TestProducerReliabilityDefaults` fails if any of these defaults are removed.

### 1.2 Broker side

| Property | Value | Notes |
|---|---|---|
| `min.insync.replicas` | 2 (RF=3) | MSK Serverless default — AWS-managed, not user-tunable |
| `unclean.leader.election.enable` | `false` | MSK Serverless default — AWS-managed |
| `num.partitions` (per topic) | 6-24 | `ingestion/kafka/topics.py:PARTITIONS_BY_TOPIC` + `infra/terraform/modules/kafka/main.tf:topic_names` |
| `default.replication.factor` | 3 | MSK Serverless managed |
| `log.retention.hours` | MSK Serverless-managed | Per-topic retention override possible via MSK API (not yet wired) |

The previous orphaned `aws_msk_configuration` resource (which set `min.insync.replicas=2`, `log.retention.hours=24` etc.) was **removed** in PR10 because it was never attached to the MSK Serverless cluster (the `aws_msk_serverless_cluster` resource doesn't accept a configuration ARN). It was dead code that misled reviewers.

### 1.3 Consumer side

| Property | Value | Where set |
|---|---|---|
| `enable.auto.commit` | `false` (explicit) | All 3 Flink Kafka source DDLs (see below) |
| `isolation.level` | `read_committed` | same |
| `auto.offset.reset` | `earliest` (safety net for first startup) | same |
| Manual offset commit | via Flink checkpoint committer (EXACTLY_ONCE) | Each Flink job sets `execution.checkpointing.mode = EXACTLY_ONCE` |
| Idempotent downstream | Flink dedupes by `event_id` (see `inventory_bronze_job.py` dedup CTE); Silver's hourly aggregation is window-keyed | per job |

Flink Kafka source DDLs:
- `streaming/flink_jobs/inventory_bronze_job.py` — `inventory_events_kafka` source table
- `streaming/flink_jobs/clickstream_bronze_job.py` — `clickstream_events` source table
- `streaming/flink_jobs/inventory_silver_job.py` — `inventory_events` source table

**Unit-test guard:** `tests/unit/test_flink_kafka_source.py` lints each Kafka source block (paren-balanced scan) for the three properties above, and rejects `enable.auto.commit=true` overrides.

## 2. Performance Tuning

### 2.1 Producer batching

| Property | Value | Notes |
|---|---|---|
| `batch.size` | `131072` (128 KB) | In checklist range 64KB-256KB |
| `linger.ms` | `10` | In checklist range 5-50ms |
| `compression.type` | `lz4` | Best throughput/CPU ratio for JSON payloads |

**Override example:** a high-volume producer can pass `**{"batch.size": 262144}` to `build_producer_config()` to double the batch size.

**Unit-test guard:** `tests/unit/test_msk_config.py::TestProducerPerformanceDefaults`.

### 2.2 Broker threads / JVM heap / OS tuning — **N/A for MSK Serverless**

`num.network.threads`, `num.io.threads`, JVM heap (`-Xmx6g`), and `vm.dirty_ratio` are broker OS-level tunings. MSK Serverless manages these; the platform can't set them. The checklist's recommendation to keep JVM heap small (under 6GB) so the OS page cache can handle the workload is the rationale AWS uses for MSK Serverless's automatic configuration.

For a self-managed Kafka deployment (not this platform's path), these would live in the broker's `server.properties` + `kafka-server-start.sh` env vars.

### 2.3 Consumer pulling — **N/A for Flink**

`fetch.min.bytes` and `max.poll.records` are Kafka consumer poll-loop tunings. Flink's Kafka source doesn't use the consumer poll loop — it uses the Kafka consumer's `poll()` API under the hood but manages fetch sizing via the Flink runtime, not via these properties. Tuning Flink throughput is done via `parallelism` (in `streaming/config/flink_conf.yaml`) and TaskManager slots.

## 3. Troubleshooting & Operations

### 3.1 High consumer lag

**Detection:** CloudWatch alarm on `PCTConsumerLag` (AWS/MSK namespace) fires when a consumer group is >50% behind the max offset for >=3 consecutive 5-minute periods. Alarms are wired per `(consumer_group, topic)` pair in `infra/terraform/modules/kafka/main.tf:aws_cloudwatch_metric_alarm.consumer_lag`. SNS target: `aws_sns_topic.alerts` (platform stack).

**Action:**
1. Check the Flink job status in the EMR console — if the job is failed/restarting, restart it from the last successful checkpoint (see `docs/runbooks/upstream-incident-response.md`).
2. If the job is healthy but lag is growing, **increase Flink parallelism** in `streaming/config/flink_conf.yaml` (cloud: re-trigger `streaming_manual_flink_jobs` DAG with a higher parallelism env var). Match parallelism to partition count — e.g. `inventory.events.v1` has 12 partitions, so `inventory_parallelism: 4` → 6 is fine; going beyond 12 doesn't help.
3. If lag is due to data skew (a single store or product dominates the stream), the producer's default hash partitioner (`store_id:product_id` key) is fine for our data distribution. A future custom partitioner could be added to `inventory_producer.py` if skew emerges — but only after measuring per-partition throughput.

### 3.2 Frequent rebalancing — **N/A for Flink**

Flink manages its own consumer group membership; it doesn't use the poll-driven heartbeat protocol that triggers `session.timeout.ms` rebalances. `CooperativeSticky` assignor and `group.instance.id` (Static Membership) are Kafka client-side patterns for poll-based consumers — Flink's connector bypasses them. No action needed.

### 3.3 Frequent broker full GC — **N/A for MSK Serverless**

AWS manages broker JVM. If GC time grows, AWS scales the cluster.

### 3.4 Leader imbalance — **N/A for MSK Serverless**

AWS runs preferred leader election automatically.

### 3.5 Disks filling up

MSK Serverless manages log retention automatically. To override for a specific topic (e.g. shorter retention for a DLQ), use the AWS MSK API or `aws kafka update-configuration` — not yet wired into Terraform. If a DLQ topic grows too large, consider switching its retention to time-based 7 days.

## 4. Advanced Architecture

### 4.1 KRaft Mode — **In use (managed)**

MSK Serverless uses KRaft (Kafka 3.3+) internally — AWS replaced ZooKeeper on the broker side. The platform doesn't configure this; it's transparent.

### 4.2 Exactly-Once Semantics — **In use**

Flink EXACTLY_ONCE checkpoints + Kafka idempotent producer (`enable.idempotence=true` in producer defaults) + Kafka consumer `isolation.level=read_committed` (set on all Flink sources). This is the Flink-Kafka exactly-once stack — no Kafka transactions needed because our producers don't write transactionally across multiple partitions.

### 4.3 CDC (Kafka Connect + Debezium) — **Not applicable**

The POS source is a daily Parquet batch (`ingestion/batch/generate_pos_parquet.py`), not a transactional database. CDC would be applicable if the upstream changed to MySQL/PostgreSQL row-level changes. Deferred — not in scope for the current portfolio platform.

### 4.4 Disaster Recovery (MirrorMaker 2) — **Deferred**

Current deployment is single-region (`ap-southeast-1`). A multi-region active-active deployment would add:
- A second MSK Serverless cluster in a second region.
- MirrorMaker 2 (or MSK Replicator, the AWS-managed equivalent) to replicate topics.
- Producer-side `client.id` + `cluster` routing in `build_producer_config()`.

Deferred to a future ADR — not needed for the current portfolio scope.

### 4.5 Tiered Storage (Kafka 3.6+) — **Transparent on MSK Serverless**

MSK Serverless automatically tiers older log segments to S3-backed storage. No configuration needed.

### 4.6 Proactive Monitoring & Schema Management

**Consumer lag monitoring:** CloudWatch alarms on `PCTConsumerLag` (see section 3.1). This replaces the interview checklist's Burrow recommendation — for an MSK Serverless estate, CloudWatch native metrics are operationally simpler (no extra service to run).

**Schema Registry:** the project uses JSON Schemas in `ingestion/schemas/` as data contracts, version-bumped per the contract rules in `AGENTS.md`. Flink jobs validate against inline schemas (see `streaming/flink_jobs/inventory_bronze_job.py` JSON parsing + DLQ routing). A runtime Confluent Schema Registry would be a future enhancement if the team wants centralised schema enforcement — for the current scale, file-based contracts + DLQ routing is sufficient. **Action if adding Schema Registry:** wire the registry URL into `build_producer_config()` as `schema.registry.url` and switch `compression.type` to `snappy` (Avro's preferred codec).

## 5. Operations cheatsheet

| Symptom | First action |
|---|---|
| CloudWatch `consumer_lag` alarm fires | Check Flink job status in EMR; restart from last checkpoint if failed |
| Producer `delivery_failed` log spam | Check MSK cluster health (CloudWatch `ActiveControllerCount`); check IAM auth token expiry |
| Bronze table stops growing | Check Kafka topic last message time (`aws kafka list-topics`); check Flink job `numRecordsIn` metric |
| DLQ topic grows fast | Inspect DLQ message bodies — schema drift is the most common cause; bump `schema_version` per the contract rules |
| Consumer group offset stuck | Flink checkpoint committer is failing — check `checkpoints_bucket` S3 write IAM permissions on the EMR role |

## 6. Where to look in the codebase

| Concern | File |
|---|---|
| Producer config (reliability + performance) | `ingestion/kafka/msk_config.py` |
| Topic catalog + partition counts | `ingestion/kafka/topics.py` |
| Flink Kafka source DDLs | `streaming/flink_jobs/{inventory_bronze,clickstream_bronze,inventory_silver}_job.py` |
| Flink runtime config (parallelism, consumer groups) | `streaming/config/flink_conf.yaml` |
| MSK cluster + CloudWatch alarms | `infra/terraform/modules/kafka/main.tf` |
| Platform alert SNS topic | `infra/terraform/main.tf:aws_sns_topic.alerts` |
| Producer reliability unit tests | `tests/unit/test_msk_config.py` |
| Flink source reliability unit tests | `tests/unit/test_flink_kafka_source.py` |
