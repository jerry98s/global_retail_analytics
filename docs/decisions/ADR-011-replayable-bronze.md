# ADR-011: Preserve raw Kafka records; interpret them in Silver

Status: accepted, implemented on `local-testing-version` (2026-09-17).

## Decision

`bronze.clickstream_records` and `bronze.inventory_records` are append-only
Iceberg ledgers. Flink records binary key/value (including null values), stable
`source_system`, Kafka topic/partition/offset/timestamp, `ingested_at` and
`ingestion_date`. Partition by ingestion date; malformed event timestamps must
not prevent landing. No JSON parsing, validation, watermark, business filter,
or event-ID deduplication runs before the raw sink.

Capture reads committed Kafka records. First startup reads earliest retained
offsets; normal recovery restores checkpoints. A fresh submission may recapture
offsets: Bronze preserves those deliveries rather than hiding them. Set
`BRONZE_SOURCE_SYSTEM` to a stable cluster/topic incarnation, and change it after
destructive topic recreation so reused offsets cannot collide.

The Spark step pins each Bronze Iceberg snapshot and recomputes:

- `silver.{clickstream,inventory}_events`: one valid row per business event ID.
- `silver.{clickstream,inventory}_quarantine`: original bytes, metadata and reason.
- `silver.{clickstream,inventory}_duplicates`: excluded valid deliveries, with
  `transport_replay` or `business_duplicate` classification.

Validation is centralized in `spark/identity_resolution/event_contract.py`.
All records with invalid contracts are quarantined, including repeated invalid
deliveries. Among valid records, transport identity is
`(source_system, kafka_topic, kafka_partition, kafka_offset)`; earliest ingestion
wins repeated transport delivery. Business identity is `event_id` within each
stream, globally across its sources. IDs represent immutable events, not updates.
Conflicting valid business copies choose latest event time, then Kafka timestamp,
then lexicographically greatest transport tuple. This is deterministic, not an
assertion that the winner is a business correction. Audit retained losers before
changing policy. A versioned/update event contract requires a separate decision.

Every run checks `input = canonical + quarantine + duplicates`, and logs counts
with its Bronze snapshot ID. No event-time age or state TTL limits replay.
Full recomputation favors correctness over scan cost; incremental optimization
must preserve cross-batch deduplication and provide an explicit backfill path.

## Consumers and ownership

Promotion runs before identity resolution in the existing Spark/marketing step.
`--canonical-only` skips POS and GraphFrames for a standalone replay. dbt staging,
GE and local dashboard clickstream read canonical Silver. Plain-Parquet
`consumer_current/<name>/` exports write a new run directory and then
atomically replace the `manifest` Spectrum reads, so each table switches from
one complete run to the next; do not scan Iceberg `data/` directories because
old snapshot files can remain there.

Each table switches on its own: the set is NOT an atomic multi-table
publication. Serialize writers and pause readers during replay; resume only
after the full job succeeds. A failed run leaves the previous manifests in
place and must be rerun before consumption. Gold retains its existing WAP audits/publish.
Fidelity-mode DuckDB requires Spark exports; fixtures are explicit seed mode only.

The independent Flink `silver.inventory_hourly` writer remains the live fast path.
It still has a watermark/TTL and may differ from the durable canonical events.
Replaying canonical inventory does NOT repair its hourly rows or Gold inventory
automatically. Reconciliation/backfill needs an explicit maintenance window and
one-writer handoff; do not run a second writer against the live aggregate.

## Retention and migration

Raw logical rows are not expired by this change. Iceberg compaction/snapshot
expiration can remove superseded files, never rows referenced by the current
snapshot. Keep replay duration shorter than retained historical snapshot lifetime
(currently seven days); pause maintenance for long replays. S3 Bronze lifecycle
currently tiers storage without expiring current objects. Access/retention policies
must account for raw identifiers and malformed sensitive payloads as well.

Old `bronze.*_events` remain legacy filtered history, not a lossless source.
Records discarded by old code cannot be reconstructed unless retained upstream.
No legacy tables/data are dropped by this migration. See the
[replay runbook](../runbooks/bronze-replay.md).
