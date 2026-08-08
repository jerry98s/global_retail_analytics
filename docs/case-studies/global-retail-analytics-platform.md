# Case Study: Design a Global Retail Analytics Platform

This project designs and implements a production-style data platform for a
global retail business.

The platform supports:

- Daily POS sales analytics
- Real-time inventory event ingestion
- Clickstream behavior tracking
- Customer 360 and identity resolution
- Finance and marketing marts
- Store and product performance reporting
- Data quality governance
- Cloud deployment on AWS
- Local simulation with Docker, Flink, and DuckDB
- Production operations runbooks for Kafka, Flink, Iceberg, Airflow, and dbt

The central challenge is that retail data has different shapes, ownership
models, and latency requirements.

A single database cannot efficiently handle:

- High-volume clickstream events
- Real-time inventory changes
- Daily POS batch files
- Customer identity mapping
- Slowly changing product attributes
- Finance-grade reporting
- Hourly marketing refreshes
- Historical replay and audit requirements

We therefore model the platform as several connected systems.

---

## 1. Start with Business Requirements

Before choosing Kafka, Flink, Iceberg, Redshift, or dbt, identify what the
business needs.

### Store Operations Requirements

Store and inventory teams need to know:

- Current stock movement
- Late or duplicated inventory events
- Hourly inventory position
- Products with fast depletion
- Store-level operational exceptions

These users care about freshness and operational visibility.

### Finance Requirements

Finance wants to know:

- Daily net revenue
- Gross margin
- Units sold
- Sales by store
- Sales by product
- Voided transactions
- Reconciled daily facts

Finance cares more about correctness and reconciliation than sub-second
freshness.

### Marketing Requirements

Marketing wants to know:

- Customer sessions
- RFM segments
- Lifetime value
- Churn risk
- Marketing consent
- Customer 360 profiles
- Cross-channel identity resolution

Marketing needs customer-level models that combine POS loyalty data and
clickstream identifiers.

### Platform Requirements

The data platform must also support:

- Schema evolution
- Replay from raw data
- Dead-letter handling for invalid events
- Data quality checks
- Infrastructure-as-code deployment
- Local development without AWS credentials
- Clear documentation for interview and portfolio review

These workloads have different latency expectations.

| Requirement | Expected Latency |
|---|---:|
| Inventory event to Bronze | Seconds |
| Clickstream event to Bronze | Seconds |
| Inventory hourly snapshot | Minutes |
| Customer 360 refresh | Hourly |
| POS sales mart | Daily / T+8 hours |
| Executive reports | Daily |
| Data quality checks | Per pipeline run |
| Historical rebuilds | On demand |

That distinction determines the architecture.

---

## 2. Identify Sources, Entities, and Events

The project has three source domains.

### Source 1: POS Batch

POS transactions represent sales activity.

Examples:

- Transaction header
- Transaction line item
- Store
- Product
- Loyalty ID
- Quantity sold
- Gross revenue
- Net revenue
- Gross margin
- Void flag

POS is modeled as batch data because retail transactions are often reconciled
daily before finance reporting.

### Source 2: Inventory Stream

Inventory events represent stock movement.

Examples:

- Product received
- Product sold
- Product adjusted
- Stock count changed
- Late inventory event
- Duplicate inventory event

Inventory is modeled as streaming data because operations need faster feedback
than the daily finance cycle.

### Source 3: Clickstream Stream

Clickstream events represent digital customer behavior.

Examples:

- Page view
- Search
- Add to cart
- Checkout
- Login
- Product view
- Session activity
- Marketing consent signal

Clickstream is high-volume behavioral data. It is useful for Customer 360,
session reconstruction, RFM analysis, and marketing segmentation.

This distinction matters:

> Entity tables answer: "What is true now?"
>
> Event tables answer: "What happened over time?"

A product's current category is entity state.

A checkout event is history.

---

## 3. Operational vs Analytical Modeling

An operational retail system would normally store current business state in
transactional databases.

Examples:

- Current products
- Current store metadata
- Current customer account state
- Current inventory balance
- Active promotions
- Order status

Those systems prioritize:

- Strong consistency
- Fast point lookups
- Controlled updates
- Transaction integrity

The analytics platform has a different purpose.

It needs to preserve history, replay events, build aggregates, and serve many
consumers.

This project therefore separates:

- Operational source systems
- Kafka topics
- Bronze raw lake tables
- Silver cleaned or aggregated lake tables
- Gold dimensional marts
- Serving views and dashboards

The goal is not to make one table serve every use case.

The goal is to produce fit-for-purpose data products.

---

## 4. Why Not Store Everything in Redshift Directly?

It would be tempting to load POS, inventory, and clickstream directly into a
warehouse.

That becomes a problem at scale.

Clickstream and inventory streams create:

- High insert volume
- Duplicates from retries
- Late and out-of-order events
- Schema evolution
- Invalid payloads
- Reprocessing needs
- Different consumer patterns

If all raw events are written directly into Gold tables, the warehouse becomes
both an ingestion system and a reporting system.

That creates contention and makes replay difficult.

Instead, the platform uses a lakehouse pattern:

```text
Sources
   |
   v
Kafka + Batch Landing
   |
   v
Bronze Iceberg Tables
   |
   v
Silver / Intermediate Transformations
   |
   v
Gold Redshift Marts
   |
   v
Serving Views and Dashboards
```

Bronze preserves raw truth.

Gold serves business questions.

---

## 5. Event Design

Streaming events should contain enough information to interpret them later.

A simplified inventory event might look like this:

```json
{
  "event_id": "inv-984234",
  "event_time": "2026-07-04T09:15:32Z",
  "store_id": "ST-001",
  "product_id": "SKU-1001",
  "qty_delta": -3,
  "event_type": "sale",
  "scanner_id": "scanner-22",
  "schema_version": "1.0"
}
```

A simplified clickstream event might look like this:

```json
{
  "event_id": "clk-123",
  "event_type": "checkout",
  "event_time": "2026-07-04T10:01:05Z",
  "session_id": "session-88",
  "client_id": "device-cookie-abc",
  "customer_id": "LOYALTY-1001",
  "platform": "web",
  "app_version": "1.4.0",
  "properties": {
    "marketing_consent": true,
    "analytics_consent": true
  },
  "schema_version": "1.0"
}
```

Important fields include:

- `event_id` for technical deduplication
- `event_type` for routing and business interpretation
- `event_time` for event-time processing
- `schema_version` for compatibility management
- Business identifiers such as `store_id`, `product_id`, `customer_id`, and
  `client_id`
- Producer metadata for debugging and lineage

---

## 6. Kafka Topic Design

The project models Kafka topics by source domain.

```text
inventory.events.v1
clickstream.events.v1
pos.transactions.v1
dlq.events.v1
dlq.clickstream.schema_violations
dlq.clickstream.business_violations
dlq.inventory.schema_violations
```

Do not automatically create one topic for every event type.

A better principle is:

> Events with similar ownership, retention, security, throughput, and consumer
> patterns can share a topic.

### Partitioning

The partition key controls ordering.

For inventory, useful keys include:

- `store_id`
- `product_id`
- `store_id + product_id`

For clickstream, useful keys include:

- `client_id`
- `customer_id`
- `session_id`

The correct key depends on the consumer.

For example:

- Session reconstruction benefits from user or session ordering.
- Product-level aggregations benefit from product ordering.
- Fraud or bot detection may need device-level ordering.

No single partitioning strategy satisfies every downstream workload.

Kafka reliability is handled in configuration rather than left to producer
defaults.

The producer config uses:

- `acks=all`
- Idempotent producer writes
- Large retry budget with a delivery timeout
- Batching and `lz4` compression

Flink consumes with checkpoint-managed offsets. The jobs keep
`enable.auto.commit=false` so Kafka offsets do not race ahead of successful
Flink checkpoints. Consumer lag is monitored from MSK CloudWatch metrics
instead.

---

## 7. Data Contracts

The project uses JSON schemas under `ingestion/schemas/`.

A contract should describe:

- Event name
- Owner
- Business meaning
- Required fields
- Optional fields
- Data types
- Enum values
- Compatibility rules
- Deduplication key
- PII classification
- Retention target
- Delivery guarantee

Example contract summary:

```text
Event name: clickstream.events.v1
Owner: Marketing / Digital Analytics
Business meaning: Customer behavior from web or app sessions
Uniqueness key: event_id
Business identifiers: client_id, customer_id, session_id
Required fields: event_id, event_type, event_time, session_id, client_id
Compatibility: backward-compatible additions require minor schema version bump
PII policy: customer identifiers are pseudonymous; PII access requires consent
Delivery guarantee: at least once
```

Schema contracts are important because Bronze is intentionally raw, but it
should still be interpretable.

---

## 8. Delivery Semantics and Deduplication

Kafka pipelines are usually designed for at-least-once delivery.

That means duplicates can happen.

The platform handles this by designing downstream consumers to be idempotent.

Conceptually:

```sql
select *
from (
    select
        *,
        row_number() over (
            partition by event_id
            order by ingestion_time desc
        ) as row_num
    from bronze.inventory_events
)
where row_num = 1;
```

Event-level deduplication solves technical duplicates.

Business duplicates may require stronger rules.

For example:

```text
store_id + product_id + event_type + scanner_id + event_time window
```

Technical duplicates and business duplicates are different problems.

The project makes this explicit in the streaming and dbt layers.

For the streaming jobs, exactly-once is not a slogan. It is a set of
coordinated choices:

- Kafka producers are idempotent.
- Flink checkpoints run in `EXACTLY_ONCE` mode.
- Kafka source offsets are committed by Flink checkpoints.
- Iceberg commits are checkpoint-bound.
- Gold models remain idempotent through stable keys and incremental filters.

---

## 9. Lakehouse Architecture

The platform uses:

- Kafka for event ingestion
- Flink for streaming validation, deduplication, and Iceberg writes
- Apache Iceberg for Bronze and Silver tables
- S3 as durable object storage in AWS
- Redshift Spectrum to query Bronze from Redshift
- dbt Core for staging, intermediate, and Gold models
- Airflow for orchestration
- Great Expectations and pytest for quality checks

The layers are:

```text
Kafka / Batch Sources
        |
        v
     Bronze
        |
        v
     Silver
        |
        v
      Gold
        |
        v
    Serving
```

---

## 10. Bronze Model

Bronze contains raw, append-only data.

Implemented Bronze tables include:

```text
bronze.clickstream_events
bronze.inventory_events
bronze.pos_transactions
```

Bronze stores what the source sent, plus ingestion metadata where applicable.

Bronze is valuable for:

- Replay
- Debugging
- Audits
- Schema evolution
- Rebuilding downstream models
- Investigating bad data

For example, clickstream events are validated by Flink. Invalid records are not
silently discarded. They are routed to DLQ topics.

```text
clickstream.events.v1
        |
        v
Flink validation
        |
        +--> bronze.clickstream_events
        |
        +--> dlq.clickstream.schema_violations
```

Bronze answers:

> What exactly did the source send us?

---

## 11. Silver Model

Silver contains cleaned, deduplicated, reusable data.

In this project, Silver is intentionally smaller than Bronze and Gold.

The main Silver table is:

```text
silver.inventory_hourly
```

It is produced by Flink from inventory events.

Typical Silver logic includes:

- Event-time processing
- Watermarks
- Deduplication
- Hourly aggregation
- Late-event handling
- Schema enforcement

Clickstream currently has no separate Silver table. Instead:

```text
bronze.clickstream_events
        |
        v
Redshift Spectrum external table
        |
        v
dbt staging + intermediate models
```

That is a conscious design choice: Customer 360 transformations are expressed
in dbt because they are easier to govern, test, and explain in SQL.

---

## 12. Late and Out-of-Order Events

Inventory and clickstream events can arrive late because:

- Mobile devices buffer events
- Networks retry
- Producers resend messages
- Devices have clock drift
- Kafka consumers restart

Streaming jobs must distinguish:

- Event time: when the business action happened
- Processing time: when the pipeline received it

Flink uses watermarks and checkpointing to process event-time windows.

The platform uses event time, not processing time, for the streaming paths.
The Bronze jobs use a tighter watermark, while the Silver inventory job uses
a wider one because hourly aggregation can absorb more lateness.

The current production posture is:

- Watermark delay handles normal late arrival.
- Invalid or malformed records are routed visibly to DLQ topics.
- Extreme late-event correction is handled through replay from retained Kafka
  and Bronze data rather than a separate side-output stream.

The inventory hourly model is provisional in streaming form. Gold tables can be
rebuilt later from Bronze for authoritative reporting.

This is an important design pattern:

> Real-time outputs are useful quickly. Batch rebuilds are authoritative later.

---

## 13. Gold Dimensional Model

Gold is a Kimball-style dimensional model in Redshift.

The model uses conformed dimensions and separate fact tables for separate
business processes.

Do not force every retail event into one giant fact table.

Different facts have different grains.

### Fact 1: Sales

Grain:

> One row per transaction line item.

```text
finance.fact_sales
------------------
transaction_id
line_item_number
date_key
store_key
product_key
customer_key
quantity_sold
gross_revenue
net_revenue
gross_margin
is_voided
```

This fact answers:

- What sold?
- Where did it sell?
- Which product drove revenue?
- Which customers purchased?
- What was margin by store and product?

### Fact 2: Inventory Snapshot

Grain:

> One row per product, store, snapshot date, and snapshot hour.

```text
finance.fact_inventory_snapshot
--------------------------------
snapshot_date_key
snapshot_hour
product_key
store_key
quantity_on_hand
quantity_delta
late_event_count
```

This fact supports inventory operations and stock movement analysis.

### Fact 3: Customer Session

Grain:

> One row per customer session.

```text
marketing.fact_customer_session
--------------------------------
session_id
customer_key
session_date_key
session_start_time
session_end_time
session_duration_seconds
page_view_count
checkout_count
```

This fact supports Customer 360, engagement, and session analytics.

---

## 14. Dimensions

The project defines shared dimensions.

### `dim_date`

Calendar dimension.

```text
date_key
date_actual
day_of_week
month
quarter
year
is_weekend
```

### `dim_store`

Current store attributes.

```text
store_key
store_id
store_name
country
region
store_format
```

### `dim_product`

Product dimension with SCD Type 2 history.

```text
product_key
product_id
product_name
category
subcategory
brand
effective_from
effective_to
is_current
record_hash
```

Only `dim_product` uses SCD Type 2 in this project.

That means historical product attributes are preserved for reporting. If a
product changes category, old facts can still join to the correct historical
product version.

### `dim_customer`

Customer dimension for Customer 360.

```text
customer_key
loyalty_id
email_hashed
loyalty_tier
rfm_segment
churn_risk_score
total_lifetime_value
marketing_consent
analytics_consent
last_updated_at
```

Customer attributes are built from identity resolution, RFM scoring, and consent
signals.

---

## 15. Conformed Dimensions

The fact tables share dimensions such as:

- `date_key`
- `store_key`
- `product_key`
- `customer_key`

These are conformed dimensions.

They allow cross-process analysis.

For example:

> Compare sales, inventory movement, and customer sessions by product category.

Without conformed dimensions, finance, marketing, and operations would define
product, customer, and date differently.

That would make metrics incompatible.

---

## 16. Customer 360 and Identity Resolution

Customer 360 is the most important modeling problem in the project.

The platform receives different identifiers from different sources:

| Source | Identifier | Meaning |
|---|---|---|
| POS | `loyalty_id` | Stable known customer identifier |
| Clickstream | `customer_id` | Authenticated digital identifier |
| Clickstream | `client_id` | Device cookie or anonymous browser identifier |

The goal is to map raw identifiers to one stable `customer_key`.

The model chain is:

```text
stg_clickstream_events + stg_pos_transactions
        |
        v
int_identity_edges
        |
        v
int_identity_public_devices
        |
        v
int_identity_components
        |
        v
int_identity_resolution
        |
        v
marketing.identity_graph
        |
        v
marketing.customer_360_view
```

### Edge Types

The graph uses deterministic edges.

```text
session_link:
client:{client_id} <-> customer:{customer_id}

loyalty_value_match:
loyalty:{loyalty_id} <-> customer:{customer_id}
```

### Connected Components

The project implements bounded multi-hop connected components in SQL.

This approximates Union-Find with a 4-hop closure.

Representative selection is deterministic:

1. Prefer `loyalty:*`
2. Then `customer:*`
3. Then `client:*`

All identifiers in the same component share the representative's `customer_key`.

### Public Device Handling

Some `client_id` values represent shared devices:

- Store kiosks
- Family tablets
- Internet cafes
- Public demo devices

If a device is linked to too many distinct customers, it is dangerous to merge.

The project flags a public device when:

```text
distinct customer_id count for a client_id >= identity_public_device_threshold
```

Default threshold:

```text
10 customers
```

Public devices:

- Receive their own `customer_key`
- Get confidence `0.3`
- Use `resolution_method = public_device_excluded`
- Are excluded from `marketing.identity_graph`
- Remain auditable in `int_identity_resolution`

This prevents Customer 360 from accidentally merging multiple people into one
profile.

---

## 17. Consent and Privacy

Marketing data is sensitive.

The project enforces consent before customer PII or Customer 360 serving logic.

Consent comes from:

- Loyalty relationship
- Clickstream properties such as `marketing_consent`
- Analytics consent signals

The important rule is:

> Marketing use requires consent. Analytics use can be governed separately.

The project also documents a consent revocation runbook for manual handling.

This is not just a SQL detail. It is a governance requirement.

---

## 18. RFM and Customer Segmentation

The marketing model computes RFM signals:

- Recency
- Frequency
- Monetary value

The intermediate model produces:

```text
int_rfm_scoring
---------------
customer_key
recency_days
frequency_orders
monetary_value
r_score
f_score
m_score
rfm_code
rfm_segment
```

These feed `dim_customer` and Customer 360.

RFM is useful because it turns raw transactions into reusable marketing
features.

Examples:

- Champions
- Loyal customers
- At-risk customers
- General customers

---

## 19. Real-Time Processing

The streaming path uses Flink.

```text
Kafka inventory.events.v1
        |
        v
Flink inventory_bronze_job
        |
        v
bronze.inventory_events   (audit / replay only — no Gold reader)

Kafka inventory.events.v1
        |
        v
Flink inventory_silver_job   (single owner of hourly aggregation + dedup)
        |
        v
silver.inventory_hourly   (Spectrum external schema)
        |
        v
dbt fact_inventory_snapshot   (running-balance window + surrogate-key joins)
        |
        v
finance.fact_inventory_snapshot   (Gold, read by dashboard)
```

This is the **kappa** pattern for the inventory flow: one stream processor
(`inventory_silver_job`) produces the authoritative silver output, and the
Gold mart is a thin SQL transform over silver rather than a second aggregation
over bronze. See [ADR-007](../decisions/ADR-007-inventory-kappa.md).

Clickstream uses Flink for validation and Bronze ingestion:

```text
Kafka clickstream.events.v1
        |
        v
Flink clickstream_bronze_job
        |
        +--> bronze.clickstream_events
        |
        +--> dlq.clickstream.schema_violations
```

Flink is a good fit because the platform needs:

- Stateful processing
- Exactly-once checkpoints for selected streams
- Watermarks
- Event-time windows
- Durable Iceberg writes
- Continuous ingestion

The production Flink configuration is intentionally conservative:

- RocksDB state backend for disk-backed state
- Incremental checkpoints
- Externalized checkpoints retained on cancellation
- 30-second minimum pause between checkpoints
- 7-day SQL state TTL
- 1-minute source idle timeout so an inactive Kafka partition does not stall
  the whole watermark
- Dynamic Kafka partition discovery every five minutes

These settings live in `streaming/config/state.yaml`,
`streaming/config/checkpoints.yaml`, and the Kafka source DDLs. The design is
documented in [`docs/runbooks/flink-operations.md`](../runbooks/flink-operations.md).

---

## 20. Batch Orchestration

Airflow orchestrates batch and scheduled processing.

Main DAGs:

```text
warehouse_daily_batch_pipeline        # 00:15 UTC — POS -> dbt finance -> row-count reconcile -> GE
marketing_hourly_customer_360_pipeline  # hourly  — identity graph -> sessions -> dim_customer -> C360
streaming_manual_flink_jobs        # ad-hoc / scheduled — submit Flink jobs with idempotency guard
catalog_bihourly_product_scd2_refresh        # 0 */2 * * * — dim_product SCD2 catch-up (intra-day catalog changes)
quality_hourly_ge_checkpoint           # 45 * * * * — GE gold_layer_daily between Flink silver writes
lakehouse_daily_iceberg_maintenance    # 03:00 UTC — compact Iceberg files + expire snapshots
```

Each DAG carries a `doc_md` block in-file covering purpose, idempotency, and
recovery — see the individual DAG files under `orchestration/airflow/dags/`.

### Daily Batch Pipeline

The daily pipeline handles finance-oriented processing:

```text
POS batch
   |
   v
Generate Parquet
   |
   v
Bronze POS table
   |
   v
dbt finance models
   |
   v
dbt tests
   |
   v
row_count_reconciliation   # day-over-day Gold mart delta, warns >20%
   |
   v
Redshift ANALYZE
   |
   v
Great Expectations gold_layer_daily checkpoint
```

The `row_count_reconciliation` task (P2.5) compares Gold mart row counts
against an Airflow-Variable-stored baseline. A >20% delta (configurable
via `row_count_delta_threshold`) emits a warning log line per affected
mart; the DAG does not fail on warning. The baseline auto-updates on the
first clean run after an incident — see
[`docs/runbooks/upstream-incident-response.md`](../runbooks/upstream-incident-response.md)
for the incident-response procedure that consumes these warnings.

### Hourly Customer 360 Pipeline

The hourly pipeline refreshes marketing models:

```text
Clickstream + POS
      |
      v
Identity graph
      |
      v
Sessions + RFM + Consent
      |
      v
Customer 360
```

This separates finance cadence from marketing cadence.

### Streaming Flink Jobs DAG

Submits the three Flink streaming jobs to EMR. A `ShortCircuitOperator`
idempotency guard (P1.4) checks existing YARN applications by name before
re-submitting, so a re-run of the DAG after a partial deploy will not
launch duplicate Flink jobs.

The DAG is the operational entry point for restart and upgrade work. The Flink
runbook describes the savepoint-first upgrade sequence: savepoint, stop, deploy,
resume.

### SCD2 Product Refresh

`dim_product` is the only SCD2 dimension in the platform. Product catalog
changes (price, category, brand, status) can land intra-day from the
merchandising team, so this DAG refreshes it every two hours via
`dbt run --select dim_product --target prod`. Cost is ~30s every 2h —
negligible vs. the cost of stale product attributes on Gold marts.

### GE Checkpoint Run

Hourly at :45 (off-peak between Flink silver writes at :00 and the daily
batch at 02:00). Runs the same `gold_layer_daily` GE checkpoint that the
daily batch runs, so quality drift is caught within an hour rather than
at the next daily run.

### Iceberg Maintenance

Runs the Flink maintenance job that calls Iceberg procedures for compaction and
snapshot expiration. This prevents streaming writes from turning the lake into
a large collection of small files.

---

## 21. Serving Layer

Different consumers need different serving models.

| Consumer | Serving Model |
|---|---|
| Finance | Redshift Gold marts |
| Marketing | Customer 360 views |
| Store operations | Inventory snapshot views |
| Dashboard | App Runner / Streamlit reading serving tables |
| Data engineers | Bronze and Silver Iceberg tables |
| Local developers | DuckDB and local Iceberg |

The project does not force one canonical table to serve everyone.

Serving views exist because dashboard consumers should not need to understand
every staging and intermediate model.

---

## 22. Data Quality Framework

The project uses multiple layers of quality checks.

### Contract Checks

JSON schemas define event shape.

They validate:

- Required fields
- Data types
- Enum values
- Schema versions

### dbt Tests

dbt tests validate model-level assumptions:

- Not-null keys
- Unique keys (single and composite)
- Accepted values on `loyalty_tier`, `rfm_segment`, `identifier_type`,
  `resolution_method`, `event_type`, `is_late`, `is_estimated`,
  `marketing_consent`, `analytics_consent`, `converted`, `platform`
- Relationship integrity (FK -> PK)
- `not_null` with `where` predicates (e.g. `customer_key` is required
  when `loyalty_id` is present in `fact_sales`)
- SCD2 invariants on `dim_product`: `no_scd2_overlaps`,
  `one_current_per_natural_key`
- Identity graph method values

### Great Expectations

GE expectation suites cover every Gold mart plus the inventory bronze
streaming source. Suites live under
`quality/great_expectations/expectations/` and are wired into the
`gold_layer_daily` checkpoint:

- `fact_sales.json` — PK uniqueness, revenue consistency, no-fact-to-fact
- `fact_inventory_snapshot.json` — PK, quantity ranges, `is_estimated`
  domain, `snapshot_hour` 0–23, NOT NULL on measures
- `fact_customer_session.json` — session grain invariants, funnel depth
- `dim_customer.json` — PK, consent flags, RFM/loyalty domain, score
  ranges
- `dim_product.json` — SCD2 invariants, valid_to >= valid_from
- `customer_360_view.json` — derived columns, churn/LTV ranges, segment
  domains
- `dim_date.json` / `dim_store.json` — PK uniqueness on platform-loaded
  dimensions (dbt tests are disabled for DuckDB targets but the GE
  suites run against Redshift)
- `inventory_bronze.json` — last-24h sample, `event_type` domain,
  not-null on all fields

### Row-count reconciliation (Gold mart drift detection)

The Airflow `row_count_reconciliation` task (P2.5) compares Gold mart
row counts against a baseline stored in the Airflow Variable
`gold_row_counts_baseline`. A day-over-day delta above
`row_count_delta_threshold` (default 20%) emits a warning log line per
affected mart — the DAG does not fail on warning, by design, so a
legitimate backfill spike does not break the pipeline. The baseline
auto-updates on the first clean run after the incident.

This is the "within-N-percent" alert that GE's built-in
`expect_row_count_to_be_within_n_percent` cannot enforce against a
previous run (GE has no run-to-run state); the Airflow Variable +
plugin pattern is the platform's pragmatic answer.

### Pytest

Pytest covers behavior that is easier to express in Python:

- SCD Type 2 overlap rules
- Identity graph merge behavior
- Public device exclusion
- Producer invariants
- Kafka topic definitions
- DLQ SQL contract (regression test that the buggy
  `WHERE event_id IS NOT NULL AND NOT ...` pattern is not reintroduced
  in either Flink bronze job — `tests/unit/test_flink_config.py`)
- POS Parquet determinism — same `--date` produces byte-identical rows
  across re-runs (`tests/unit/test_generate_pos_parquet.py`)
- dbt incremental idempotency — runs `dbt run --full-refresh` then
  `dbt run` (incremental) against a local DuckDB and asserts row
  counts match for the identity chain
  (`tests/integration/test_dbt_idempotency.py`, marker
  `integration_duckdb`)
- Row-count reconciliation logic (17 unit tests against the pure
  functions in `orchestration/airflow/plugins/row_count_reconciliation.py`)
- Kafka producer and Flink source reliability defaults
- Flink state backend, checkpoint, source-idleness, and partition-discovery
  contracts
- DAG naming, alerting, idempotency, and documentation contracts

### Data Warehouse Checklist Audit

A formal audit against a 10-item Data Model Review Checklist and a
6-item Idempotent Design Checklist identified 21 gaps (6 P1, 9 P2, 6 P3)
across the 10 audited models. All 21 gaps were closed across 8 PRs.

The same audit index now also tracks the later production-hardening passes:

- Data lake layout and Iceberg maintenance
- Airflow DAG design and naming
- Kafka reliability, monitoring, and operations
- Flink state, checkpoint, source, and upgrade practices

The status index at
[`docs/runbooks/dw-checklist-audit.md`](../runbooks/dw-checklist-audit.md)
maps each closed gap to the artefact that enforces or documents the fix
(dbt test, GE suite, Flink job, Airflow task, runbook, or regression
test).

Quality is not a final step.

It is part of the data contract.

---

## 23. Handling Bad Records

Invalid records should not disappear.

Both Flink bronze jobs (`clickstream_bronze_job.py`,
`inventory_bronze_job.py`) route invalid records to DLQ topics:

```text
dlq.clickstream.schema_violations
dlq.clickstream.business_violations
dlq.inventory.schema_violations
```

The DLQ `WHERE` clause is `WHERE NOT (valid_predicate)` — i.e. *any*
row that fails validation is routed to the DLQ, including rows with a
null `event_id`. (An earlier version of the SQL had
`WHERE event_id IS NOT NULL AND NOT (valid_predicate)`, which silently
dropped null-event_id rows instead of routing them. P1.5 closed that
gap; the regression test
`tests/unit/test_flink_config.py::TestDlqSqlContract` guards against
re-introduction.)

Bad records may include:

- Invalid JSON
- Missing event ID
- Unsupported schema version
- Impossible timestamp
- Invalid identifier
- Missing required business field

This allows debugging and replay after the issue is fixed. See
[`docs/runbooks/dlq-investigation.md`](../runbooks/dlq-investigation.md)
for the investigation flow.

The principle is:

> Reject invalid data visibly, not silently.

---

## 24. Partitioning and File Layout

The lakehouse uses Iceberg tables on object storage.

A reasonable starting point for high-volume event tables is:

```text
Partition by event date
```

Why?

Most analytical queries filter by time.

Avoid partitioning directly by high-cardinality identifiers such as:

- `event_id`
- `customer_id`
- `client_id`
- `product_id`

Those can create excessive metadata and small files.

Better options include:

- Hidden partitioning by day
- Sorting by product or store
- Clustering if supported by the engine
- Scheduled compaction

The implementation follows that principle:

- `bronze.inventory_events` partitions by `event_date` (identity on CAST(event_time AS DATE))
- `bronze.clickstream_events` partitions by `event_date` (identity on CAST(event_time AS DATE))
- `silver.inventory_hourly` partitions by `snapshot_date_key`
- POS Spectrum data is registered with a daily `dt` partition

The partition contract is guarded by
`tests/unit/test_iceberg_partitions.py`.

---

## 25. Small Files and Maintenance

Streaming jobs can write many small files.

Small files create:

- Slow query planning
- Excess metadata
- Poor scan efficiency
- Higher object-store request cost

The project recognizes the need for maintenance tasks:

- Compaction
- Snapshot expiration
- Orphan file removal
- Manifest rewrite
- Statistics refresh

Conceptually:

```text
Streaming writes small files
          |
          v
Scheduled Iceberg maintenance
          |
          v
Optimized Parquet files
```

This is essential for a lakehouse design to stay healthy over time.

In this project, `lakehouse_daily_iceberg_maintenance` submits a Flink batch
job that calls Iceberg maintenance procedures. The daily batch pipeline also
runs Redshift `ANALYZE` after mart builds so Spectrum and Gold queries have
fresh statistics.

The operational details live in
[`docs/runbooks/iceberg-maintenance.md`](../runbooks/iceberg-maintenance.md).

---

## 26. Change Data and Slowly Changing Dimensions

Retail source systems change over time.

Products can change:

- Category
- Brand
- Subcategory
- Name
- Status

The project uses SCD Type 2 only for `dim_product`.

That means each product version has:

- `effective_from`
- `effective_to`
- `is_current`
- `record_hash`

The model enforces:

- No overlapping active product versions (dbt test `no_scd2_overlaps`)
- Exactly one current record per product (dbt test
  `one_current_per_natural_key`)
- Historical facts join to the correct product version via
  `p.is_current = true` filter in `fact_sales` and
  `fact_inventory_snapshot`

This is a deliberate scope choice.

Not every dimension needs SCD Type 2.

The `catalog_bihourly_product_scd2_refresh` Airflow DAG refreshes `dim_product` every two
hours so intra-day catalog changes (price, category, brand, status) land
on `fact_sales` SCD2 joins within hours rather than at the next daily
batch.

---

## 27. Local Simulation

A portfolio project should be runnable without a full AWS account.

This project supports local testing in two ways.

### Local Streaming

Docker Compose runs:

- Kafka
- Zookeeper
- Schema Registry
- Flink JobManager
- Flink TaskManager

Then:

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task flink
```

Flink writes to a local Iceberg warehouse under `/tmp/iceberg` inside the
container.

### Local dbt with DuckDB

DuckDB lets dbt models run without Redshift.

The local profile uses:

```text
local_retail.duckdb
```

Example:

```powershell
cd transformation/dbt_project
..\..\.venv\Scripts\dbt.exe deps
..\..\.venv\Scripts\dbt.exe seed --target local --profiles-dir .
..\..\.venv\Scripts\dbt.exe run --target local --profiles-dir . --select +identity_graph --full-refresh
..\..\.venv\Scripts\python.exe ..\..\tests\integration\verify_local_identity.py
```

The DuckDB simulation verifies:

- Loyalty match
- Session link
- Multi-hop identity component
- Public device exclusion
- Device-only singleton behavior

This makes the identity graph explainable and testable on a laptop.

### Local idempotency + determinism tests

Two CI-guarded tests run against the local DuckDB sim and the POS
Parquet simulator:

- `tests/integration/test_dbt_idempotency.py` — runs `dbt run
  --full-refresh` then `dbt run` (incremental) on the `+identity_graph`
  chain and asserts row counts match for all 5 identity-chain tables.
  Marker: `integration_duckdb`. Closes P2.1 from the DW checklist audit.
- `tests/unit/test_generate_pos_parquet.py` — asserts that two runs of
  `generate_pos_parquet.py` with the same `--date` produce identical
  `transaction_id` UUIDs, line counts, and measures. Closes P3.6.

Both are part of the `dbt-duckdb` CI job in `.github/workflows/ci.yml`.

---

## 28. Infrastructure and Deployment

Cloud deployment uses Terraform.

The project separates:

```text
bootstrap stack
platform stack
```

The wrapper script is:

```powershell
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action plan
.\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev -Action apply
```

The platform stack includes:

- S3 buckets
- MSK
- MSK consumer-lag CloudWatch alarms
- EMR / Flink
- Redshift Serverless
- MWAA
- IAM roles
- Dashboard resources

Runtime deployment uses:

```powershell
.\scripts\cloud\deploy_platform.ps1 -Env dev
```

This syncs orchestration assets and submits Flink jobs.

Operational runbooks explain the parts that should not be improvised during an
incident:

- Kafka lag and producer behavior:
  [`docs/runbooks/kafka-operations.md`](../runbooks/kafka-operations.md)
- Flink savepoint-first upgrades, state, and checkpoints:
  [`docs/runbooks/flink-operations.md`](../runbooks/flink-operations.md)
- Iceberg compaction and snapshot expiration:
  [`docs/runbooks/iceberg-maintenance.md`](../runbooks/iceberg-maintenance.md)

---

## 29. Cost and Trade-Offs

The architecture deliberately chooses different tools for different jobs.

| Choice | Why |
|---|---|
| Kafka | Decouple producers and consumers, with monitored consumer lag |
| Flink | Stateful streaming, checkpoints, watermarks, and event-time recovery |
| Iceberg | Replayable lakehouse tables with schema evolution and maintenance |
| Redshift | Governed Gold marts and BI serving |
| dbt | Version-controlled SQL transformations and tests |
| Airflow | Scheduled orchestration |
| DuckDB | Local simulation and fast laptop validation |

Trade-offs:

- Real-time inventory is fresher but more complex than batch.
- Gold marts are easier for analysts but require careful modeling.
- Iceberg adds maintenance overhead but enables replay and multi-engine access.
- RocksDB-backed Flink state is slower than heap state but avoids production
  OOM failure modes.
- Kafka offset monitoring is handled through MSK metrics rather than
  `enable.auto.commit=true`, preserving checkpoint-based recovery.
- Redshift is convenient for serving but should not be the raw ingestion system.
- DuckDB is excellent for local simulation but not a substitute for AWS scale
  testing.

---

## 30. End-to-End Architecture

```text
POS Batch Files
      |
      v
Bronze POS Iceberg
      |
      v
Redshift Spectrum
      |
      v
dbt Finance Marts
      |
      v
Serving / BI


Inventory Events
      |
      v
Kafka
      |
      v
Flink
      |
      +--> Bronze Inventory Iceberg
      |
      +--> Silver Inventory Hourly
      |
      v
dbt Inventory Fact
      |
      v
Dashboard


Clickstream Events
      |
      v
Kafka
      |
      v
Flink Validation
      |
      +--> Bronze Clickstream Iceberg
      |
      +--> DLQ
      |
      v
dbt Identity + Sessions + RFM
      |
      v
Customer 360
```

---

## 31. Key Trade-Offs to Explain in an Interview

### Operational vs Analytical Models

Operational systems optimize for current state and transactions.

Analytical systems optimize for history, replay, aggregation, and business
definitions.

### Events vs Current State

Events preserve what happened.

Dimensions and serving views describe what is useful now.

The platform needs both.

### Real-Time vs Batch

Inventory and clickstream need streaming ingestion.

Finance reporting can wait for daily reconciliation.

Customer 360 sits in the middle with hourly refreshes.

### Exactness vs Latency

Streaming outputs are fast and useful.

Gold marts are governed and authoritative.

The platform separates provisional operational metrics from reconciled business
metrics.

### One Model vs Many Data Products

A single universal table would be simpler to describe but worse to use.

This project creates separate products for finance, marketing, inventory, and
serving.

### Identity Resolution Risk

Merging identifiers improves Customer 360.

Incorrect merging can harm privacy and analytics.

That is why the graph uses deterministic edges, confidence scores, and public
device exclusion.

---

## 32. Interview Answer Framework

When asked:

> Design a global retail analytics platform.

Use this sequence:

1. Clarify business requirements and latency expectations.
2. Separate operational source systems from analytical workloads.
3. Identify entities and events.
4. Define data contracts and Kafka topic ownership.
5. Explain Bronze, Silver, Gold, and Serving layers.
6. Define fact-table grains.
7. Explain dimensions and SCD Type 2 scope.
8. Describe streaming ingestion, watermarks, and late events.
9. Explain Customer 360 and identity resolution.
10. Cover consent, privacy, and public-device handling.
11. Discuss data quality, DLQ, and reconciliation.
12. Explain operations: Kafka lag, Flink checkpoints, Iceberg maintenance,
    Airflow recovery, and local testing.
13. Explain deployment and cost trade-offs.

Do not start by listing tools.

Start with requirements and data semantics.

---

## 33. Practical Portfolio Walkthrough

This project demonstrates:

- Python event generators
- Kafka topic design
- Flink streaming jobs
- Iceberg Bronze and Silver tables
- Redshift external and Gold tables
- dbt staging, intermediate, and marts
- Kimball dimensional modeling
- SCD Type 2 product history
- Customer 360 identity graph
- Public-device thresholding
- RFM scoring
- Great Expectations suites for every Gold mart + bronze inventory
- dbt idempotency integration test (DuckDB sim)
- Gold row-count reconciliation Airflow task (P2.5)
- DLQ SQL regression tests (P1.5)
- POS Parquet determinism + `--seed` override (P3.6)
- Kafka reliability tests and Flink state/source contract tests
- pytest behavioral tests (203 unit tests + integration tests)
- Terraform AWS deployment
- Airflow orchestration (6 DAGs, each with `doc_md`)
- DuckDB local simulation
- DW checklist audit — 21 gaps identified and closed across 8 PRs
  (6 P1, 9 P2, 6 P3), with later data lake, DAG, Kafka, and Flink
  production checklists tracked in the same status index at
  [`docs/runbooks/dw-checklist-audit.md`](../runbooks/dw-checklist-audit.md)
- Runbooks for backfill verification, upstream incident response,
  late-event remediation, DLQ investigation, consent revocation,
  Iceberg maintenance, Kafka operations, Flink operations, DAG review,
  and local data queries

A reviewer can evaluate both:

- Architecture reasoning
- Working implementation

That is the point of the project.

---

## Core Lesson

The most important modeling decision in this system is not the database
technology.

It is defining precisely:

- What one row represents
- What one event means
- Which history must be preserved
- Which consumer each model serves
- How identities are resolved
- How correctness is measured
- How late, duplicated, invalid, changed, and deleted data is handled

That is the real work of a data modeler and data platform engineer.
