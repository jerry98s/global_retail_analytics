# Runbook: DLQ Investigation

## Trigger

- Spike in `dlq.clickstream.schema_violations` or `dlq.clickstream.business_violations`.
- Producer deploy completed in last 24 hours.

Invalid clickstream rows are published by `clickstream_bronze_job.py` (Flink
StatementSet) to Kafka DLQ topics — **not** to a Spectrum / Iceberg table:

| Topic | When |
|---|---|
| `dlq.clickstream.schema_violations` | Envelope fails schema predicate (null required fields, bad enum/semver) |
| `dlq.clickstream.business_violations` | Envelope is schema-valid but checkout properties fail (`order_id` / `cart_value`) |

Each DLQ message includes `error_reason` and `rejected_at`.

## Known limitation — malformed JSON

`json.ignore-parse-errors = true` on the Kafka source means **bytes that are
not valid JSON never become Flink rows**, so they cannot be routed to DLQ from
this SQL job. Fixing that requires a DataStream raw-bytes source. Monitor
producer error rates and Kafka consumer lag instead for parse failures.

## Investigation Flow

1. Sample recent DLQ records by `error_reason`.
2. Classify failures: schema mismatch, enum drift, null required field, checkout business rule.
3. Validate current schema contract (`ingestion/schemas/clickstream_envelope.json` +
   `ingestion/schemas/checkout_properties.json`) against producer payload.
4. Identify affected producer version and client platform.

## Useful Checks

DLQ is Kafka-only. Consume with the Kafka CLI / console consumer (or MSK
tools), not Redshift Spectrum:

```bash
# Schema DLQ — last ~100 messages (local docker example)
docker compose -f infra/docker/compose/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic dlq.clickstream.schema_violations --from-beginning --max-messages 100
```

```bash
# Business DLQ
docker compose -f infra/docker/compose/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic dlq.clickstream.business_violations --from-beginning --max-messages 100
```

Aggregate `error_reason` offline (jq / notebook) from those samples, e.g.:

```text
missing_event_id
invalid_event_type
checkout_missing_order_id
checkout_invalid_order_id
checkout_missing_cart_value
checkout_negative_cart_value
```

## Remediation

- **Schema mismatch:** ship producer hotfix or add backward-compatible optional field.
- **Enum drift:** update contract only after ADR/data-contract review.
- **Checkout business rules:** fix producer `order_id` / `cart_value` against
  `ingestion/schemas/checkout_properties.json`.
- **Parse errors:** quarantine malformed payloads at the producer; this Flink
  job cannot DLQ them (see limitation above).

## Post-Incident

- Open incident summary with root cause and prevention action.
- Add an expectation/test to prevent recurrence.
