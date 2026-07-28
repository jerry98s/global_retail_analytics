# Runbook: marketing consent revocation (PDPA)

## Trigger

- Customer requests marketing opt-out via loyalty portal or support ticket.
- Legal/compliance flags a `customer_key` for PII removal.

## Current platform behaviour

- `dim_customer.marketing_consent` and `analytics_consent` drive Customer 360 visibility.
- `marketing.customer_360_view` and `serving.customer_360_serving` filter `marketing_consent = true`.
- `identity_graph.is_active` is always `true` today — **no automated deactivation job**.

## Manual remediation (until automated workflow exists)

1. Identify `customer_key` from loyalty_id or support ticket.
2. In Redshift (audit session logged):

```sql
-- 1. Revoke consent on dimension
UPDATE marketing.dim_customer
SET marketing_consent = false,
    analytics_consent = false,
    email_hashed = NULL
WHERE customer_key = <key>;

-- 2. Deactivate identity graph rows
UPDATE marketing.identity_graph
SET is_active = false
WHERE customer_key = <key>;

-- 3. Verify C360 no longer exposes the customer
SELECT COUNT(*) FROM marketing.customer_360_view WHERE customer_key = <key>;
-- expect 0
```

3. Log ticket ID, operator, and timestamp in your compliance audit system.
4. Do **not** delete fact rows (finance audit trail); C360 views exclude via consent.

## Future automation (P3)

- Consent event topic → Flink/dbt incremental update
- Scheduled job to deactivate `identity_graph` and null PII fields
- Deletion log table for PDPA audit

See [ADR-003](../decisions/ADR-003-identity-graph.md) and [identity-resolution.md](../data-model/identity-resolution.md).
