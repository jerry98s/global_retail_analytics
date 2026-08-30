# Identity resolution and Customer 360 mapping

How raw identifiers become `customer_key` and flow into Customer 360.

We use **graph-based connected components** (Spark GraphFrames — ADR-010) plus a
**public-device threshold** to merge identifiers — deterministic only, no
probabilistic linking (per ADR-003).

See [ADR-003](../decisions/ADR-003-identity-graph.md) for policy and
[ADR-010](../decisions/ADR-010-spark-graphframes-identity.md) for the engine
decision.

## Identifier sources

| Source | Field | Node key in graph |
|--------|-------|-------------------|
| POS | `loyalty_id` | `loyalty:{value}` |
| Clickstream | `customer_id` (often `LOYAL-*` in simulators) | `customer:{value}` |
| Clickstream | `client_id` (device cookie) | `client:{value}` |

Inventory events carry **no** customer identifier.

## Model chain

```
bronze clickstream (Iceberg) + POS loyalty IDs (Parquet)
  → Spark GraphFrames job (spark/identity_resolution/)     [ADR-010]
      edge construction + public-device exclusion + connected components
      → silver.identity_resolution  (Iceberg; full overwrite per run)
      → silver.identity_edges       (audit copy of the graph)
      → consumer_current/*          (replace-only Parquet for Spectrum/DuckDB)
  → int_identity_resolution         (thin dbt view over the silver source; adds identity_key)
  → marketing.identity_graph        (filtered: public devices excluded)
```

`intermediate.int_identity_public_devices` remains as a dbt audit model over
staging; the Spark job is authoritative for edge exclusion.

The business rules (blank-ID normalization, edge types, threshold, rep
priority, confidence/method, `customer_key` formula) live in
`spark/identity_resolution/graph_logic.py` —
the Spark job mirrors them and the dbt seed fixture is generated from them.

## Edge types (`silver.identity_edges`)

| Edge type | From → To | Rule |
|-----------|-----------|------|
| `session_link` | `client:{id}` ↔ `customer:{id}` | Both present in the same clickstream event |
| `loyalty_value_match` | `loyalty:{id}` ↔ `customer:{id}` | POS loyalty_id value equals a clickstream customer_id |

Public devices (see below) are **excluded** from edges so they appear as
isolated singletons in the component computation.

## Connected components (Spark GraphFrames)

True connected components over the symmetric edge set via GraphFrames
`connectedComponents` — no hop bound (the pre-ADR-010 dbt implementation used
a bounded N-hop SQL closure, default 6, which silently failed to merge chains
deeper than N). Every known identifier is a vertex, so isolated nodes still
appear as singletons.

Component representative = priority-ordered minimum node:

1. `loyalty:*` (anchor preference — stablest identifier)
2. `customer:*` (authenticated clickstream ID)
3. `client:*` (device cookie — weakest)

All nodes in the same component share the representative's `customer_key`
(deterministic MD5 hash of the rep node string — identical formula in
`graph_logic.py`, the Spark job, and dbt's `generate_customer_key` macro, so
keys are stable across engines).

## Public devices (`intermediate.int_identity_public_devices`)

A `client_id` is flagged as a public device when it appears with
`>= var('identity_public_device_threshold', 10)` distinct `customer_id`s in
clickstream (internet cafés, shared family tablets, store iPads).

```bash
dbt run --select int_identity_public_devices --vars '{"identity_public_device_threshold": 15}'
```

Public devices:
- Get their **own** `customer_key` at confidence **0.3**
- Are labeled `resolution_method = 'public_device_excluded'`
- Are **filtered out** of `marketing.identity_graph` (so Customer 360 joins
  do not accidentally merge multiple people into one customer_key)
- Remain in `int_identity_resolution` for audit

## Resolution methods (`intermediate.int_identity_resolution`)

| Method | Identifier | When | Confidence |
|--------|------------|------|------------|
| `component_anchor` | loyalty_id | This loyalty is the component representative | 1.0 |
| `loyalty_member` | loyalty_id | In a component anchored by a different loyalty | 1.0 |
| `loyalty_match` | customer_id | Component representative is a loyalty_id | 1.0 |
| `customer_id_standalone` | customer_id | No loyalty in component | 0.9 |
| `session_linked` | client_id | Component contains a customer/loyalty | 0.85 |
| `device_only` | client_id | Singleton, no edges | 0.5 |
| `public_device_excluded` | client_id | Flagged public device | 0.3 |

## Consent (`intermediate.int_customer_consent`)

- **Marketing:** `true` if customer has `loyalty_id`, or explicit `properties.marketing_consent` on login events.
- **Analytics:** from `properties.analytics_consent` when present; defaults `true` for loyalty holders.

`marketing.customer_360_view` and `serving.customer_360_serving` filter `marketing_consent = true`.

## Customer 360 output chain

```
bronze.clickstream_events + bronze.pos_transactions
  → Spark GraphFrames job → silver.identity_resolution (Iceberg)   [ADR-010]
                           → consumer_current/identity_resolution (Parquet)
  → staging.* + int_identity_public_devices (audit)
  → int_identity_resolution (thin view over the silver source)
  → int_session_reconstruction → fact_customer_session
  → int_rfm_scoring (omnichannel: POS via loyalty + converted clickstream sessions)
  → dim_customer (+ int_customer_consent)
  → identity_graph (public devices excluded)
  → customer_360_view → serving.customer_360_serving
```

## Out of scope (P2 / ADR)

- Probabilistic device graph (linking client_ids without a shared authenticated session)
- Email / PII ingest (`email_hashed` column reserved; no hash pipeline yet)
- Automated consent revocation — manual runbook: [consent-revocation.md](../runbooks/consent-revocation.md)
- identity_graph TTL enforcement (data freshness — old relationships should expire)
