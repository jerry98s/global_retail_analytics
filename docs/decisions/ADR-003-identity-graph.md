# ADR-003: Customer Identity Resolution

**Status:** Accepted (upgraded 2026 to graph-based connected components)
**Date:** 2024-01-15
**Author:** Data platform team

---

## Context

Three sources emit customer identifiers inconsistently:
- POS: loyalty_id (known), card_token (pseudonymous), NULL (cash/anonymous)
- Clickstream: customer_id (authenticated), client_id (device cookie, anonymous)
- Inventory: no customer identifier

A Customer 360 that joins on loyalty_id alone covers only ~40% of customers.

## Identity States

| State | Identifiers | % of Customers | Included in C360 |
|---|---|---|---|
| Known | loyalty_id + customer_id | ~40% | ✅ Deterministic |
| Partially known | loyalty_id only (browses anonymously) | ~30% | ✅ Via device graph |
| Probabilistic | client_id only (never logged in) | ~20% | ⚠️ Pending legal review |
| Anonymous | No stable identifier | ~10% | ❌ Not includable |

## Decision

Build an `identity_graph` table that maps raw identifiers → `customer_key`
before any fact table joins. Include States 1 and 2 (deterministic only).
Exclude State 3 pending PDPA (Malaysia) legal review of probabilistic linking.

## Implementation (2026 upgrade; engine revisited by ADR-010)

Identity resolution is **graph-based connected components**, not just pairwise
equality joins. Since ADR-010 (2026-08-29) the graph runs in a Spark
GraphFrames batch job; dbt keeps a thin view over its Iceberg output.

```
spark/identity_resolution (GraphFrames connectedComponents)
  → silver.identity_resolution + silver.identity_edges   (Iceberg)
int_identity_public_devices     client_ids linked to ≥ N distinct customers (dbt audit model)
int_identity_resolution         thin dbt view over the silver source (adds identity_key)
marketing.identity_graph        public devices excluded
```

### Edge types

| Edge type | Rule |
|-----------|------|
| `session_link` | `client_id` and `customer_id` appear together in a clickstream event |
| `loyalty_value_match` | POS `loyalty_id` value equals a clickstream `customer_id` |

### Component representative

Priority-ordered minimum node in each connected component:

1. `loyalty:*` (anchor preference — stablest identifier)
2. `customer:*` (authenticated clickstream ID)
3. `client:*` (device cookie — weakest)

All nodes in the same component share the representative's `customer_key`.

### Public device threshold

A `client_id` linked to `>= identity_public_device_threshold` (default 10)
distinct `customer_id`s is flagged as a public device (internet cafés, shared
family tablets, store iPads). It is:

- Excluded from edges (so it does not merge multiple customers into one component)
- Given its own `customer_key` at confidence **0.3** with method
  `public_device_excluded`
- Filtered out of `marketing.identity_graph` (so C360 joins do not accidentally
  merge multiple people into one customer_key)
- Retained in `int_identity_resolution` for audit

### Why true connected components (post-ADR-010)

The original dbt implementation used a configurable N-hop SQL closure
(default **6**) because true Union-Find is not native to Redshift SQL. That
bound was a correctness ceiling — deeper chains silently failed to merge —
and each extra hop cost a combinatorial join. ADR-010 moved the graph to
Spark GraphFrames `connectedComponents` on the existing EMR cluster: no hop
bound, linear-ish scaling in edges. The business rules (rep priority,
confidence/method, customer_key formula) are unchanged and live in
`spark/identity_resolution/graph_logic.py`.

## Why an Identity Graph (Not a Simple Join)

A simple `JOIN ON loyalty_id` misses:
1. Anonymous sessions that later authenticate (linkable retroactively)
2. Multi-device customers (same person, two client_ids)
3. Sessions where customer_id is populated mid-session (pre-login events)

The identity graph resolves all three by maintaining every known
identifier → customer_key mapping with confidence scores. The graph-based
approach extends this to multi-hop transitive links
(device A → user_1 → phone X ← user_2 ← device B all merge into one component).

## PDPA Compliance Notes

- Raw email never stored in DW (hashed only: SHA-256)
- `marketing_consent` and `analytics_consent` stored on `dim_customer`
- Consent revocation triggers: removal from identity_graph,
  nullification of PII fields, deletion log for audit
- Probabilistic matching (State 3) requires explicit consent — deferred
- Public device exclusion prevents accidental cross-person merges (PDPA-safe)

## Consequences

- Identity resolution runs before all Customer 360 dbt models (Spark step
  first in `marketing_hourly_customer_360_pipeline`, ADR-010)
- The Spark GraphFrames job is the most sensitive step in the DAG
- Any breach of identity_graph is a PDPA reportable incident
- Retention policy: identity_graph records expire 2 years after last_seen_at
  (TTL enforcement still pending — see identity-resolution.md out-of-scope)
- Public device threshold is a dbt variable — tune per environment
