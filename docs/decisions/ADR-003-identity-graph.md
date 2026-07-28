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

## Implementation (2026 upgrade)

Identity resolution is **graph-based connected components** (bounded Union-Find
in SQL), not just pairwise equality joins.

```
int_identity_edges              typed co-occurrence + cross-source equality edges
int_identity_public_devices     client_ids linked to ≥ N distinct customers
int_identity_components         bounded N-hop transitive closure (default 6; var identity_component_hops)
int_identity_resolution         per-identifier confidence + method + public_device flag
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

### Why bounded closure, not true Union-Find

True Union-Find is iterative and not native to Redshift SQL. A configurable
N-hop closure (dbt var `identity_component_hops`, default **6**) covers the
practical identifier chains in this platform
(`device → customer → loyalty → sibling_customer → sibling_device`).
Override at run time:

```bash
dbt run --select int_identity_components --vars '{"identity_component_hops": 8}'
```

For deeper chains or >1M identifiers, migrate to a Python Union-Find job
(documented as a future optimization).

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

- Identity resolution runs before all Customer 360 dbt models
- `int_identity_components` is the most sensitive model in the DAG
- Any breach of identity_graph is a PDPA reportable incident
- Retention policy: identity_graph records expire 2 years after last_seen_at
  (TTL enforcement still pending — see identity-resolution.md out-of-scope)
- Public device threshold is a dbt variable — tune per environment
