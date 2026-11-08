# ADR-003: Customer Identity Resolution

**Status:** Accepted (upgraded 2026 to graph-based connected components;
revised 2026-11: verified loyalty links are the only merge evidence)
**Date:** 2024-01-15
**Author:** Data platform team

---

## Context

Three sources emit customer identifiers inconsistently:
- POS: loyalty_id (known), card_token (pseudonymous), NULL (cash/anonymous)
- Clickstream: customer_id (authenticated account), client_id (device cookie),
  and from envelope 1.3.0 a `session_instance_id` plus verified loyalty links
  on login
- Inventory: no customer identifier

A Customer 360 that joins on loyalty_id alone covers only ~40% of customers.

## Identity States

| State | Identifiers | Included in C360 |
|---|---|---|
| Known | account verified to a loyalty card | ✅ One customer per verified component |
| Account only | customer_id, no verified card | ✅ Standalone customer |
| Device only | client_id, never signed in | ❌ Anonymous: `customer_key = NULL` in session facts |
| Anonymous | No stable identifier | ❌ Not includable |

## Decision

Build an `identity_graph` table that maps raw identifiers → `customer_key`
before any fact table joins. Merging is deterministic and evidence-based:

1. **Only a verified account-to-loyalty link merges.** A login whose
   properties carry `loyalty_id` with `loyalty_link_verified: true`
   (envelope 1.3.0+) creates one `verified_loyalty_link` edge. Unverified
   claims are recorded for audit and never merge.
2. **Devices never merge customers.** A shared device is not evidence that
   two accounts are one person. Device sign-ins are kept as time-bounded
   device-session associations, used for attribution only.
3. **No inference from value equality or legacy events.** A clickstream
   customer_id that happens to equal a loyalty id is not a link. Events
   before 1.3.0 carry no verification evidence, so they create no edges.
4. Probabilistic linking stays out of scope pending PDPA (Malaysia) review.

## Implementation (engine revisited by ADR-010)

Identity resolution is **graph-based connected components**. Since ADR-010
the graph runs in a Spark GraphFrames batch job; dbt keeps a thin view over
its Iceberg output.

```
spark/identity_resolution (GraphFrames connectedComponents)
  → silver.identity_resolution        one row per loyalty_id / customer_id
  → silver.identity_edges             verified loyalty links that merged
  → silver.loyalty_link_claims        every login naming a card (audit)
  → silver.device_session_associations signed-in intervals per device (attribution)
int_identity_resolution         thin dbt view over the silver source (adds identity_key)
int_identity_changes            current resolution vs the live published identity_graph
int_identity_shared_devices     devices used by more than one account (audit)
marketing.identity_graph        full rebuild each run
```

### Nodes and edges

| Node | Source |
|---|---|
| `loyalty:{id}` | POS loyalty ids and the loyalty side of verified links |
| `customer:{id}` | Clickstream accounts |

| Edge type | Rule |
|---|---|
| `verified_loyalty_link` | A v1.3 login by `customer_id` with `loyalty_id` and `loyalty_link_verified: true`. One edge per (loyalty, account); the evidence event is the earliest verifying login. |

Two accounts verified to the same card join one component through that card.

### Component representative

Priority-ordered minimum node in each connected component:

1. `loyalty:*` (anchor preference — stablest identifier)
2. `customer:*` (authenticated account)

All nodes in the same component share the representative's `customer_key`.

### Device-session associations

`silver.device_session_associations` has one row per
`(client_id, session_instance_id, customer_id)`: `valid_from`, `valid_to`
(logout or last signed-in event), `end_reason`, `evidence`
(`verified_login` or `signed_in_session`), `event_count`, and `is_ambiguous`
when more than one account signed in within the same instance. Session
attribution reads these intervals; the graph never does.

### Identity changes

`int_identity_changes` compares the current resolution with the live
published `marketing.identity_graph` and lists `added`, `removed` and
`rekeyed` identifiers. Before the first publish every identifier is
`added`. Facts keyed by a rekeyed or removed identifier must be restated or
held behind the publication gate.

### Why true connected components (post-ADR-010)

The original dbt implementation used a configurable N-hop SQL closure
(default **6**) because true Union-Find is not native to Redshift SQL. That
bound was a correctness ceiling — deeper chains silently failed to merge —
and each extra hop cost a combinatorial join. ADR-010 moved the graph to
Spark GraphFrames `connectedComponents` on the existing EMR cluster: no hop
bound, linear-ish scaling in edges. The rules (edge evidence, rep priority,
confidence/method, customer_key formula) live in
`spark/identity_resolution/graph_logic.py`.

## Why an Identity Graph (Not a Simple Join)

A simple `JOIN ON loyalty_id` misses:
1. Online accounts verified to a card (the account's sessions and the card's
   POS sales become one customer)
2. Several accounts verified to one card (one household member, two logins)
3. Changes over time: a newly verified link rekeys an account, and
   `int_identity_changes` shows exactly which identifiers moved

## PDPA Compliance Notes

- Raw email never stored in DW (hashed only: SHA-256)
- `marketing_consent` and `analytics_consent` stored on `dim_customer`, each
  from the customer's latest explicit statement for that purpose; with no
  statement, false (loyalty membership is not consent)
- Consent revocation triggers: removal from identity_graph,
  nullification of PII fields, deletion log for audit
- Probabilistic matching requires explicit consent — deferred
- Devices never merge customers, so a shared tablet cannot join two people

## Consequences

- Identity resolution runs before all Customer 360 dbt models (Spark step
  first in `marketing_hourly_customer_360_pipeline`, ADR-010)
- Anonymous sessions no longer receive a device-derived customer_key; they
  stay in session facts with `customer_key = NULL` and count in conversion
  denominators
- A session is attributed only when exactly one account signed in during its
  session instance; the whole session, pre-login events included, then
  belongs to that customer. Pre-1.3 sessions are never attributed
  (identity-resolution.md, "Sessions and attribution")
- Session facts restate on change: a session whose fingerprint changes after a
  late event or a rekey is rewritten, and POS days with stale customer keys
  are reprocessed
- Customers who only ever browsed under pre-1.3 events are not merged with
  their loyalty card until they log in with a verified link
- The Spark GraphFrames job is the most sensitive step in the DAG
- Any breach of identity_graph is a PDPA reportable incident
- Retention policy: identity_graph records expire 2 years after last_seen_at
  (TTL enforcement still pending — see identity-resolution.md out-of-scope)
