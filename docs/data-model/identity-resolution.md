# Identity resolution and Customer 360 mapping

How raw identifiers become `customer_key` and flow into Customer 360.

We use **graph-based connected components** (Spark GraphFrames — ADR-010)
over one kind of evidence: an account verified to a loyalty card. Devices
never merge customers, and there is no probabilistic linking (ADR-003).

See [ADR-003](../decisions/ADR-003-identity-graph.md) for policy and
[ADR-010](../decisions/ADR-010-spark-graphframes-identity.md) for the engine
decision.

## Identifier sources

| Source | Field | Role |
|--------|-------|------|
| POS | `loyalty_id` | Graph node `loyalty:{value}` |
| Clickstream | `customer_id` (signed-in account, `CUST-*` in simulators) | Graph node `customer:{value}` |
| Clickstream login (v1.3+) | `properties.loyalty_id` + `properties.loyalty_link_verified` | Merge edge when verified |
| Clickstream | `client_id` + `session_instance_id` | Device-session association only, never a node |

Inventory events carry **no** customer identifier.

## Model chain

```
canonical clickstream (Iceberg) + POS loyalty IDs (Parquet)
  → Spark GraphFrames job (spark/identity_resolution/)     [ADR-010]
      → silver.identity_resolution        (full overwrite per run)
      → silver.identity_edges             (verified loyalty links that merged)
      → silver.loyalty_link_claims        (every login naming a card, verified or not)
      → silver.device_session_associations (signed-in intervals per device)
      → consumer_current/*                (run dir + atomic manifest for Spectrum)
  → int_identity_resolution         (thin dbt view over the silver source; adds identity_key)
  → int_identity_changes            (current vs the live published identity_graph)
  → marketing.identity_graph        (full rebuild each run)
int_identity_shared_devices         (audit: devices used by more than one account)
```

The business rules (blank-ID normalization, the merge edge, rep priority,
confidence/method, `customer_key` formula, associations) live in
`spark/identity_resolution/graph_logic.py` — the Spark job mirrors them and
the dbt seed fixtures are generated from them.

## Edges (`silver.identity_edges`)

| Edge type | From → To | Rule |
|-----------|-----------|------|
| `verified_loyalty_link` | `loyalty:{id}` ↔ `customer:{id}` | A v1.3 login by the account with `loyalty_link_verified: true` |

One edge per (loyalty, account) pair with `first_verified_at`,
`last_verified_at` and `evidence_event_id` (the earliest verifying login).
Unverified claims and events before envelope 1.3.0 create no edges; the
claims audit table keeps every claim.

## Connected components (Spark GraphFrames)

True connected components via GraphFrames `connectedComponents` — no hop
bound. Every known identifier is a vertex, so accounts and cards without a
verified link appear as singletons.

Component representative = priority-ordered minimum node:

1. `loyalty:*` (anchor preference — stablest identifier)
2. `customer:*` (authenticated account)

All nodes in the same component share the representative's `customer_key`
(deterministic MD5 hash of the rep node string — identical formula in
`graph_logic.py`, the Spark job, and dbt's `generate_customer_key` macro, so
keys are stable across engines).

## Resolution methods (`intermediate.int_identity_resolution`)

| Method | Identifier | When | Confidence |
|--------|------------|------|------------|
| `component_anchor` | loyalty_id | This loyalty is the component representative | 1.0 |
| `loyalty_member` | loyalty_id | In a component anchored by a different loyalty | 1.0 |
| `loyalty_match` | customer_id | Verified to a loyalty card | 1.0 |
| `customer_id_standalone` | customer_id | No verified card | 0.9 |

## Device-session associations (`silver.device_session_associations`)

Grain `(client_id, session_instance_id, customer_id)`: the interval an
account was signed in on a device within one session instance.

| Column | Meaning |
|---|---|
| `valid_from` / `valid_to` | First signed-in event; logout, else last signed-in event |
| `end_reason` | `logout` or `last_event` |
| `evidence` | `verified_login` (a login in the instance) or `signed_in_session` (remembered sign-in) |
| `is_ambiguous` | More than one account signed in within the instance |

`intermediate.int_identity_shared_devices` lists devices used by more than
one account, for audit. Neither feeds the graph.

## Identity changes (`intermediate.int_identity_changes`)

Full outer join of `int_identity_resolution` against the live published
`marketing.identity_graph` (`source('gold_marketing', 'identity_graph')`, so
it reads live, never the WAP pending clone). `change_type` is `added`,
`removed` or `rekeyed`, with both keys. Before the first publish every
identifier is `added`.

## Sessions and attribution (`intermediate.int_session_reconstruction`)

One row per session, rebuilt in full from canonical Silver each run.

| Events | Session key | Customer |
|---|---|---|
| Envelope 1.3+ | `session_instance_id` (producer-issued; new after 30 idle minutes, a logout or an account switch) | See below |
| Before 1.3 | `legacy:<client_id>:<producer session_id>`, `is_legacy = true` | Always anonymous |

`attribution_status` comes from `silver.device_session_associations` for the
instance:

| Accounts signed in during the instance | `attribution_status` | `customer_key` |
|---|---|---|
| Exactly one (login or remembered sign-in) | `attributed` | That account's key, for every event in the session, pre-login included |
| None | `anonymous` | NULL |
| More than one | `ambiguous` | NULL |

Anonymous and ambiguous sessions stay in `fact_customer_session` and in
`summary.sessions_daily_platform.session_count`, the conversion denominator.

Restatement:
- `fact_customer_session` delete+inserts every session whose
  `session_fingerprint` changed (a late event, or a rekeyed customer) and
  deletes sessions that no longer exist upstream. This relies on Silver
  holding the full clickstream history.
- `sessions_daily_platform` is rebuilt in full.
- `finance.fact_sales` reprocesses any day whose stored `customer_key` no
  longer matches the line's loyalty_id key.

## Consent

Consent needs an explicit statement for each purpose (marketing, analytics).
Nothing is inferred: loyalty membership and signing in are not consent.

- `intermediate.int_consent_events` holds one row per statement and purpose,
  from signed-in envelope 1.3+ events whose flag is a boolean with a
  `consent_source` (the Spark contract quarantines any other 1.3 consent
  flag). Pre-1.3 flags carry no source and are ignored.
- `intermediate.int_customer_consent` takes the latest statement per
  `customer_key` and purpose. Statements follow the account's current
  `customer_key`, so after a merge the latest statement across the merged
  accounts wins.
- `dim_customer` uses that statement; with none, the flag is `false`. An
  opt-out takes effect on the next run.

`marketing.customer_360_view` and `serving.customer_360_serving` filter `marketing_consent = true`.

## RFM and order reconciliation

- `int_rfm_scoring` uses confirmed POS transactions only (via `loyalty_id`).
  Scores run 1 to 5, where 5 is the most recent, most frequent or highest
  spend. Recency is measured from the latest non-voided transaction date in
  the data, so a replay scores the same.
- Online checkouts are observations, not revenue. `int_order_reconciliation`
  lists one row per `order_id` with its session and customer. POS has no
  `order_id` yet, so every order is `unconfirmed`; matching needs a POS
  contract change.

## Customer 360 output chain

```
bronze.clickstream_events + bronze.pos_transactions
  → Spark GraphFrames job → silver.identity_resolution (Iceberg)   [ADR-010]
                           → consumer_current/identity_resolution (Parquet)
  → int_identity_resolution (thin view over the silver source)
  → int_session_reconstruction → fact_customer_session
  → int_rfm_scoring (confirmed POS via loyalty_id)
  → int_consent_events → int_customer_consent
  → dim_customer
  → int_order_reconciliation (checkout observations, unconfirmed)
  → identity_graph
  → customer_360_view (customer × session)
  → serving.customer_360_serving (one row per customer, sessions rolled up)
```

## Out of scope (P2 / ADR)

- Probabilistic device graph and cross-device inference
- Email / PII ingest (`email_hashed` column reserved; no hash pipeline yet)
- Automated consent revocation — not implemented; `marketing_consent` is the gate on Customer 360
- identity_graph TTL enforcement (data freshness — old relationships should expire)
