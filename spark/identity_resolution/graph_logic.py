"""Reference implementation of the identity-resolution rules (ADR-003).

This module is the single source of truth for the *rules* of identity
resolution: node naming, the one merge edge, component representative
priority, confidence scores, resolution methods, the deterministic
customer_key formula, loyalty-link claims and device-session associations.

Merge rule: only a login that the account verified to a loyalty card
(``loyalty_link_verified: true``, envelope 1.3.0+) creates an edge. Devices
never merge customers; their signed-in intervals are kept separately as
device-session associations for attribution. Events from before 1.3.0 carry
no verification evidence, so they never create edges.

Two engines execute these rules:

- ``identity_resolution_job.py`` — PySpark + GraphFrames on EMR (cloud) or
  local PySpark, writing ``silver.identity_resolution``,
  ``silver.identity_edges``, ``silver.loyalty_link_claims`` and
  ``silver.device_session_associations``.
- ``generate_fixture.py`` — regenerates the dbt seed fixtures used by the
  DuckDB CI chain, so the fixtures can never drift from the rules without CI
  failing.

customer_key parity: the formula below is byte-identical to the dbt macro
``generate_customer_key`` (md5, first 8 hex chars, base-16, mod 1e8, +1).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable, Optional

# Node ID prefixes (canonical graph node = "<prefix><identifier value>").
PREFIX_LOYALTY = "loyalty:"
PREFIX_CUSTOMER = "customer:"

IDENTIFIER_TYPE_BY_PREFIX = {
    PREFIX_LOYALTY: "loyalty_id",
    PREFIX_CUSTOMER: "customer_id",
}

EDGE_VERIFIED_LOYALTY_LINK = "verified_loyalty_link"
EDGE_TYPES = (EDGE_VERIFIED_LOYALTY_LINK,)

METHOD_COMPONENT_ANCHOR = "component_anchor"
METHOD_LOYALTY_MEMBER = "loyalty_member"
METHOD_LOYALTY_MATCH = "loyalty_match"
METHOD_CUSTOMER_STANDALONE = "customer_id_standalone"
RESOLUTION_METHODS = (
    METHOD_COMPONENT_ANCHOR,
    METHOD_LOYALTY_MEMBER,
    METHOD_LOYALTY_MATCH,
    METHOD_CUSTOMER_STANDALONE,
)

CONFIDENCE_ANCHOR = 1.0
CONFIDENCE_LOYALTY_MATCH = 1.0
CONFIDENCE_CUSTOMER_STANDALONE = 0.9

EVIDENCE_VERIFIED_LOGIN = "verified_login"
EVIDENCE_SIGNED_IN_SESSION = "signed_in_session"
END_LOGOUT = "logout"
END_LAST_EVENT = "last_event"

CUSTOMER_KEY_MODULUS = 100_000_000


def normalize_identifier(value: object) -> Optional[str]:
    """Return a trimmed, non-empty identifier or ``None``.

    Empty strings are permitted by older clickstream envelopes and must not
    become shared graph nodes such as ``customer:``.  The Spark implementation
    applies the same rule before constructing edges and vertices.
    """
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def customer_key(canonical_node: str) -> int:
    """Deterministic key in [1, 1e8) — identical to dbt generate_customer_key."""
    digest_prefix = hashlib.md5(canonical_node.encode("utf-8")).hexdigest()[:8]
    return int(digest_prefix, 16) % CUSTOMER_KEY_MODULUS + 1


def split_node(node: str) -> tuple[str, str]:
    """'loyalty:L1001' -> ('loyalty_id', 'L1001')."""
    prefix, _, value = node.partition(":")
    return IDENTIFIER_TYPE_BY_PREFIX[prefix + ":"], value


def node_sort_key(node: str) -> str:
    """Component representative priority: loyalty < customer."""
    if node.startswith(PREFIX_LOYALTY):
        return "1:" + node
    if node.startswith(PREFIX_CUSTOMER):
        return "2:" + node
    return "9:" + node


@dataclass(frozen=True)
class LoyaltyClaim:
    """A v1.3 login that named a loyalty card, verified or not."""

    event_id: str
    event_time: str
    client_id: str
    session_instance_id: str
    customer_id: str
    loyalty_id: str
    is_verified: bool


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    edge_type: str
    first_verified_at: str
    last_verified_at: str
    evidence_event_id: str


@dataclass(frozen=True)
class ResolutionRow:
    identifier_type: str
    identifier_value: str
    customer_key: int
    confidence_score: float
    resolution_method: str
    component_rep_node: str
    component_rep_type: str


@dataclass(frozen=True)
class DeviceSessionAssociation:
    """One account's signed-in interval on one device in one session instance."""

    client_id: str
    session_instance_id: str
    customer_id: str
    valid_from: str
    valid_to: str
    end_reason: str
    evidence: str
    event_count: int
    is_ambiguous: bool


def _properties(row: dict) -> dict:
    value = row.get("properties")
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _signed_in_v13(row: dict) -> Optional[tuple[str, str, str]]:
    """(client_id, session_instance_id, customer_id) for a signed-in v1.3 event."""
    instance = normalize_identifier(row.get("session_instance_id"))
    client_id = normalize_identifier(row.get("client_id"))
    customer_id = normalize_identifier(row.get("customer_id"))
    if instance and client_id and customer_id:
        return client_id, instance, customer_id
    return None


def loyalty_claims(clickstream_rows: Iterable[dict]) -> list[LoyaltyClaim]:
    """Logins that name a loyalty card. Only v1.3 events carry a session instance."""
    claims = []
    for row in clickstream_rows:
        signed_in = _signed_in_v13(row)
        if row.get("event_type") != "login" or signed_in is None:
            continue
        properties = _properties(row)
        loyalty_id = normalize_identifier(properties.get("loyalty_id"))
        if loyalty_id is None:
            continue
        client_id, instance, customer_id = signed_in
        claims.append(
            LoyaltyClaim(
                event_id=str(row["event_id"]),
                event_time=str(row["event_time"]),
                client_id=client_id,
                session_instance_id=instance,
                customer_id=customer_id,
                loyalty_id=loyalty_id,
                is_verified=properties.get("loyalty_link_verified") is True,
            )
        )
    return sorted(claims, key=lambda c: (c.event_time, c.event_id))


def build_edges(claims: Iterable[LoyaltyClaim]) -> list[Edge]:
    """One verified_loyalty_link edge per (loyalty, account) pair.

    The evidence event is the earliest verifying login (event_time, event_id).
    """
    by_pair: dict[tuple[str, str], list[LoyaltyClaim]] = {}
    for claim in claims:
        if claim.is_verified:
            by_pair.setdefault((claim.loyalty_id, claim.customer_id), []).append(claim)
    edges = []
    for (loyalty_id, customer_id), found in sorted(by_pair.items()):
        ordered = sorted(found, key=lambda c: (c.event_time, c.event_id))
        edges.append(
            Edge(
                src=PREFIX_LOYALTY + loyalty_id,
                dst=PREFIX_CUSTOMER + customer_id,
                edge_type=EDGE_VERIFIED_LOYALTY_LINK,
                first_verified_at=ordered[0].event_time,
                last_verified_at=ordered[-1].event_time,
                evidence_event_id=ordered[0].event_id,
            )
        )
    return edges


class _UnionFind:
    def __init__(self, nodes: Iterable[str]) -> None:
        self._parent = {node: node for node in nodes}

    def find(self, node: str) -> str:
        root = node
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[node] != root:
            self._parent[node], node = root, self._parent[node]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[max(ra, rb)] = min(ra, rb)


def assign_resolution(nodes: Iterable[str], edges: Iterable[Edge]) -> list[ResolutionRow]:
    """Connected components + per-identifier confidence/method assignment."""
    node_list = sorted(set(nodes))
    uf = _UnionFind(node_list)
    for edge in edges:
        uf.union(edge.src, edge.dst)

    members_by_root: dict[str, list[str]] = {}
    for node in node_list:
        members_by_root.setdefault(uf.find(node), []).append(node)

    rep_by_node: dict[str, str] = {}
    for members in members_by_root.values():
        rep = min(members, key=node_sort_key)
        for member in members:
            rep_by_node[member] = rep

    rows: list[ResolutionRow] = []
    for node in node_list:
        identifier_type, identifier_value = split_node(node)
        rep = rep_by_node[node]
        rep_type, _ = split_node(rep)
        if identifier_type == "loyalty_id":
            confidence = CONFIDENCE_ANCHOR
            method = METHOD_COMPONENT_ANCHOR if node == rep else METHOD_LOYALTY_MEMBER
        elif rep_type == "loyalty_id":
            confidence = CONFIDENCE_LOYALTY_MATCH
            method = METHOD_LOYALTY_MATCH
        else:
            confidence = CONFIDENCE_CUSTOMER_STANDALONE
            method = METHOD_CUSTOMER_STANDALONE
        rows.append(
            ResolutionRow(
                identifier_type=identifier_type,
                identifier_value=identifier_value,
                customer_key=customer_key(rep),
                confidence_score=confidence,
                resolution_method=method,
                component_rep_node=rep,
                component_rep_type=rep_type,
            )
        )
    return rows


def device_session_associations(clickstream_rows: Iterable[dict]) -> list[DeviceSessionAssociation]:
    """Signed-in intervals per (client_id, session_instance_id, customer_id).

    Attribution only: these never feed the identity graph. An instance with
    more than one account is ambiguous and flagged on every row it produces.
    """
    groups: dict[tuple[str, str, str], list[dict]] = {}
    for row in clickstream_rows:
        signed_in = _signed_in_v13(row)
        if signed_in is not None:
            groups.setdefault(signed_in, []).append(row)
    accounts_per_instance: dict[tuple[str, str], set[str]] = {}
    for client_id, instance, customer_id in groups:
        accounts_per_instance.setdefault((client_id, instance), set()).add(customer_id)

    associations = []
    for (client_id, instance, customer_id), rows in sorted(groups.items()):
        times = sorted(str(r["event_time"]) for r in rows)
        logouts = sorted(str(r["event_time"]) for r in rows if r.get("event_type") == "logout")
        associations.append(
            DeviceSessionAssociation(
                client_id=client_id,
                session_instance_id=instance,
                customer_id=customer_id,
                valid_from=times[0],
                valid_to=logouts[-1] if logouts else times[-1],
                end_reason=END_LOGOUT if logouts else END_LAST_EVENT,
                evidence=(
                    EVIDENCE_VERIFIED_LOGIN
                    if any(r.get("event_type") == "login" for r in rows)
                    else EVIDENCE_SIGNED_IN_SESSION
                ),
                event_count=len(rows),
                is_ambiguous=len(accounts_per_instance[(client_id, instance)]) > 1,
            )
        )
    return associations


def resolve_identity_graph(
    clickstream_rows: Iterable[dict],
    pos_rows: Iterable[dict],
) -> tuple[list[Edge], list[ResolutionRow], list[LoyaltyClaim]]:
    """End-to-end reference pipeline over plain dicts.

    ``clickstream_rows`` are canonical Silver events (event_id, event_type,
    event_time, session_instance_id, client_id, customer_id, properties);
    ``pos_rows`` need loyalty_id. Returns (edges, resolution, claims).
    """
    clickstream = list(clickstream_rows)
    claims = loyalty_claims(clickstream)
    edges = build_edges(claims)

    nodes = {
        PREFIX_LOYALTY + loyalty_id
        for row in pos_rows
        if (loyalty_id := normalize_identifier(row.get("loyalty_id")))
    }
    nodes |= {edge.src for edge in edges}
    nodes |= {
        PREFIX_CUSTOMER + customer_id
        for row in clickstream
        if (customer_id := normalize_identifier(row.get("customer_id")))
    }
    return edges, assign_resolution(nodes, edges), claims
