"""Reference implementation of the identity-resolution graph rules.

This module is the single source of truth for the *rules* of identity
resolution (ADR-003): node naming, edge types, public-device exclusion,
component representative priority, confidence scores, resolution methods,
and the deterministic customer_key formula.

Two engines execute these rules:

- ``identity_resolution_job.py`` — PySpark + GraphFrames on EMR (cloud) or
  local PySpark, reading bronze Iceberg/Parquet and writing
  ``silver.identity_resolution`` (+ ``silver.identity_edges`` audit table).
- ``generate_fixture.py`` — regenerates the dbt seed fixture
  ``seeds/silver/identity_resolution.csv`` used by the DuckDB CI chain, so
  the fixture can never drift from the rules without CI failing.

The dbt model ``int_identity_resolution`` is a thin view over the Spark
output; all downstream consumers (sessions, RFM, dim_customer, fact_sales,
identity_graph) are unchanged.

customer_key parity: the formula below is byte-identical to the dbt macro
``generate_customer_key`` (md5, first 8 hex chars, base-16, mod 1e8, +1), so
Spark-produced keys match keys previously produced by dbt/Redshift/DuckDB.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Optional

# Node ID prefixes (canonical graph node = "<prefix><identifier value>").
PREFIX_LOYALTY = "loyalty:"
PREFIX_CUSTOMER = "customer:"
PREFIX_CLIENT = "client:"

IDENTIFIER_TYPE_BY_PREFIX = {
    PREFIX_LOYALTY: "loyalty_id",
    PREFIX_CUSTOMER: "customer_id",
    PREFIX_CLIENT: "client_id",
}

EDGE_SESSION_LINK = "session_link"
EDGE_LOYALTY_VALUE_MATCH = "loyalty_value_match"
EDGE_TYPES = (EDGE_SESSION_LINK, EDGE_LOYALTY_VALUE_MATCH)

METHOD_COMPONENT_ANCHOR = "component_anchor"
METHOD_LOYALTY_MEMBER = "loyalty_member"
METHOD_LOYALTY_MATCH = "loyalty_match"
METHOD_CUSTOMER_STANDALONE = "customer_id_standalone"
METHOD_SESSION_LINKED = "session_linked"
METHOD_DEVICE_ONLY = "device_only"
METHOD_PUBLIC_DEVICE_EXCLUDED = "public_device_excluded"

CONFIDENCE_PUBLIC_DEVICE = 0.3
CONFIDENCE_ANCHOR = 1.0
CONFIDENCE_LOYALTY_MATCH = 1.0
CONFIDENCE_CUSTOMER_STANDALONE = 0.9
CONFIDENCE_SESSION_LINKED = 0.85
CONFIDENCE_DEVICE_ONLY = 0.5

DEFAULT_PUBLIC_DEVICE_THRESHOLD = 10

CUSTOMER_KEY_MODULUS = 100_000_000


def customer_key(canonical_node: str) -> int:
    """Deterministic key in [1, 1e8) — identical to dbt generate_customer_key."""
    digest_prefix = hashlib.md5(canonical_node.encode("utf-8")).hexdigest()[:8]
    return int(digest_prefix, 16) % CUSTOMER_KEY_MODULUS + 1


def split_node(node: str) -> tuple[str, str]:
    """'loyalty:L1001' -> ('loyalty_id', 'L1001')."""
    prefix, _, value = node.partition(":")
    return IDENTIFIER_TYPE_BY_PREFIX[prefix + ":"], value


def node_sort_key(node: str) -> str:
    """Component representative priority: loyalty < customer < client."""
    if node.startswith(PREFIX_LOYALTY):
        return "1:" + node
    if node.startswith(PREFIX_CUSTOMER):
        return "2:" + node
    if node.startswith(PREFIX_CLIENT):
        return "3:" + node
    return "9:" + node


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    edge_type: str
    last_observed_at: Optional[str]


@dataclass(frozen=True)
class ResolutionRow:
    identifier_type: str
    identifier_value: str
    customer_key: int
    confidence_score: float
    resolution_method: str
    is_public_device: bool
    component_rep_node: str
    component_rep_type: str


def find_public_devices(
    clickstream_pairs: Iterable[tuple[str, str, object]],
    threshold: int = DEFAULT_PUBLIC_DEVICE_THRESHOLD,
) -> set[str]:
    """client_ids linked to >= threshold distinct customer_ids (shared kiosks)."""
    customers_by_client: dict[str, set[str]] = {}
    for client_id, customer_id, _ in clickstream_pairs:
        customers_by_client.setdefault(client_id, set()).add(customer_id)
    return {
        client_id
        for client_id, customers in customers_by_client.items()
        if len(customers) >= threshold
    }


def build_edges(
    clickstream_pairs: Iterable[tuple[str, str, object]],
    loyalty_ids: Iterable[str],
    threshold: int = DEFAULT_PUBLIC_DEVICE_THRESHOLD,
) -> tuple[list[Edge], set[str]]:
    """Build session_link + loyalty_value_match edges.

    Public devices are excluded from edges so they stay isolated singletons.
    ``clickstream_pairs`` yields (client_id, customer_id, event_time) with both
    IDs non-null. ``loyalty_ids`` are distinct POS loyalty IDs.
    """
    pairs = list(clickstream_pairs)
    public_devices = find_public_devices(pairs, threshold)

    last_seen_by_pair: dict[tuple[str, str], object] = {}
    last_seen_by_customer: dict[str, object] = {}
    for client_id, customer_id, event_time in pairs:
        key = (client_id, customer_id)
        if key not in last_seen_by_pair or event_time > last_seen_by_pair[key]:
            last_seen_by_pair[key] = event_time
        if customer_id not in last_seen_by_customer or event_time > last_seen_by_customer[customer_id]:
            last_seen_by_customer[customer_id] = event_time

    edges: list[Edge] = [
        Edge(
            src=PREFIX_CLIENT + client_id,
            dst=PREFIX_CUSTOMER + customer_id,
            edge_type=EDGE_SESSION_LINK,
            last_observed_at=str(last_seen),
        )
        for (client_id, customer_id), last_seen in sorted(last_seen_by_pair.items())
        if client_id not in public_devices
    ]

    clickstream_customers = set(last_seen_by_customer)
    for loyalty_id in sorted(set(loyalty_ids)):
        if loyalty_id in clickstream_customers:
            edges.append(
                Edge(
                    src=PREFIX_LOYALTY + loyalty_id,
                    dst=PREFIX_CUSTOMER + loyalty_id,
                    edge_type=EDGE_LOYALTY_VALUE_MATCH,
                    last_observed_at=str(last_seen_by_customer[loyalty_id]),
                )
            )

    return edges, public_devices


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


def assign_resolution(
    nodes: Iterable[str],
    edges: Iterable[Edge],
    public_devices: Iterable[str],
) -> list[ResolutionRow]:
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

    public = set(public_devices)
    rows: list[ResolutionRow] = []
    for node in node_list:
        identifier_type, identifier_value = split_node(node)
        rep = rep_by_node[node]
        rep_type, _ = split_node(rep)
        is_public = identifier_type == "client_id" and identifier_value in public

        if is_public:
            key = customer_key(PREFIX_CLIENT + identifier_value)
            confidence = CONFIDENCE_PUBLIC_DEVICE
            method = METHOD_PUBLIC_DEVICE_EXCLUDED
        elif identifier_type == "loyalty_id":
            key = customer_key(rep)
            confidence = CONFIDENCE_ANCHOR
            method = METHOD_COMPONENT_ANCHOR if node == rep else METHOD_LOYALTY_MEMBER
        elif identifier_type == "customer_id":
            key = customer_key(rep)
            if rep_type == "loyalty_id":
                confidence = CONFIDENCE_LOYALTY_MATCH
                method = METHOD_LOYALTY_MATCH
            else:
                confidence = CONFIDENCE_CUSTOMER_STANDALONE
                method = METHOD_CUSTOMER_STANDALONE
        else:  # client_id
            key = customer_key(rep)
            if rep_type in ("loyalty_id", "customer_id"):
                confidence = CONFIDENCE_SESSION_LINKED
                method = METHOD_SESSION_LINKED
            else:
                confidence = CONFIDENCE_DEVICE_ONLY
                method = METHOD_DEVICE_ONLY

        rows.append(
            ResolutionRow(
                identifier_type=identifier_type,
                identifier_value=identifier_value,
                customer_key=key,
                confidence_score=confidence,
                resolution_method=method,
                is_public_device=is_public,
                component_rep_node=rep,
                component_rep_type=rep_type,
            )
        )
    return rows


def resolve_identity_graph(
    clickstream_rows: Iterable[dict],
    pos_rows: Iterable[dict],
    threshold: int = DEFAULT_PUBLIC_DEVICE_THRESHOLD,
) -> tuple[list[Edge], list[ResolutionRow], set[str]]:
    """End-to-end reference pipeline over plain dicts.

    ``clickstream_rows`` need client_id/customer_id/event_time keys;
    ``pos_rows`` need loyalty_id. Returns (edges, resolution, public_devices).
    """
    pairs = [
        (row["client_id"], row["customer_id"], row["event_time"])
        for row in clickstream_rows
        if row.get("client_id") and row.get("customer_id")
    ]
    loyalty_ids = [row["loyalty_id"] for row in pos_rows if row.get("loyalty_id")]

    edges, public_devices = build_edges(pairs, loyalty_ids, threshold)

    nodes = {PREFIX_LOYALTY + loyalty_id for loyalty_id in loyalty_ids}
    nodes |= {PREFIX_CUSTOMER + row["customer_id"] for row in clickstream_rows if row.get("customer_id")}
    nodes |= {PREFIX_CLIENT + row["client_id"] for row in clickstream_rows if row.get("client_id")}

    return edges, assign_resolution(nodes, edges, public_devices), public_devices
