"""Remap one Bronze replica so it cannot merge with another replica.

The identity job deduplicates on ``event_id`` and on the Kafka transport
tuple, and it merges accounts only through loyalty ids. A scaled copy therefore
needs its own event ids, its own transport partition, and its own people.
Store and product ids stay put: they are not people, and the identity graph
does not read them.
"""

from __future__ import annotations

import json

# Added to kafka_partition for replica N. Larger than any local partition
# count, so each replica keeps the source offset sequence on its own partition
# and the gap check does not see a break between copies.
PARTITION_STRIDE = 1000

CLICKSTREAM_IDS = (
    "event_id",
    "session_id",
    "session_instance_id",
    "client_id",
    "customer_id",
)
INVENTORY_IDS = ("event_id",)


def tag_identifier(value: object, replica: int) -> object:
    """Suffix a non-blank string. Nulls and blanks stay as they are."""
    if replica < 0:
        raise ValueError(f"replica must be >= 0, got {replica}")
    if not isinstance(value, str) or not value.strip():
        return value
    return f"{value}#r{replica}"


def remap_event(payload: bytes | bytearray | str | None, replica: int, stream: str) -> bytes | None:
    """Return the payload with this replica's people and event ids.

    Invalid JSON is returned unchanged so the canonicalizer still quarantines
    it. ``replica`` is ignored for that pass-through.
    """
    if payload is None:
        return None
    if replica < 0:
        raise ValueError(f"replica must be >= 0, got {replica}")
    if stream not in {"clickstream", "inventory"}:
        raise ValueError(f"Unknown stream: {stream}")
    original = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    try:
        event = json.loads(original.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return original
    if not isinstance(event, dict):
        return original
    fields = CLICKSTREAM_IDS if stream == "clickstream" else INVENTORY_IDS
    for field in fields:
        if field in event:
            event[field] = tag_identifier(event[field], replica)
    if stream == "clickstream":
        properties = event.get("properties")
        if isinstance(properties, dict) and "loyalty_id" in properties:
            properties["loyalty_id"] = tag_identifier(properties["loyalty_id"], replica)
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def remap_clickstream(payload: bytes | bytearray | None, replica: int | None) -> bytes | None:
    """Spark UDF entry: clickstream payload for one replica."""
    if payload is None or replica is None:
        return payload
    return remap_event(payload, int(replica), "clickstream")


def remap_inventory(payload: bytes | bytearray | None, replica: int | None) -> bytes | None:
    """Spark UDF entry: inventory payload for one replica."""
    if payload is None or replica is None:
        return payload
    return remap_event(payload, int(replica), "inventory")
