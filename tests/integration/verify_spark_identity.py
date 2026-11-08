"""Execute the real PySpark/GraphFrames identity path on a tiny graph.

This is intentionally a standalone spark-submit smoke test rather than a
pytest unit test. It verifies that the DataFrame implementation matches the
plain-Python rules in graph_logic: verified loyalty links are the only merge
edge, devices never merge accounts, unverified claims and pre-1.3 events
create no edges, blank identifiers are dropped, and device-session
associations match. It also checks the consumer-export manifest.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import astuple
from pathlib import Path

from pyspark.sql import SparkSession

from spark.identity_resolution import graph_logic as reference
from spark.identity_resolution.consumer_exports import read_current
from spark.identity_resolution.identity_resolution_job import (
    assign_resolution,
    build_edges,
    build_vertices,
    device_session_associations,
    loyalty_claims,
    write_consumer_exports,
)

CLICKSTREAM_SCHEMA = (
    "event_id string, event_type string, event_time string, session_instance_id string, "
    "client_id string, customer_id string, properties string"
)


def _login(event_id, time, instance, client, customer, loyalty=None, verified=None):
    properties = {"page": "login"}
    if loyalty is not None:
        properties["loyalty_id"] = loyalty
        properties["loyalty_link_verified"] = verified
    return {
        "event_id": event_id, "event_type": "login", "event_time": time,
        "session_instance_id": instance, "client_id": client, "customer_id": customer,
        "properties": json.dumps(properties, sort_keys=True),
    }


def _event(event_id, event_type, time, instance, client, customer):
    return {
        "event_id": event_id, "event_type": event_type, "event_time": time,
        "session_instance_id": instance, "client_id": client, "customer_id": customer,
        "properties": "{}",
    }


CLICKSTREAM_ROWS = [
    # Verified link, padded identifiers; logged in twice (earliest is evidence).
    _login("e01", "2026-08-30 00:00:05", "si-1", " c-anchor ", " C1001 ", " L1001 ", True),
    _event("e02", "logout", "2026-08-30 00:01:00", "si-1", "c-anchor", "C1001"),
    _login("e03", "2026-08-30 02:00:00", "si-2", "c-anchor", "C1001", "L1001", True),
    # Two accounts verified to one card on different devices.
    _login("e04", "2026-08-30 03:00:00", "si-3", "c-web", "C2002", "L2002", True),
    _login("e05", "2026-08-30 03:10:00", "si-4", "c-app", "C2003", "L2002", True),
    # Shared tablet: two accounts, one after the other.
    _login("e06", "2026-08-30 04:00:00", "si-5", "c-tab", "C3001", "L3001", True),
    _event("e07", "logout", "2026-08-30 04:05:00", "si-5", "c-tab", "C3001"),
    _login("e08", "2026-08-30 04:10:00", "si-6", "c-tab", "C3002"),
    _event("e09", "page_view", "2026-08-30 04:11:00", "si-6", "c-tab", "C3002"),
    # Unverified claims on someone else's card; a string flag is not verification.
    _login("e10", "2026-08-30 05:00:00", "si-7", "c-claim", "C4001", "L1001", False),
    _login("e16", "2026-08-30 05:30:00", "si-10", "c-claim", "C4002", "L1001", "true"),
    # Two accounts inside one instance: ambiguous.
    _login("e11", "2026-08-30 06:00:00", "si-8", "c-amb", "C6001"),
    _login("e12", "2026-08-30 06:01:00", "si-8", "c-amb", "C6002"),
    # Pre-1.3 event: no session instance, so no claim even if it names a card.
    _login("e13", "2026-08-30 07:00:00", None, "c-legacy", "L1001", "L1001", True),
    # Blank identifiers never become nodes or associations.
    _event("e14", "page_view", "2026-08-30 08:00:00", "si-9", "c-blank", " "),
    _event("e15", "page_view", "2026-08-30 08:01:00", "si-9", " ", "C-orphan"),
]

POS_ROWS = [{"loyalty_id": " L1001 "}, {"loyalty_id": " "}, {"loyalty_id": "L9001"}]


def _reference():
    edges, rows, claims = reference.resolve_identity_graph(CLICKSTREAM_ROWS, POS_ROWS)
    associations = reference.device_session_associations(CLICKSTREAM_ROWS)
    return (
        {(r.identifier_type, r.identifier_value): astuple(r)[2:] for r in rows},
        sorted(astuple(e) for e in edges),
        sorted(astuple(c) for c in claims),
        sorted(astuple(a) for a in associations),
    )


def _resolution_row(row):
    return (
        row.customer_key,
        round(float(row.confidence_score), 4),
        row.resolution_method,
        row.component_rep_node,
        row.component_rep_type,
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="spark-identity-checkpoint-") as tmp:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("verify-spark-identity")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            spark.sparkContext.setCheckpointDir(str(Path(tmp).resolve()))
            clickstream = spark.createDataFrame(CLICKSTREAM_ROWS, CLICKSTREAM_SCHEMA)
            pos = spark.createDataFrame(POS_ROWS, "loyalty_id string")
            claims = loyalty_claims(clickstream)
            edges = build_edges(claims)
            resolution = assign_resolution(spark, build_vertices(clickstream, pos, edges), edges)
            associations = device_session_associations(clickstream)
            actual = (
                {(r.identifier_type, r.identifier_value): _resolution_row(r) for r in resolution.collect()},
                sorted(tuple(r) for r in edges.collect()),
                sorted(tuple(r) for r in claims.collect()),
                sorted(tuple(r) for r in associations.collect()),
            )

            # Consumers never glob an Iceberg table's data directory. Verify
            # the manifest exposes exactly the latest run and old runs prune.
            consumer_root = (Path(tmp) / "consumer-current").resolve().as_uri()
            write_consumer_exports(resolution, edges, claims, associations, consumer_root)
            second = resolution.filter("identifier_value = 'L1001'")
            expected_count = second.count()
            for _ in range(2):
                write_consumer_exports(second, edges.limit(1), claims, associations, consumer_root)
            exported = read_current(spark, consumer_root, "identity_resolution")
            if exported.count() != expected_count or read_current(spark, consumer_root, "identity_edges").count() != 1:
                raise SystemExit("consumer_current manifest exposed superseded run files")
            if exported.filter("identifier_value != 'L1001'").count() > 0:
                raise SystemExit("consumer_current manifest did not expose the latest run")
            runs = [p for p in (Path(tmp) / "consumer-current" / "identity_resolution").iterdir()
                    if p.name.startswith("run=")]
            if len(runs) != 2:
                raise SystemExit(f"expected current + previous run kept, found {len(runs)}")
        finally:
            spark.stop()

    expected = _reference()
    for name, want, got in zip(("resolution", "edges", "claims", "associations"), expected, actual):
        if want != got:
            raise SystemExit(f"Spark/reference {name} mismatch:\n  reference={want}\n  spark    ={got}")

    resolution_keys = actual[0]
    key = {identifier: row[0] for identifier, row in resolution_keys.items()}
    checks = {
        "dual accounts share a key": key[("customer_id", "C2002")] == key[("customer_id", "C2003")],
        "shared tablet keeps two keys": key[("customer_id", "C3001")] != key[("customer_id", "C3002")],
        "unverified claim does not merge": key[("customer_id", "C4001")] != key[("loyalty_id", "L1001")],
        "string flag does not merge": key[("customer_id", "C4002")] != key[("loyalty_id", "L1001")],
        "legacy value match does not merge": key[("customer_id", "L1001")] != key[("loyalty_id", "L1001")],
        "no device identities": all(t != "client_id" for t, _ in resolution_keys),
        "no blank identifiers": all(v.strip() for _, v in resolution_keys),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise SystemExit(f"identity rules violated: {failed}")

    print(f"PASS: Spark GraphFrames matches reference logic ({len(resolution_keys)} identifiers, "
          f"{len(actual[1])} edges, {len(actual[2])} claims, {len(actual[3])} associations).")


if __name__ == "__main__":
    main()
