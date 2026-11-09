import json

from scripts.local.benchmark_identity import parse_mem_usage
from spark.identity_resolution.scale_ids import (
    remap_clickstream,
    remap_event,
    remap_inventory,
    tag_identifier,
)


def test_tag_leaves_blanks_and_suffixes_the_rest() -> None:
    assert tag_identifier(None, 2) is None
    assert tag_identifier("  ", 2) == "  "
    assert tag_identifier("CUST-1", 2) == "CUST-1#r2"


def test_clickstream_replica_changes_people_and_keeps_the_verified_flag() -> None:
    payload = json.dumps({
        "event_id": "e1",
        "session_id": "s1",
        "session_instance_id": "i1",
        "client_id": "device-1",
        "customer_id": "CUST-1",
        "event_type": "login",
        "properties": {"loyalty_id": "LOYAL-1", "loyalty_link_verified": True},
    }).encode()
    remapped = json.loads(remap_event(payload, 3, "clickstream"))
    assert remapped["customer_id"] == "CUST-1#r3"
    assert remapped["client_id"] == "device-1#r3"
    assert remapped["session_instance_id"] == "i1#r3"
    assert remapped["event_id"] == "e1#r3"
    assert remapped["properties"]["loyalty_id"] == tag_identifier("LOYAL-1", 3)
    assert remapped["properties"]["loyalty_link_verified"] is True
    assert remapped["event_type"] == "login"


def test_anonymous_customer_stays_null_and_bad_json_is_kept() -> None:
    payload = json.dumps({"event_id": "e", "customer_id": None, "properties": {}}).encode()
    remapped = json.loads(remap_clickstream(payload, 0))
    assert remapped["customer_id"] is None
    assert remap_clickstream(b"\xff", 1) == b"\xff"


def test_inventory_retags_only_the_event() -> None:
    payload = json.dumps({"event_id": "e", "store_id": "S1", "product_id": "P1"}).encode()
    remapped = json.loads(remap_inventory(payload, 4))
    assert remapped == {"event_id": "e#r4", "store_id": "S1", "product_id": "P1"}


def test_replicas_do_not_share_ids() -> None:
    payload = json.dumps({"event_id": "e", "customer_id": "C"}).encode()
    first = json.loads(remap_event(payload, 0, "clickstream"))
    second = json.loads(remap_event(payload, 1, "clickstream"))
    assert first["customer_id"] != second["customer_id"]


def test_memory_usage_parses_docker_stats() -> None:
    assert parse_mem_usage("1.5GiB / 8GiB") == 1536
    assert parse_mem_usage("512MiB / 8GiB") == 512
    assert parse_mem_usage("") is None
    assert parse_mem_usage("n/a") is None
