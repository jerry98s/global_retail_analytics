"""Promotion of the published inventory run's frontier (ADR-012)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orchestration.airflow.plugins import inventory_frontier as plugin
from orchestration.airflow.plugins.inventory_frontier import (
    local_store,
    promote_frontier,
    published_inventory_run,
    s3_store,
)
from spark.identity_resolution import kafka_frontier

pytestmark = pytest.mark.unit


def _candidate(root: Path, run_id: str, closed: int | None = 2026112200) -> bytes:
    body = json.dumps({"inventory_run_id": run_id, "closed_hour_exclusive": closed, "end_offsets": {"0": 5}})
    path = root / "inventory_frontier" / f"run={run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return body.encode("utf-8")


def test_names_match_the_spark_job() -> None:
    assert plugin.FRONTIER_DIR == kafka_frontier.FRONTIER_DIR
    assert plugin.PUBLISHED_FRONTIER == kafka_frontier.PUBLISHED_FRONTIER
    assert kafka_frontier.frontier_candidate("R1") == "run=R1.json"


def test_published_run_is_the_one_live_control_row() -> None:
    assert published_inventory_run([]) is None
    assert published_inventory_run([(None,)]) is None
    assert published_inventory_run([("R1",)]) == "R1"
    with pytest.raises(ValueError, match="one row"):
        published_inventory_run([("R1",), ("R2",)])


def test_promotes_only_the_published_run(tmp_path: Path) -> None:
    published = _candidate(tmp_path, "20261122T011500Z")
    _candidate(tmp_path, "20261122T013000Z")
    read, write = local_store(tmp_path)
    assert "20261122T011500Z" in promote_frontier("20261122T011500Z", read, write)
    assert (tmp_path / "inventory_frontier" / "published.json").read_bytes() == published
    assert kafka_frontier.parse_frontier(published.decode("utf-8"))[1] == 2026112200


def test_repeating_the_promotion_is_a_no_op(tmp_path: Path) -> None:
    published = _candidate(tmp_path, "R1")
    read, write = local_store(tmp_path)
    promote_frontier("R1", read, write)
    promote_frontier("R1", read, write)
    assert (tmp_path / "inventory_frontier" / "published.json").read_bytes() == published
    assert not list((tmp_path / "inventory_frontier").glob("*.tmp"))


def test_no_published_run_writes_nothing(tmp_path: Path) -> None:
    read, write = local_store(tmp_path)
    assert promote_frontier(None, read, write) == "no published inventory run"
    assert not (tmp_path / "inventory_frontier").exists()


def test_missing_frontier_for_the_published_run_fails(tmp_path: Path) -> None:
    read, write = local_store(tmp_path)
    with pytest.raises(FileNotFoundError, match="R9"):
        promote_frontier("R9", read, write)


def test_frontier_for_another_run_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "inventory_frontier" / "run=R1.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"inventory_run_id": "R2"}), encoding="utf-8")
    read, write = local_store(tmp_path)
    with pytest.raises(ValueError, match="R1"):
        promote_frontier("R1", read, write)


class _NoSuchKey(Exception):
    pass


class _FakeS3:
    class exceptions:  # noqa: N801 - mirrors boto3's client.exceptions
        NoSuchKey = _NoSuchKey

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, Bucket: str, Key: str):
        if (Bucket, Key) not in self.objects:
            raise _NoSuchKey(Key)

        class _Body:
            def __init__(self, data: bytes) -> None:
                self.data = data

            def read(self) -> bytes:
                return self.data

        return {"Body": _Body(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:
        self.objects[(Bucket, Key)] = Body


def test_s3_store_keys_sit_under_the_consumer_root() -> None:
    client = _FakeS3()
    body = json.dumps({"inventory_run_id": "R1"}).encode("utf-8")
    client.objects[("silver", "iceberg/consumer_current/inventory_frontier/run=R1.json")] = body
    read, write = s3_store("s3://silver/iceberg/consumer_current", client)
    promote_frontier("R1", read, write)
    assert client.objects[("silver", "iceberg/consumer_current/inventory_frontier/published.json")] == body
    assert read("inventory_frontier/run=R2.json") is None


def test_s3_store_rejects_a_local_root() -> None:
    with pytest.raises(ValueError, match="S3"):
        s3_store("/tmp/iceberg/consumer_current", _FakeS3())
