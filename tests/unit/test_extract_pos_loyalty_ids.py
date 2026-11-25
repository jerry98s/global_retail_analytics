"""Distinct per-date POS loyalty extracts, and the gap the identity job reports."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.batch.extract_pos_loyalty_ids import (
    distinct_loyalty_table,
    extract_local,
    extract_s3,
    parse_dates,
)
from spark.identity_resolution.graph_logic import resolve_identity_graph
from spark.identity_resolution.pos_loyalty import (
    dates_without_loyalty,
    local_parquet_bytes,
    local_partition_dates,
    local_path,
    missing_loyalty_message,
    pos_data_path_beside,
)

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_JOB = _REPO / "spark" / "identity_resolution" / "identity_resolution_job.py"


def _pos_file(directory: Path, txn_date: str, rows: list[dict], name: str = "part-00000.parquet") -> None:
    part = directory / "data" / f"dt={txn_date}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / name)


def _ids(path: Path) -> list[str]:
    return pq.read_table(path).column("loyalty_id").to_pylist()


def test_extract_keeps_distinct_non_blank_ids_from_every_file(tmp_path: Path) -> None:
    day = "2026-11-25"
    _pos_file(
        tmp_path / "pos",
        day,
        [
            {"loyalty_id": " L1 ", "gross_revenue": 10},
            {"loyalty_id": "L1", "gross_revenue": 4},
            {"loyalty_id": " ", "gross_revenue": 1},
            {"loyalty_id": None, "gross_revenue": 1},
        ],
    )
    _pos_file(tmp_path / "pos", day, [{"loyalty_id": "L9"}, {"loyalty_id": "L2"}], "part-identity-change.parquet")
    written = extract_local(tmp_path / "pos", [day], tmp_path / "loyalty")
    assert _ids(Path(written[0])) == ["L1", "L2", "L9"]
    assert pq.read_schema(written[0]).names == ["loyalty_id"]


def test_extract_matches_the_graph_built_from_the_full_pos_lines(tmp_path: Path) -> None:
    day = "2026-11-25"
    lines = [
        {"loyalty_id": " L1 ", "gross_revenue": 10},
        {"loyalty_id": "L1", "gross_revenue": 4},
        {"loyalty_id": " ", "gross_revenue": 1},
        {"loyalty_id": None, "gross_revenue": 1},
        {"loyalty_id": "L9", "gross_revenue": 3},
    ]
    _pos_file(tmp_path / "pos", day, lines)
    written = extract_local(tmp_path / "pos", [day], tmp_path / "loyalty")
    clickstream = [{
        "event_id": "e1",
        "event_type": "login",
        "event_time": "2026-11-25 00:00:00",
        "session_instance_id": "si-1",
        "client_id": "c-1",
        "customer_id": "C1",
        "properties": '{"loyalty_id": "L1", "loyalty_link_verified": true}',
    }]
    extracted = [{"loyalty_id": value} for value in _ids(Path(written[0]))]
    full = resolve_identity_graph(clickstream, lines)[1]
    reduced = resolve_identity_graph(clickstream, extracted)[1]
    assert {(row.identifier_type, row.identifier_value, row.customer_key) for row in full} == {
        (row.identifier_type, row.identifier_value, row.customer_key) for row in reduced
    }


def test_restated_day_replaces_only_itself(tmp_path: Path) -> None:
    pos, loyalty = tmp_path / "pos", tmp_path / "loyalty"
    _pos_file(pos, "2026-11-24", [{"loyalty_id": "L1"}])
    _pos_file(pos, "2026-11-25", [{"loyalty_id": "L2"}, {"loyalty_id": "L2"}])
    extract_local(pos, ["2026-11-24", "2026-11-25"], loyalty)
    kept = (loyalty / "data" / "dt=2026-11-24" / "part-00000.parquet").read_bytes()
    _pos_file(pos, "2026-11-25", [{"loyalty_id": "L8"}])
    # A previous extra file in the loyalty partition must not survive the rewrite.
    stale = loyalty / "data" / "dt=2026-11-25" / "part-00001.parquet"
    stale.write_bytes(b"stale")
    extract_local(pos, ["2026-11-25"], loyalty)
    assert _ids(loyalty / "data" / "dt=2026-11-25" / "part-00000.parquet") == ["L8"]
    assert not stale.exists()
    assert (loyalty / "data" / "dt=2026-11-24" / "part-00000.parquet").read_bytes() == kept


def test_missing_pos_partition_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="2026-11-25"):
        extract_local(tmp_path / "pos", ["2026-11-25"], tmp_path / "loyalty")


def test_empty_pos_file_writes_an_empty_loyalty_file(tmp_path: Path) -> None:
    day = "2026-11-25"
    part = tmp_path / "pos" / "data" / f"dt={day}"
    part.mkdir(parents=True)
    pq.write_table(pa.table({"loyalty_id": pa.array([], type=pa.string())}), part / "part-00000.parquet")
    written = extract_local(tmp_path / "pos", [day], tmp_path / "loyalty")
    assert _ids(Path(written[0])) == []


def test_dates_must_be_real_calendar_days() -> None:
    assert parse_dates("2026-11-25, 2026-11-25") == ["2026-11-25"]
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_dates("20261125")
    with pytest.raises(ValueError, match="not a real date"):
        parse_dates("2026-02-30")
    with pytest.raises(ValueError, match="at least one"):
        parse_dates(" , ")


def test_distinct_table_rejects_a_file_without_loyalty_id() -> None:
    with pytest.raises(ValueError, match="loyalty_id"):
        distinct_loyalty_table(pa.table({"store_id": ["S1"]}))


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        store = self

        class _Pager:
            def paginate(self, Bucket: str, Prefix: str):
                contents = [
                    {"Key": key}
                    for (bucket, key) in store.objects
                    if bucket == Bucket and key.startswith(Prefix)
                ]
                return [{"Contents": contents}] if contents else [{}]

        return _Pager()

    def get_object(self, Bucket: str, Key: str):
        return {"Body": _Body(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)


def _parquet_bytes(rows: list[dict]) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    return buffer.getvalue()


def test_s3_extract_replaces_one_date_and_drops_a_stale_file() -> None:
    client = _FakeS3()
    day = "2026-11-25"
    source = ("bronze", f"iceberg/bronze/pos_transactions/data/dt={day}/part-00000.parquet")
    client.objects[source] = _parquet_bytes([{"loyalty_id": " L1 "}, {"loyalty_id": "L1"}, {"loyalty_id": " "}])
    stale = ("bronze", f"iceberg/bronze/pos_loyalty_ids/data/dt={day}/part-00001.parquet")
    client.objects[stale] = b"stale"
    written = extract_s3(
        "s3://bronze/iceberg/bronze/pos_transactions/",
        "s3://bronze/iceberg/bronze/pos_loyalty_ids/",
        [day],
        client,
    )
    assert written == [f"s3://bronze/iceberg/bronze/pos_loyalty_ids/data/dt={day}/part-00000.parquet"]
    assert stale not in client.objects
    body = client.objects[("bronze", f"iceberg/bronze/pos_loyalty_ids/data/dt={day}/part-00000.parquet")]
    assert pq.read_table(io.BytesIO(body)).column("loyalty_id").to_pylist() == ["L1"]


def test_s3_extract_rejects_a_missing_partition() -> None:
    with pytest.raises(FileNotFoundError, match="2026-11-25"):
        extract_s3(
            "s3://bronze/iceberg/bronze/pos_transactions/",
            "s3://bronze/iceberg/bronze/pos_loyalty_ids/",
            ["2026-11-25"],
            _FakeS3(),
        )


def test_missing_loyalty_dates_are_named_and_sized_locally(tmp_path: Path) -> None:
    pos = tmp_path / "bronze" / "pos_transactions" / "data"
    loyalty = tmp_path / "bronze" / "pos_loyalty_ids" / "data"
    _pos_file(pos.parent, "2026-11-24", [{"loyalty_id": "L1"}])
    _pos_file(pos.parent, "2026-11-25", [{"loyalty_id": "L2"}])
    extract_local(pos.parent, ["2026-11-24"], loyalty.parent)
    missing = dates_without_loyalty(
        local_partition_dates(str(pos)),
        local_partition_dates(str(loyalty)),
    )
    assert missing == ["2026-11-25"]
    assert "2026-11-25" in missing_loyalty_message(missing)
    assert local_parquet_bytes(str(loyalty)) > 0
    assert pos_data_path_beside(str(loyalty)) == str(pos)
    assert local_path("s3://bronze/iceberg/bronze/pos_loyalty_ids/data") is None
    assert local_partition_dates("s3://bronze/iceberg/bronze/pos_loyalty_ids/data") is None


def test_identity_job_reads_the_loyalty_extract_and_sizes_it_with_hadoop() -> None:
    src = _JOB.read_text(encoding="utf-8")
    assert '--pos-loyalty-path' in src
    assert "pos-parquet-path" not in src
    assert "rglob" not in src
    assert "listFiles" in src
    assert "missing_loyalty_message" in src
    assert "_read_loyalty(spark, args.pos_loyalty_path)" in src
