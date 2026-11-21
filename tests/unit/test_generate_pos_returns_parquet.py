from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.batch import generate_pos_parquet as pos
from ingestion.batch import generate_pos_returns_parquet as ret

pytestmark = pytest.mark.unit

SOLD_ON = date(2026, 11, 1)


def _sales(count: int = 3000) -> list[ret.SaleLine]:
    return [
        ret.SaleLine(f"T{n}", 1, SOLD_ON, "STORE-001", "PROD-0001", 3, Decimal("100.00"), datetime(2026, 11, 1, 21, 50))
        for n in range(count)
    ]


def _write_pos(pos_dir: Path, sold_on: date, count: int) -> None:
    rows = pos.generate_rows(sold_on, transaction_count=count, max_lines=4)
    pos.write_local_parquet(str(pos_dir), sold_on.isoformat(), pos.rows_to_table(rows))


def test_plans_stay_within_the_original_line() -> None:
    planned = [(sale, ret.plan_returns(sale)) for sale in _sales()]
    returned = [(sale, events) for sale, events in planned if events]
    assert 0.01 < len(returned) / len(planned) < 0.06
    assert any(len(events) == 2 for _, events in returned)
    assert any(events[0].return_date == SOLD_ON for _, events in returned)
    for sale, events in returned:
        assert sum(e.quantity_returned for e in events) <= sale.quantity_sold
        assert sum(e.refund_amount for e in events) <= sale.net_revenue
        assert all(e.quantity_returned >= 1 and e.refund_amount > 0 for e in events)
        days = [e.return_date for e in events]
        assert days == sorted(set(days)) and days[0] >= SOLD_ON
        assert days[-1] <= SOLD_ON + timedelta(days=ret.LOOKBACK_DAYS)


def test_a_full_split_return_refunds_exactly_the_net_revenue() -> None:
    sales = (ret.SaleLine(f"T{n}", 1, SOLD_ON, "STORE-001", "PROD-0001", 3, Decimal("10.00"), None) for n in range(50000))
    events = next(
        events
        for events in map(ret.plan_returns, sales)
        if len(events) == 2 and sum(e.quantity_returned for e in events) == 3
    )
    assert [e.refund_amount for e in events] in ([Decimal("3.33"), Decimal("6.67")], [Decimal("6.67"), Decimal("3.33")])
    assert sum(e.refund_amount for e in events) == Decimal("10.00")


def test_any_return_date_regenerates_alone_with_the_same_lines() -> None:
    sales = _sales()
    days = [SOLD_ON + timedelta(days=n) for n in range(10)]
    forward = {d: ret.returns_for_date(sales, d) for d in days}
    backward = {d: ret.returns_for_date(list(reversed(sales)), d) for d in reversed(days)}
    assert forward == backward
    assert sum(len(rows) for rows in forward.values()) > 0


def test_one_return_id_per_original_ticket_and_day() -> None:
    sales = [
        ret.SaleLine(f"T{n}", line, SOLD_ON, "STORE-001", "PROD-0001", 2, Decimal("20.00"), None)
        for n in range(3000)
        for line in (1, 2, 3)
    ]
    rows = [row for d in range(31) for row in ret.returns_for_date(sales, SOLD_ON + timedelta(days=d))]
    tickets: dict[str, list[ret.ReturnLine]] = {}
    for row in rows:
        tickets.setdefault(row.return_id, []).append(row)
    assert any(len(lines) > 1 for lines in tickets.values())
    for lines in tickets.values():
        assert [line.return_line_number for line in lines] == list(range(1, len(lines) + 1))
        assert len({(line.original_transaction_id, line.return_date, line.return_status, line.return_store_id) for line in lines}) == 1
    assert {row.return_status for row in rows} == {ret.COMPLETED, ret.REJECTED}


def test_same_day_return_is_processed_after_the_sale() -> None:
    for row in ret.returns_for_date(_sales(), SOLD_ON):
        assert row.processed_at >= datetime(2026, 11, 1, 22, 5)


def test_reads_only_non_voided_lines_in_the_window(tmp_path: Path) -> None:
    pos_dir = tmp_path / "pos_transactions"
    _write_pos(pos_dir, SOLD_ON, 400)
    _write_pos(pos_dir, SOLD_ON - timedelta(days=ret.LOOKBACK_DAYS + 1), 50)
    legacy = pa.table(
        {"transaction_id": ["L1", "L2"], "line_item_number": [1, 1], "transaction_date": [SOLD_ON] * 2,
         "store_id": ["STORE-001"] * 2, "product_id": ["PROD-0001"] * 2, "quantity_sold": [1, 1],
         "net_revenue": pa.array([Decimal("5.00")] * 2, pa.decimal128(18, 2)), "is_voided": [False, None]}
    )
    pq.write_table(legacy, pos_dir / "data" / f"dt={SOLD_ON}" / "part-legacy.parquet")

    sales = ret.read_sales(pos_dir, SOLD_ON + timedelta(days=3))
    source = pq.read_table(pos_dir / "data" / f"dt={SOLD_ON}" / "part-00000.parquet").to_pylist()
    active = {(r["transaction_id"], r["line_item_number"]) for r in source if not r["is_voided"]}
    assert {(s.transaction_id, s.line_item_number) for s in sales} == active | {("L1", 1)}
    assert any(r["is_voided"] for r in source)
    assert {s.transaction_date for s in sales} == {SOLD_ON}


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"line_status": "ACTIVE", "transaction_status": "VOIDED", "is_voided": False}, True),
        ({"line_status": "VOIDED", "transaction_status": "COMPLETED", "is_voided": False}, True),
        ({"line_status": "ACTIVE", "transaction_status": "COMPLETED", "is_voided": True}, False),
        ({"is_voided": True}, True),
        ({"is_voided": False}, False),
        ({"is_voided": None}, None),
        ({"line_status": None, "transaction_status": "COMPLETED", "is_voided": None}, None),
    ],
)
def test_void_status_follows_staging_precedence(row: dict, expected: bool | None) -> None:
    assert ret.sale_is_voided(row) is expected


def test_a_voided_transaction_never_becomes_a_return_candidate(tmp_path: Path) -> None:
    pos_dir = tmp_path / "pos_transactions"
    part = pos_dir / "data" / f"dt={SOLD_ON}"
    part.mkdir(parents=True)
    table = pa.table(
        {"transaction_id": ["V1", "A1"], "line_item_number": [1, 1], "transaction_date": [SOLD_ON] * 2,
         "store_id": ["STORE-001"] * 2, "product_id": ["PROD-0001"] * 2, "quantity_sold": [1, 1],
         "net_revenue": pa.array([Decimal("5.00")] * 2, pa.decimal128(18, 2)),
         "is_voided": [False, False], "line_status": ["ACTIVE", "ACTIVE"],
         "transaction_status": ["VOIDED", "COMPLETED"], "source_updated_at": [None, None]}
    )
    pq.write_table(table, part / "part-00000.parquet")
    assert [s.transaction_id for s in ret.read_sales(pos_dir, SOLD_ON)] == ["A1"]


def test_writes_an_explicit_empty_day(tmp_path: Path) -> None:
    path = Path(ret.write_local_parquet(str(tmp_path), "2026-11-21", ret.rows_to_table([])))
    assert path.parent.name == "dt=2026-11-21"
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_rows == 0
    assert parquet.schema_arrow.equals(ret.RETURNS_SCHEMA)
