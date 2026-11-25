"""
Generate daily POS returns Parquet for bronze from delivered POS sales.

A return is a new business transaction (ADR-015): it references the original
sale line and never changes it. Output layout:
  <output-dir>/data/dt=<return-date>/part-00000.parquet

Determinism:
  Every non-voided sale line gets a fixed return plan derived from its own
  (transaction_id, line_item_number): whether it is returned, on which days,
  and how much each time. The file for a return date holds the plan events
  that fall on that date, read from the POS bronze files in the lookback
  window. Any date can be regenerated alone, in any order, with the same
  result while those POS files are unchanged. A day with no returns is
  written as a zero-row file, so an empty delivery is explicit.

Limits per sale line: cumulative quantity returned never exceeds
quantity_sold, and cumulative refund never exceeds net_revenue. Rejected
returns keep their row for audit; only completed returns move money.
Timestamps are naive UTC.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Iterable, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ingestion.kafka.sim_entities import STORES

log = structlog.get_logger()

RETURNS_SCHEMA = pa.schema(
    [
        ("return_id", pa.string()),
        ("return_line_number", pa.int32()),
        ("return_date", pa.date32()),
        ("original_transaction_id", pa.string()),
        ("original_line_item_number", pa.int32()),
        ("return_store_id", pa.string()),
        ("product_id", pa.string()),
        ("quantity_returned", pa.int32()),
        ("refund_amount", pa.decimal128(18, 2)),
        ("return_status", pa.string()),
        ("reason_code", pa.string()),
        ("processed_at", pa.timestamp("us")),
    ]
)

COMPLETED = "COMPLETED"
REJECTED = "REJECTED"
TICKET_RETURN_RATE = 0.08
LINE_RETURN_RATE = 0.5
SPLIT_RATE = 0.3
REJECT_RATE = 0.05
OTHER_STORE_RATE = 0.1
REASON_CODES = ("DAMAGED_ITEM", "WRONG_SIZE", "CHANGED_MIND", "NOT_AS_DESCRIBED")
# First return: same day, within a week, or within 30 days.
FIRST_OFFSET_BANDS = ((0, 0, 0.25), (1, 7, 0.5), (8, 30, 0.25))
MAX_SECOND_GAP_DAYS = 7
LOOKBACK_DAYS = FIRST_OFFSET_BANDS[-1][1] + MAX_SECOND_GAP_DAYS
STORE_OPEN = time(8)
STORE_CLOSE = time(22)

_RETURN_NAMESPACE = uuid.UUID("a4f3b2c1-0000-0000-0000-00000052544e")


@dataclass(frozen=True)
class SaleLine:
    transaction_id: str
    line_item_number: int
    transaction_date: date
    store_id: str
    product_id: str
    quantity_sold: int
    net_revenue: Decimal
    rung_up_at: Optional[datetime]


@dataclass(frozen=True)
class ReturnEvent:
    return_date: date
    quantity_returned: int
    refund_amount: Decimal
    reason_code: str


@dataclass
class ReturnLine:
    return_id: str
    return_line_number: int
    return_date: date
    original_transaction_id: str
    original_line_item_number: int
    return_store_id: str
    product_id: str
    quantity_returned: int
    refund_amount: Decimal
    return_status: str
    reason_code: str
    processed_at: datetime


def _rng(*parts: object) -> random.Random:
    digest = hashlib.sha256("#".join(str(p) for p in parts).encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _first_offset(rng: random.Random) -> int:
    draw = rng.random()
    for low, high, weight in FIRST_OFFSET_BANDS:
        if draw < weight:
            return rng.randint(low, high)
        draw -= weight
    return FIRST_OFFSET_BANDS[-1][1]


def plan_returns(sale: SaleLine) -> list[ReturnEvent]:
    """The fixed return plan of one non-voided sale line (often empty).

    The ticket decides whether the customer comes back and on which day, so
    lines of one purchase go back together; each line then decides whether
    it is part of that trip and how much of it.
    """
    ticket = _rng("return-trip", sale.transaction_id)
    if ticket.random() >= TICKET_RETURN_RATE:
        return []
    first_day = sale.transaction_date + timedelta(days=_first_offset(ticket))
    rng = _rng("return-plan", sale.transaction_id, sale.line_item_number)
    if rng.random() >= LINE_RETURN_RATE:
        return []
    quantities = [rng.randint(1, sale.quantity_sold)]
    days = [first_day]
    if quantities[0] < sale.quantity_sold and rng.random() < SPLIT_RATE:
        quantities.append(rng.randint(1, sale.quantity_sold - quantities[0]))
        days.append(first_day + timedelta(days=rng.randint(1, MAX_SECOND_GAP_DAYS)))
    events = []
    refunded = Decimal("0.00")
    returned = 0
    for day, quantity in zip(days, quantities):
        returned += quantity
        cumulative = _money(sale.net_revenue * returned / sale.quantity_sold)
        events.append(ReturnEvent(day, quantity, cumulative - refunded, rng.choice(REASON_CODES)))
        refunded = cumulative
    return events


def _processed_at(sale: SaleLine, return_date: date, rng: random.Random) -> datetime:
    opens = datetime.combine(return_date, STORE_OPEN)
    closes = datetime.combine(return_date, STORE_CLOSE)
    if return_date == sale.transaction_date and sale.rung_up_at is not None:
        opens = max(opens, sale.rung_up_at + timedelta(minutes=15))
    span = max(int((closes - opens).total_seconds()), 0)
    return opens + timedelta(seconds=rng.randint(0, span))


def returns_for_date(sales: Iterable[SaleLine], return_date: date) -> list[ReturnLine]:
    """Return lines processed on return_date, one return_id per original ticket."""
    by_ticket: dict[str, list[tuple[SaleLine, ReturnEvent]]] = defaultdict(list)
    for sale in sales:
        for event in plan_returns(sale):
            if event.return_date == return_date:
                by_ticket[sale.transaction_id].append((sale, event))
    rows: list[ReturnLine] = []
    for transaction_id in sorted(by_ticket):
        lines = sorted(by_ticket[transaction_id], key=lambda item: item[0].line_item_number)
        ticket = _rng("return-ticket", transaction_id, return_date.isoformat())
        status = REJECTED if ticket.random() < REJECT_RATE else COMPLETED
        store = lines[0][0].store_id
        if ticket.random() < OTHER_STORE_RATE:
            store = ticket.choice([s for s in STORES if s != store] or [store])
        processed_at = max(_processed_at(sale, return_date, ticket) for sale, _ in lines)
        return_id = str(uuid.uuid5(_RETURN_NAMESPACE, f"{transaction_id}#{return_date.isoformat()}"))
        for number, (sale, event) in enumerate(lines, start=1):
            rows.append(
                ReturnLine(
                    return_id=return_id,
                    return_line_number=number,
                    return_date=return_date,
                    original_transaction_id=sale.transaction_id,
                    original_line_item_number=sale.line_item_number,
                    return_store_id=store,
                    product_id=sale.product_id,
                    quantity_returned=event.quantity_returned,
                    refund_amount=event.refund_amount,
                    return_status=status,
                    reason_code=event.reason_code,
                    processed_at=processed_at,
                )
            )
    return rows


def sale_is_voided(row: dict) -> Optional[bool]:
    """Effective void flag with the precedence of stg_pos_transactions (ADR-015).

    Explicit line_status / transaction_status win over the stored is_voided
    flag. Files written before the status columns map is_voided to a line void
    of a completed transaction. None when the status cannot be determined.
    """
    legacy = row.get("is_voided")
    line_status = row.get("line_status")
    if line_status is None and legacy is not None:
        line_status = "VOIDED" if legacy else "ACTIVE"
    transaction_status = row.get("transaction_status")
    if transaction_status is None and legacy is not None:
        transaction_status = "COMPLETED"
    if line_status == "VOIDED" or transaction_status == "VOIDED":
        return True
    if line_status is not None and transaction_status is not None:
        return False
    return None


def read_sales(pos_dir: Path, return_date: date) -> list[SaleLine]:
    """Non-voided POS lines sold in the lookback window ending on return_date."""
    first = return_date - timedelta(days=LOOKBACK_DAYS)
    sales: list[SaleLine] = []
    for partition in sorted((pos_dir / "data").glob("dt=*")):
        try:
            sold_on = date.fromisoformat(partition.name.removeprefix("dt="))
        except ValueError:
            continue
        if not first <= sold_on <= return_date:
            continue
        for path in sorted(partition.glob("*.parquet")):
            table = pq.read_table(path)
            has_rung_up = "source_updated_at" in table.column_names
            for row in table.to_pylist():
                if sale_is_voided(row) is not False:
                    continue
                sales.append(
                    SaleLine(
                        transaction_id=row["transaction_id"],
                        line_item_number=row["line_item_number"],
                        transaction_date=row["transaction_date"],
                        store_id=row["store_id"],
                        product_id=row["product_id"],
                        quantity_sold=row["quantity_sold"],
                        net_revenue=row["net_revenue"],
                        rung_up_at=row["source_updated_at"] if has_rung_up else None,
                    )
                )
    return sales


def rows_to_table(rows: list[ReturnLine]) -> pa.Table:
    return pa.Table.from_pylist([vars(row) for row in rows], schema=RETURNS_SCHEMA)


def write_local_parquet(output_dir: str, return_date: str, table: pa.Table) -> str:
    part_dir = Path(output_dir) / "data" / f"dt={return_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / "part-00000.parquet"
    pq.write_table(table, path, compression="snappy")
    log.info("pos_returns_parquet_written", path=str(path), rows=table.num_rows)
    return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily POS returns Parquet bronze from POS sales.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Return processing date YYYY-MM-DD.")
    parser.add_argument("--pos-dir", required=True, help="POS bronze root holding data/dt=<date>/*.parquet.")
    parser.add_argument("--output-dir", required=True, help="Returns bronze root; writes data/dt=<date>/part-00000.parquet.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    return_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    rows = returns_for_date(read_sales(Path(args.pos_dir), return_date), return_date)
    path = write_local_parquet(args.output_dir, return_date.isoformat(), rows_to_table(rows))
    print(f"Wrote {len(rows)} return lines to {path}")


if __name__ == "__main__":
    main()
