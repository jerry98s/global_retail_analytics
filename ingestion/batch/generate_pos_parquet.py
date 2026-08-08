"""
Generate a daily POS Parquet snapshot for bronze.

Used by the warehouse_daily_batch_pipeline Airflow DAG (and manually for
dev/local). Writes the same layout Flink bronze jobs use under ``data/``.

Output layouts:
  s3://<bucket>/iceberg/bronze/pos_transactions/data/dt=<YYYY-MM-DD>/part-00000.parquet
  <local-dir>/data/dt=<YYYY-MM-DD>/part-00000.parquet   (--output-dir for local dbt)

Idempotency (P3.6, docs/runbooks/dw-checklist-audit.md):
  The default seed is derived deterministically from `--date` so re-running
  the script with the same `--date` produces byte-identical Parquet rows
  (same transaction_id UUIDs, same line counts, same quantities/revenues).
  This makes Bronze reproducible: a corrupted partition can be regenerated
  exactly. To opt into a different draw (e.g. for additive demo data),
  pass `--seed <int>` to override the date-derived seed.
"""

from __future__ import annotations

import argparse
import io
import os
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ingestion.kafka.sim_entities import LOYALTY_IDS, PRODUCTS, STORES

log = structlog.get_logger()

POS_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("line_item_number", pa.int32()),
        ("transaction_date", pa.date32()),
        ("store_id", pa.string()),
        ("product_id", pa.string()),
        ("loyalty_id", pa.string()),
        ("quantity_sold", pa.int32()),
        ("gross_revenue", pa.decimal128(18, 2)),
        ("net_revenue", pa.decimal128(18, 2)),
        ("gross_margin", pa.decimal128(18, 2)),
        ("is_voided", pa.bool_()),
    ]
)


@dataclass
class PosLine:
    transaction_id: str
    line_item_number: int
    transaction_date: date
    store_id: str
    product_id: str
    loyalty_id: Optional[str]
    quantity_sold: int
    gross_revenue: Decimal
    net_revenue: Decimal
    gross_margin: Decimal
    is_voided: bool

    @classmethod
    def generate(cls, transaction_id: str, line_item_number: int, txn_date: date) -> "PosLine":
        quantity = random.randint(1, 4)
        unit_price = round(random.uniform(4.0, 120.0), 2)
        gross = round(quantity * unit_price, 2)
        discount = round(gross * random.uniform(0.0, 0.2), 2)
        net = round(gross - discount, 2)
        margin = round(net * random.uniform(0.1, 0.35), 2)
        return cls(
            transaction_id=transaction_id,
            line_item_number=line_item_number,
            transaction_date=txn_date,
            store_id=random.choice(STORES),
            product_id=random.choice(PRODUCTS),
            loyalty_id=random.choice(LOYALTY_IDS) if random.random() < 0.7 else None,
            quantity_sold=quantity,
            gross_revenue=Decimal(str(gross)),
            net_revenue=Decimal(str(net)),
            gross_margin=Decimal(str(margin)),
            is_voided=random.random() < 0.01,
        )


# Fixed namespace for uuid5 — arbitrary but must be valid UUID and stable
# across runs so the same (--date, transaction index) maps to the same
# transaction_id. Generated once with uuid.uuid4(); pinned here.
_TXN_NAMESPACE = uuid.UUID("a4f3b2c1-0000-0000-0000-000000504f53")


def _derive_seed(txn_date: date, override: Optional[int]) -> int:
    """Date-derived deterministic seed; honours an explicit `--seed` override."""
    if override is not None:
        return override
    # days since epoch gives a stable per-day integer; mixing in a constant
    # keeps this seed space distinct from other callers using date.toordinal().
    return txn_date.toordinal() * 1_000_003 + 17


def generate_rows(
    txn_date: date,
    transaction_count: int,
    max_lines: int,
    seed_override: Optional[int] = None,
) -> list[PosLine]:
    # Seed the module-level `random` instance so the *entire* row set
    # (transaction counts, line counts, quantities, revenues, store/product
    # selection, loyalty_id nullness, is_voided) is reproducible for a given
    # --date. Re-running with the same args produces byte-identical Parquet.
    random.seed(_derive_seed(txn_date, seed_override))
    rows: list[PosLine] = []
    for n in range(transaction_count):
        # uuid5 is deterministic from (namespace, name) — replays for the
        # same date yield the same transaction_id, which means Bronze
        # partitions can be regenerated exactly. (uuid4 would give a fresh
        # id per run and break the "same input -> same output" contract.)
        txn_id = str(uuid.uuid5(_TXN_NAMESPACE, f"{txn_date.isoformat()}#{n}"))
        for line_no in range(1, random.randint(1, max_lines) + 1):
            rows.append(PosLine.generate(txn_id, line_no, txn_date))
    return rows


def rows_to_table(rows: list[PosLine]) -> pa.Table:
    if not rows:
        raise ValueError("Cannot build Parquet table from zero rows")
    return pa.Table.from_pylist(
        [
            {
                "transaction_id": row.transaction_id,
                "line_item_number": row.line_item_number,
                "transaction_date": row.transaction_date,
                "store_id": row.store_id,
                "product_id": row.product_id,
                "loyalty_id": row.loyalty_id,
                "quantity_sold": row.quantity_sold,
                "gross_revenue": row.gross_revenue,
                "net_revenue": row.net_revenue,
                "gross_margin": row.gross_margin,
                "is_voided": row.is_voided,
            }
            for row in rows
        ],
        schema=POS_SCHEMA,
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected s3://bucket/prefix URI, got: {uri}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return parsed.netloc, prefix


def upload_parquet(output_s3: str, txn_date: str, table: pa.Table) -> str:
    import boto3  # lazy: only the S3 path needs the AWS SDK; local Parquet writes don't

    bucket, prefix = parse_s3_uri(output_s3)
    key = f"{prefix}data/dt={txn_date}/part-00000.parquet"
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    payload = buffer.getvalue()
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=payload)
    uri = f"s3://{bucket}/{key}"
    log.info("pos_parquet_uploaded", uri=uri, rows=table.num_rows, bytes=len(payload))
    return uri


def write_local_parquet(output_dir: str, txn_date: str, table: pa.Table) -> str:
    """Write POS Parquet under a local Iceberg-style bronze prefix.

    Layout (matches Spectrum Hive partition naming used in cloud)::

        <output_dir>/data/dt=<YYYY-MM-DD>/part-00000.parquet

    Pass ``--output-dir .local/iceberg/bronze/pos_transactions`` for the
    local Flink warehouse bind-mount so ``load_iceberg_to_duckdb.py`` can
    pick it up alongside stream tables.
    """
    root = Path(output_dir)
    part_dir = root / "data" / f"dt={txn_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / "part-00000.parquet"
    pq.write_table(table, path, compression="snappy")
    log.info("pos_parquet_written", path=str(path), rows=table.num_rows)
    return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate daily POS Parquet bronze. "
            "Upload to S3 (--output-s3) or write locally (--output-dir)."
        )
    )
    parser.add_argument("--date", default=date.today().isoformat(), help="Transaction date YYYY-MM-DD.")
    parser.add_argument(
        "--output-s3",
        default=None,
        help=(
            "S3 prefix for bronze POS Parquet (trailing slash optional). "
            "Default from POS_BRONZE_S3_PATH when --output-dir is omitted."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Local directory for bronze POS Parquet "
            "(writes data/dt=<date>/part-00000.parquet). "
            "Use for local dbt/DuckDB fidelity instead of --output-s3."
        ),
    )
    parser.add_argument("--transaction-count", type=int, default=5000)
    parser.add_argument("--max-lines-per-txn", type=int, default=5)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "Optional integer seed to override the date-derived default. "
            "Default is deterministic from --date so replays produce "
            "byte-identical Parquet."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.output_dir and args.output_s3:
        raise SystemExit("Pass only one of --output-dir or --output-s3.")
    txn_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    rows = generate_rows(
        txn_date,
        args.transaction_count,
        args.max_lines_per_txn,
        seed_override=args.seed,
    )
    table = rows_to_table(rows)
    if args.output_dir:
        path = write_local_parquet(args.output_dir, txn_date.isoformat(), table)
        print(f"Wrote {len(rows)} line items to {path}")
        return
    output_s3 = args.output_s3 or os.environ.get(
        "POS_BRONZE_S3_PATH",
        "s3://retail-platform-dev-bronze/iceberg/bronze/pos_transactions/",
    )
    uri = upload_parquet(output_s3, txn_date.isoformat(), table)
    print(f"Uploaded {len(rows)} line items to {uri}")


if __name__ == "__main__":
    main()
