"""Copy marketing_pending.dim_product to live marketing on a cold DuckDB.

Local ``-Task dbt`` builds catalog + warehouse in one process. Finance facts
join live ``marketing.dim_product`` via ``wap_live_ref`` (catalog DAG owns
the table on cloud). A first run has no live table yet; this script publishes
the pending SCD2 table once so those joins resolve. Later runs clone live
into pending as usual (ADR-009).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def bootstrap(duckdb_path: Path) -> str:
    con = duckdb.connect(str(duckdb_path))
    try:
        has_pending = con.execute(
            """
            select count(*) from information_schema.tables
            where table_schema = 'marketing_pending' and table_name = 'dim_product'
            """
        ).fetchone()[0]
        has_live = con.execute(
            """
            select count(*) from information_schema.tables
            where table_schema = 'marketing' and table_name = 'dim_product'
            """
        ).fetchone()[0]
        if has_pending and not has_live:
            con.execute("CREATE SCHEMA IF NOT EXISTS marketing")
            con.execute(
                "CREATE TABLE marketing.dim_product AS "
                "SELECT * FROM marketing_pending.dim_product"
            )
            return "copied marketing_pending.dim_product -> marketing.dim_product"
        if has_live:
            return "live marketing.dim_product already present"
        raise SystemExit(
            "dim_product pending table missing after dbt run --select dim_product"
        )
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True, type=Path)
    args = parser.parse_args()
    print(bootstrap(args.duckdb))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
