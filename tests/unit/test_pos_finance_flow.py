"""Static contracts for the POS → finance.fact_sales path."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAG = (
    _REPO / "orchestration" / "airflow" / "dags" / "warehouse_daily_batch_pipeline.py"
)
_REDSHIFT = _REPO / "transformation" / "redshift"
_BOOTSTRAP = _REPO / "scripts" / "cloud" / "bootstrap_redshift.ps1"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _model_columns(model: str) -> set[str]:
    """Output column names of a mart's final select (`expr as name` or `alias.name`)."""
    src = _read(_DBT / "models" / "marts" / "finance" / f"{model}.sql")
    final = src[src.rindex("\nselect") : src.index("\nfrom", src.rindex("\nselect"))]
    columns = set()
    for line in final.splitlines():
        line = line.strip().rstrip(",")
        if line and line != "select":
            columns.add(re.split(r"\s+as\s+|\.", line)[-1].strip())
    return columns


def _ddl_columns(ddl: str) -> set[str]:
    create = ddl.index("CREATE TABLE")
    body = ddl[ddl.index("(", create) + 1 : ddl.index("PRIMARY KEY")]
    return {line.split()[0] for line in body.splitlines() if line.strip()}


class TestPosFinanceFlow:
    def test_fact_sales_reads_staging_not_bronze(self) -> None:
        src = _read(_DBT / "models" / "marts" / "finance" / "fact_sales.sql")
        assert "ref('stg_pos_transactions')" in src
        assert "source('bronze'" not in src
        assert "{{ product_versions() }}" in src
        assert "product_as_of('p', 'b.transaction_date')" in src
        assert "as-of SCD2" in src
        assert "and dt >=" in src

    def test_stg_pos_exposes_spectrum_dt(self) -> None:
        src = _read(_DBT / "models" / "staging" / "stg_pos_transactions.sql")
        assert " as dt" in src or "\ndt\n" in src
        assert "source('bronze', 'pos_transactions')" in src

    def test_ddl_matches_model_loyalty_id(self) -> None:
        assert "loyalty_id" in _read(_REDSHIFT / "ddl" / "07_fact_sales.sql")
        model = _read(_DBT / "models" / "marts" / "finance" / "fact_sales.sql")
        assert "b.loyalty_id" in model

    @pytest.mark.parametrize(
        ("model", "ddl"),
        [
            ("fact_sales", "07_fact_sales.sql"),
            ("fact_returns", "13_fact_returns.sql"),
            ("fact_inventory_snapshot", "08_fact_inventory_snapshot.sql"),
        ],
    )
    def test_redshift_ddl_has_the_model_columns(self, model: str, ddl: str) -> None:
        assert _ddl_columns(_read(_REDSHIFT / "ddl" / ddl)) == _model_columns(model)

    def test_bootstrap_creates_returns_and_publication_tables(self) -> None:
        src = _read(_BOOTSTRAP)
        assert "'ddl/12_inventory_publication.sql'" in src
        assert "'ddl/13_fact_returns.sql'" in src

    def test_spectrum_declares_returns_and_void_columns(self) -> None:
        src = _read(_REDSHIFT / "spectrum" / "bronze_external_tables.sql")
        pos = src[src.index("CREATE EXTERNAL TABLE bronze.pos_transactions") :]
        for column in ("line_status", "transaction_status", "voided_at", "void_reason", "source_updated_at"):
            assert column in pos[: pos.index("PARTITIONED BY")]
        returns = src[src.index("CREATE EXTERNAL TABLE bronze.pos_returns") :]
        assert "iceberg/bronze/pos_returns/data/'" in returns
        from ingestion.batch.generate_pos_returns_parquet import RETURNS_SCHEMA

        declared = returns[returns.index("(") + 1 : returns.index("PARTITIONED BY")]
        assert {line.split()[0] for line in declared.splitlines() if line.strip().rstrip(")")} == set(
            RETURNS_SCHEMA.names
        )

    def test_warehouse_registers_spectrum_partition(self) -> None:
        src = _read(_DAG)
        assert "register_pos_spectrum_partition" in src
        # Rendered SQL per daily/restatement run is checked in test_pos_restatement.py.
        assert "ADD IF NOT EXISTS" in src and "PARTITION (dt='{{ dt }}')" in src
        assert "pos_bronze_s3_path" in src
        assert "generate_pos_parquet" in src
        assert "pos_returns_bronze_s3_path" in src
        assert "register_pos_returns_spectrum_partition" in src
        assert (
            ">> generate_pos_parquet\n"
            "        >> generate_pos_returns_parquet\n"
            "        >> register_pos_partition\n"
            "        >> register_pos_returns_partition\n"
            "        >> dbt_staging"
        ) in src
        assert "run_date" not in src
        assert "MSCK" not in src
