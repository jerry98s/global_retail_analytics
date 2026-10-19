"""Static contracts for the POS → finance.fact_sales path."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAG = (
    _REPO / "orchestration" / "airflow" / "dags" / "warehouse_daily_batch_pipeline.py"
)
_DDL = _REPO / "transformation" / "redshift" / "ddl" / "07_fact_sales.sql"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestPosFinanceFlow:
    def test_fact_sales_reads_staging_not_bronze(self) -> None:
        src = _read(_DBT / "models" / "marts" / "finance" / "fact_sales.sql")
        assert "ref('stg_pos_transactions')" in src
        assert "source('bronze'" not in src
        assert "ref('dim_product')" in src or "wap_live_ref('dim_product')" in src
        assert "is_current = true" in src
        assert "as-of SCD2" in src
        assert "and dt >=" in src

    def test_stg_pos_exposes_spectrum_dt(self) -> None:
        src = _read(_DBT / "models" / "staging" / "stg_pos_transactions.sql")
        assert " as dt" in src or "\ndt\n" in src
        assert "source('bronze', 'pos_transactions')" in src

    def test_ddl_matches_model_loyalty_id(self) -> None:
        ddl = _read(_DDL)
        assert "loyalty_id" in ddl
        model = _read(_DBT / "models" / "marts" / "finance" / "fact_sales.sql")
        assert "b.loyalty_id" in model

    def test_warehouse_registers_spectrum_partition(self) -> None:
        src = _read(_DAG)
        assert "register_pos_spectrum_partition" in src
        assert "ADD IF NOT EXISTS PARTITION" in src
        assert "pos_bronze_s3_path" in src
        assert "generate_pos_parquet" in src
        assert src.index("generate_pos_parquet") < src.index("register_pos_partition")
        assert src.index("register_pos_partition") < src.index("dbt_staging")
        assert "run_date" not in src
        assert "MSCK" not in src
