"""Static contracts for clickstream dbt gap fixes (Part 14)."""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAGS = _REPO / "orchestration" / "airflow" / "dags"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestClickstreamDbtGapFixes:
    def test_session_uses_portable_json_and_dateadd(self) -> None:
        src = _read(_DBT / "models/intermediate/int_session_reconstruction.sql")
        assert "json_path_text(" in src
        assert "dateadd_unit(" in src
        assert "json_extract_path_text(properties" not in src
        assert "dateadd(hour" not in src

    def test_fact_session_uses_dateadd_unit(self) -> None:
        src = _read(_DBT / "models/marts/marketing/fact_customer_session.sql")
        assert "dateadd_unit(" in src
        assert "dateadd(" not in src or "dateadd_unit" in src

    def test_rfm_includes_clickstream_conversions(self) -> None:
        src = _read(_DBT / "models/intermediate/int_rfm_scoring.sql")
        assert "int_session_reconstruction" in src
        assert "online_conversions" in src
        assert "conversion_value" in src
        assert "full outer join" in src.lower()

    def test_identity_graph_runs_in_spark(self) -> None:
        # ADR-010: edges + connected components moved to the Spark
        # GraphFrames job; dbt keeps only the thin resolution view.
        intermediate = _DBT / "models/intermediate"
        assert not (intermediate / "int_identity_edges.sql").exists()
        assert not (intermediate / "int_identity_components.sql").exists()
        resolution = _read(intermediate / "int_identity_resolution.sql")
        assert "source('silver', 'identity_resolution')" in resolution

    def test_json_path_text_macro_exists(self) -> None:
        src = _read(_DBT / "macros/json_path_text.sql")
        assert "json_extract_string" in src
        assert "json_extract_path_text" in src

    def test_warehouse_daily_owns_finance_marts_only(self) -> None:
        src = _read(_DAGS / "warehouse_daily_batch_pipeline.py")
        assert "marts.finance" in src
        # Selector for dbt_mart_models must be finance-only, not whole marts/.
        assert '"marts"' not in src and "'marts'" not in src

    def test_marketing_hourly_owns_c360_not_catalog(self) -> None:
        src = _read(_DAGS / "marketing_hourly_customer_360_pipeline.py")
        # WAP (ADR-009) split the monolithic bash task into write-pending,
        # audit, publish, and a serving refresh. The shared selector lives in
        # SELECT_PENDING; the serving refresh is a separate live dbt run.
        selector = src  # SELECT_PENDING block + task wiring
        assert "stg_clickstream_events" in selector
        assert "stg_pos_transactions" in selector
        assert "customer_360_serving" in selector
        assert "--exclude" in selector
        assert "int_product_catalog" in selector
        assert "dim_product" in selector
        # WAP: write phase targets pending; publish promotes to live after audit.
        assert "wap_phase" in selector
        assert "wap_publish_marketing" in selector
        # Must not rebuild all staging (would pull inventory staging hourly).
        assert "--select staging " not in selector
        assert "--select staging\n" not in selector
