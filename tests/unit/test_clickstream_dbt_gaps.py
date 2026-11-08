"""Static contracts for clickstream dbt gap fixes (Part 14)."""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_DBT = _REPO / "transformation" / "dbt_project"
_DAGS = _REPO / "orchestration" / "airflow" / "dags"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestClickstreamDbtGapFixes:
    def test_session_keys_on_instance_and_attributes_via_associations(self) -> None:
        src = _read(_DBT / "models/intermediate/int_session_reconstruction.sql")
        assert "json_path_text(" in src
        assert "json_extract_path_text(properties" not in src
        assert "session_instance_id" in src
        assert "source('silver', 'device_session_associations')" in src
        assert "account_count = 1" in src
        assert "'client_id'" not in src

    def test_fact_session_restates_on_fingerprint(self) -> None:
        src = _read(_DBT / "models/marts/marketing/fact_customer_session.sql")
        assert "session_fingerprint <> s.session_fingerprint" in src
        assert "column_exists(this, 'session_fingerprint')" in src
        assert "post_hook" in src
        assert "dateadd(" not in src

    def test_rfm_counts_confirmed_pos_revenue_only(self) -> None:
        src = _read(_DBT / "models/intermediate/int_rfm_scoring.sql")
        assert "stg_pos_transactions" in src
        assert "int_session_reconstruction" not in src
        assert "current_date" not in src

    def test_consent_has_no_loyalty_default(self) -> None:
        src = _read(_DBT / "models/marts/marketing/dim_customer.sql")
        assert "coalesce(c.marketing_consent, false)" in src
        assert "coalesce(c.analytics_consent, false)" in src
        consent = _read(_DBT / "models/intermediate/int_customer_consent.sql")
        assert "client_id" not in consent

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
        # Live staging/intermediate models come from the single-owner sets in
        # plugins/dbt_selectors.py (stg_pos_transactions is warehouse-owned,
        # int_product_catalog catalog-owned).
        pending = src.split("SELECT_PENDING = (", 1)[1].split(")\n", 1)[0]
        assert "select(MARKETING_LIVE_MODELS" in pending
        assert "stg_pos_transactions" not in pending
        assert "customer_360_serving" in selector
        assert "--exclude" in selector
        assert "dim_product" in selector
        # WAP: write phase targets pending; publish promotes to live after audit.
        assert "wap_phase" in selector
        assert "wap_publish_marketing" in selector
        # Must not rebuild all staging (would pull inventory staging hourly).
        assert "--select staging " not in selector
        assert "--select staging\n" not in selector
