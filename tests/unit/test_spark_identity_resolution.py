"""Unit tests for the Spark identity-resolution reference logic (ADR-010).

``graph_logic.py`` is the single source of truth for the identity graph
rules; the PySpark/GraphFrames job mirrors it and the dbt seed fixture is
generated from it. These tests run the same scenarios encoded in
``seeds/bronze/*.csv`` (loyalty match, session link, multi-hop, public
device, singleton) so the rules stay pinned without a Spark runtime.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from spark.identity_resolution import graph_logic as gl

_REPO = Path(__file__).resolve().parents[2]
_SEEDS = _REPO / "transformation" / "dbt_project" / "seeds"
_DBT = _REPO / "transformation" / "dbt_project"

pytestmark = pytest.mark.unit


def _load_fixture() -> tuple[list[dict], list[dict]]:
    with (_SEEDS / "bronze" / "clickstream_events.csv").open(newline="", encoding="utf-8") as fh:
        clickstream = list(csv.DictReader(fh))
    with (_SEEDS / "bronze" / "pos_transactions.csv").open(newline="", encoding="utf-8") as fh:
        pos = list(csv.DictReader(fh))
    return clickstream, pos


@pytest.fixture(scope="module")
def resolved() -> tuple[list[gl.Edge], list[gl.ResolutionRow], set[str]]:
    clickstream, pos = _load_fixture()
    return gl.resolve_identity_graph(clickstream, pos)


class TestCustomerKeyParity:
    """Spark/dbt/DuckDB must produce identical keys (dbt generate_customer_key)."""

    def test_known_vector(self) -> None:
        # md5('loyalty:L1001')[:8] = '47e4e738' -> int(...) % 1e8 + 1
        assert gl.customer_key("loyalty:L1001") == int("47e4e738", 16) % 100_000_000 + 1

    def test_range_and_determinism(self) -> None:
        for node in ("loyalty:L1001", "customer:C2001", "client:c-aaa"):
            key = gl.customer_key(node)
            assert 1 <= key < 100_000_001
            assert key == gl.customer_key(node)


class TestEdges:
    def test_session_and_loyalty_edges(self, resolved) -> None:
        edges, _, _ = resolved
        found_session = {(e.src, e.dst) for e in edges if e.edge_type == gl.EDGE_SESSION_LINK}
        found_loyalty = {(e.src, e.dst) for e in edges if e.edge_type == gl.EDGE_LOYALTY_VALUE_MATCH}
        assert found_session == {
            ("client:c-aaa", "customer:L1001"),
            ("client:c-aaa", "customer:C2001"),
            ("client:c-bbb", "customer:L2002"),
        }
        assert found_loyalty == {
            ("loyalty:L1001", "customer:L1001"),
            ("loyalty:L2002", "customer:L2002"),
        }

    def test_public_device_excluded_from_edges(self, resolved) -> None:
        edges, _, public_devices = resolved
        assert public_devices == {"c-shared"}
        assert all("c-shared" not in (e.src + e.dst) for e in edges)

    def test_threshold_boundary(self) -> None:
        pairs = [("c-x", f"C-{i:02d}", f"2026-07-01 10:{i:02d}:00") for i in range(9)]
        assert gl.find_public_devices(pairs, threshold=10) == set()
        pairs.append(("c-x", "C-10", "2026-07-01 10:10:00"))
        assert gl.find_public_devices(pairs, threshold=10) == {"c-x"}

    def test_blank_and_whitespace_identifiers_are_not_nodes(self) -> None:
        clickstream = [
            {
                "client_id": " client-1 ",
                "customer_id": " ",
                "event_time": "2026-08-30T00:00:00Z",
            },
            {
                "client_id": " client-2 ",
                "customer_id": " customer-2 ",
                "event_time": "2026-08-30T00:00:01Z",
            },
            {
                "client_id": "",
                "customer_id": "customer-3",
                "event_time": "2026-08-30T00:00:02Z",
            },
        ]
        pos = [{"loyalty_id": " "}, {"loyalty_id": " customer-2 "}]

        edges, rows, _ = gl.resolve_identity_graph(clickstream, pos)
        nodes = {
            f"{row.identifier_type}:{row.identifier_value}"
            for row in rows
        }

        assert "customer_id:" not in nodes
        assert "client_id:" not in nodes
        assert "loyalty_id:" not in nodes
        assert "client_id:client-1" in nodes
        assert "client_id:client-2" in nodes
        assert "customer_id:customer-2" in nodes
        assert "loyalty_id:customer-2" in nodes
        assert all(edge.src not in {"client:", "customer:", "loyalty:"} for edge in edges)
        assert all(edge.dst not in {"client:", "customer:", "loyalty:"} for edge in edges)


class TestResolution:
    def _row(self, rows: list[gl.ResolutionRow], type_: str, value: str) -> gl.ResolutionRow:
        matches = [r for r in rows if r.identifier_type == type_ and r.identifier_value == value]
        assert len(matches) == 1, f"expected 1 row for {type_}:{value}, got {len(matches)}"
        return matches[0]

    def test_multi_hop_component_shares_anchor_key(self, resolved) -> None:
        _, rows, _ = resolved
        members = [
            self._row(rows, "loyalty_id", "L1001"),
            self._row(rows, "customer_id", "L1001"),
            self._row(rows, "client_id", "c-aaa"),
            self._row(rows, "customer_id", "C2001"),
        ]
        assert {r.customer_key for r in members} == {gl.customer_key("loyalty:L1001")}
        assert {r.component_rep_node for r in members} == {"loyalty:L1001"}

    def test_methods_and_confidence(self, resolved) -> None:
        _, rows, _ = resolved
        anchor = self._row(rows, "loyalty_id", "L1001")
        assert (anchor.resolution_method, anchor.confidence_score) == ("component_anchor", 1.0)
        linked = self._row(rows, "client_id", "c-aaa")
        assert (linked.resolution_method, linked.confidence_score) == ("session_linked", 0.85)
        solo = self._row(rows, "client_id", "c-solo")
        assert (solo.resolution_method, solo.confidence_score) == ("device_only", 0.5)
        standalone = self._row(rows, "customer_id", "C-pub-01")
        assert (standalone.resolution_method, standalone.confidence_score) == (
            "customer_id_standalone",
            0.9,
        )

    def test_public_device_gets_own_key(self, resolved) -> None:
        _, rows, _ = resolved
        shared = self._row(rows, "client_id", "c-shared")
        assert shared.is_public_device is True
        assert shared.resolution_method == "public_device_excluded"
        assert shared.confidence_score == pytest.approx(0.3)
        assert shared.customer_key == gl.customer_key("client:c-shared")

    def test_row_count_matches_fixture_universe(self, resolved) -> None:
        _, rows, _ = resolved
        # 2 loyalty + 13 customers (L1001, L2002, C2001, C-pub-01..10) + 4 clients
        assert len(rows) == 19


class TestDbtHandoffContract:
    """The dbt side of the ADR-010 handoff: source declared, thin view, no
    leftover SQL graph models, fixture wired into seeds."""

    def test_silver_source_declared(self) -> None:
        src = (_DBT / "models/staging/_sources.yml").read_text(encoding="utf-8")
        assert "identity_resolution" in src

    def test_resolution_is_thin_view_over_source(self) -> None:
        src = (_DBT / "models/intermediate/int_identity_resolution.sql").read_text(encoding="utf-8")
        assert "source('silver', 'identity_resolution')" in src
        assert "materialized='view'" in src
        assert "generate_surrogate_key" in src

    def test_sql_graph_models_retired(self) -> None:
        assert not (_DBT / "models/intermediate/int_identity_edges.sql").exists()
        assert not (_DBT / "models/intermediate/int_identity_components.sql").exists()

    def test_seed_fixture_configured(self) -> None:
        project = (_DBT / "dbt_project.yml").read_text(encoding="utf-8")
        assert "identity_resolution" in project
        seed = _SEEDS / "silver" / "identity_resolution.csv"
        assert seed.exists()
        header = seed.read_text(encoding="utf-8").splitlines()[0]
        assert "identifier_type" in header and "customer_key" in header

    def test_spectrum_reads_current_export_not_iceberg_data_glob(self) -> None:
        ddl = (
            _REPO / "transformation/redshift/spectrum/silver_external_tables.sql"
        ).read_text(encoding="utf-8")
        assert "consumer_current/identity_resolution/" in ddl
        assert "consumer_current/identity_edges/" in ddl
        assert "silver/identity_resolution/data/" not in ddl
