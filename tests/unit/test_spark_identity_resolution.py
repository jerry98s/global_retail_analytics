"""Unit tests for the Spark identity-resolution reference logic (ADR-003, ADR-010).

``graph_logic.py`` is the single source of truth for the identity graph
rules; the PySpark/GraphFrames job mirrors it and the dbt seed fixtures are
generated from it. These tests run the scenarios encoded in
``seeds/bronze/clickstream_events.csv`` (verified link, two accounts on one
card, shared tablet, unverified claim, remembered sign-in, ambiguous
instance, legacy v1.2, anonymous device) so the rules stay pinned without a
Spark runtime.
"""

from __future__ import annotations

import csv
import json
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
def fixture_rows() -> tuple[list[dict], list[dict]]:
    return _load_fixture()


@pytest.fixture(scope="module")
def resolved(fixture_rows) -> tuple[list[gl.Edge], list[gl.ResolutionRow], list[gl.LoyaltyClaim]]:
    clickstream, pos = fixture_rows
    return gl.resolve_identity_graph(clickstream, pos)


@pytest.fixture(scope="module")
def associations(fixture_rows) -> list[gl.DeviceSessionAssociation]:
    return gl.device_session_associations(fixture_rows[0])


def _key(rows: list[gl.ResolutionRow], type_: str, value: str) -> int:
    return _row(rows, type_, value).customer_key


def _row(rows: list[gl.ResolutionRow], type_: str, value: str) -> gl.ResolutionRow:
    matches = [r for r in rows if r.identifier_type == type_ and r.identifier_value == value]
    assert len(matches) == 1, f"expected 1 row for {type_}:{value}, got {len(matches)}"
    return matches[0]


def _login(event_id, customer, loyalty=None, verified=None, instance="si-1", client="c-1",
           time="2026-08-30 00:00:00"):
    properties = {} if loyalty is None else {"loyalty_id": loyalty, "loyalty_link_verified": verified}
    return {
        "event_id": event_id, "event_type": "login", "event_time": time,
        "session_instance_id": instance, "client_id": client, "customer_id": customer,
        "properties": json.dumps(properties),
    }


class TestCustomerKeyParity:
    """Spark/dbt/DuckDB must produce identical keys (dbt generate_customer_key)."""

    def test_known_vector(self) -> None:
        # md5('loyalty:L1001')[:8] = '47e4e738' -> int(...) % 1e8 + 1
        assert gl.customer_key("loyalty:L1001") == int("47e4e738", 16) % 100_000_000 + 1

    def test_range_and_determinism(self) -> None:
        for node in ("loyalty:L1001", "customer:C2001"):
            key = gl.customer_key(node)
            assert 1 <= key < 100_000_001
            assert key == gl.customer_key(node)


class TestEdges:
    def test_only_verified_links_become_edges(self, resolved) -> None:
        edges, _, _ = resolved
        assert {(e.src, e.dst) for e in edges} == {
            ("loyalty:L1001", "customer:C1001"),
            ("loyalty:L2002", "customer:C2002"),
            ("loyalty:L2002", "customer:C2003"),
            ("loyalty:L3001", "customer:C3001"),
        }
        assert {e.edge_type for e in edges} == {gl.EDGE_VERIFIED_LOYALTY_LINK}

    def test_no_device_nodes(self, resolved) -> None:
        edges, rows, _ = resolved
        assert all(not e.src.startswith("client:") and not e.dst.startswith("client:") for e in edges)
        assert {r.identifier_type for r in rows} == {"loyalty_id", "customer_id"}

    def test_evidence_is_the_earliest_verifying_login(self) -> None:
        claims = gl.loyalty_claims([
            _login("e2", "C1", "L1", True, time="2026-08-30 02:00:00"),
            _login("e1", "C1", "L1", True, time="2026-08-30 01:00:00"),
        ])
        (edge,) = gl.build_edges(claims)
        assert (edge.evidence_event_id, edge.first_verified_at, edge.last_verified_at) == (
            "e1", "2026-08-30 01:00:00", "2026-08-30 02:00:00",
        )

    def test_unverified_and_pre_v13_claims_create_no_edges(self) -> None:
        claims = gl.loyalty_claims([
            _login("e1", "C1", "L1", False),
            _login("e2", "C2", "L1", "true"),  # a string is not verification
            _login("e3", "C3", "L1", True, instance=""),
        ])
        assert [(c.event_id, c.is_verified) for c in claims] == [("e1", False), ("e2", False)]
        assert gl.build_edges(claims) == []

    def test_claims_audit_includes_the_unverified_claim(self, resolved) -> None:
        _, _, claims = resolved
        unverified = [c for c in claims if not c.is_verified]
        assert [(c.customer_id, c.loyalty_id) for c in unverified] == [("C4001", "L1001")]

    def test_blank_and_whitespace_identifiers_are_not_nodes(self) -> None:
        clickstream = [
            _login("e1", " ", client=" client-1 "),
            _login("e2", " customer-2 ", " L2 ", True, client=" client-2 "),
            _login("e3", "customer-3", client=""),
        ]
        edges, rows, _ = gl.resolve_identity_graph(clickstream, [{"loyalty_id": " "}])
        nodes = {f"{row.identifier_type}:{row.identifier_value}" for row in rows}
        assert nodes == {"customer_id:customer-2", "customer_id:customer-3", "loyalty_id:L2"}
        assert [(e.src, e.dst) for e in edges] == [("loyalty:L2", "customer:customer-2")]


class TestResolution:
    def test_verified_link_shares_the_anchor_key(self, resolved) -> None:
        _, rows, _ = resolved
        assert _key(rows, "customer_id", "C1001") == _key(rows, "loyalty_id", "L1001")
        assert _row(rows, "loyalty_id", "L1001").resolution_method == "component_anchor"
        assert _row(rows, "customer_id", "C1001").resolution_method == "loyalty_match"

    def test_two_accounts_on_one_card_merge(self, resolved) -> None:
        _, rows, _ = resolved
        keys = {_key(rows, "customer_id", c) for c in ("C2002", "C2003")}
        assert keys == {_key(rows, "loyalty_id", "L2002")}

    def test_shared_tablet_keeps_accounts_separate(self, resolved) -> None:
        _, rows, _ = resolved
        assert _key(rows, "customer_id", "C3001") == _key(rows, "loyalty_id", "L3001")
        assert _key(rows, "customer_id", "C3002") != _key(rows, "customer_id", "C3001")
        assert _row(rows, "customer_id", "C3002").resolution_method == "customer_id_standalone"

    def test_unverified_claim_stays_standalone(self, resolved) -> None:
        _, rows, _ = resolved
        c4001 = _row(rows, "customer_id", "C4001")
        assert c4001.customer_key != _key(rows, "loyalty_id", "L1001")
        assert (c4001.resolution_method, c4001.confidence_score) == ("customer_id_standalone", 0.9)

    def test_legacy_value_equality_does_not_merge(self, resolved) -> None:
        _, rows, _ = resolved
        assert _key(rows, "customer_id", "L1001") != _key(rows, "loyalty_id", "L1001")

    def test_rows_match_the_generated_fixture(self, resolved) -> None:
        _, rows, _ = resolved
        with (_SEEDS / "silver" / "identity_resolution.csv").open(newline="", encoding="utf-8") as fh:
            fixture = {(r["identifier_type"], r["identifier_value"]): int(r["customer_key"])
                       for r in csv.DictReader(fh)}
        assert fixture == {(r.identifier_type, r.identifier_value): r.customer_key for r in rows}


class TestDeviceSessionAssociations:
    def _assoc(self, associations, client, instance, customer):
        matches = [a for a in associations
                   if (a.client_id, a.session_instance_id, a.customer_id) == (client, instance, customer)]
        assert len(matches) == 1
        return matches[0]

    def test_shared_tablet_has_one_interval_per_account(self, associations) -> None:
        tablet = sorted((a.session_instance_id, a.customer_id, a.end_reason)
                        for a in associations if a.client_id == "c-tab")
        assert [(i, c) for i, c, _ in tablet] == [("si-t1", "C3001"), ("si-t2", "C3002")]
        assert tablet[0][2] == gl.END_LOGOUT
        assert all(not a.is_ambiguous for a in associations if a.client_id == "c-tab")

    def test_remembered_sign_in_is_signed_in_session_evidence(self, associations) -> None:
        login = self._assoc(associations, "c-eee", "si-e1", "C5001")
        remembered = self._assoc(associations, "c-eee", "si-e2", "C5001")
        assert login.evidence == gl.EVIDENCE_VERIFIED_LOGIN
        assert remembered.evidence == gl.EVIDENCE_SIGNED_IN_SESSION

    def test_two_accounts_in_one_instance_are_ambiguous(self, associations) -> None:
        rows = [a for a in associations if a.client_id == "c-fff"]
        assert {a.customer_id for a in rows} == {"C6001", "C6002"}
        assert all(a.is_ambiguous for a in rows)

    def test_anonymous_and_legacy_devices_have_no_association(self, associations) -> None:
        assert not {"c-solo", "c-legacy"} & {a.client_id for a in associations}

    def test_rows_match_the_generated_fixture(self, associations) -> None:
        with (_SEEDS / "silver" / "device_session_associations.csv").open(newline="", encoding="utf-8") as fh:
            fixture = {(r["client_id"], r["session_instance_id"], r["customer_id"]) for r in csv.DictReader(fh)}
        assert fixture == {(a.client_id, a.session_instance_id, a.customer_id) for a in associations}


class TestDbtHandoffContract:
    """The dbt side of the ADR-010 handoff: sources declared, thin view, no
    leftover SQL graph or public-device models, fixtures wired into seeds."""

    def test_silver_sources_declared(self) -> None:
        src = (_DBT / "models/staging/_sources.yml").read_text(encoding="utf-8")
        assert "identity_resolution" in src
        assert "device_session_associations" in src

    def test_resolution_is_thin_view_over_source(self) -> None:
        src = (_DBT / "models/intermediate/int_identity_resolution.sql").read_text(encoding="utf-8")
        assert "source('silver', 'identity_resolution')" in src
        assert "materialized='view'" in src
        assert "generate_surrogate_key" in src

    def test_retired_models_are_gone(self) -> None:
        for name in ("int_identity_edges", "int_identity_components", "int_identity_public_devices"):
            assert not (_DBT / f"models/intermediate/{name}.sql").exists()

    def test_identity_changes_compares_with_the_published_graph(self) -> None:
        src = (_DBT / "models/intermediate/int_identity_changes.sql").read_text(encoding="utf-8")
        assert "source('gold_marketing', 'identity_graph')" in src
        assert "ref('int_identity_resolution')" in src

    def test_seed_fixtures_configured(self) -> None:
        project = (_DBT / "dbt_project.yml").read_text(encoding="utf-8")
        for name in ("identity_resolution", "device_session_associations"):
            assert name in project
            assert (_SEEDS / "silver" / f"{name}.csv").exists()

    def test_job_publishes_every_export_the_duckdb_bridge_reads(self) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "load_iceberg_to_duckdb", _REPO / "scripts/local/load_iceberg_to_duckdb.py"
        )
        bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bridge)

        job = (_REPO / "spark/identity_resolution/identity_resolution_job.py").read_text(encoding="utf-8")
        events = (_REPO / "spark/identity_resolution/canonical_events.py").read_text(encoding="utf-8")
        for call in (
            'publish(resolution, root, "identity_resolution", run_id)',
            'publish(edges, root, "identity_edges", run_id)',
            'publish(claims, root, "loyalty_link_claims", run_id)',
            'publish(associations, root, "device_session_associations", run_id)',
            'publish(frame, root, "inventory_hourly", run_id or new_run_id())',
        ):
            assert call in job
        assert "publish(frame, root, f'{stream}_{suffix}', run_id)" in events
        assert "publish(gaps, root, 'inventory_offset_gaps', run_id)" in events
        assert set(bridge._EXPORTS) == {
            "clickstream_events", "inventory_events", "inventory_hourly",
            "inventory_offset_gaps", "identity_resolution", "identity_edges",
            "loyalty_link_claims", "device_session_associations",
        }
