"""Unit tests for the producer simulators' event-generation logic.

Only the pure ``generate()`` constructors are exercised — no Kafka broker is
touched (``confluent_kafka`` is stubbed in conftest). A fixed RNG seed plus a
large sample makes the probabilistic branches deterministic enough to assert
invariants over.
"""

from __future__ import annotations

import random
import re
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ingestion.kafka.producer_sim.clickstream_producer import (
    INACTIVITY_GAP,
    SCENARIO_ACCOUNTS,
    Ids,
    acceptance_scenarios,
    identity_change_messages,
    load_messages,
)
from ingestion.kafka.producer_sim.inventory_producer import (
    QTY_BY_EVENT_TYPE,
    InventoryEvent,
    message_key,
    next_message,
)
from ingestion.kafka.producer_sim.pos_producer import PosTransactionLine
from ingestion.kafka.sim_entities import (
    ACCOUNT_IDS,
    IDENTITY_CHANGE_ACCOUNT,
    IDENTITY_CHANGE_CARD,
    IDENTITY_CHANGE_CLIENT,
    IDENTITY_CHANGE_OTHER_CARD,
    LOYALTY_IDS,
    PRODUCTS,
    STORES,
    verified_loyalty_id,
)
from spark.identity_resolution.event_contract import parse_event
from streaming.flink_jobs.event_types import (
    CLICKSTREAM_EVENT_TYPES,
    INVENTORY_EVENT_TYPES,
    PLATFORMS,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _seeded_rng():
    random.seed(1234)
    yield


ANCHOR = datetime(2026, 11, 7, 6, 0, tzinfo=timezone.utc)


def _scenario(name):
    return [m.event for m in acceptance_scenarios(ANCHOR) if m.scenario == name and m.kind == "event"]


def _contract(event):
    return parse_event(event.payload(), "clickstream")


def _check_device_timeline(events):
    """Producer invariants per device, in event-time order."""
    by_client = {}
    for event in events:
        by_client.setdefault(event.client_id, []).append(event)
    for timeline in by_client.values():
        timeline.sort(key=lambda e: e.event_time)
        previous = None
        for event in timeline:
            if previous is not None and event.session_instance_id == previous.session_instance_id:
                gap = datetime.fromisoformat(event.event_time) - datetime.fromisoformat(previous.event_time)
                assert gap <= INACTIVITY_GAP
                assert previous.event_type != "logout"
                if previous.customer_id and event.customer_id:
                    assert previous.customer_id == event.customer_id
            if event.event_type in ("login", "logout"):
                assert event.customer_id is not None
            previous = event


class TestClickstreamScenarios:
    def test_every_scenario_event_passes_the_replay_contract(self):
        for message in acceptance_scenarios(ANCHOR):
            event = message.event
            assert event.schema_version == "1.3.0"
            assert event.event_type in CLICKSTREAM_EVENT_TYPES
            assert event.platform in PLATFORMS
            assert _contract(event)["error_reason"] is None, message
            assert datetime.fromisoformat(event.event_time) <= ANCHOR

    def test_same_anchor_repeats_the_same_bytes(self):
        first = [m.event.payload() for m in acceptance_scenarios(ANCHOR)]
        again = [m.event.payload() for m in acceptance_scenarios(ANCHOR)]
        later = [m.event.payload() for m in acceptance_scenarios(ANCHOR + timedelta(hours=1))]
        assert first == again
        assert set(first).isdisjoint(later)

    def test_journey_is_one_attributable_session(self):
        events = _scenario("journey")
        assert [e.event_type for e in events] == ["product_view", "login", "add_to_cart", "checkout", "logout"]
        assert len({e.session_instance_id for e in events}) == 1
        assert [e.customer_id for e in events] == [None] + ["CUST-000001"] * 4
        login = events[1].properties
        assert login["loyalty_id"] == verified_loyalty_id("CUST-000001")
        assert login["loyalty_link_verified"] is True
        assert re.fullmatch(r"ORDER-[0-9A-F]{12}", events[3].properties["order_id"])

    def test_inactivity_gap_keeps_session_id_but_splits_the_instance(self):
        events = _scenario("inactivity_gap")
        assert len({e.session_id for e in events}) == 1
        instances = [e.session_instance_id for e in events]
        assert instances[0] == instances[1] != instances[2]
        assert all(e.customer_id is None for e in events)

    def test_shared_tablet_keeps_each_person_in_their_own_instance(self):
        events = _scenario("shared_tablet")
        assert len({e.client_id for e in events}) == 1
        customers_by_instance = {}
        for event in events:
            customers_by_instance.setdefault(event.session_instance_id, set()).add(event.customer_id)
        assert sorted(customers_by_instance.values(), key=sorted) == [{"CUST-000002"}, {"CUST-000003"}]

    def test_dual_accounts_are_verified_to_one_loyalty_card(self):
        logins = [e for e in _scenario("dual_accounts") if e.event_type == "login"]
        assert {e.customer_id for e in logins} == {"CUST-000004", "CUST-010004"}
        assert {e.properties["loyalty_id"] for e in logins} == {"LOYAL-000004"}
        assert {verified_loyalty_id(e.customer_id) for e in logins} == {"LOYAL-000004"}
        assert len({e.client_id for e in logins}) == 2

    def test_consent_change_ends_opted_out_of_marketing(self):
        statements = [e for e in _scenario("consent_change") if "marketing_consent" in e.properties]
        assert [e.properties["marketing_consent"] for e in statements] == [True, False]
        assert [e.properties["consent_source"] for e in statements] == ["login_prompt", "preference_center"]
        assert all(e.customer_id == "CUST-000005" for e in statements)

    def test_unverified_loyalty_claim_is_marked_unverified(self):
        login = next(e for e in _scenario("unverified_loyalty") if e.event_type == "login")
        assert login.properties["loyalty_id"] == "LOYAL-000099"
        assert login.properties["loyalty_link_verified"] is False
        assert verified_loyalty_id("CUST-000006") != "LOYAL-000099"

    def test_late_and_retry_delivery_order(self):
        messages = [m for m in acceptance_scenarios(ANCHOR) if m.scenario == "late_and_retry"]
        kinds = [m.kind for m in messages]
        assert kinds == ["event", "event", "retry", "late"]
        retry = messages[2].event
        assert retry.payload() == messages[0].event.payload()
        late = messages[3].event
        assert messages[0].event.event_time < late.event_time < messages[1].event.event_time

    def test_scenario_timelines_hold_the_producer_invariants(self):
        _check_device_timeline([m.event for m in acceptance_scenarios(ANCHOR) if m.kind == "event"])


class TestClickstreamLoad:
    def _sample(self, count=5000):
        clock = [ANCHOR]

        def now():
            clock[0] += timedelta(seconds=1)
            return clock[0]

        stream = load_messages(random.Random(7), Ids(seed="load-test"), now, device_pool=300)
        return [next(stream) for _ in range(count)]

    def test_same_release_time_does_not_order_the_events_themselves(self):
        import heapq

        events = self._sample(2)
        with pytest.raises(TypeError):
            sorted([(1.0, 0, events[0], b"a"), (1.0, 0, events[1], b"b")])
        held: list = []
        for seq, event in enumerate(events):
            heapq.heappush(held, (1.0, seq, event, event.payload()))
        released = [item[1] for item in sorted(held, key=lambda item: (item[0], item[1]))]
        assert released == [0, 1]

    def test_late_hold_uses_its_own_sequence(self):
        src = Path(__file__).resolve().parents[2].joinpath(
            "ingestion/kafka/producer_sim/clickstream_producer.py"
        ).read_text(encoding="utf-8")
        push = src.split("heappush(held,", 1)[1].split(")", 1)[0]
        assert "seq" in push and "emitted" not in push
        assert "key=lambda item: (item[0], item[1])" in src

    def test_load_events_pass_the_replay_contract(self):
        for event in self._sample(2000):
            assert _contract(event)["error_reason"] is None, event

    def test_load_devices_are_reused_with_coherent_sessions(self):
        events = self._sample()
        _check_device_timeline(events)
        visits = {}
        for event in events:
            visits.setdefault(event.client_id, set()).add(event.session_instance_id)
        assert any(len(instances) > 1 for instances in visits.values())

    def test_load_has_anonymous_and_signed_in_journeys(self):
        events = self._sample()
        assert any(e.customer_id is None for e in events)
        assert any(e.event_type == "login" for e in events)
        assert any(e.event_type == "checkout" for e in events)
        assert all(e.customer_id not in SCENARIO_ACCOUNTS for e in events)
        assert all(e.client_id.startswith("CLIENT-") for e in events)

    def test_some_load_devices_are_shared_by_two_accounts(self):
        events = self._sample()
        accounts = {}
        for event in events:
            if event.customer_id:
                accounts.setdefault(event.client_id, set()).add(event.customer_id)
        assert any(len(found) > 1 for found in accounts.values())


class TestPosTransactionLine:
    def test_passthrough_keys(self):
        line = PosTransactionLine.generate("TXN-1", 3)
        assert line.transaction_id == "TXN-1"
        assert line.line_item_number == 3

    def test_value_invariants_over_sample(self):
        for i in range(500):
            line = PosTransactionLine.generate("TXN", i)
            assert 1 <= line.quantity_sold <= 4
            assert line.gross_revenue > 0
            assert line.net_revenue <= line.gross_revenue
            assert line.gross_margin >= 0
            assert isinstance(line.is_voided, bool)
            assert line.store_id in STORES
            assert line.product_id in PRODUCTS
            assert line.loyalty_id is None or line.loyalty_id in LOYALTY_IDS


class TestInventoryEvent:
    ALLOWED_TYPES = set(INVENTORY_EVENT_TYPES)

    def test_schema_version_pinned(self):
        assert InventoryEvent.generate().schema_version == "2.0.0"

    def test_on_time_event_not_flagged_late(self):
        assert InventoryEvent.generate().is_late is False

    def test_late_event_flagged(self):
        assert InventoryEvent.generate(late_seconds=120).is_late is True

    def test_boundary_late_seconds_not_late(self):
        # is_late is strictly > 60
        assert InventoryEvent.generate(late_seconds=60).is_late is False

    def test_event_id_passthrough_for_duplicates(self):
        ev = InventoryEvent.generate(event_id="EVT-FIXED")
        assert ev.event_id == "EVT-FIXED"

    def test_invariants_over_sample(self):
        for _ in range(500):
            ev = InventoryEvent.generate()
            assert ev.event_type in self.ALLOWED_TYPES
            assert ev.qty_delta in QTY_BY_EVENT_TYPE[ev.event_type]
            assert ev.store_id in STORES
            assert ev.product_id in PRODUCTS
            assert re.match(r"^SCANNER-\d{2}$", ev.scanner_id)
            assert re.match(r"^\d+\.\d+\.\d+$", ev.schema_version)

    def test_quantity_sign_follows_event_type(self):
        events = [InventoryEvent.generate() for _ in range(2000)]
        by_type = {t: [e.qty_delta for e in events if e.event_type == t] for t in self.ALLOWED_TYPES}
        assert all(q < 0 for q in by_type["sale_deduction"])
        assert all(q < 0 for q in by_type["transfer"])
        assert all(q > 0 for q in by_type["receipt"])
        assert {q > 0 for q in by_type["adjustment"]} == {True, False}
        assert set(QTY_BY_EVENT_TYPE) == self.ALLOWED_TYPES


class TestInventoryScenarios:
    RATES = {"duplicate_pct": 0.1, "conflict_pct": 0.1, "late_event_pct": 0.3}

    def _seed(self):
        recent = deque(maxlen=10)
        _, original, payload = next_message(0.99, recent, **self.RATES)
        return recent, original, payload

    def test_retry_resends_the_original_bytes_and_key(self):
        recent, original, payload = self._seed()
        kind, event, resent = next_message(0.05, recent, **self.RATES)
        assert kind == "retry"
        assert resent == payload
        assert message_key(event) == message_key(original)
        assert len(recent) == 1

    def test_conflict_keeps_id_and_key_but_changes_the_payload(self):
        recent, original, payload = self._seed()
        kind, event, sent = next_message(0.15, recent, **self.RATES)
        assert kind == "conflict"
        assert event.event_id == original.event_id
        assert message_key(event) == message_key(original)
        assert event.qty_delta != original.qty_delta
        assert event.event_time > original.event_time
        assert sent != payload
        assert len(recent) == 1

    def test_no_retry_before_any_event_was_sent(self):
        kind, _, _ = next_message(0.05, deque(maxlen=10), **self.RATES)
        assert kind == "late"


class TestIdentityChange:
    def _logins(self, phase: int):
        messages = identity_change_messages(phase, datetime(2026, 11, 8, tzinfo=timezone.utc))
        return [m.event for m in messages if m.event.event_type == "login"]

    def test_ids_are_outside_the_random_ranges(self):
        assert IDENTITY_CHANGE_ACCOUNT not in ACCOUNT_IDS
        assert IDENTITY_CHANGE_CARD not in LOYALTY_IDS
        assert IDENTITY_CHANGE_OTHER_CARD not in LOYALTY_IDS

    def test_phase_1_names_the_card_without_verifying_it(self):
        logins = self._logins(1)
        assert len(logins) == 1
        assert logins[0].customer_id == IDENTITY_CHANGE_ACCOUNT
        assert logins[0].client_id == IDENTITY_CHANGE_CLIENT
        assert logins[0].properties["loyalty_id"] == IDENTITY_CHANGE_CARD
        assert logins[0].properties["loyalty_link_verified"] is False

    def test_phase_2_verifies_both_cards_on_separate_instances(self):
        logins = self._logins(2)
        assert [e.properties["loyalty_id"] for e in logins] == [IDENTITY_CHANGE_CARD, IDENTITY_CHANGE_OTHER_CARD]
        assert all(e.properties["loyalty_link_verified"] is True for e in logins)
        assert len({e.session_instance_id for e in logins}) == 2

    def test_the_lower_card_stays_the_anchor(self):
        from spark.identity_resolution.graph_logic import PREFIX_LOYALTY, node_sort_key

        assert node_sort_key(PREFIX_LOYALTY + IDENTITY_CHANGE_CARD) < node_sort_key(
            PREFIX_LOYALTY + IDENTITY_CHANGE_OTHER_CARD
        )
