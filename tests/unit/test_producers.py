"""Unit tests for the producer simulators' event-generation logic.

Only the pure ``generate()`` constructors are exercised — no Kafka broker is
touched (``confluent_kafka`` is stubbed in conftest). A fixed RNG seed plus a
large sample makes the probabilistic branches deterministic enough to assert
invariants over.
"""

from __future__ import annotations

import random
import re

import pytest

from ingestion.kafka.producer_sim.clickstream_producer import ClickstreamEvent
from ingestion.kafka.producer_sim.inventory_producer import InventoryEvent
from ingestion.kafka.producer_sim.pos_producer import PosTransactionLine
from ingestion.kafka.sim_entities import LOYALTY_IDS, PRODUCTS, STORES
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


class TestClickstreamEvent:
    def test_schema_version_pinned(self):
        assert ClickstreamEvent.generate().schema_version == "1.2.0"

    def test_invariants_over_sample(self):
        product_re = re.compile(r"^PROD-\d{4}$")
        for _ in range(500):
            ev = ClickstreamEvent.generate()
            assert ev.event_type in CLICKSTREAM_EVENT_TYPES
            assert ev.platform in PLATFORMS
            assert ev.customer_id is None or ev.customer_id.startswith("LOYAL-")
            if ev.event_type == "login":
                assert "marketing_consent" in ev.properties
            assert ev.session_id.startswith("SESS-")
            assert ev.client_id.startswith("CLIENT-")
            assert product_re.match(ev.properties["product_id"])
            # order_id exists iff this was a checkout event
            if ev.event_type == "checkout":
                assert ev.properties["order_id"].startswith("ORDER-")
            else:
                assert "order_id" not in ev.properties

    def test_checkout_branch_is_actually_covered(self):
        # Guard against the sample never hitting the conditional branch.
        assert any(
            ClickstreamEvent.generate().event_type == "checkout" for _ in range(2000)
        )


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
    ALLOWED_DELTAS = {-3, -2, -1, 5, 10, 20}
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
            assert ev.qty_delta in self.ALLOWED_DELTAS
            assert ev.event_type in self.ALLOWED_TYPES
            assert ev.store_id in STORES
            assert ev.product_id in PRODUCTS
            assert re.match(r"^SCANNER-\d{2}$", ev.scanner_id)
            assert re.match(r"^\d+\.\d+\.\d+$", ev.schema_version)
