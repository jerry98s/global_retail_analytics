"""
Clickstream producer simulator (envelope v1.3.0).

Devices keep their client_id and session_id across visits, so events form
journeys: anonymous browsing, a verified login, cart and checkout, logout.
The producer issues an immutable session_instance_id per visit: a new one
after 30 minutes of inactivity, after a logout, or when another account
signs in. customer_id is set only while an account is signed in.

Two modes:
- load (default): random journeys at a target event rate, with exact
  retries and late deliveries.
- ``--scenario acceptance``: fixed, deterministic journeys for the Customer
  360 checks. The same ``--anchor`` reproduces the same bytes.
"""

from __future__ import annotations

import heapq
import json
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import structlog
from confluent_kafka import Producer

from ingestion.kafka.msk_config import build_producer_config
from ingestion.kafka.sim_entities import ACCOUNT_IDS, PRODUCTS, verified_loyalty_id
from ingestion.kafka.topics import TOPIC_CLICKSTREAM
from streaming.flink_jobs.event_types import PLATFORMS

log = structlog.get_logger()

KAFKA_CONFIG = build_producer_config(
    "clickstream-producer-sim",
    **{
        "batch.size": 500000,
        "linger.ms": 5,
        "queue.buffering.max.messages": 200000,
        "queue.buffering.max.kbytes": 2097152,
    },
)

TOPIC = TOPIC_CLICKSTREAM
SCHEMA_VERSION = "1.3.0"
INACTIVITY_GAP = timedelta(minutes=30)
_ID_NAMESPACE = uuid.UUID("6f1f2b7e-3c55-4d0e-9a51-2a8f0c1d7b42")


@dataclass
class ClickstreamEvent:
    """Canonical clickstream envelope."""

    event_id: str
    event_type: str
    event_time: str
    session_id: str
    session_instance_id: str
    client_id: str
    customer_id: Optional[str]
    platform: str
    app_version: str
    properties: Dict[str, Any]
    schema_version: str = SCHEMA_VERSION

    def payload(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


class Ids:
    """Random ids, or deterministic ones derived from ``seed``."""

    def __init__(self, seed: Optional[str] = None) -> None:
        self._seed = seed
        self._count = 0

    def _uuid(self) -> uuid.UUID:
        if self._seed is None:
            return uuid.uuid4()
        self._count += 1
        return uuid.uuid5(_ID_NAMESPACE, f"{self._seed}:{self._count}")

    def event_id(self) -> str:
        return str(self._uuid())

    def session_id(self) -> str:
        return "SESS-" + self._uuid().hex[:24]

    def session_instance_id(self) -> str:
        return "SI-" + self._uuid().hex[:24]

    def order_id(self) -> str:
        return "ORDER-" + self._uuid().hex[:12].upper()


@dataclass
class Device:
    """One browser or app install and its session state."""

    client_id: str
    platform: str
    app_version: str
    accounts: Tuple[str, ...] = ()
    session_id: Optional[str] = None
    session_instance_id: Optional[str] = None
    customer_id: Optional[str] = None
    last_event_at: Optional[datetime] = None


def _iso(at: datetime) -> str:
    return at.astimezone(timezone.utc).isoformat()


@dataclass
class Visit:
    """Emits a device's events in time order and maintains its session state."""

    device: Device
    ids: Ids
    events: List[ClickstreamEvent] = field(default_factory=list)

    def _emit(self, event_type: str, at: datetime, properties: Dict[str, Any]) -> ClickstreamEvent:
        device = self.device
        if device.last_event_at is not None and at < device.last_event_at:
            raise ValueError(f"{device.client_id}: events must be emitted in time order")
        if device.session_id is None:
            device.session_id = self.ids.session_id()
        if (
            device.session_instance_id is None
            or device.last_event_at is None
            or at - device.last_event_at > INACTIVITY_GAP
        ):
            device.session_instance_id = self.ids.session_instance_id()
        device.last_event_at = at
        event = ClickstreamEvent(
            event_id=self.ids.event_id(),
            event_type=event_type,
            event_time=_iso(at),
            session_id=device.session_id,
            session_instance_id=device.session_instance_id,
            client_id=device.client_id,
            customer_id=device.customer_id,
            platform=device.platform,
            app_version=device.app_version,
            properties=properties,
        )
        self.events.append(event)
        return event

    def browse(self, at: datetime, event_type: str = "page_view", **properties: Any) -> ClickstreamEvent:
        return self._emit(event_type, at, dict(properties))

    def login(
        self,
        at: datetime,
        account_id: str,
        *,
        loyalty_id: Optional[str] = None,
        loyalty_verified: bool = True,
        consent: Optional[Dict[str, bool]] = None,
        consent_source: str = "login_prompt",
    ) -> ClickstreamEvent:
        device = self.device
        if device.customer_id is not None and device.customer_id != account_id:
            device.session_instance_id = None
        device.customer_id = account_id
        properties: Dict[str, Any] = {"page": "login", "auth_method": "password"}
        if loyalty_id is not None:
            properties["loyalty_id"] = loyalty_id
            properties["loyalty_link_verified"] = loyalty_verified
        if consent:
            properties.update(consent)
            properties["consent_source"] = consent_source
        return self._emit("login", at, properties)

    def update_consent(self, at: datetime, consent: Dict[str, bool], source: str) -> ClickstreamEvent:
        if self.device.customer_id is None:
            raise ValueError("consent statements need a signed-in account")
        return self._emit("page_view", at, {"page": "preferences", **consent, "consent_source": source})

    def add_to_cart(self, at: datetime, product_id: str, cart_value: float) -> ClickstreamEvent:
        return self._emit("add_to_cart", at, {"page": "product", "product_id": product_id, "cart_value": cart_value})

    def checkout(self, at: datetime, cart_value: float) -> ClickstreamEvent:
        return self._emit(
            "checkout", at, {"page": "checkout", "order_id": self.ids.order_id(), "cart_value": cart_value}
        )

    def logout(self, at: datetime) -> ClickstreamEvent:
        if self.device.customer_id is None:
            raise ValueError("logout needs a signed-in account")
        event = self._emit("logout", at, {"page": "account"})
        self.device.customer_id = None
        self.device.session_instance_id = None
        return event


# ---------------------------------------------------------------------------
# Acceptance scenarios
# ---------------------------------------------------------------------------

SCENARIO_ACCOUNTS = frozenset(
    {"CUST-000001", "CUST-000002", "CUST-000003", "CUST-000004", "CUST-010004", "CUST-000005", "CUST-000006"}
)


@dataclass(frozen=True)
class Message:
    scenario: str
    event: ClickstreamEvent
    kind: str = "event"  # event | retry | late


def _device(index: int, platform: str) -> Device:
    return Device(client_id=f"CLIENT-SCN-{index:04d}", platform=platform, app_version="4.2.0")


def acceptance_scenarios(anchor: datetime) -> List[Message]:
    """Fixed journeys ending at or before ``anchor``, in emission order."""
    ids = Ids(seed=f"acceptance:{_iso(anchor)}")
    minutes = lambda m: anchor - timedelta(minutes=m)  # noqa: E731
    messages: List[Message] = []

    def add(scenario: str, visit: Visit) -> None:
        messages.extend(Message(scenario, event) for event in visit.events)

    # Anonymous view, verified login, cart, checkout, logout: one session.
    journey = Visit(_device(1, "web"), ids)
    journey.browse(minutes(60), "product_view", page="product", product_id="PROD-0001")
    journey.login(minutes(59), "CUST-000001", loyalty_id="LOYAL-000001",
                  consent={"marketing_consent": True, "analytics_consent": True})
    journey.add_to_cart(minutes(58), "PROD-0001", 42.50)
    journey.checkout(minutes(57), 42.50)
    journey.logout(minutes(56))
    add("journey", journey)

    # Same session_id after 45 idle minutes: two session instances, anonymous.
    gap = Visit(_device(2, "ios"), ids)
    gap.browse(minutes(80), "page_view", page="home")
    gap.browse(minutes(79), "product_view", page="product", product_id="PROD-0002")
    gap.browse(minutes(34), "page_view", page="home")
    add("inactivity_gap", gap)

    # One tablet, two people: A signs out before B signs in.
    tablet = Visit(_device(3, "android"), ids)
    tablet.login(minutes(50), "CUST-000002", loyalty_id="LOYAL-000002")
    tablet.browse(minutes(49), "product_view", page="product", product_id="PROD-0003")
    tablet.logout(minutes(48))
    tablet.login(minutes(40), "CUST-000003", loyalty_id="LOYAL-000003")
    tablet.add_to_cart(minutes(39), "PROD-0004", 15.00)
    tablet.logout(minutes(38))
    add("shared_tablet", tablet)

    # Web and app accounts each verified to the same loyalty card.
    web = Visit(_device(4, "web"), ids)
    web.login(minutes(30), "CUST-000004", loyalty_id="LOYAL-000004")
    web.logout(minutes(29))
    add("dual_accounts", web)
    app = Visit(_device(5, "ios"), ids)
    app.login(minutes(20), "CUST-010004", loyalty_id="LOYAL-000004")
    app.browse(minutes(19), "product_view", page="product", product_id="PROD-0005")
    app.logout(minutes(18))
    add("dual_accounts", app)

    # Opt in at login, opt out of marketing later in the visit.
    consent = Visit(_device(6, "web"), ids)
    consent.login(minutes(70), "CUST-000005", loyalty_id="LOYAL-000005",
                  consent={"marketing_consent": True, "analytics_consent": True})
    consent.update_consent(minutes(69), {"marketing_consent": False}, "preference_center")
    consent.logout(minutes(68))
    add("consent_change", consent)

    # A typed-in loyalty number that the account has not verified.
    unverified = Visit(_device(7, "web"), ids)
    unverified.login(minutes(25), "CUST-000006", loyalty_id="LOYAL-000099", loyalty_verified=False)
    unverified.logout(minutes(24))
    add("unverified_loyalty", unverified)

    # One event delivered last although it happened second, one exact retry.
    late = Visit(_device(8, "web"), ids)
    first = late.browse(minutes(15), "page_view", page="home")
    delayed = late.browse(minutes(14), "product_view", page="product", product_id="PROD-0006")
    late.browse(minutes(13), "search", page="search")
    messages.extend(Message("late_and_retry", event) for event in late.events if event is not delayed)
    messages.append(Message("late_and_retry", first, kind="retry"))
    messages.append(Message("late_and_retry", delayed, kind="late"))

    return messages


# ---------------------------------------------------------------------------
# Load mode
# ---------------------------------------------------------------------------

LOAD_ACCOUNTS: List[str] = [account for account in ACCOUNT_IDS if account not in SCENARIO_ACCOUNTS]
SHARED_DEVICE_RATE = 0.05
LOGIN_RATE = 0.5
CONSENT_STATEMENT_RATE = 0.3
MARKETING_OPT_IN_RATE = 0.85

Step = Tuple[int, Callable[[Visit, datetime], Any]]


def new_device(index: int, rng: random.Random) -> Device:
    household = 2 if rng.random() < SHARED_DEVICE_RATE else 1
    return Device(
        client_id=f"CLIENT-{index:08d}",
        platform=rng.choice(PLATFORMS),
        app_version=f"{rng.randint(1, 4)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}",
        accounts=tuple(rng.sample(LOAD_ACCOUNTS, household)),
    )


def plan_journey(device: Device, rng: random.Random) -> List[Step]:
    """Offsets in seconds from the journey start, each with a Visit action."""
    steps: List[Step] = []
    offset = 0

    def then(action: Callable[[Visit, datetime], Any]) -> None:
        nonlocal offset
        steps.append((offset, action))
        offset += rng.randint(5, 90)

    product = rng.choice(PRODUCTS)
    cart_value = round(rng.uniform(20.0, 500.0), 2)
    for _ in range(rng.randint(1, 4)):
        event_type = rng.choice(["page_view", "product_view", "search"])
        then(lambda v, at, t=event_type: v.browse(at, t, page="product", product_id=product))

    account = rng.choice(device.accounts) if device.accounts and rng.random() < LOGIN_RATE else None
    if account is not None:
        consent = None
        if rng.random() < CONSENT_STATEMENT_RATE:
            consent = {"marketing_consent": rng.random() < MARKETING_OPT_IN_RATE, "analytics_consent": True}
        loyalty = verified_loyalty_id(account)
        then(lambda v, at: v.login(at, account, loyalty_id=loyalty, consent=consent))
    if rng.random() < 0.5:
        then(lambda v, at: v.add_to_cart(at, product, cart_value))
        if rng.random() < (0.4 if account else 0.1):
            then(lambda v, at: v.browse(at, "checkout_start", page="checkout"))
            then(lambda v, at: v.checkout(at, cart_value))
    if account is not None and rng.random() < 0.6:
        then(lambda v, at: v.logout(at))
    return steps


def play_journey(
    device: Device, ids: Ids, rng: random.Random, end: datetime
) -> Optional[List[ClickstreamEvent]]:
    """Emit one journey whose last event is at ``end``.

    Returns None, leaving the device untouched, when the journey would start
    before the device's previous event.
    """
    steps = plan_journey(device, rng)
    start = end - timedelta(seconds=steps[-1][0])
    if device.last_event_at is not None and start < device.last_event_at:
        return None
    visit = Visit(device, ids)
    for offset, action in steps:
        action(visit, start + timedelta(seconds=offset))
    return visit.events


def load_messages(
    rng: random.Random,
    ids: Ids,
    now: Callable[[], datetime],
    device_pool: int = 20_000,
) -> Iterator[ClickstreamEvent]:
    """Endless journeys on reused devices, each ending at the current time."""
    devices: Dict[int, Device] = {}
    while True:
        index = rng.randrange(device_pool)
        if index not in devices:
            devices[index] = new_device(index, rng)
        events = play_journey(devices[index], ids, rng, now())
        if events is not None:
            yield from events


def delivery_report(err, msg) -> None:
    """Kafka delivery callback."""
    if err:
        log.error("delivery_failed", error=str(err))


def _produce(producer: Producer, event: ClickstreamEvent, payload: bytes) -> None:
    while True:
        try:
            producer.produce(
                topic=TOPIC,
                key=event.client_id.encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            return
        except BufferError:
            producer.poll(0.05)


def run_scenario(anchor: Optional[datetime] = None) -> None:
    """Emit the acceptance scenarios once."""
    anchor = anchor or datetime.now(timezone.utc)
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    anchor = anchor.replace(second=0, microsecond=0)
    producer = Producer(KAFKA_CONFIG)
    messages = acceptance_scenarios(anchor)
    for message in messages:
        _produce(producer, message.event, message.event.payload())
    producer.flush()
    log.info("clickstream_scenario_finished", anchor=_iso(anchor), emitted=len(messages))


def run_producer(
    events_per_second: int = 10000,
    duration_seconds: int = 60,
    retry_pct: float = 0.01,
    late_pct: float = 0.01,
    late_delay_seconds: int = 120,
    seed: Optional[int] = None,
    device_pool: int = 20_000,
) -> None:
    """Emit load-mode journeys at the configured rate.

    A smaller ``device_pool`` gives more repeat visits per device.
    """
    rng = random.Random(seed)
    producer = Producer(KAFKA_CONFIG)
    started_at = time.time()
    emitted = 0
    # Sequence is the only tie-break. Late events share a release time and do
    # not advance `emitted`, and ClickstreamEvent has no ordering.
    seq = 0
    held: List[Tuple[float, int, ClickstreamEvent, bytes]] = []
    stream = load_messages(rng, Ids(), lambda: datetime.now(timezone.utc), device_pool)

    log.info(
        "clickstream_producer_started",
        events_per_second=events_per_second,
        duration_seconds=duration_seconds,
    )

    while (time.time() - started_at) < duration_seconds:
        tick_started = time.time()
        while held and held[0][0] <= tick_started:
            _, _, event, payload = heapq.heappop(held)
            _produce(producer, event, payload)
            emitted += 1
        for _ in range(events_per_second):
            event = next(stream)
            payload = event.payload()
            if rng.random() < late_pct:
                heapq.heappush(held, (tick_started + late_delay_seconds, seq, event, payload))
                seq += 1
                continue
            _produce(producer, event, payload)
            emitted += 1
            if rng.random() < retry_pct:
                _produce(producer, event, payload)
                emitted += 1
            if emitted % 500 == 0:
                producer.poll(0)

        producer.poll(0)
        elapsed_tick = time.time() - tick_started
        if elapsed_tick < 1.0:
            time.sleep(1.0 - elapsed_tick)

    for _, _, event, payload in sorted(held, key=lambda item: (item[0], item[1])):
        _produce(producer, event, payload)
        emitted += 1
    producer.flush()
    elapsed_total = round(time.time() - started_at, 2)
    log.info("clickstream_producer_finished", emitted=emitted, elapsed_seconds=elapsed_total)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clickstream Kafka producer simulator")
    parser.add_argument("--eps", type=int, default=10000, help="Events per second (load mode)")
    parser.add_argument("--duration", type=int, default=30, help="Run duration in seconds (load mode)")
    parser.add_argument("--devices", type=int, default=20_000, help="Device pool size (load mode)")
    parser.add_argument("--scenario", choices=["acceptance"], help="Emit fixed scenarios instead of load")
    parser.add_argument("--anchor", type=datetime.fromisoformat,
                        help="Scenario end time (ISO 8601, UTC); the same anchor repeats the same bytes")
    args = parser.parse_args()
    if args.scenario:
        run_scenario(args.anchor)
    else:
        run_producer(events_per_second=args.eps, duration_seconds=args.duration, device_pool=args.devices)
