"""Canonical event-type enums and platform values for typed event processing.

Raw Bronze never filters by these enums (ADR-011). Spark's separately packaged
replay parser is parity-tested against these constants. The values MUST
match the JSON Schema enums in `ingestion/schemas/*.json` —
`tests/unit/test_event_types.py` pins that contract.

Why a separate module (not inline in each job):
  * Avoids drift between the two Flink jobs and the producers
  * The JSON Schema files are the authoritative contract; this module is the
    Python-friendly view of the same enums
  * Flink packages this module flat via `-pyfs`,
    so the jobs import it as `from event_types import INVENTORY_EVENT_TYPES`
"""

from __future__ import annotations

# Inventory event_type enum — must match ingestion/schemas/inventory_event.json
INVENTORY_EVENT_TYPES: tuple[str, ...] = (
    "sale_deduction",
    "receipt",
    "adjustment",
    "transfer",
)

# Clickstream event_type enum — must match ingestion/schemas/clickstream_envelope.json
CLICKSTREAM_EVENT_TYPES: tuple[str, ...] = (
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "checkout",
    "login",
    "logout",
    "error",
)

# Clickstream platform enum — must match ingestion/schemas/clickstream_envelope.json
PLATFORMS: tuple[str, ...] = ("web", "ios", "android")
