"""Unit tests pinning the event_types constants to the JSON Schema enums.

The JSON Schema files in ``ingestion/schemas/`` are the authoritative data
contract. The Python constants in ``streaming.flink_jobs.event_types`` are a
convenience view used by Flink jobs and producer sims. This test asserts the
two stay in sync — if a new event_type is added to the schema, the Python
constant must be updated too (and vice versa).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming.flink_jobs.event_types import (
    CLICKSTREAM_EVENT_TYPES,
    INVENTORY_EVENT_TYPES,
    PLATFORMS,
)

pytestmark = pytest.mark.unit

_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "ingestion" / "schemas"


def _load_schema_enum(filename: str, field: str) -> list[str]:
    """Read a JSON Schema file and return the enum values for ``field``."""
    path = _SCHEMA_DIR / filename
    schema = json.loads(path.read_text(encoding="utf-8"))
    return schema["properties"][field]["enum"]


class TestEventTypesMatchSchemas:
    def test_inventory_event_types_match_schema(self):
        schema_enum = _load_schema_enum("inventory_event.json", "event_type")
        assert list(INVENTORY_EVENT_TYPES) == schema_enum

    def test_clickstream_event_types_match_schema(self):
        schema_enum = _load_schema_enum("clickstream_envelope.json", "event_type")
        assert list(CLICKSTREAM_EVENT_TYPES) == schema_enum

    def test_platforms_match_schema(self):
        schema_enum = _load_schema_enum("clickstream_envelope.json", "platform")
        assert list(PLATFORMS) == schema_enum


class TestEventTypesShape:
    def test_inventory_event_types_are_unique(self):
        assert len(INVENTORY_EVENT_TYPES) == len(set(INVENTORY_EVENT_TYPES))

    def test_clickstream_event_types_are_unique(self):
        assert len(CLICKSTREAM_EVENT_TYPES) == len(set(CLICKSTREAM_EVENT_TYPES))

    def test_platforms_are_unique(self):
        assert len(PLATFORMS) == len(set(PLATFORMS))

    def test_all_values_are_nonempty_strings(self):
        for value in (*INVENTORY_EVENT_TYPES, *CLICKSTREAM_EVENT_TYPES, *PLATFORMS):
            assert isinstance(value, str)
            assert value
