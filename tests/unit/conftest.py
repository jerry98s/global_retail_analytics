"""Unit-test bootstrap.

Keeps the unit suite fast and fully offline:

* Puts the repo root on ``sys.path`` so source packages import as
  ``streaming.flink_jobs._config`` etc. (implicit namespace packages).
* Installs lightweight stand-ins for heavy optional runtime deps
  (``confluent_kafka``, ``structlog``) so importing producer/topic modules
  never pulls in the full streaming stack.
  Stubs are only registered when the real package is not already importable.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _new_module(name: str) -> types.ModuleType:
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


def _stub_structlog() -> None:
    if "structlog" in sys.modules:
        return

    class _NoopLogger:
        def __getattr__(self, _name):
            def _noop(*_args, **_kwargs):
                return None

            return _noop

    structlog = _new_module("structlog")
    structlog.get_logger = lambda *_a, **_k: _NoopLogger()  # type: ignore[attr-defined]


def _stub_confluent_kafka() -> None:
    if "confluent_kafka" not in sys.modules:
        ck = _new_module("confluent_kafka")
        ck.Producer = MagicMock(name="Producer")  # type: ignore[attr-defined]

    if "confluent_kafka.admin" not in sys.modules:
        admin = _new_module("confluent_kafka.admin")
        admin.AdminClient = MagicMock(name="AdminClient")  # type: ignore[attr-defined]

        class NewTopic:  # mirrors the real attribute surface we assert on
            def __init__(self, topic, num_partitions=1, replication_factor=1, config=None):
                self.topic = topic
                self.num_partitions = num_partitions
                self.replication_factor = replication_factor
                self.config = config or {}

        admin.NewTopic = NewTopic  # type: ignore[attr-defined]
        sys.modules["confluent_kafka"].admin = admin  # type: ignore[attr-defined]


_stub_structlog()
_stub_confluent_kafka()
