"""Unit tests for MSK/local Kafka producer configuration."""

from __future__ import annotations

import pytest

from ingestion.kafka.msk_config import build_producer_config

pytestmark = pytest.mark.unit


class TestMskConfig:
    def test_plaintext_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        cfg = build_producer_config("test-client")
        assert cfg["bootstrap.servers"] == "127.0.0.1:9092"
        assert cfg["client.id"] == "test-client"
        assert "oauth_cb" not in cfg

    def test_sasl_ssl_adds_oauth_cb(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("AWS_REGION", "ap-southeast-1")
        cfg = build_producer_config("msk-client")
        assert cfg["security.protocol"] == "SASL_SSL"
        assert cfg["sasl.mechanism"] == "OAUTHBEARER"
        assert callable(cfg["oauth_cb"])


# --- K-PROD from the Kafka checklist: reliability + performance defaults --- #


class TestProducerReliabilityDefaults:
    """Three-tier defense (producer side) + performance tuning knobs.

    Guards the contract that build_producer_config() returns the checklist
    defaults so a future refactor doesn't silently drop them. Override
    behavior is also tested.
    """

    def test_acks_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["acks"] == "all"

    def test_idempotence_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["enable.idempotence"] is True

    def test_retries_large(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["retries"] >= 2 ** 30  # effectively forever

    def test_max_in_flight_5_when_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # 5 is the max safe value when idempotence is on (broker cap).
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["max.in.flight.requests.per.connection"] == 5

    def test_delivery_timeout_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # retries=forever without a delivery timeout can hang the producer
        # on broker outage — the checklist pairs them.
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["delivery.timeout.ms"] == 120_000


class TestProducerPerformanceDefaults:
    def test_batch_size_within_checklist_range(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Checklist recommends 64KB - 256KB.
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert 64 * 1024 <= cfg["batch.size"] <= 256 * 1024

    def test_linger_ms_within_checklist_range(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Checklist recommends 5 - 50ms.
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert 5 <= cfg["linger.ms"] <= 50

    def test_compression_lz4_or_zstd(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client")
        assert cfg["compression.type"] in {"lz4", "zstd"}


class TestProducerOverride:
    def test_caller_can_override_batch_size(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client", **{"batch.size": 262144})
        assert cfg["batch.size"] == 262144
        # Other defaults remain.
        assert cfg["acks"] == "all"
        assert cfg["enable.idempotence"] is True

    def test_caller_can_disable_idempotence_for_local(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL", raising=False)
        cfg = build_producer_config("any-client", **{"enable.idempotence": False})
        assert cfg["enable.idempotence"] is False

