"""Unit tests for the shared Flink YAML/env config loader.

This module is the seam that keeps local docker-compose and EMR/prod runs on
identical job code, so its env-substitution and type-coercion branches are
worth pinning down precisely.
"""

from __future__ import annotations

import pytest

from streaming.flink_jobs import _config

pytestmark = pytest.mark.unit


class TestExpandEnv:
    def test_substitutes_existing_var(self, monkeypatch):
        monkeypatch.setenv("BUCKET", "my-bucket")
        assert _config._expand_env("s3://${BUCKET}/path") == "s3://my-bucket/path"

    def test_uses_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        assert _config._expand_env("${MISSING_VAR:-fallback}") == "fallback"

    def test_env_wins_over_default(self, monkeypatch):
        monkeypatch.setenv("PORT", "9092")
        assert _config._expand_env("${PORT:-1234}") == "9092"

    def test_missing_without_default_becomes_empty(self, monkeypatch):
        monkeypatch.delenv("NOPE", raising=False)
        assert _config._expand_env("a${NOPE}b") == "ab"

    def test_multiple_substitutions(self, monkeypatch):
        monkeypatch.setenv("A", "x")
        monkeypatch.setenv("B", "y")
        assert _config._expand_env("${A}-${B}") == "x-y"


class TestParseScalar:
    @pytest.mark.parametrize("raw,expected", [("true", True), ("false", False), ("True", True)])
    def test_booleans(self, raw, expected):
        assert _config._parse_scalar(raw) is expected

    @pytest.mark.parametrize("raw,expected", [("123", 123), ("-5", -5), ("0", 0)])
    def test_integers(self, raw, expected):
        assert _config._parse_scalar(raw) == expected
        assert isinstance(_config._parse_scalar(raw), int)

    @pytest.mark.parametrize("raw", ["1.5", "PLAINTEXT", "s3://bucket/x", "1.2.0"])
    def test_strings_stay_strings(self, raw):
        result = _config._parse_scalar(raw)
        assert isinstance(result, str)
        assert result == raw

    def test_int_coercion_after_env_expansion(self, monkeypatch):
        monkeypatch.setenv("PORT", "9092")
        assert _config._parse_scalar("${PORT}") == 9092


class TestLoadSimpleYaml:
    def test_parses_keys_comments_and_types(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WAREHOUSE", "s3://wh/iceberg")
        conf = tmp_path / "flink_conf.yaml"
        conf.write_text(
            "\n".join(
                [
                    "# a comment line",
                    "",
                    "parallelism: 4",
                    "checkpointing_enabled: true",
                    "kafka_security_protocol: PLAINTEXT",
                    "iceberg_warehouse: ${WAREHOUSE}",
                    "bootstrap: ${KAFKA:-127.0.0.1:9092}",
                ]
            ),
            encoding="utf-8",
        )

        cfg = _config.load_simple_yaml(conf)

        assert cfg["parallelism"] == 4
        assert cfg["checkpointing_enabled"] is True
        assert cfg["kafka_security_protocol"] == "PLAINTEXT"
        assert cfg["iceberg_warehouse"] == "s3://wh/iceberg"
        assert cfg["bootstrap"] == "127.0.0.1:9092"
        assert "# a comment line" not in cfg

    def test_value_containing_colon_is_preserved(self, tmp_path):
        conf = tmp_path / "c.yaml"
        conf.write_text("endpoint: https://host:443/path", encoding="utf-8")
        cfg = _config.load_simple_yaml(conf)
        assert cfg["endpoint"] == "https://host:443/path"


class TestKafkaSecurityOptions:
    def test_plaintext_returns_empty(self):
        assert _config.kafka_security_options({}) == ""
        assert _config.kafka_security_options({"kafka_security_protocol": "plaintext"}) == ""

    def test_msk_iam_fragment(self):
        out = _config.kafka_security_options(
            {"kafka_security_protocol": "SASL_SSL", "kafka_sasl_mechanism": "AWS_MSK_IAM"}
        )
        assert "'properties.security.protocol' = 'SASL_SSL'" in out
        assert "'properties.sasl.mechanism' = 'AWS_MSK_IAM'" in out
        assert "IAMLoginModule" in out
        assert "IAMClientCallbackHandler" in out

    def test_generic_sasl_has_no_iam_jaas(self):
        out = _config.kafka_security_options(
            {"kafka_security_protocol": "SASL_SSL", "kafka_sasl_mechanism": "SCRAM-SHA-512"}
        )
        assert "'properties.security.protocol' = 'SASL_SSL'" in out
        assert "'properties.sasl.mechanism' = 'SCRAM-SHA-512'" in out
        assert "IAMLoginModule" not in out


class TestResolveConfigPath:
    def test_falls_back_to_repo_config(self):
        # /opt/flink-config does not exist off-EMR, so the repo path is used.
        path = _config.resolve_config_path("flink_conf.yaml")
        assert path.parts[-3:] == ("streaming", "config", "flink_conf.yaml")


class TestFlinkConfContract:
    """Pin the topic/consumer-group keys every Flink job depends on.

    Adding a new Flink job or DLQ sink requires a new entry here so the
    config-keys-vs-job-code contract stays explicit.
    """

    REQUIRED_KEYS = {
        "kafka_bootstrap_servers",
        "inventory_topic",
        "clickstream_topic",
        "clickstream_dlq_topic",
        "clickstream_business_dlq_topic",
        "inventory_dlq_topic",
        "consumer_group_inventory",
        "consumer_group_inventory_bronze",
        "consumer_group_clickstream",
        "iceberg_catalog_name",
        "iceberg_catalog_type",
        "iceberg_warehouse_bronze",
        "iceberg_warehouse_silver",
    }

    def test_required_keys_present(self):
        path = _config.resolve_config_path("flink_conf.yaml")
        cfg = _config.load_simple_yaml(path)
        missing = self.REQUIRED_KEYS - set(cfg)
        assert not missing, f"flink_conf.yaml missing keys: {sorted(missing)}"

    def test_inventory_dlq_topic_matches_contract(self):
        path = _config.resolve_config_path("flink_conf.yaml")
        cfg = _config.load_simple_yaml(path)
        assert cfg["inventory_dlq_topic"] == "dlq.inventory.schema_violations"


class TestReplayQuarantineContract:
    """Malformed IDs and invalid checkout data remain auditable after raw capture."""

    @pytest.mark.parametrize('stream', ['inventory', 'clickstream'])
    def test_missing_event_id_is_quarantined(self, stream):
        from spark.identity_resolution.event_contract import parse_event

        assert parse_event(b'{}', stream)['error_reason'] == 'missing_event_id'

    def test_clickstream_routes_checkout_business_violations(self):
        from spark.identity_resolution.event_contract import parse_event
        import json

        event = dict(event_id='e', event_type='checkout', event_time='2026-09-17T00:00:00Z',
                     session_id='s', client_id='c', platform='web', app_version='1.0.0',
                     schema_version='1.0.0', properties={'cart_value': 12})
        assert parse_event(json.dumps(event).encode(), 'clickstream')['error_reason'] == 'checkout_invalid_order_id'
        event['properties']['order_id'] = 'ORDER-123456789ABC'
        event['properties']['cart_value'] = -1
        assert parse_event(json.dumps(event).encode(), 'clickstream')['error_reason'] == 'checkout_invalid_cart_value'
