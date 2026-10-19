"""
Kafka producer configuration for local PLAINTEXT and cloud MSK IAM (SASL_SSL).

Set KAFKA_BOOTSTRAP_SERVERS and, for MSK, KAFKA_SECURITY_PROTOCOL=SASL_SSL.
AWS credentials come from the default provider chain (EMR instance profile, env, SSO).

Reliability + performance defaults (K-PROD from the Kafka checklist applied
2026-07-05):
  - acks=all                    : wait for all in-sync replicas to confirm.
  - enable.idempotence=true     : producer dedupes retried batches on the
                                  broker side, so retries don't duplicate.
  - retries=2**31 - 1           : effectively "retry forever" until the
                                  delivery timeout hits. With idempotence
                                  on, this is safe.
  - max.in.flight.requests.per.connection=5
                                : the max safe value when idempotence is
                                  enabled (Kafka broker caps at 5).
  - delivery.timeout.ms=120000  : hard ceiling on retry duration. retries
                                  without a delivery timeout can hang a
                                  producer indefinitely on broker outage.
  - batch.size=131072 (128KB)   : max batch size — larger batches = fewer
                                  producer requests = higher throughput.
  - linger.ms=10                : wait up to 10ms for the batch to fill
                                  before sending. Trades 10ms latency
                                  for ~10x throughput on small events.
  - compression.type=lz4        : best throughput / CPU ratio for JSON
                                  payloads. Switch to zstd if CPU head-
                                  room allows and payloads are larger.

These defaults are overridable via **extra to build_producer_config() —
e.g. a high-volume producer can pass batch.size=262144 to double the
batch size. Tests/unit/test_kafka_producer_config.py guards the contract.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict

# Default producer reliability + performance knobs. See module docstring
# for the rationale per key. Overridable via **extra in
# build_producer_config().
_PRODUCER_DEFAULTS: Dict[str, Any] = {
    # --- Reliability (three-tier defense — producer side) ---
    "acks": "all",
    "enable.idempotence": True,
    "retries": 2 ** 31 - 1,  # confluent-kafka uses int64; this is "effectively forever"
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 120_000,
    # --- Performance tuning (producer batching) ---
    "batch.size": 131_072,   # 128 KB
    "linger.ms": 10,
    "compression.type": "lz4",
}


def _msk_oauth_cb(region: str) -> Callable[..., tuple[str, float]]:
    def oauth_cb(_oauth_config: str) -> tuple[str, float]:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return oauth_cb


def build_producer_config(client_id: str, **extra: Any) -> Dict[str, Any]:
    """Return a confluent-kafka Producer config dict.

    Reliability + performance defaults are applied first, then security,
    then caller-supplied **extra (which wins on conflicts).
    """
    cfg: Dict[str, Any] = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        "client.id": client_id,
    }
    cfg.update(_PRODUCER_DEFAULTS)
    cfg.update(extra)

    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if protocol == "SASL_SSL":
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"))
        cfg.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "oauth_cb": _msk_oauth_cb(region),
            }
        )
    return cfg
