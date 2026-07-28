"""Shared YAML config loader for Flink jobs.

Supports `${VAR}` and `${VAR:-default}` substitution from environment so the
same `streaming/config/flink_conf.yaml` file works for local docker-compose and
for production EMR (where MSK brokers and Iceberg warehouse paths are injected
via env vars set on the EMR step).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:  # pragma: no cover — type-check only
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _expand_env(value: str) -> str:
    def repl(match: "re.Match[str]") -> str:
        var_name, default = match.group(1), match.group(2) or ""
        return os.environ.get(var_name, default)

    return _ENV_PATTERN.sub(repl, value)


def _parse_scalar(value: str) -> Any:
    raw = _expand_env(value.strip())
    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def load_simple_yaml(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, value = stripped.split(":", 1)
        config[key.strip()] = _parse_scalar(value)
    return config


def kafka_security_options(cfg: Dict[str, Any]) -> str:
    """Return Flink Kafka connector `WITH` clause fragments for the configured
    security protocol. Empty string when running plaintext (local docker)."""
    protocol = str(cfg.get("kafka_security_protocol", "PLAINTEXT")).upper()
    if protocol == "PLAINTEXT":
        return ""

    mechanism = str(cfg.get("kafka_sasl_mechanism", "AWS_MSK_IAM"))
    if mechanism == "AWS_MSK_IAM":
        jaas = "software.amazon.msk.auth.iam.IAMLoginModule required;"
        callback = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        return f"""
          ,'properties.security.protocol' = '{protocol}'
          ,'properties.sasl.mechanism' = '{mechanism}'
          ,'properties.sasl.jaas.config' = '{jaas}'
          ,'properties.sasl.client.callback.handler.class' = '{callback}'
        """.strip()

    return f"""
      ,'properties.security.protocol' = '{protocol}'
      ,'properties.sasl.mechanism' = '{mechanism}'
    """.strip()


def resolve_config_path(filename: str) -> Path:
    """Look up `streaming/config/<filename>` from one of two well-known places.

    Local dev: `<repo_root>/streaming/config/<filename>`
    EMR/prod : `/opt/flink-config/<filename>` (populated by the bootstrap action)
    """
    emr_path = Path("/opt/flink-config") / filename
    if emr_path.exists():
        return emr_path
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "streaming" / "config" / filename


def resolve_parallelism(flink_cfg: Dict[str, Any], topic_key: str) -> int:
    """Prefer per-topic parallelism, fall back to the generic ``parallelism`` key.

    ``topic_key`` is ``inventory_parallelism`` or ``clickstream_parallelism``.
    Local docker-compose overrides these to 1 via env-var substitution so a
    single TaskManager is not thrashed; cloud sets them to match Kafka
    partition counts (see ``ingestion/kafka/topics.py``).
    """
    if topic_key in flink_cfg and flink_cfg[topic_key] is not None:
        return int(flink_cfg[topic_key])
    return int(flink_cfg.get("parallelism", 4))


def watermark_interval_sql(
    flink_cfg: Dict[str, Any], key: str, default_seconds: int
) -> str:
    """Return a Flink SQL ``INTERVAL 'N' SECOND`` literal from config.

    Used by inventory bronze/silver (and clickstream) so watermark delay is
    tunable from ``flink_conf.yaml`` without editing job SQL. Defaults match
    the documented P3.2 asymmetry (bronze 30s, silver 60s).
    """
    seconds = int(flink_cfg.get(key, default_seconds))
    if seconds < 0:
        raise ValueError(f"{key} must be >= 0, got {seconds}")
    return f"INTERVAL '{seconds}' SECOND"


def apply_state_config(
    env: "StreamExecutionEnvironment",
    t_env: "StreamTableEnvironment",
    state_cfg: Dict[str, Any],
) -> None:
    """Apply the F-STATE config (rocksdb backend, incremental checkpoints,
    state TTL, source idle timeout) to the StreamExecutionEnvironment and
    StreamTableEnvironment.

    Called from every streaming Flink job's `run()` after the env + t_env
    are created, before any `execute_sql` calls. The maintenance batch
    job (iceberg_maintenance.py) does NOT call this — it has no
    long-running state.

    Runtime-level knobs (state.backend, state.backend.incremental) are applied
    through a PyFlink `Configuration`; SQL-level knobs
    (table.exec.state.ttl, table.exec.source.idle-timeout) go on the
    `TableConfig` configuration. This matches the PyFlink 1.17 APIs.
    """
    # Runtime config: state backend + incremental checkpoints.
    from pyflink.common import Configuration

    env_config = Configuration()
    env_config.set_string("state.backend", str(state_cfg["state_backend"]))
    env_config.set_string(
        "state.backend.incremental",
        str(bool(state_cfg["state_backend_incremental"])).lower(),
    )
    env.configure(env_config)

    # SQL config: state TTL (mandatory for the dedup self-join in
    # inventory_silver_job) + source idle timeout (prevents a stalled
    # Kafka partition from stalling the whole pipeline's watermark).
    t_config = t_env.get_config().get_configuration()
    t_config.set_string(
        "table.exec.state.ttl", str(state_cfg["state_ttl"])
    )
    t_config.set_string(
        "table.exec.source.idle-timeout", str(state_cfg["source_idle_timeout"])
    )
