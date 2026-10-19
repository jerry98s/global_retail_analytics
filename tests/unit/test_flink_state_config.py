"""Unit tests for the Flink production checklist (F-STATE + F-SRC applied
2026-07-05).

These tests statically lint:
  - streaming/config/state.yaml — the rocksdb backend / incremental
    checkpoints / state TTL / source idle timeout config.
  - streaming/config/checkpoints.yaml — the min_pause_between_checkpoints
    safeguard (bumped from 5s to 30s per the checklist).
  - streaming/config/flink_conf.yaml — per-topic parallelism config keys
    matching the Kafka partition counts.
  - streaming/flink_jobs/_config.py:apply_state_config — the helper that
    wires state.yaml into the env + t_env.
  - All 3 streaming Flink job files — that they call apply_state_config
    in their run() and declare partition.discovery.interval.ms in the
    Kafka source DDL.

PyFlink is a heavy dependency (~600MB of JARs) and isn't on the test
PATH, so we don't import the job modules — we lint the source text
and the config files directly.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from streaming.flink_jobs import _config

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STREAMING = _REPO_ROOT / "streaming"
_FLINK_JOBS = _STREAMING / "flink_jobs"
_CONFIG = _STREAMING / "config"

_STREAMING_JOB_FILES = [
    "inventory_bronze_job.py",
    "clickstream_bronze_job.py",
    "inventory_silver_job.py",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# --- F-STATE: state.yaml contract --- #


class TestStateConfigFile:
    """state.yaml must declare the F-STATE keys, and they must match the
    checklist recommendations (rocksdb, incremental, 7d TTL, 1min idle)."""

    @pytest.fixture(scope="class")
    def state_cfg(self) -> dict:
        return _config.load_simple_yaml(_CONFIG / "state.yaml")

    def test_state_backend_is_rocksdb(self, state_cfg: dict) -> None:
        assert str(state_cfg["state_backend"]).lower() == "rocksdb", (
            "F-STATE: state.backend must be 'rocksdb' for production — "
            "HashMap OOMs at ~10GB state. See docs/runbooks/flink-operations.md § 2.1."
        )

    def test_incremental_checkpoints_enabled(self, state_cfg: dict) -> None:
        assert state_cfg["state_backend_incremental"] is True, (
            "F-STATE: state.backend.incremental must be true with rocksdb — "
            "full checkpoints of multi-GB state take minutes vs seconds."
        )

    def test_state_ttl_set_to_7_days(self, state_cfg: dict) -> None:
        ttl = str(state_cfg["state_ttl"]).strip().lower()
        # Accept "7 d" or "7d" — both are valid Flink duration syntax.
        assert re.match(r"^7\s*d$", ttl), (
            f"F-STATE: table.exec.state.ttl should be '7 d' (got {ttl!r}). "
            f"Mandatory for the inventory_silver_job dedup self-join."
        )

    def test_source_idle_timeout_set(self, state_cfg: dict) -> None:
        idle = str(state_cfg["source_idle_timeout"]).strip().lower()
        # Accept "1 min" / "1min" / "60 s" / etc. Just assert it's non-empty
        # and parses as a duration.
        assert re.match(r"^\d+\s*(min|s|ms|h)$", idle), (
            f"F-STATE: table.exec.source.idle-timeout should be a duration "
            f"like '1 min' (got {idle!r}). Without this, a stalled Kafka "
            f"partition stalls the whole pipeline's watermark."
        )


# --- F-STATE: checkpoints.yaml min_pause bumped to 30s --- #


class TestCheckpointsMinPause:
    """F-STATE: min_pause_between_checkpoints_ms must be >= 30000 (30s)
    per the checklist recommendation. The previous 5000 (5s) was below
    the threshold and could cause checkpoint piling under backpressure."""

    def test_min_pause_at_least_30s(self) -> None:
        cfg = _config.load_simple_yaml(_CONFIG / "checkpoints.yaml")
        assert int(cfg["min_pause_between_checkpoints_ms"]) >= 30_000, (
            "F-STATE: min_pause_between_checkpoints_ms must be >= 30000 (30s) "
            "per the Flink production checklist — prevents checkpoint piling."
        )


# --- F-STATE: apply_state_config helper --- #


class TestApplyStateConfigHelper:
    """apply_state_config() must set the four F-STATE keys on the env and
    t_env configs. Mocks PyFlink Configuration so we can assert without
    installing the Flink JARs."""

    def _apply(self, env, t_env, cfg):
        mock_cfg_cls = MagicMock()
        mock_cfg_inst = MagicMock()
        mock_cfg_cls.return_value = mock_cfg_inst
        with pytest.MonkeyPatch.context() as mp:
            # Inject a fake pyflink.common module so the lazy import succeeds.
            import sys
            import types

            fake_common = types.ModuleType("pyflink.common")
            fake_common.Configuration = mock_cfg_cls  # type: ignore[attr-defined]
            fake_pyflink = types.ModuleType("pyflink")
            fake_pyflink.common = fake_common  # type: ignore[attr-defined]
            mp.setitem(sys.modules, "pyflink", fake_pyflink)
            mp.setitem(sys.modules, "pyflink.common", fake_common)
            _config.apply_state_config(env, t_env, cfg)
        return mock_cfg_inst

    def test_sets_state_backend_on_env(self) -> None:
        env, t_env = MagicMock(), MagicMock()
        cfg = {
            "state_backend": "rocksdb",
            "state_backend_incremental": True,
            "state_ttl": "7 d",
            "source_idle_timeout": "1 min",
        }
        mock_cfg = self._apply(env, t_env, cfg)
        mock_cfg.set_string.assert_any_call("state.backend", "rocksdb")
        env.configure.assert_called_once_with(mock_cfg)

    def test_sets_incremental_on_env(self) -> None:
        env, t_env = MagicMock(), MagicMock()
        cfg = {
            "state_backend": "rocksdb",
            "state_backend_incremental": True,
            "state_ttl": "7 d",
            "source_idle_timeout": "1 min",
        }
        mock_cfg = self._apply(env, t_env, cfg)
        mock_cfg.set_string.assert_any_call("state.backend.incremental", "true")

    def test_sets_state_ttl_on_t_env(self) -> None:
        env, t_env = MagicMock(), MagicMock()
        cfg = {
            "state_backend": "rocksdb",
            "state_backend_incremental": True,
            "state_ttl": "7 d",
            "source_idle_timeout": "1 min",
        }
        self._apply(env, t_env, cfg)
        t_env.get_config().get_configuration().set_string.assert_any_call(
            "table.exec.state.ttl", "7 d"
        )

    def test_sets_source_idle_timeout_on_t_env(self) -> None:
        env, t_env = MagicMock(), MagicMock()
        cfg = {
            "state_backend": "rocksdb",
            "state_backend_incremental": True,
            "state_ttl": "7 d",
            "source_idle_timeout": "1 min",
        }
        self._apply(env, t_env, cfg)
        t_env.get_config().get_configuration().set_string.assert_any_call(
            "table.exec.source.idle-timeout", "1 min"
        )

    def test_false_incremental_serialises_to_string(self) -> None:
        # When state_backend_incremental is False (e.g. for a future
        # hashmap state backend), the value passed to Flink should be
        # the lowercase string 'false'.
        env, t_env = MagicMock(), MagicMock()
        cfg = {
            "state_backend": "hashmap",
            "state_backend_incremental": False,
            "state_ttl": "7 d",
            "source_idle_timeout": "1 min",
        }
        mock_cfg = self._apply(env, t_env, cfg)
        mock_cfg.set_string.assert_any_call("state.backend.incremental", "false")


# --- F-STATE: each streaming Flink job calls apply_state_config --- #


class TestJobsCallApplyStateConfig:
    """Each streaming Flink job's run() must call apply_state_config(env,
    t_env, state_cfg) after creating the env + t_env, otherwise the
    state.yaml settings don't take effect for that job."""

    @pytest.mark.parametrize("job_file", _STREAMING_JOB_FILES)
    def test_imports_apply_state_config(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        assert re.search(
            r"from\s+_config\s+import\s+[^\)]*apply_state_config",
            src,
            re.DOTALL,
        ), (
            f"{job_file}: must import apply_state_config from _config — "
            f"F-STATE contract."
        )

    @pytest.mark.parametrize("job_file", _STREAMING_JOB_FILES)
    def test_loads_state_yaml(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        assert re.search(
            r'load_simple_yaml\s*\(\s*resolve_config_path\s*\(\s*["\']state\.yaml["\']\s*\)\s*\)',
            src,
        ), (
            f"{job_file}: must load state.yaml via load_simple_yaml — "
            f"F-STATE contract."
        )

    @pytest.mark.parametrize("job_file", _STREAMING_JOB_FILES)
    def test_calls_apply_state_config(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        assert re.search(
            r"apply_state_config\s*\(\s*env\s*,\s*t_env\s*,\s*state_cfg\s*\)",
            src,
        ), (
            f"{job_file}: must call apply_state_config(env, t_env, state_cfg) "
            f"in run() — F-STATE contract."
        )

    def test_maintenance_job_does_not_call_apply_state_config(self) -> None:
        # The maintenance batch job has no long-running state — calling
        # apply_state_config on it would be a no-op but also misleading.
        src = _read(_FLINK_JOBS / "iceberg_maintenance.py")
        assert "apply_state_config" not in src, (
            "iceberg_maintenance.py: must NOT call apply_state_config — "
            "batch jobs have no long-running state."
        )


# --- F-SRC: Kafka source DDLs declare partition.discovery.interval.ms --- #


def _kafka_source_blocks(source: str) -> list[str]:
    """Paren-balanced scan for `) WITH ( ... )` blocks that contain
    'connector' = 'kafka'. Reused logic from test_flink_kafka_source.py
    — kept inline rather than imported so each test file is standalone."""
    blocks: list[str] = []
    for m in re.finditer(r"\)\s*WITH\s*\(", source):
        open_pos = m.end() - 1
        depth = 0
        i = open_pos
        while i < len(source):
            c = source[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    blocks.append(source[open_pos + 1 : i])
                    break
            i += 1
    return [
        b for b in blocks
        if re.search(r"'connector'\s*=\s*'kafka'", b, re.IGNORECASE)
    ]


def _is_dlq_block(block: str) -> bool:
    return "scan.startup.mode" not in block


class TestKafkaSourcePartitionDiscovery:
    """F-SRC: every Kafka *source* (not DLQ sink) must declare
    'properties.partition.discovery.interval.ms' = '300000' so newly-added
    Kafka partitions are picked up without a job restart."""

    @pytest.mark.parametrize("job_file", _STREAMING_JOB_FILES)
    def test_partition_discovery_declared(self, job_file: str) -> None:
        src = _read(_FLINK_JOBS / job_file)
        source_blocks = [b for b in _kafka_source_blocks(src) if not _is_dlq_block(b)]
        assert source_blocks, f"{job_file}: no Kafka source block found"
        for block in source_blocks:
            assert re.search(
                r"'properties\.partition\.discovery\.interval\.ms'\s*=\s*'\d+'",
                block,
                re.IGNORECASE,
            ), (
                f"{job_file}: Kafka source must declare "
                f"'properties.partition.discovery.interval.ms' — otherwise "
                f"newly-added Kafka partitions require a job restart."
            )


# --- F-SRC: flink_conf.yaml declares per-topic parallelism --- #


class TestFlinkConfPerTopicParallelism:
    """F-SRC: flink_conf.yaml must declare inventory_parallelism and
    clickstream_parallelism so cloud deployments can match the Flink job
    parallelism to the Kafka partition count."""

    def test_inventory_parallelism_key_present(self) -> None:
        cfg = _config.load_simple_yaml(_CONFIG / "flink_conf.yaml")
        assert "inventory_parallelism" in cfg, (
            "flink_conf.yaml: must declare inventory_parallelism — F-SRC "
            "contract. Cloud value should match the inventory.events.v1 "
            "partition count (12)."
        )

    def test_clickstream_parallelism_key_present(self) -> None:
        cfg = _config.load_simple_yaml(_CONFIG / "flink_conf.yaml")
        assert "clickstream_parallelism" in cfg, (
            "flink_conf.yaml: must declare clickstream_parallelism — F-SRC "
            "contract. Cloud value should match the clickstream.events.v1 "
            "partition count (24)."
        )

    def test_inventory_watermark_keys_present(self) -> None:
        cfg = _config.load_simple_yaml(_CONFIG / "flink_conf.yaml")
        assert "inventory_bronze_watermark_delay_seconds" in cfg
        assert "inventory_silver_watermark_delay_seconds" in cfg
        assert int(cfg["inventory_bronze_watermark_delay_seconds"]) == 30
        assert int(cfg["inventory_silver_watermark_delay_seconds"]) == 60


class TestResolveParallelism:
    def test_prefers_topic_key(self) -> None:
        assert _config.resolve_parallelism(
            {"inventory_parallelism": 12, "parallelism": 4},
            "inventory_parallelism",
        ) == 12

    def test_falls_back_to_generic(self) -> None:
        assert _config.resolve_parallelism(
            {"parallelism": 4}, "inventory_parallelism"
        ) == 4

    def test_jobs_call_resolve_parallelism(self) -> None:
        for job_file, topic_key in [
            ("inventory_bronze_job.py", "inventory_parallelism"),
            ("inventory_silver_job.py", "inventory_parallelism"),
            ("clickstream_bronze_job.py", "clickstream_parallelism"),
        ]:
            src = _read(_FLINK_JOBS / job_file)
            assert "resolve_parallelism" in src, (
                f"{job_file}: must call resolve_parallelism() so per-topic "
                f"parallelism from flink_conf.yaml is applied."
            )
            assert topic_key in src, (
                f"{job_file}: must pass {topic_key!r} to resolve_parallelism."
            )


class TestWatermarkIntervalSql:
    def test_builds_interval_literal(self) -> None:
        assert (
            _config.watermark_interval_sql(
                {"inventory_bronze_watermark_delay_seconds": 30},
                "inventory_bronze_watermark_delay_seconds",
                30,
            )
            == "INTERVAL '30' SECOND"
        )

    def test_uses_default_when_missing(self) -> None:
        assert (
            _config.watermark_interval_sql({}, "missing_key", 60)
            == "INTERVAL '60' SECOND"
        )

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError):
            _config.watermark_interval_sql({"k": -1}, "k", 30)

    def test_inventory_jobs_use_watermark_helper(self) -> None:
        for job_file, key in [
            ("inventory_bronze_job.py", "inventory_bronze_watermark_delay_seconds"),
            ("inventory_silver_job.py", "inventory_silver_watermark_delay_seconds"),
        ]:
            src = _read(_FLINK_JOBS / job_file)
            assert "watermark_interval_sql" in src
            assert key in src

