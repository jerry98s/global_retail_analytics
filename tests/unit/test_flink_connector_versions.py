"""Unit tests guarding the Flink connector version pins against drift.

The local Flink Docker image (infra/docker/flink/Dockerfile) installs
Iceberg, flink-sql-connector-kafka, hadoop-aws, and aws-java-sdk-bundle.
infra/docker/flink/versions.env is the source of truth for those four pins.
This test parses both files and asserts they agree.

If you intentionally bump a version:
  1. Update infra/docker/flink/versions.env.
  2. Update the matching ENV <NAME>=<value> line in the Dockerfile.
  3. Re-run this test — it should pass.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

VERSIONS_ENV = REPO_ROOT / "infra" / "docker" / "flink" / "versions.env"
DOCKERFILE = REPO_ROOT / "infra" / "docker" / "flink" / "Dockerfile"
SHARED_PINS = (
    "ICEBERG_VERSION",
    "KAFKA_CONNECTOR_VERSION",
    "HADOOP_AWS_VERSION",
    "AWS_SDK_VERSION",
)


def _parse_versions_env(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE lines from versions.env, skipping comments and blanks."""
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        out[key.strip()] = value.strip()
    return out


def _parse_dockerfile_args(path: Path) -> dict[str, str]:
    """Extract `ENV <NAME>=<value>` pins from the Dockerfile.

    Dockerfile ENV can span multiple lines via `\\` continuation, e.g.:
        ENV ICEBERG_VERSION=1.4.3 \\
            KAFKA_CONNECTOR_VERSION=1.17.1
    We first collapse `\\\\\\n` to a single space, then regex every
    `NAME=value` token that follows an ENV keyword.
    """
    text = path.read_text(encoding="utf-8")
    # Collapse backslash-newline continuations so multi-line ENV/RUN blocks
    # become one logical line.
    flat = re.sub(r"\\\s*\n", " ", text)
    pattern = re.compile(
        r"^\s*ENV\s+([A-Z_][A-Z0-9_]*)\s*=\s*(\S+)",
        re.MULTILINE,
    )
    pins: dict[str, str] = dict(pattern.findall(flat))
    # ENV also accepts multiple `NAME=value` pairs on one line (after the
    # initial ENV keyword). The first one is captured above; pick up the rest.
    multi_pattern = re.compile(
        r"^\s*ENV\s+([A-Z_][A-Z0-9_]*=\S+(?:\s+[A-Z_][A-Z0-9_]*=\S+)+)",
        re.MULTILINE,
    )
    for group in multi_pattern.findall(flat):
        for token in group.split():
            if "=" not in token:
                continue
            key, _, value = token.partition("=")
            pins[key] = value
    return pins


def test_versions_env_file_exists() -> None:
    assert VERSIONS_ENV.is_file(), f"Missing {VERSIONS_ENV}"
    assert SHARED_PINS and all(
        name in _parse_versions_env(VERSIONS_ENV) for name in SHARED_PINS
    ), f"versions.env must define all of {SHARED_PINS}"


def test_dockerfile_pins_match_versions_env() -> None:
    env_pins = _parse_versions_env(VERSIONS_ENV)
    dockerfile_pins = _parse_dockerfile_args(DOCKERFILE)
    for name in SHARED_PINS:
        assert name in dockerfile_pins, (
            f"Dockerfile is missing `ARG {name}=...`. "
            f"Add it and set it to {env_pins[name]} to match versions.env."
        )
        assert dockerfile_pins[name] == env_pins[name], (
            f"Dockerfile ARG {name}={dockerfile_pins[name]} does not match "
            f"versions.env {name}={env_pins[name]}. "
            f"Update the Dockerfile to match versions.env (or vice versa)."
        )


@pytest.mark.parametrize("name", SHARED_PINS)
def test_each_shared_pin_is_present_everywhere(name: str) -> None:
    env_pins = _parse_versions_env(VERSIONS_ENV)
    dockerfile_pins = _parse_dockerfile_args(DOCKERFILE)
    assert env_pins.get(name) is not None
    assert dockerfile_pins.get(name) is not None
    assert env_pins[name] == dockerfile_pins[name]
