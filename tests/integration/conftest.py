"""Conftest for the dbt/DuckDB idempotency suite (P2.1).

Puts the repo root on ``sys.path`` (mirrors tests/unit/conftest.py) so
any shared helpers import cleanly, and exposes a session-scoped fixture
for the dbt project directory + DuckDB file path used by the
idempotency test.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="session")
def dbt_project_dir() -> Path:
    """Path to the dbt project (contains dbt_project.yml, profiles.yml.example)."""
    return REPO_ROOT / "transformation" / "dbt_project"


@pytest.fixture(scope="session")
def duckdb_path(dbt_project_dir: Path) -> Path:
    """DuckDB file path used by the local `--target local` profile.

    Tests that materialize models will write here; cleanup is the test's
    responsibility (we don't auto-delete to allow inspection on failure).
    """
    return dbt_project_dir / "local_retail.duckdb"
