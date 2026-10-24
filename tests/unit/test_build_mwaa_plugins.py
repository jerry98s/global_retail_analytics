"""MWAA plugins.zip carries the plugins, the shared observer, and its catalogs."""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from scripts.cloud import build_mwaa_plugins as bp

pytestmark = pytest.mark.unit


def test_zip_bundles_observer_and_catalogs(tmp_path: Path) -> None:
    out = tmp_path / "plugins.zip"
    bp.build(out)
    names = set(zipfile.ZipFile(out).namelist())
    assert {
        "metadata_airflow.py",
        "wap_publish.py",
        "metadata_observer.py",
        "metadata_catalog/layer_catalog.yml",
        "metadata_catalog/metric_catalog.yml",
        "metadata_catalog/semantic/semantic_models.yml",
        "metadata_catalog/semantic/metrics.yml",
    } <= names
    assert not any("__pycache__" in n for n in names)


def test_zip_is_reproducible(tmp_path: Path) -> None:
    first, second = tmp_path / "a.zip", tmp_path / "b.zip"
    bp.build(first)
    bp.build(second)
    assert first.read_bytes() == second.read_bytes()


def test_extracted_observer_seeds_from_bundled_catalogs(tmp_path: Path) -> None:
    out = tmp_path / "plugins.zip"
    bp.build(out)
    plugins = tmp_path / "mwaa" / "plugins"
    zipfile.ZipFile(out).extractall(plugins)
    script = (
        "import metadata_observer as mo\n"
        "w = mo.DuckDBMetadataWriter(mo.Path('local_metadata.duckdb'))\n"
        "w.ensure_schema()\n"
        "mo.seed_catalogs(w)\n"
        "print(len(mo.freshness_targets()), "
        "w.fetchone('SELECT COUNT(*) FROM meta.metric_catalog')[0])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=plugins,
        capture_output=True,
        text=True,
        check=True,
    )
    targets, metrics = map(int, result.stdout.split())
    assert targets >= 10
    assert metrics >= 10


def test_plugins_zip_is_built_not_committed() -> None:
    gitignore = (bp.REPO / ".gitignore").read_text(encoding="utf-8")
    assert "infra/terraform/modules/mwaa/plugins.zip" in gitignore
