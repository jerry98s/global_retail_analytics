"""Build the MWAA plugins.zip that the platform Terraform stack uploads.

MWAA loads plugins only from ``mwaa/plugins.zip``. The zip holds the Airflow
plugins, the shared ``scripts/common/metadata_observer.py``, and the catalog and
semantic-layer YAML the observer seeds ``meta.*`` from (``metadata_catalog/``
beside it). Entries use a fixed timestamp, so unchanged sources give the same
bytes and Terraform does not push a new plugins version.

Usage::

    python scripts/cloud/build_mwaa_plugins.py [--output PATH]
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO / "infra" / "terraform" / "modules" / "mwaa" / "plugins.zip"
_FIXED_DATE = (1980, 1, 1, 0, 0, 0)


def plugin_entries(repo: Path = REPO) -> list[tuple[Path, str]]:
    """(source file, archive name) pairs; raises if any source is missing."""
    plugins = repo / "orchestration" / "airflow" / "plugins"
    entries = [(p, p.name) for p in sorted(plugins.glob("*.py"))]
    entries.append((repo / "scripts" / "common" / "metadata_observer.py", "metadata_observer.py"))
    for name in ("layer_catalog.yml", "metric_catalog.yml"):
        entries.append((repo / "metadata" / "catalog" / name, f"metadata_catalog/{name}"))
    semantic = repo / "transformation" / "dbt_project" / "models" / "semantic"
    for name in ("semantic_models.yml", "metrics.yml"):
        entries.append((semantic / name, f"metadata_catalog/semantic/{name}"))
    missing = [str(src) for src, _ in entries if not src.is_file()]
    if missing:
        raise FileNotFoundError(f"plugins.zip sources missing: {missing}")
    return entries


def build(output: Path = DEFAULT_OUTPUT, repo: Path = REPO) -> list[str]:
    entries = plugin_entries(repo)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w") as zf:
        for src, arcname in entries:
            info = zipfile.ZipInfo(arcname, date_time=_FIXED_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, src.read_bytes())
    return [arcname for _, arcname in entries]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    names = build(args.output)
    print(f"Built {args.output} ({len(names)} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
