"""Committed POS loyalty extracts the identity job reads.

The daily POS step writes one file of distinct loyalty ids per date under a
new run directory, then replaces one manifest only after every requested date
has succeeded. The hourly job reads that manifest and no other files. A POS
date the manifest does not cover blocks identity publication.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

_DT_DIR = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")
_REMOTE = ("s3://", "s3a://", "hdfs://", "wasb://", "abfs://", "gs://")


def partition_dates_from_names(names) -> set[str]:
    """``dt=YYYY-MM-DD`` directory names, ignoring anything else."""
    dates: set[str] = set()
    for name in names:
        leaf = str(name).rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1]
        match = _DT_DIR.fullmatch(leaf)
        if match:
            dates.add(match.group(1))
    return dates


def dates_without_loyalty(pos_dates, loyalty_dates) -> list[str]:
    """POS dates that have no committed loyalty file, oldest first."""
    return sorted(set(pos_dates) - set(loyalty_dates))


def pos_data_path_beside(loyalty_path: str) -> str:
    """POS ``data`` directory next to a ``pos_loyalty_ids`` root, or ``""``."""
    needle = "pos_loyalty_ids"
    if needle not in loyalty_path:
        return ""
    sibling = loyalty_path.replace(needle, "pos_transactions", 1).rstrip("/\\")
    return sibling + "/data"


def parse_manifest(payload: dict) -> dict:
    """Reject a manifest whose dates, files, or paths are not usable."""
    if not isinstance(payload, dict):
        raise ValueError("loyalty manifest must be an object")
    files = payload.get("files")
    dates = payload.get("dates")
    run_id = payload.get("run_id")
    if not isinstance(files, list) or not isinstance(dates, list) or not run_id:
        raise ValueError("loyalty manifest is missing run_id, dates, or files")
    seen: list[str] = []
    for entry in files:
        if not isinstance(entry, dict):
            raise ValueError("loyalty manifest entry is invalid")
        day = entry.get("date")
        relative = entry.get("relative_path")
        if not isinstance(day, str) or not day or not isinstance(relative, str):
            raise ValueError("loyalty manifest entry is invalid")
        parts = relative.replace("\\", "/").split("/")
        if not relative or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("loyalty manifest path escapes the extract root")
        seen.append(day)
    if set(seen) != set(dates):
        raise ValueError("loyalty manifest dates do not match its files")
    return payload


def merge_manifest(previous: dict | None, updates: list[tuple[str, str]], run_id: str) -> dict:
    """New files replace their dates. Every other committed date stays."""
    by_date: dict[str, list[str]] = {}
    if previous:
        for entry in previous["files"]:
            by_date.setdefault(entry["date"], []).append(entry["relative_path"])
    replaced = {day for day, _relative in updates}
    for day in replaced:
        by_date.pop(day, None)
    for day, relative in updates:
        by_date.setdefault(day, []).append(relative)
    dates = sorted(by_date)
    files = [
        {"date": day, "relative_path": relative}
        for day in dates
        for relative in by_date[day]
    ]
    return parse_manifest({"run_id": run_id, "dates": dates, "files": files})


def require_committed_coverage(pos_dates, manifest: dict | None) -> None:
    """Fail when a POS date has no file in the committed manifest."""
    covered = set(manifest["dates"]) if manifest is not None else set()
    missing = dates_without_loyalty(pos_dates, covered)
    if missing:
        raise ValueError(
            "POS dates have no committed loyalty extract; identity publication blocked: "
            + ", ".join(missing)
        )


def resolve_files(root: str, manifest: dict) -> list[str]:
    """Absolute paths for the files a manifest commits, in manifest order."""
    base = root.rstrip("/\\")
    paths: list[str] = []
    for entry in manifest["files"]:
        relative = entry["relative_path"].replace("\\", "/")
        if "://" in base:
            paths.append(base + "/" + relative)
        else:
            paths.append(str(Path(base) / relative))
    return paths


def local_path(uri: str) -> Path | None:
    """Local filesystem path, or ``None`` when ``uri`` is a remote store."""
    text = uri.strip()
    if text.startswith("file:"):
        return Path(url2pathname(unquote(urlparse(text).path)))
    if text.startswith(_REMOTE) or "://" in text:
        return None
    return Path(text)


def local_partition_dates(uri: str) -> set[str] | None:
    """Date partitions under a local ``data`` directory.

    ``None`` means the URI is remote and the caller must list it another way.
    A missing local directory is an empty set.
    """
    root = local_path(uri)
    if root is None:
        return None
    if not root.is_dir():
        return set()
    return partition_dates_from_names(child.name for child in root.iterdir() if child.is_dir())


def local_parquet_bytes(uri: str) -> int | None:
    """Bytes of local Parquet files under ``uri``. ``None`` when it is remote."""
    root = local_path(uri)
    if root is None:
        return None
    if not root.exists():
        return 0
    if root.is_file():
        return root.stat().st_size if root.name.endswith(".parquet") else 0
    return sum(path.stat().st_size for path in root.rglob("*.parquet") if path.is_file())
