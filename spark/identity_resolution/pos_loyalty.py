"""Per-date POS loyalty extracts the identity job reads.

The daily POS step writes one file of distinct loyalty ids beside the POS
partitions. The hourly job reads those files. A POS date with no file is a
gap to report; the job still resolves the dates that exist.
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
    """POS dates that have no loyalty file, oldest first."""
    return sorted(set(pos_dates) - set(loyalty_dates))


def pos_data_path_beside(loyalty_path: str) -> str:
    """POS ``data`` directory next to a ``pos_loyalty_ids`` path, or ``""``."""
    needle = "pos_loyalty_ids"
    if needle not in loyalty_path:
        return ""
    return loyalty_path.replace(needle, "pos_transactions", 1)


def missing_loyalty_message(dates: list[str]) -> str:
    listed = ", ".join(dates)
    return (
        "WARNING: POS dates have no loyalty file; "
        f"identity uses the loyalty dates that exist: {listed}"
    )


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
