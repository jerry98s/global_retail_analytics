"""Time the local Spark identity job at 1x, 5x and 10x Bronze.

Each scale is built in ``.local/benchmark`` by replicating the current Bronze
with remapped ids. The real ``.local/iceberg`` warehouse is not written. The
measured command is the same ``spark-submit`` the local stack uses, pointed at
the copy, with a 2g driver heap. The image default of 1g completes 1x and 5x
and runs out of heap at 10x. This is a Spark scaling check, not a streaming
throughput result.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
COMPOSE = REPO / "infra" / "docker" / "compose" / "docker-compose.yml"
SCALES = (1, 5, 10)
CONTAINER = "gra-benchmark-spark"

_MEM = re.compile(r"^([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)")
_MEM_UNITS = {"b": 1 / 1024 / 1024, "kib": 1 / 1024, "kb": 1 / 1024,
              "mib": 1.0, "mb": 1.0, "gib": 1024.0, "gb": 1024.0}


def parse_mem_usage(text: str) -> float | None:
    """First token of ``docker stats`` MemUsage, in MiB."""
    token = text.strip().split()[0] if text.strip() else ""
    match = _MEM.match(token)
    if match is None:
        return None
    unit = _MEM_UNITS.get(match.group(2).lower())
    if unit is None:
        return None
    return float(match.group(1)) * unit


def _docker_stats(name: str) -> float | None:
    listed = subprocess.run(
        ["docker", "ps", "-q", "--filter", "label=gra-benchmark=identity"],
        capture_output=True, text=True, check=False,
    )
    target = listed.stdout.strip().split()[0] if listed.returncode == 0 and listed.stdout.strip() else name
    proc = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", target],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        return None
    return parse_mem_usage(proc.stdout)


class _PeakMemory:
    def __init__(self, name: str) -> None:
        self.name = name
        self.peak_mib = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> _PeakMemory:
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            used = _docker_stats(self.name)
            if used is not None:
                self.peak_mib = max(self.peak_mib, used)
            self._stop.wait(1)


def _compose_script(body: str, benchmark_mount: Path) -> list[str]:
    return [
        "docker", "compose", "-f", str(COMPOSE), "--profile", "spark",
        "run", "--rm", "--no-deps", "--name", CONTAINER,
        "--label", "gra-benchmark=identity",
        "-v", f"{benchmark_mount}:/tmp/benchmark",
        "spark-identity", body,
    ]


def _input_counts(log_text: str) -> dict:
    line = next(line for line in reversed(log_text.splitlines()) if line.startswith("BENCHMARK_INPUT "))
    return json.loads(line[len("BENCHMARK_INPUT "):])


def _manifest_files(warehouse: Path, name: str) -> list[Path]:
    export_dir = warehouse / "consumer_current" / name
    manifest = export_dir / "manifest"
    marker = f"/consumer_current/{name}/"
    files = []
    for entry in json.loads(manifest.read_text(encoding="utf-8"))["entries"]:
        url = str(entry["url"]).replace("\\", "/")
        if marker not in url:
            raise ValueError(f"{manifest}: entry outside {marker}: {url}")
        files.append(export_dir / url.split(marker, 1)[1])
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"{manifest} lists missing files: {missing}")
    return files


def _output_counts(warehouse: Path) -> dict[str, int]:
    import duckdb

    resolution = [str(path) for path in _manifest_files(warehouse, "identity_resolution")]
    edges = [str(path) for path in _manifest_files(warehouse, "identity_edges")]
    con = duckdb.connect()
    try:
        rows, components = con.execute(
            "select count(*), count(distinct component_rep_node) from read_parquet(?)",
            [resolution],
        ).fetchone()
        edge_rows = con.execute("select count(*) from read_parquet(?)", [edges]).fetchone()[0]
    finally:
        con.close()
    return {"resolution_rows": int(rows), "edge_rows": int(edge_rows), "components": int(components)}


def _scale_dir(root: Path, copies: int) -> Path:
    return root / f"scale-{copies}"


def run_scale(copies: int, source: Path, benchmark: Path, driver_memory: str, prepare: bool) -> dict:
    import time

    pos = source / "bronze" / "pos_transactions" / "data"
    if not pos.exists():
        raise SystemExit(f"POS Parquet not found: {pos}")
    dest = _scale_dir(benchmark, copies)
    dest.mkdir(parents=True, exist_ok=True)
    mount = benchmark.resolve()
    replicate = (
        "mkdir -p /tmp/benchmark/scale-{n}/checkpoints && "
        "exec /opt/spark/bin/spark-submit --master local[*] --driver-memory {memory} --py-files scale_ids.py "
        "replicate_bronze.py "
        "--source-warehouse file:///tmp/iceberg "
        "--dest-warehouse file:///tmp/benchmark/scale-{n} "
        "--copies {n} "
        "--pos-source /tmp/iceberg/bronze/pos_transactions/data "
        "--pos-dest /tmp/benchmark/scale-{n}/pos"
    ).format(n=copies, memory=driver_memory)
    identity = (
        "exec /opt/spark/bin/spark-submit --master local[*] --driver-memory {memory} "
        "--py-files graph_logic.py,canonical_events.py,consumer_exports.py,event_contract.py "
        "identity_resolution_job.py --local "
        "--bronze-warehouse file:///tmp/benchmark/scale-{n} "
        "--silver-warehouse file:///tmp/benchmark/scale-{n} "
        "--pos-parquet-path /tmp/benchmark/scale-{n}/pos "
        "--checkpoint-dir /tmp/benchmark/scale-{n}/checkpoints"
    ).format(n=copies, memory=driver_memory)
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    replicate_log = dest / "replicate.log"
    started = time.perf_counter()
    if prepare or not replicate_log.is_file():
        with replicate_log.open("w", encoding="utf-8") as log:
            proc = subprocess.run(
                _compose_script(replicate, mount),
                cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=False,
            )
        if proc.returncode != 0:
            raise SystemExit(f"replicate {copies}x failed; see {replicate_log}")
    prepared = _input_counts(replicate_log.read_text(encoding="utf-8"))
    identity_log = dest / "identity.log"
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    measured_at = time.perf_counter()
    with _PeakMemory(CONTAINER) as peak, identity_log.open("w", encoding="utf-8") as log:
        proc = subprocess.run(
            _compose_script(identity, mount),
            cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=False,
        )
    if proc.returncode != 0:
        raise SystemExit(f"identity {copies}x failed; see {identity_log}")
    result = {
        "scale": copies,
        "driver_memory": driver_memory,
        "wall_seconds": round(time.perf_counter() - measured_at, 1),
        "prepare_seconds": round(measured_at - started, 1),
        "peak_memory_mib": round(peak.peak_mib, 1),
        **prepared,
        **_output_counts(dest),
    }
    print(json.dumps(result, sort_keys=True))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=REPO / ".local" / "iceberg")
    parser.add_argument("--benchmark", type=Path, default=REPO / ".local" / "benchmark")
    parser.add_argument("--scale", type=int, action="append", choices=SCALES, dest="scales")
    parser.add_argument("--driver-memory", default="2g",
                        help="Spark driver heap. 1g is the image default and runs out at 10x.")
    parser.add_argument("--skip-prepare", action="store_true")
    args = parser.parse_args(argv)
    if not args.source.is_dir():
        raise SystemExit(f"Bronze warehouse not found: {args.source}")
    args.benchmark.mkdir(parents=True, exist_ok=True)
    results = [
        run_scale(copies, args.source, args.benchmark, args.driver_memory, not args.skip_prepare)
        for copies in (args.scales or list(SCALES))
    ]
    baseline = next((row for row in results if row["scale"] == 1), None)
    if baseline is not None:
        for row in results:
            factor = row["scale"]
            for key in ("clickstream_rows", "inventory_rows", "pos_rows",
                        "resolution_rows", "edge_rows", "components"):
                if row[key] != baseline[key] * factor:
                    raise SystemExit(
                        f"{factor}x {key} is {row[key]}, expected {baseline[key] * factor}"
                    )
    out = args.benchmark / "results.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
