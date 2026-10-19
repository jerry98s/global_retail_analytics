"""Extract graphframes Python modules from the Maven/spark-packages JAR."""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

jar = Path("/opt/spark/jars") / f"graphframes-{os.environ['GRAPHFRAMES_VERSION']}.jar"
dest = Path("/opt/graphframes-python")
dest.mkdir(parents=True, exist_ok=True)

if not jar.is_file() or jar.stat().st_size < 10_000:
    raise SystemExit(f"graphframes jar missing or too small: {jar} ({jar.stat().st_size if jar.is_file() else 0} bytes)")

with zipfile.ZipFile(jar) as zf:
    for name in zf.namelist():
        norm = name.replace("\\", "/")
        if "__pycache__" in norm or not norm.endswith(".py"):
            continue
        if norm.startswith("graphframes/"):
            rel = norm
        elif "/graphframes/" in norm:
            rel = "graphframes/" + norm.split("graphframes/", 1)[1]
        else:
            continue
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(zf.read(name))

py_files = list(dest.rglob("*.py"))
if not py_files:
    raise SystemExit(f"no graphframes Python modules in {jar}")
print(f"extracted {len(py_files)} graphframes py files")
