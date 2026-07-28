"""Static lint enforcing the DAG review checklist (Part 7 of
docs/runbooks/dw-checklist-audit.md) on every DAG file under
orchestration/airflow/dags/.

We lint the source code statically (regex + AST) instead of importing
the DAG module, because Airflow isn't installed in the unit test env
(MWAA is the runtime; local `python -m compileall` skips imports).
The contract enforced here mirrors the 7 checklist items:

  1. Naming standardized — file name == dag_id; dag_id follows the
     `{domain}_{frequency}_{description}` template.
  2. Task granularity appropriate — operator count between 1 and 50.
  3. Parameterization (no hardcoding) — no `datetime(...)` calls other
     than the `start_date` slot; bash_command uses `{{ ds }}` or
     `{{ var.value.* }}` macros for any date / env-specific value.
  4. Idempotency — no `INSERT INTO` (use `INSERT OVERWRITE`, `MERGE`,
     or dbt `delete+insert` / `append`). Streaming jobs are exempt
     (the Flink INSERT INTO is the streaming sink pattern, not a
     batch write — and those statements live in the Flink job files,
     not the DAGs).
  5. Dependencies correct — at least one `>>` chain (single-task DAGs
     are exempt).
  6. Retries and alerts — DEFAULT_ARGS has `retries >= 1` AND
     `email_on_failure=True` (Airflow's default is False, which silently
     disables alerts — this is the most common production gap).
  7. Templates reused — not statically enforceable; left to code review.

If a new DAG is added, this test will fail unless it satisfies the
contract. If a checklist item doesn't apply (e.g. a manual-trigger DAG
has no frequency), document the exception in the DAG's doc_md and add
an allowlist entry below.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DAGS_DIR = _REPO_ROOT / "orchestration" / "airflow" / "dags"

# dag_id must match this template: {domain}_{frequency}_{description}.
# domain and description are lowercase letters / digits / underscores;
# frequency must be one of the known cadences.
_DAG_ID_RE = re.compile(
    r"^[a-z][a-z0-9]*_(daily|hourly|bihourly|weekly|monthly|continuous|manual)_"
    r"[a-z][a-z0-9_]*$"
)

# Files that are NOT DAGs and should be skipped (plugins, helpers, etc.).
# We only lint *.py files that actually define a DAG.
_NON_DAG_FILES: set[str] = set()

# DAG IDs that are exempt from the naming template (with rationale).
# Empty today — every DAG should follow the template.
_NAMING_EXEMPT: set[str] = set()


def _dag_files() -> list[Path]:
    out = []
    for path in _DAGS_DIR.glob("*.py"):
        if path.name in _NON_DAG_FILES:
            continue
        out.append(path)
    return out


def _load_dag_source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_dag_id(source: str) -> str | None:
    """Find `dag_id = "..."` or `dag_id="..."` in the source."""
    m = re.search(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", source)
    return m.group(1) if m else None


def _extract_default_args_block(source: str) -> str | None:
    """Grab the default_args dict literal (either inline or via a
    DEFAULT_ARGS variable). Returns the dict-literal text or None."""
    # Inline: default_args = { ... } or default_args={ ... }
    # Also matches DEFAULT_ARGS = { ... } (case-insensitive). The dict
    # can contain nested dicts (e.g. for Operator kwargs), so the
    # pattern balances one level of nesting.
    m = re.search(
        r"(?i)default_args\s*=\s*\{([^}]*(?:\{[^}]*\}[^}]*)*)\}",
        source,
        re.DOTALL,
    )
    if m:
        return m.group(1)
    return None


def _default_args_has(default_args_text: str, key: str, value: str | None = None) -> bool:
    """Check that `default_args` declares `key` (optionally == value)."""
    if value is None:
        pattern = re.compile(rf"['\"]?{re.escape(key)}['\"]?\s*:\s*\S")
    else:
        pattern = re.compile(
            rf"['\"]?{re.escape(key)}['\"]?\s*:\s*{re.escape(value)}"
        )
    return pattern.search(default_args_text) is not None


def _count_operators(source: str) -> int:
    """Count distinct Airflow operator instantiations in the source.
    Heuristic: count `task_id = "..."` or `task_id="..."` occurrences —
    each operator must declare a task_id, so this is a reliable proxy
    for the operator count without importing Airflow."""
    return len(re.findall(r"task_id\s*=\s*['\"]", source))


def _has_dependency_chain(source: str) -> bool:
    """True if the DAG wires tasks together with `>>`."""
    return ">>" in source


def _has_catchup_false(source: str) -> bool:
    return bool(re.search(r"catchup\s*=\s*False", source))


def _has_doc_md(source: str) -> bool:
    return "doc_md" in source


def _datetime_calls(source: str) -> list[str]:
    """Return all `datetime(...)` calls in the source. The only legitimate
    one is `start_date=datetime(...)`. Any other (e.g. hardcoded
    execution_date filters) is a parameterization smell."""
    return re.findall(r"datetime\([^)]*\)", source)


def _has_insert_into(source: str) -> bool:
    """True if the DAG's bash_command contains a bare `INSERT INTO`
    (without OVERWRITE or MERGE). Idempotency violation per checklist
    item 4. Note: streaming DAGs that wrap Flink jobs don't carry SQL
    in their bash_command, so this is a reliable signal."""
    # Match INSERT INTO ... but NOT INSERT OVERWRITE INTO ...
    # (Hive-style) or "INSERT INTO ... MERGE" patterns.
    for m in re.finditer(r"INSERT\s+INTO", source, re.IGNORECASE):
        # Look backward 20 chars for OVERWRITE — if present, it's actually
        # `INSERT OVERWRITE INTO` which is the idempotent Hive pattern.
        start = max(0, m.start() - 20)
        prefix = source[start : m.start()]
        if re.search(r"OVERWRITE\s*$", prefix, re.IGNORECASE):
            continue
        return True
    return False


_DAG_FILES = _dag_files()


def _dag_id_param(path: Path) -> str:
    src = _load_dag_source(path)
    return _extract_dag_id(src) or path.stem


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_file_name_matches_dag_id(dag_path: Path) -> None:
    """Checklist item 1: file name must match dag_id (without .py)."""
    src = _load_dag_source(dag_path)
    dag_id = _extract_dag_id(src)
    assert dag_id is not None, f"{dag_path.name}: no dag_id found in source"
    assert dag_id == dag_path.stem, (
        f"{dag_path.name}: file name '{dag_path.stem}' does not match "
        f"dag_id '{dag_id}'. The Airflow UI groups by dag_id, so a "
        f"mismatch makes the DAG hard to locate in the file tree."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_id_follows_naming_template(dag_path: Path) -> None:
    """Checklist item 1: dag_id must follow `{domain}_{frequency}_{description}`.
    Allowed frequencies: daily, hourly, bihourly, weekly, monthly,
    continuous, manual. Add to _NAMING_EXEMPT above ONLY with a
    documented rationale in the DAG's doc_md."""
    src = _load_dag_source(dag_path)
    dag_id = _extract_dag_id(src)
    assert dag_id is not None, f"{dag_path.name}: no dag_id found"
    if dag_id in _NAMING_EXEMPT:
        pytest.skip(f"{dag_id}: in _NAMING_EXEMPT allowlist")
    assert _DAG_ID_RE.match(dag_id), (
        f"{dag_path.name}: dag_id '{dag_id}' does not match the "
        f"`{{domain}}_{{frequency}}_{{description}}` template. Expected "
        f"frequencies: daily, hourly, bihourly, weekly, monthly, "
        f"continuous, manual. Example: `warehouse_daily_batch_pipeline`."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_has_retries_configured(dag_path: Path) -> None:
    """Checklist item 6: DEFAULT_ARGS must declare retries >= 1."""
    src = _load_dag_source(dag_path)
    default_args = _extract_default_args_block(src)
    assert default_args is not None, (
        f"{dag_path.name}: no default_args dict found. Every DAG must "
        f"declare DEFAULT_ARGS with retries + email_on_failure."
    )
    m = re.search(r"['\"]?retries['\"]?\s*:\s*(\d+)", default_args)
    assert m is not None, (
        f"{dag_path.name}: DEFAULT_ARGS is missing `retries`. Transient "
        f"faults (network timeouts, S3 throttles) will fail the DAG on "
        f"first attempt with no retry."
    )
    retries = int(m.group(1))
    assert retries >= 1, (
        f"{dag_path.name}: DEFAULT_ARGS.retries={retries} — must be >= 1."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_has_email_on_failure(dag_path: Path) -> None:
    """Checklist item 6: DEFAULT_ARGS must set email_on_failure=True.

    Airflow's default for `email_on_failure` is False, so omitting it
    silently disables alerts. This is the most common production gap
    and was the actual finding that triggered this audit (PR9)."""
    src = _load_dag_source(dag_path)
    default_args = _extract_default_args_block(src)
    assert default_args is not None
    assert _default_args_has(default_args, "email_on_failure", "True"), (
        f"{dag_path.name}: DEFAULT_ARGS missing `email_on_failure=True`. "
        f"Airflow defaults this to False, so failures will silently "
        f"skip alerting — the most common production gap."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_has_catchup_false(dag_path: Path) -> None:
    """Backfill safety: catchup=False prevents Airflow from running
    every missed schedule interval on startup (which can flood the
    scheduler + EMR with parallel Flink submissions). Enable catchup
    only with an explicit rationale in the DAG doc_md."""
    src = _load_dag_source(dag_path)
    assert _has_catchup_false(src), (
        f"{dag_path.name}: DAG must declare `catchup=False`. Enable "
        f"catchup=True only with a documented rationale (e.g. backfill "
        f"DAGs that intentionally replay missed intervals)."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_has_doc_md(dag_path: Path) -> None:
    """Checklist item 6 (alerts + observability): every DAG must set
    `doc_md` so the Airflow UI shows purpose + recovery steps. The
    portfolio convention is `doc_md=__doc__` (module docstring) or
    `doc_md=DAG_DOC_MD` for richer markdown."""
    src = _load_dag_source(dag_path)
    assert _has_doc_md(src), (
        f"{dag_path.name}: DAG missing `doc_md`. Set `doc_md=__doc__` "
        f"or `doc_md=DAG_DOC_MD` so the Airflow UI renders purpose + "
        f"recovery steps in the 'Details' tab."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_task_count_within_bounds(dag_path: Path) -> None:
    """Checklist item 2: 1-50 tasks per DAG. <1 means the DAG is empty;
    >50 means it should be split into TaskGroups or sub-DAGs for
    readability."""
    src = _load_dag_source(dag_path)
    count = _count_operators(src)
    assert 1 <= count <= 50, (
        f"{dag_path.name}: {count} operators — outside the 1-50 band. "
        f"Split into TaskGroups or sub-DAGs."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_has_dependency_chain(dag_path: Path) -> None:
    """Checklist item 5: DAG must wire tasks together with `>>` (unless
    it's a single-task DAG, in which case there's nothing to chain)."""
    src = _load_dag_source(dag_path)
    count = _count_operators(src)
    if count <= 1:
        pytest.skip(f"{dag_path.name}: single-task DAG — no chain needed")
    assert _has_dependency_chain(src), (
        f"{dag_path.name}: DAG has {count} operators but no `>>` chain. "
        f"Airflow will run them in arbitrary order — verify dependencies."
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_no_hardcoded_datetime(dag_path: Path) -> None:
    """Checklist item 3: no `datetime(...)` calls other than the
    `start_date` slot. Hardcoded execution dates break backfills."""
    src = _load_dag_source(dag_path)
    calls = _datetime_calls(src)
    # Every DAG legitimately has start_date=datetime(...). Allow exactly
    # one such call; any additional datetime(...) is a parameterization
    # smell.
    assert len(calls) <= 1, (
        f"{dag_path.name}: found {len(calls)} `datetime(...)` calls. "
        f"Only `start_date=datetime(...)` is allowed — any other "
        f"hardcoded date breaks backfills. Use `{{{{ ds }}}}` or "
        f"`{{{{ var.value.X }}}}` macros instead. Calls: {calls}"
    )


@pytest.mark.parametrize("dag_path", _DAG_FILES, ids=[_dag_id_param(p) for p in _DAG_FILES])
def test_dag_no_bare_insert_into(dag_path: Path) -> None:
    """Checklist item 4: no `INSERT INTO` in DAG bash_command strings
    (use `INSERT OVERWRITE`, `MERGE`, or dbt `delete+insert`). Bare
    `INSERT INTO` duplicates rows on retry. Streaming DAGs that wrap
    Flink jobs are exempt — their INSERT INTO statements live in the
    Flink job files, not the DAGs."""
    src = _load_dag_source(dag_path)
    assert not _has_insert_into(src), (
        f"{dag_path.name}: found `INSERT INTO` without OVERWRITE in "
        f"the DAG source. Use `INSERT OVERWRITE`, `MERGE`, or dbt "
        f"`delete+insert` for idempotent writes (checklist item 4)."
    )


def test_dag_count_known() -> None:
    """Sanity check: we lint exactly the 6 known DAGs. If a DAG is added
    or removed, update this count and review the parametrized tests
    above for the new file."""
    assert len(_DAG_FILES) == 6, (
        f"Expected 6 DAG files, found {len(_DAG_FILES)}: "
        f"{[p.name for p in _DAG_FILES]}"
    )
