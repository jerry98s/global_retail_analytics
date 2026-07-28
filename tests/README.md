# tests/

Pure-Python **unit tests** — fast, no DB/dbt/Iceberg deps, run in CI on every push.

Run: `pytest tests/unit/ -v -m unit`

## tests/ vs quality/pytest/

| Location | Purpose | Dependencies |
|---|---|---|
| `tests/unit/` | Pure-Python unit tests (config parsing, macros, helpers) | None — runs anywhere |
| `quality/pytest/` | Integration tests against built dbt models (identity graph, SCD2, referential integrity, session reconstruction) | dbt build artifacts — Redshift (cloud) or DuckDB (local `--target local`) |

If your test needs to read from a built table or run dbt, put it in `quality/pytest/`.
If it only exercises Python logic (YAML loading, macro rendering, schema validation), put it here.
