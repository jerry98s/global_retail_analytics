# tests/

Offline unit tests live in `tests/unit/` and run with:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/ -q -m unit
```

Built-table checks live in `tests/integration/` and are opt-in
(`pytest tests/integration -m integration_duckdb`). The idempotency test
deletes `local_retail.duckdb` in its checkout, so run it in a disposable
copy.
