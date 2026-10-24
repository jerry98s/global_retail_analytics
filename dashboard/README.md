# Dashboard — Streamlit

## Local Gold (DuckDB)

Reads the Gold and summary tables that `-Task dbt` builds in
`transformation/dbt_project/local_retail.duckdb`, read-only:

```powershell
$env:DASHBOARD_MODE = "duckdb"
.\.venv\Scripts\streamlit.exe run dashboard/app.py
```

Sales, inventory and session numbers come from the dbt semantic layer
(ADR-013). `semantic_queries.py` holds the SQL MetricFlow compiled for DuckDB
and Redshift; regenerate it after changing
`transformation/dbt_project/models/semantic/*.yml`:

```powershell
cd transformation/dbt_project; ..\..\.venv\Scripts\dbt.exe parse --profiles-dir . --target local; cd ..\..
.\.venv\Scripts\python.exe scripts/common/generate_semantic_queries.py
```

## Local Iceberg (Docker)

Reads the local Iceberg Parquet written by Flink and Spark.

```powershell
docker compose -f infra/docker/compose/docker-compose.yml -f infra/docker/compose/docker-compose.dashboard.yml up -d --build dashboard
```

Open <http://localhost:8501>. The compose file mounts the shared Iceberg
directory read-only. The AWS App Runner deploy is on `main`.
