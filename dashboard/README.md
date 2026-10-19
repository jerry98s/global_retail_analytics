# Dashboard — Streamlit

Reads the local Iceberg Parquet written by Flink and Spark.

```powershell
docker compose -f infra/docker/compose/docker-compose.yml -f infra/docker/compose/docker-compose.dashboard.yml up -d --build dashboard
```

Open <http://localhost:8501>. The compose file mounts the shared Iceberg
directory read-only. The AWS App Runner deploy is on `main`.
