# Docker artifacts

Local-dev Docker artifacts for the Global Retail Analytics platform. Cloud
runs on EMR (no Docker) — see `../emr-bootstrap/` for that side.

## Layout

| Path | Purpose |
|------|---------|
| `compose/docker-compose.yml` | Local dev stack: Zookeeper + Kafka + schema-registry + Flink JobManager/TaskManager. Iceberg warehouse on a local named volume (`file:///tmp/iceberg`). |
| `compose/docker-compose.dashboard.yml` | Optional overlay: Streamlit dashboard reading the shared `flink-iceberg` volume. |
| `flink/Dockerfile` | Local Flink 1.17.1 image, pinned to match EMR 6.15.0. Installs Iceberg + Kafka + Hadoop-AWS + AWS SDK connector JARs. |
| `flink/versions.env` | Single source of truth for the four shared connector version pins (Iceberg, Kafka, Hadoop-AWS, AWS SDK). |
| `../emr-bootstrap/install_flink_connectors.sh` | EMR bootstrap action that installs the same connector JARs on cluster nodes. Versions kept in sync with `flink/versions.env` via `tests/unit/test_flink_connector_versions.py`. |
| `../../dashboard/Dockerfile` | Streamlit app image (co-located with the app, not under this dir — standard practice). |

## Running the local stack

The preferred entry point is the wrapper script, which resolves the compose
file path itself so you can invoke it from any cwd:

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task flink
```

Ad-hoc `docker compose` calls need the `-f` flag since the compose files
no longer live at the repo root:

```powershell
docker compose -f infra/docker/compose/docker-compose.yml up -d
docker compose -f infra/docker/compose/docker-compose.yml -f infra/docker/compose/docker-compose.dashboard.yml up -d dashboard
docker compose -f infra/docker/compose/docker-compose.yml exec flink-taskmanager ls -R /tmp/iceberg/
```

## Why a `versions.env` file

Neither Docker `ENV` statements nor an EMR bootstrap action can natively
`source` an env file at build/runtime. Both `flink/Dockerfile` and
`../emr-bootstrap/install_flink_connectors.sh` therefore pin the four
shared connector versions (Iceberg, Kafka, Hadoop-AWS, AWS SDK)
independently. `flink/versions.env` is the single source of truth; the
unit test `tests/unit/test_flink_connector_versions.py` parses all three
files and fails CI on drift.

When bumping a version:

1. Edit `flink/versions.env`.
2. Update the matching `ENV <NAME>=<value>` line in `flink/Dockerfile`.
3. Update the matching `<NAME>="<value>"` line in
   `../emr-bootstrap/install_flink_connectors.sh`.
4. Run `python -m pytest tests/unit/test_flink_connector_versions.py -v`.

Intentionally-different pins (not in `versions.env`):

- `aws-msk-iam-auth-1.1.9` — EMR-only; local Kafka uses PLAINTEXT.
- `commons-logging-1.2` and the `flink-s3-fs-hadoop` plugin — Docker-only;
  EMR ships Hadoop natively.
