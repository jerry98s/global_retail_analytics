-- Metadata database schema and tables.
-- Run while connected to database `metadata` (after 00_create_database.sql).
--
-- Portable types: VARCHAR for JSON payloads (not SUPER) so local DuckDB can
-- share the same logical contract via scripts/common/metadata_observer.py.

CREATE SCHEMA IF NOT EXISTS meta;

-- Governed object registry (idempotent upserts from metadata/catalog/*.yml).
CREATE TABLE IF NOT EXISTS meta.layer_catalog (
    object_fqn           VARCHAR(256)  NOT NULL,
    object_type          VARCHAR(32)   NOT NULL,  -- table | view | external
    platform_layer       VARCHAR(32)   NOT NULL,  -- bronze|silver|staging|intermediate|gold|summary|serving
    domain               VARCHAR(64),
    owner                VARCHAR(128),
    grain                VARCHAR(256),
    timestamp_column     VARCHAR(128),
    freshness_sla_minutes INTEGER,
    is_active            BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    updated_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    collector_version    VARCHAR(32),
    PRIMARY KEY (object_fqn)
);

-- Metric definitions (business KPIs and ops metrics).
CREATE TABLE IF NOT EXISTS meta.metric_catalog (
    metric_name          VARCHAR(128)  NOT NULL,
    description          VARCHAR(1024),
    source_relation      VARCHAR(256)  NOT NULL,
    expression           VARCHAR(1024) NOT NULL,
    grain                VARCHAR(256),
    unit                 VARCHAR(64),
    owner                VARCHAR(128),
    is_active            BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    updated_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    collector_version    VARCHAR(32),
    PRIMARY KEY (metric_name)
);

-- One row per pipeline execution / attempt.
CREATE TABLE IF NOT EXISTS meta.pipeline_run (
    execution_id         VARCHAR(64)   NOT NULL,
    orchestrator_run_id  VARCHAR(256),
    pipeline_name        VARCHAR(128)  NOT NULL,
    environment          VARCHAR(32)   NOT NULL,
    trigger_type         VARCHAR(64),
    logical_date         TIMESTAMP,
    started_at           TIMESTAMP     NOT NULL,
    ended_at             TIMESTAMP,
    status               VARCHAR(32)   NOT NULL,  -- RUNNING|SUCCESS|FAILED|SKIPPED
    duration_seconds     INTEGER,
    error_text           VARCHAR(4000),
    collector_version    VARCHAR(32),
    created_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    updated_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (execution_id)
);

-- Append-only freshness / volume observations.
CREATE TABLE IF NOT EXISTS meta.table_freshness (
    execution_id         VARCHAR(64)   NOT NULL,
    schema_name          VARCHAR(128)  NOT NULL,
    table_name           VARCHAR(128)  NOT NULL,
    row_count            BIGINT,
    max_event_ts         TIMESTAMP,
    lag_minutes          INTEGER,
    sla_status           VARCHAR(32),  -- ok|warn|breach|unknown
    measured_at          TIMESTAMP     NOT NULL,
    collector_version    VARCHAR(32),
    created_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (execution_id, schema_name, table_name)
);

-- Append-only dbt / GE / reconciliation outcomes.
CREATE TABLE IF NOT EXISTS meta.dq_check_result (
    execution_id         VARCHAR(64)   NOT NULL,
    check_system         VARCHAR(32)   NOT NULL,  -- dbt|ge|reconciliation
    check_name           VARCHAR(512)  NOT NULL,
    target_object        VARCHAR(256),
    status               VARCHAR(32)   NOT NULL,  -- pass|fail|warn|error|skipped
    failed_count         INTEGER,
    duration_seconds     DOUBLE PRECISION,
    detail_json          VARCHAR(8000),
    measured_at          TIMESTAMP     NOT NULL,
    collector_version    VARCHAR(32),
    created_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (execution_id, check_system, check_name)
);
