-- DiMer PostgreSQL Schema
-- Apply with: psql -d mydb -f postgres_schema.sql
--
-- Notes:
--   UUIDs use the uuid-ossp extension
--   JSON stored as JSONB
--   Booleans stored as BOOLEAN
--   Timestamps stored as TIMESTAMPTZ

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS project (
    project_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        VARCHAR NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS "user" (
    user_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email      VARCHAR UNIQUE,
    name       VARCHAR NOT NULL,
    local_cli  BOOLEAN NOT NULL DEFAULT FALSE  -- TRUE for auto-created CLI user
);

CREATE TABLE IF NOT EXISTS project_source (
    source_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id  UUID NOT NULL REFERENCES project(project_id),
    source_type VARCHAR NOT NULL,             -- 'postgresql', 'snowflake', etc.
    source_name VARCHAR NOT NULL,             -- human label unique within the project
    host        VARCHAR,
    port        INTEGER,
    db_name     VARCHAR,
    user_id     UUID REFERENCES "user"(user_id),
    UNIQUE (project_id, source_type, source_name)
);

-- Tracks each comparison job configuration
CREATE TABLE IF NOT EXISTS diff_job (
    job_id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id               UUID NOT NULL REFERENCES project(project_id),
    source_a_id              UUID NOT NULL REFERENCES project_source(source_id),
    source_a_asset           VARCHAR NOT NULL,   -- fully-qualified table name
    source_b_id              UUID NOT NULL REFERENCES project_source(source_id),
    source_b_asset           VARCHAR NOT NULL,
    key_columns              JSONB NOT NULL,     -- e.g. ["id", "tenant_id"]
    snapshot_retention_count INTEGER NOT NULL DEFAULT 10,
    save_original_values     BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (source_a_id, source_a_asset, source_b_id, source_b_asset, key_columns)
);

-- Each compare run
-- Multiple runs could exist for a diff_job (configuration)
CREATE TABLE IF NOT EXISTS diff_run (
    run_id                 UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id                 UUID NOT NULL REFERENCES diff_job(job_id),
    run_at                 TIMESTAMPTZ NOT NULL,
    status                 VARCHAR NOT NULL,     -- 'success' | 'failed'
    algorithm              VARCHAR,              -- 'JOIN_DIFF' | 'CROSS_DB_DIFF'
    execution_time_seconds DOUBLE PRECISION,
    match                  BOOLEAN,              -- TRUE = tables identical
    error                  TEXT                 -- error message if status = 'failed'
);

-- Similar to diff_run but with additional details
-- Primarily this helps in storing the details of the run for historical reference if job configuration changes
-- Additionally this can store other details about the asset/table that was compared, like row count, etc
CREATE TABLE IF NOT EXISTS diff_run_detail (
    run_id              UUID PRIMARY KEY REFERENCES diff_run(run_id),
    source_a_asset      VARCHAR,                 -- FQ name at time of run
    source_a_row_count  BIGINT,
    source_b_asset      VARCHAR,
    source_b_row_count  BIGINT,
    common_columns      JSONB,
    schema_differences  JSONB,
    columns_not_matched JSONB                    -- {"source_only": [], "target_only": []}
);

CREATE TABLE IF NOT EXISTS diff_result (
    run_id         UUID PRIMARY KEY REFERENCES diff_run(run_id),
    job_id         UUID NOT NULL,
    added_count    BIGINT NOT NULL DEFAULT 0,
    deleted_count  BIGINT NOT NULL DEFAULT 0,
    modified_count BIGINT NOT NULL DEFAULT 0,
    matched_count  BIGINT NOT NULL DEFAULT 0,
    diffed_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS diff_row (
    run_id             UUID NOT NULL REFERENCES diff_run(run_id),
    key_hash           CHAR(32) NOT NULL,         -- MD5(sorted JSON of key_values)
    key_values         JSONB NOT NULL,            -- Key column values used for comparison as JSON object, e.g. {"key_col_name": key_col_value}  
    status             VARCHAR NOT NULL,           -- 'added' | 'deleted' | 'modified'
    mismatched_columns JSONB,                      -- array of columns that caused the mismatch for this row - MODIFIED rows only
    source_values      JSONB,                      -- populated when save_original_values=true
    target_values      JSONB,                      -- populated when save_original_values=true
    PRIMARY KEY (run_id, key_hash)
);
