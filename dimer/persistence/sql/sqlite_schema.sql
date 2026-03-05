-- DiMer SQLite Schema
-- Apply with: sqlite3 ~/.dimer/dimer.db < sqlite_schema.sql
--
-- Notes:
--   UUIDs stored as TEXT (36 chars)
--   Booleans stored as INTEGER (0 / 1)
--   Timestamps stored as TEXT (ISO-8601)
--   JSON stored as TEXT

CREATE TABLE IF NOT EXISTS project (
    project_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS "user" (
    user_id    TEXT PRIMARY KEY,
    email      TEXT UNIQUE,
    name       TEXT NOT NULL,
    local_cli  INTEGER NOT NULL DEFAULT 0   -- 1 for auto-created CLI user
);

CREATE TABLE IF NOT EXISTS project_source (
    source_id   TEXT PRIMARY KEY,
    project_id  TEXT NOT NULL REFERENCES project(project_id),
    source_type TEXT NOT NULL,              -- 'postgresql', 'snowflake', etc.
    source_name TEXT NOT NULL,              -- human label unique within the project
    host        TEXT,
    port        INTEGER,
    db_name     TEXT,
    user_id     TEXT REFERENCES "user"(user_id),
    UNIQUE (project_id, source_type, source_name)
);

CREATE TABLE IF NOT EXISTS diff_job (
    job_id                   TEXT PRIMARY KEY,
    project_id               TEXT NOT NULL REFERENCES project(project_id),
    source_a_id              TEXT NOT NULL REFERENCES project_source(source_id),
    source_a_asset           TEXT NOT NULL,  -- fully-qualified table name
    source_b_id              TEXT NOT NULL REFERENCES project_source(source_id),
    source_b_asset           TEXT NOT NULL,
    key_columns              TEXT NOT NULL,  -- JSON array, e.g. ["id", "tenant_id"]
    snapshot_retention_count INTEGER NOT NULL DEFAULT 10,
    save_original_values     INTEGER NOT NULL DEFAULT 0,
    UNIQUE (source_a_id, source_a_asset, source_b_id, source_b_asset, key_columns)
);

CREATE TABLE IF NOT EXISTS diff_run (
    run_id                 TEXT PRIMARY KEY,
    job_id                 TEXT NOT NULL REFERENCES diff_job(job_id),
    run_at                 TEXT NOT NULL,    -- ISO-8601 UTC timestamp
    status                 TEXT NOT NULL,   -- 'success' | 'failed'
    algorithm              TEXT,            -- 'JOIN_DIFF' | 'CROSS_DB_DIFF'
    execution_time_seconds REAL,
    match                  INTEGER,         -- 1 = tables identical, 0 = differ
    error                  TEXT            -- error message if status = 'failed'
);

CREATE TABLE IF NOT EXISTS diff_run_detail (
    run_id              TEXT PRIMARY KEY REFERENCES diff_run(run_id),
    source_a_asset      TEXT,               -- FQ name at time of run (historical)
    source_a_row_count  INTEGER,
    source_b_asset      TEXT,
    source_b_row_count  INTEGER,
    common_columns      TEXT,               -- JSON array
    schema_differences  TEXT,               -- JSON object
    columns_not_matched TEXT                -- JSON object {source_only: [], target_only: []}
);

CREATE TABLE IF NOT EXISTS diff_result (
    run_id         TEXT PRIMARY KEY REFERENCES diff_run(run_id),
    job_id         TEXT NOT NULL,
    added_count    INTEGER NOT NULL DEFAULT 0,
    deleted_count  INTEGER NOT NULL DEFAULT 0,
    modified_count INTEGER NOT NULL DEFAULT 0,
    matched_count  INTEGER NOT NULL DEFAULT 0,
    diffed_at      TEXT NOT NULL            -- ISO-8601 UTC timestamp
);

CREATE TABLE IF NOT EXISTS diff_row (
    run_id             TEXT NOT NULL REFERENCES diff_run(run_id),
    key_hash           TEXT NOT NULL,        -- MD5(sorted JSON of key_values)
    key_values         TEXT NOT NULL,        -- JSON {"col": "val"}
    status             TEXT NOT NULL,        -- 'added' | 'deleted' | 'modified'
    mismatched_columns TEXT,                 -- JSON array; MODIFIED rows only
    source_values      TEXT,                 -- JSON; populated when save_original_values=true
    target_values      TEXT,                 -- JSON; populated when save_original_values=true
    PRIMARY KEY (run_id, key_hash)
);
