-- config/schema.sql
-- Production PostgreSQL / Supabase Database Schema

-- 1. Pipelines Configuration Table
CREATE TABLE IF NOT EXISTS pipelines (
    job_id        TEXT PRIMARY KEY,
    tool_type     TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    source_config TEXT NOT NULL,
    target_type   TEXT NOT NULL,
    target_config TEXT NOT NULL,
    tool_config   TEXT NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipelines_job_id ON pipelines(job_id);

-- 2. Pipeline Runs Log Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    pipeline_name TEXT,
    status TEXT,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration INTEGER,
    tool_name TEXT,
    rows_read BIGINT,
    rows_written BIGINT,
    error_message TEXT,
    raw_log JSONB,
    execution_mode TEXT,
    triggered_by TEXT,
    orchestrator_tool TEXT,
    orchestrator_dag_id TEXT,
    orchestrator_task_id TEXT,
    orchestrator_run_id TEXT,
    saved_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id);

-- 3. Source Asset Metadata Table
CREATE TABLE IF NOT EXISTS source_asset_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name TEXT,
    system_type TEXT,
    database_name TEXT,
    schema_name TEXT,
    object_name TEXT,
    object_type TEXT,
    row_count BIGINT,
    column_count INTEGER,
    size_bytes BIGINT,
    last_updated_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_source_asset_metadata_run_id ON source_asset_metadata(run_id);

-- 4. Target Asset Metadata Table
CREATE TABLE IF NOT EXISTS target_asset_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name TEXT,
    system_type TEXT,
    database_name TEXT,
    schema_name TEXT,
    object_name TEXT,
    object_type TEXT,
    row_count BIGINT,
    column_count INTEGER,
    size_bytes BIGINT,
    last_updated_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_target_asset_metadata_run_id ON target_asset_metadata(run_id);
