"""
config/results_db.py
---------------------
Centralized database layer for pipeline run results.
Supports AWS RDS MySQL (when CENTRAL_DB_HOST is set), Supabase / PostgreSQL
(when DATABASE_URL is set), and SQLite (local dev fallback).

Tables created:
  - pipeline_runs (orchestrator + execution logs)
  - source_asset_metadata (source system snapshots)
  - target_asset_metadata (target system snapshots)
"""

import json
import logging
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

try:
    import pymysql  # type: ignore # pyright: ignore[reportMissingImports]
    import pymysql.cursors  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    pymysql = None

try:
    import psycopg2  # type: ignore # pyright: ignore[reportMissingImports]
    import psycopg2.extras  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    psycopg2 = None

load_dotenv()

logger = logging.getLogger(__name__)

_LOCAL_DB_PATH = Path(__file__).parent / "results.db"


def is_mysql() -> bool:
    return bool(os.getenv("CENTRAL_DB_HOST") or os.getenv("MYSQL_HOST"))


def is_postgres() -> bool:
    url = os.getenv("DATABASE_URL", "")
    return (url.startswith("postgresql://") or url.startswith("postgres://")) and not is_mysql()


def _get_mysql_conn():
    if pymysql is None:
        raise RuntimeError("pymysql is not installed")
    host = os.getenv("CENTRAL_DB_HOST") or os.getenv("MYSQL_HOST") or "localhost"
    port = int(os.getenv("CENTRAL_DB_PORT") or os.getenv("MYSQL_PORT") or 3306)
    db = os.getenv("CENTRAL_DB_NAME") or os.getenv("MYSQL_DATABASE") or "webhooks_db"
    user = os.getenv("CENTRAL_DB_USER") or os.getenv("MYSQL_USER") or "admin"
    password = os.getenv("CENTRAL_DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or ""
    dict_cursor = getattr(getattr(pymysql, "cursors", None), "DictCursor", None)
    return pymysql.connect(  # type: ignore # pyright: ignore
        host=host, port=port, user=user, password=password,
        database=db, charset="utf8mb4", cursorclass=dict_cursor, autocommit=True
    )


def _get_pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed")
    url = os.getenv("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(url)  # type: ignore # pyright: ignore
    return conn


def _get_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_LOCAL_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _to_valid_uuid(val: Any) -> str:
    """Ensure val is formatted as a valid UUID string for PostgreSQL/MySQL."""
    if not val:
        return str(uuid.uuid4())
    val_str = str(val)
    try:
        return str(uuid.UUID(val_str))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, val_str))


# ---------------------------------------------------------------------------
# DDL Statements
# ---------------------------------------------------------------------------

MYSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id VARCHAR(64) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    pipeline_name VARCHAR(255),
    status VARCHAR(64),
    start_time DATETIME NULL,
    end_time DATETIME NULL,
    duration INT,
    tool_name VARCHAR(64),
    rows_read BIGINT,
    rows_written BIGINT,
    error_message TEXT,
    raw_log JSON,
    execution_mode VARCHAR(64),
    triggered_by VARCHAR(255),
    orchestrator_tool VARCHAR(64),
    orchestrator_dag_id VARCHAR(255),
    orchestrator_task_id VARCHAR(255),
    orchestrator_run_id VARCHAR(255),
    saved_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS source_asset_metadata (
    id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64),
    system_name VARCHAR(255),
    system_type VARCHAR(64),
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    object_name VARCHAR(255),
    object_type VARCHAR(64),
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT,
    last_updated_at DATETIME NULL,
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_source_run_id FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS target_asset_metadata (
    id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64),
    system_name VARCHAR(255),
    system_type VARCHAR(64),
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    object_name VARCHAR(255),
    object_type VARCHAR(64),
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT,
    last_updated_at DATETIME NULL,
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_target_run_id FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                   UUID PRIMARY KEY,
    pipeline_id          TEXT NOT NULL,
    pipeline_name        TEXT,
    status               TEXT,
    start_time           TIMESTAMPTZ,
    end_time             TIMESTAMPTZ,
    duration             INT,
    tool_name            TEXT,
    rows_read            BIGINT,
    rows_written         BIGINT,
    error_message        TEXT,
    raw_log              JSONB,
    execution_mode       TEXT,
    triggered_by         TEXT,
    orchestrator_tool    TEXT,
    orchestrator_dag_id  TEXT,
    orchestrator_task_id TEXT,
    orchestrator_run_id  TEXT,
    saved_at             TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_asset_metadata (
    id              UUID PRIMARY KEY,
    run_id          UUID REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name     TEXT,
    system_type     TEXT,
    database_name   TEXT,
    schema_name     TEXT,
    object_name     TEXT,
    object_type     TEXT,
    row_count       BIGINT,
    column_count    INT,
    size_bytes      BIGINT,
    last_updated_at TIMESTAMPTZ,
    observed_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS target_asset_metadata (
    id              UUID PRIMARY KEY,
    run_id          UUID REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name     TEXT,
    system_type     TEXT,
    database_name   TEXT,
    schema_name     TEXT,
    object_name     TEXT,
    object_type     TEXT,
    row_count       BIGINT,
    column_count    INT,
    size_bytes      BIGINT,
    last_updated_at TIMESTAMPTZ,
    observed_at     TIMESTAMPTZ DEFAULT now()
);
"""

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                   TEXT PRIMARY KEY,
    pipeline_id          TEXT NOT NULL,
    pipeline_name        TEXT,
    status               TEXT,
    start_time           TEXT,
    end_time             TEXT,
    duration             INTEGER,
    tool_name            TEXT,
    rows_read            INTEGER,
    rows_written         INTEGER,
    error_message        TEXT,
    raw_log              TEXT,
    execution_mode       TEXT,
    triggered_by         TEXT,
    orchestrator_tool    TEXT,
    orchestrator_dag_id  TEXT,
    orchestrator_task_id TEXT,
    orchestrator_run_id  TEXT,
    saved_at             TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_asset_metadata (
    id              TEXT PRIMARY KEY,
    run_id          TEXT REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name     TEXT,
    system_type     TEXT,
    database_name   TEXT,
    schema_name     TEXT,
    object_name     TEXT,
    object_type     TEXT,
    row_count       INTEGER,
    column_count    INTEGER,
    size_bytes      INTEGER,
    last_updated_at TEXT,
    observed_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS target_asset_metadata (
    id              TEXT PRIMARY KEY,
    run_id          TEXT REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    system_name     TEXT,
    system_type     TEXT,
    database_name   TEXT,
    schema_name     TEXT,
    object_name     TEXT,
    object_type     TEXT,
    row_count       INTEGER,
    column_count    INTEGER,
    size_bytes      INTEGER,
    last_updated_at TEXT,
    observed_at     TEXT DEFAULT (datetime('now'))
);
"""


def _init_results_db():
    if is_mysql():
        try:
            conn = _get_mysql_conn()
            with conn.cursor() as cur:
                statements = [s.strip() for s in MYSQL_SCHEMA.split(";") if s.strip()]
                for stmt in statements:
                    cur.execute(stmt)
            conn.close()
            logger.info("AWS RDS MySQL results DB initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise MySQL results DB: %s", exc)
            raise
    elif is_postgres():
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(POSTGRES_SCHEMA)
                conn.commit()
            logger.info("Supabase / Postgres results DB initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise Postgres DB: %s", exc)
            raise
    else:
        with _get_sqlite_conn() as conn:
            conn.executescript(SQLITE_SCHEMA)
        logger.info("Local SQLite results DB initialised at %s", _LOCAL_DB_PATH)


def init_results_db():
    _init_results_db()


_init_results_db()


# ---------------------------------------------------------------------------
# Write Operations
# ---------------------------------------------------------------------------

def _parse_duration_seconds(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    val_str = str(val).strip()
    if not val_str:
        return None
    if ":" in val_str:
        parts = val_str.split(":")
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(float(parts[1]))
        except ValueError:
            return None
    try:
        return int(float(val_str))
    except ValueError:
        return None


def save_pipeline_run(
    run_id: str,
    log_data: Dict[str, Any],
) -> str:
    """Save execution run log to pipeline_runs table."""
    valid_uuid = _to_valid_uuid(run_id)
    raw_log_str = json.dumps(log_data)

    params = (
        valid_uuid,
        str(log_data.get("pipeline_id") or log_data.get("job_id") or "unknown"),
        log_data.get("pipeline_name"),
        log_data.get("status", "unknown"),
        log_data.get("start_time"),
        log_data.get("end_time"),
        _parse_duration_seconds(log_data.get("duration")),
        log_data.get("tool_name", "dbt"),
        int(log_data["rows_read"]) if log_data.get("rows_read") is not None else None,
        int(log_data["rows_written"]) if log_data.get("rows_written") is not None else None,
        log_data.get("error_message"),
        raw_log_str,
        log_data.get("execution_mode", "orchestrated"),
        log_data.get("triggered_by"),
        log_data.get("orchestrator_tool"),
        log_data.get("orchestrator_dag_id"),
        log_data.get("orchestrator_task_id"),
        log_data.get("orchestrator_run_id"),
    )

    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    id, pipeline_id, pipeline_name, status, start_time, end_time,
                    duration, tool_name, rows_read, rows_written, error_message,
                    raw_log, execution_mode, triggered_by, orchestrator_tool,
                    orchestrator_dag_id, orchestrator_task_id, orchestrator_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    pipeline_id = VALUES(pipeline_id),
                    pipeline_name = VALUES(pipeline_name),
                    status = VALUES(status),
                    start_time = VALUES(start_time),
                    end_time = VALUES(end_time),
                    duration = VALUES(duration),
                    tool_name = VALUES(tool_name),
                    rows_read = VALUES(rows_read),
                    rows_written = VALUES(rows_written),
                    error_message = VALUES(error_message),
                    raw_log = VALUES(raw_log),
                    execution_mode = VALUES(execution_mode),
                    triggered_by = VALUES(triggered_by),
                    orchestrator_tool = VALUES(orchestrator_tool),
                    orchestrator_dag_id = VALUES(orchestrator_dag_id),
                    orchestrator_task_id = VALUES(orchestrator_task_id),
                    orchestrator_run_id = VALUES(orchestrator_run_id)
            """, params)
        conn.close()
        logger.info("Saved pipeline_run to MySQL: run_id=%s uuid=%s", run_id, valid_uuid)
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs (
                        id, pipeline_id, pipeline_name, status, start_time, end_time,
                        duration, tool_name, rows_read, rows_written, error_message,
                        raw_log, execution_mode, triggered_by, orchestrator_tool,
                        orchestrator_dag_id, orchestrator_task_id, orchestrator_run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        pipeline_id          = EXCLUDED.pipeline_id,
                        pipeline_name        = EXCLUDED.pipeline_name,
                        status               = EXCLUDED.status,
                        start_time           = EXCLUDED.start_time,
                        end_time             = EXCLUDED.end_time,
                        duration             = EXCLUDED.duration,
                        tool_name            = EXCLUDED.tool_name,
                        rows_read            = EXCLUDED.rows_read,
                        rows_written         = EXCLUDED.rows_written,
                        error_message        = EXCLUDED.error_message,
                        raw_log              = EXCLUDED.raw_log,
                        execution_mode       = EXCLUDED.execution_mode,
                        triggered_by         = EXCLUDED.triggered_by,
                        orchestrator_tool    = EXCLUDED.orchestrator_tool,
                        orchestrator_dag_id  = EXCLUDED.orchestrator_dag_id,
                        orchestrator_task_id = EXCLUDED.orchestrator_task_id,
                        orchestrator_run_id  = EXCLUDED.orchestrator_run_id
                """, params)
            conn.commit()
        logger.info("Saved pipeline_run to Supabase: run_id=%s uuid=%s", run_id, valid_uuid)
    else:
        with _get_sqlite_conn() as conn:
            conn.execute("""
                INSERT INTO pipeline_runs (
                    id, pipeline_id, pipeline_name, status, start_time, end_time,
                    duration, tool_name, rows_read, rows_written, error_message,
                    raw_log, execution_mode, triggered_by, orchestrator_tool,
                    orchestrator_dag_id, orchestrator_task_id, orchestrator_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    pipeline_id          = excluded.pipeline_id,
                    pipeline_name        = excluded.pipeline_name,
                    status               = excluded.status,
                    start_time           = excluded.start_time,
                    end_time             = excluded.end_time,
                    duration             = excluded.duration,
                    tool_name            = excluded.tool_name,
                    rows_read            = excluded.rows_read,
                    rows_written         = excluded.rows_written,
                    error_message        = excluded.error_message,
                    raw_log              = excluded.raw_log,
                    execution_mode       = excluded.execution_mode,
                    triggered_by         = excluded.triggered_by,
                    orchestrator_tool    = excluded.orchestrator_tool,
                    orchestrator_dag_id  = excluded.orchestrator_dag_id,
                    orchestrator_task_id = excluded.orchestrator_task_id,
                    orchestrator_run_id  = excluded.orchestrator_run_id
            """, params)

    return valid_uuid


def save_source_asset_metadata(
    run_id: str,
    source_data: Dict[str, Any],
) -> str:
    """Save source snapshot to source_asset_metadata table."""
    meta_id = str(uuid.uuid4())
    run_uuid = _to_valid_uuid(run_id)

    cols_val = source_data.get("columns") or source_data.get("column_names")
    col_names_str = json.dumps(cols_val) if isinstance(cols_val, (list, tuple)) else (cols_val if isinstance(cols_val, str) else None)

    params = (
        meta_id,
        run_uuid,
        source_data.get("system_name"),
        source_data.get("system_type"),
        source_data.get("database_name"),
        source_data.get("schema_name"),
        source_data.get("object_name"),
        source_data.get("object_type"),
        int(source_data["row_count"]) if source_data.get("row_count") is not None else None,
        int(source_data["column_count"]) if source_data.get("column_count") is not None else None,
        int(source_data["size_bytes"]) if source_data.get("size_bytes") is not None else None,
        col_names_str,
        source_data.get("last_updated_at"),
    )

    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO source_asset_metadata (
                    id, run_id, system_name, system_type, database_name,
                    schema_name, object_name, object_type, row_count,
                    column_count, size_bytes, column_names, last_updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    system_name = VALUES(system_name),
                    system_type = VALUES(system_type),
                    database_name = VALUES(database_name),
                    schema_name = VALUES(schema_name),
                    object_name = VALUES(object_name),
                    object_type = VALUES(object_type),
                    row_count = VALUES(row_count),
                    column_count = VALUES(column_count),
                    size_bytes = VALUES(size_bytes),
                    column_names = VALUES(column_names),
                    last_updated_at = VALUES(last_updated_at)
            """, params)
        conn.close()
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO source_asset_metadata (
                        id, run_id, system_name, system_type, database_name,
                        schema_name, object_name, object_type, row_count,
                        column_count, size_bytes, column_names, last_updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        system_name     = EXCLUDED.system_name,
                        system_type     = EXCLUDED.system_type,
                        database_name   = EXCLUDED.database_name,
                        schema_name     = EXCLUDED.schema_name,
                        object_name     = EXCLUDED.object_name,
                        object_type     = EXCLUDED.object_type,
                        row_count       = EXCLUDED.row_count,
                        column_count    = EXCLUDED.column_count,
                        size_bytes      = EXCLUDED.size_bytes,
                        column_names    = EXCLUDED.column_names,
                        last_updated_at = EXCLUDED.last_updated_at
                """, params)
            conn.commit()
    else:
        with _get_sqlite_conn() as conn:
            conn.execute("""
                INSERT INTO source_asset_metadata (
                    id, run_id, system_name, system_type, database_name,
                    schema_name, object_name, object_type, row_count,
                    column_count, size_bytes, column_names, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    system_name     = excluded.system_name,
                    system_type     = excluded.system_type,
                    database_name   = excluded.database_name,
                    schema_name     = excluded.schema_name,
                    object_name     = excluded.object_name,
                    object_type     = excluded.object_type,
                    row_count       = excluded.row_count,
                    column_count    = excluded.column_count,
                    size_bytes      = excluded.size_bytes,
                    column_names    = excluded.column_names,
                    last_updated_at = excluded.last_updated_at
            """, params)

    return meta_id


def save_target_asset_metadata(
    run_id: str,
    target_data: Dict[str, Any],
) -> str:
    """Save target snapshot to target_asset_metadata table."""
    meta_id = str(uuid.uuid4())
    run_uuid = _to_valid_uuid(run_id)

    cols_val = target_data.get("columns") or target_data.get("column_names")
    col_names_str = json.dumps(cols_val) if isinstance(cols_val, (list, tuple)) else (cols_val if isinstance(cols_val, str) else None)

    params = (
        meta_id,
        run_uuid,
        target_data.get("system_name"),
        target_data.get("system_type"),
        target_data.get("database_name"),
        target_data.get("schema_name"),
        target_data.get("object_name"),
        target_data.get("object_type"),
        int(target_data["row_count"]) if target_data.get("row_count") is not None else None,
        int(target_data["column_count"]) if target_data.get("column_count") is not None else None,
        int(target_data["size_bytes"]) if target_data.get("size_bytes") is not None else None,
        col_names_str,
        target_data.get("last_updated_at"),
    )

    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO target_asset_metadata (
                    id, run_id, system_name, system_type, database_name,
                    schema_name, object_name, object_type, row_count,
                    column_count, size_bytes, column_names, last_updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    system_name = VALUES(system_name),
                    system_type = VALUES(system_type),
                    database_name = VALUES(database_name),
                    schema_name = VALUES(schema_name),
                    object_name = VALUES(object_name),
                    object_type = VALUES(object_type),
                    row_count = VALUES(row_count),
                    column_count = VALUES(column_count),
                    size_bytes = VALUES(size_bytes),
                    column_names = VALUES(column_names),
                    last_updated_at = VALUES(last_updated_at)
            """, params)
        conn.close()
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO target_asset_metadata (
                        id, run_id, system_name, system_type, database_name,
                        schema_name, object_name, object_type, row_count,
                        column_count, size_bytes, column_names, last_updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        system_name     = EXCLUDED.system_name,
                        system_type     = EXCLUDED.system_type,
                        database_name   = EXCLUDED.database_name,
                        schema_name     = EXCLUDED.schema_name,
                        object_name     = EXCLUDED.object_name,
                        object_type     = EXCLUDED.object_type,
                        row_count       = EXCLUDED.row_count,
                        column_count    = EXCLUDED.column_count,
                        size_bytes      = EXCLUDED.size_bytes,
                        column_names    = EXCLUDED.column_names,
                        last_updated_at = EXCLUDED.last_updated_at
                """, params)
            conn.commit()
    else:
        with _get_sqlite_conn() as conn:
            conn.execute("""
                INSERT INTO target_asset_metadata (
                    id, run_id, system_name, system_type, database_name,
                    schema_name, object_name, object_type, row_count,
                    column_count, size_bytes, column_names, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    system_name     = excluded.system_name,
                    system_type     = excluded.system_type,
                    database_name   = excluded.database_name,
                    schema_name     = excluded.schema_name,
                    object_name     = excluded.object_name,
                    object_type     = excluded.object_type,
                    row_count       = excluded.row_count,
                    column_count    = excluded.column_count,
                    size_bytes      = excluded.size_bytes,
                    column_names    = excluded.column_names,
                    last_updated_at = excluded.last_updated_at
            """, params)

    return meta_id


# ---------------------------------------------------------------------------
# Read Operations
# ---------------------------------------------------------------------------

def list_recent_runs(limit: int = 50) -> List[Dict[str, Any]]:
    """Return a list of recent pipeline runs."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM pipeline_runs
                ORDER BY saved_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:
                cur.execute("""
                    SELECT * FROM pipeline_runs
                    ORDER BY saved_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]
    else:
        with _get_sqlite_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM pipeline_runs
                ORDER BY saved_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_run_with_assets(run_id: str) -> Optional[Dict[str, Any]]:
    """Return full execution record (run + source_asset + target_asset)."""
    valid_uuid = _to_valid_uuid(run_id)

    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipeline_runs WHERE id = %s", (valid_uuid,))
            run_row = cur.fetchone()
            if not run_row:
                conn.close()
                return None

            cur.execute("SELECT * FROM source_asset_metadata WHERE run_id = %s", (valid_uuid,))
            src_row = cur.fetchone()

            cur.execute("SELECT * FROM target_asset_metadata WHERE run_id = %s", (valid_uuid,))
            tgt_row = cur.fetchone()

        conn.close()
        return {
            "run": dict(run_row),
            "source_asset": dict(src_row) if src_row else None,
            "target_asset": dict(tgt_row) if tgt_row else None,
        }
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:
                cur.execute("SELECT * FROM pipeline_runs WHERE id = %s", (valid_uuid,))
                run_row = cur.fetchone()
                if not run_row:
                    return None

                cur.execute("SELECT * FROM source_asset_metadata WHERE run_id = %s", (valid_uuid,))
                src_row = cur.fetchone()

                cur.execute("SELECT * FROM target_asset_metadata WHERE run_id = %s", (valid_uuid,))
                tgt_row = cur.fetchone()

        return {
            "run": dict(run_row),
            "source_asset": dict(src_row) if src_row else None,
            "target_asset": dict(tgt_row) if tgt_row else None,
        }
    else:
        with _get_sqlite_conn() as conn:
            run_row = conn.execute("SELECT * FROM pipeline_runs WHERE id = ?", (valid_uuid,)).fetchone()
            if not run_row:
                return None

            src_row = conn.execute("SELECT * FROM source_asset_metadata WHERE run_id = ?", (valid_uuid,)).fetchone()
            tgt_row = conn.execute("SELECT * FROM target_asset_metadata WHERE run_id = ?", (valid_uuid,)).fetchone()

        return {
            "run": dict(run_row),
            "source_asset": dict(src_row) if src_row else None,
            "target_asset": dict(tgt_row) if tgt_row else None,
        }
