"""
config/db.py
-------------
Config DB access layer — supports AWS RDS MySQL (when CENTRAL_DB_HOST is set),
Supabase / PostgreSQL (when DATABASE_URL is set), and SQLite fallback.

Stores pipeline configurations in table: `pipelines`
"""

import json
import logging
import os
import sqlite3
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

_DB_PATH = Path(__file__).parent / "pipelines.db"


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
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

MYSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id VARCHAR(255) PRIMARY KEY,
    tool_type VARCHAR(64) NOT NULL,
    source_type VARCHAR(64) NOT NULL,
    source_config JSON NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_config JSON NOT NULL,
    tool_config JSON NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

PG_SCHEMA = """
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
"""

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id        TEXT PRIMARY KEY,
    tool_type     TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    source_config TEXT NOT NULL,
    target_type   TEXT NOT NULL,
    target_config TEXT NOT NULL,
    tool_config   TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
"""


def _init_db():
    if is_mysql():
        try:
            conn = _get_mysql_conn()
            with conn.cursor() as cur:
                cur.execute(MYSQL_SCHEMA)
            conn.close()
            logger.info("AWS RDS MySQL pipelines table initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise MySQL DB: %s", exc)
            raise
    elif is_postgres():
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(PG_SCHEMA)
                conn.commit()
            logger.info("Supabase / Postgres pipelines table initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise Postgres DB: %s", exc)
            raise
    else:
        with _get_sqlite_conn() as conn:
            conn.execute(SQLITE_SCHEMA)
        logger.info("Local SQLite pipelines DB initialised at %s", _DB_PATH)


def init_db():
    _init_db()


_init_db()


# ---------------------------------------------------------------------------
# CRUD API
# ---------------------------------------------------------------------------

def register_pipeline(
    job_id: str,
    tool_type: str,
    source_type: str,
    source_config: Dict[str, Any],
    target_type: str,
    target_config: Dict[str, Any],
    tool_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Upsert a pipeline configuration."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipelines
                    (job_id, tool_type, source_type, source_config, target_type, target_config, tool_config)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tool_type     = VALUES(tool_type),
                    source_type   = VALUES(source_type),
                    source_config = VALUES(source_config),
                    target_type   = VALUES(target_type),
                    target_config = VALUES(target_config),
                    tool_config   = VALUES(tool_config),
                    updated_at    = CURRENT_TIMESTAMP
            """, (
                str(job_id),
                tool_type,
                source_type,
                json.dumps(source_config),
                target_type,
                json.dumps(target_config),
                json.dumps(tool_config or {}),
            ))
        conn.close()
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipelines
                        (job_id, tool_type, source_type, source_config, target_type, target_config, tool_config)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        tool_type     = EXCLUDED.tool_type,
                        source_type   = EXCLUDED.source_type,
                        source_config = EXCLUDED.source_config,
                        target_type   = EXCLUDED.target_type,
                        target_config = EXCLUDED.target_config,
                        tool_config   = EXCLUDED.tool_config,
                        updated_at    = now()
                """, (
                    str(job_id),
                    tool_type,
                    source_type,
                    json.dumps(source_config),
                    target_type,
                    json.dumps(target_config),
                    json.dumps(tool_config or {}),
                ))
            conn.commit()
    else:
        with _get_sqlite_conn() as conn:
            conn.execute("""
                INSERT INTO pipelines
                    (job_id, tool_type, source_type, source_config, target_type, target_config, tool_config)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    tool_type     = excluded.tool_type,
                    source_type   = excluded.source_type,
                    source_config = excluded.source_config,
                    target_type   = excluded.target_type,
                    target_config = excluded.target_config,
                    tool_config   = excluded.tool_config,
                    updated_at    = datetime('now')
            """, (
                str(job_id),
                tool_type,
                source_type,
                json.dumps(source_config),
                target_type,
                json.dumps(target_config),
                json.dumps(tool_config or {}),
            ))
    logger.info("Pipeline registered: job_id=%s", job_id)


def get_pipeline(job_id: str) -> Optional[Dict[str, Any]]:
    """Return a pipeline config dict, or None if not found."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipelines WHERE job_id = %s", (str(job_id),))  # type: ignore # pyright: ignore
            row = cur.fetchone()  # type: ignore # pyright: ignore
        conn.close()
        return _parse_json_cols(dict(row)) if row else None
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:  # type: ignore # pyright: ignore
                cur.execute("SELECT * FROM pipelines WHERE job_id = %s", (str(job_id),))  # type: ignore # pyright: ignore
                row = cur.fetchone()  # type: ignore # pyright: ignore
        return _parse_json_cols(dict(row)) if row else None
    else:
        with _get_sqlite_conn() as conn:
            row = conn.execute("SELECT * FROM pipelines WHERE job_id = ?", (str(job_id),)).fetchone()  # type: ignore # pyright: ignore
        return _parse_json_cols(dict(row)) if row else None


def list_pipelines() -> List[Dict[str, Any]]:
    """Return all registered pipelines."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipelines ORDER BY created_at DESC")  # type: ignore # pyright: ignore
            rows = cur.fetchall()  # type: ignore # pyright: ignore
        conn.close()
        return [_parse_json_cols(dict(r)) for r in rows]
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:  # type: ignore # pyright: ignore
                cur.execute("SELECT * FROM pipelines ORDER BY created_at DESC")  # type: ignore # pyright: ignore
                rows = cur.fetchall()  # type: ignore # pyright: ignore
        return [_parse_json_cols(dict(r)) for r in rows]
    else:
        with _get_sqlite_conn() as conn:
            rows = conn.execute("SELECT * FROM pipelines ORDER BY created_at DESC").fetchall()  # type: ignore # pyright: ignore
        return [_parse_json_cols(dict(r)) for r in rows]


def delete_pipeline(job_id: str) -> bool:
    """Delete a pipeline configuration row."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pipelines WHERE job_id = %s", (str(job_id),))  # type: ignore # pyright: ignore
            count = cur.rowcount  # type: ignore # pyright: ignore
        conn.close()
        return count > 0
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pipelines WHERE job_id = %s", (str(job_id),))  # type: ignore # pyright: ignore
                count = cur.rowcount  # type: ignore # pyright: ignore
            conn.commit()
        return count > 0
    else:
        with _get_sqlite_conn() as conn:
            sql_cur = conn.execute("DELETE FROM pipelines WHERE job_id = ?", (str(job_id),))  # type: ignore # pyright: ignore
            count = sql_cur.rowcount  # type: ignore # pyright: ignore
        return count > 0


def _parse_json_cols(d: Dict[str, Any]) -> Dict[str, Any]:
    for col in ("source_config", "target_config", "tool_config"):
        if col in d and isinstance(d[col], str):
            try:
                d[col] = json.loads(d[col])
            except Exception:
                pass
    return d
