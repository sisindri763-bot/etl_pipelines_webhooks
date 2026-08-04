"""
config/db.py
-------------
Config DB access layer — supports AWS RDS MySQL (when CENTRAL_DB_HOST is set),
Supabase / PostgreSQL (when DATABASE_URL is set), and SQLite fallback.

Stores pipeline configurations in table: `pipelines`
Uses explicit relational columns for all configuration properties.
"""

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


def _get_mysql_conn() -> Any:
    if pymysql is None:
        raise RuntimeError("pymysql is not installed")
    host = os.getenv("CENTRAL_DB_HOST") or os.getenv("MYSQL_HOST") or "localhost"
    port_val = os.getenv("CENTRAL_DB_PORT") or os.getenv("MYSQL_PORT") or "3306"
    port = int(port_val)
    db = os.getenv("CENTRAL_DB_NAME") or os.getenv("MYSQL_DATABASE") or "webhooks_db"
    user = os.getenv("CENTRAL_DB_USER") or os.getenv("MYSQL_USER") or "admin"
    password = os.getenv("CENTRAL_DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or ""
    
    dict_cursor = getattr(getattr(pymysql, "cursors", None), "DictCursor", None)

    return pymysql.connect(  # type: ignore # pyright: ignore
        host=host, port=port, user=user, password=password,
        database=db, charset="utf8mb4", cursorclass=dict_cursor, autocommit=True
    )


def _get_pg_conn() -> Any:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed")
    url = os.getenv("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)  # type: ignore # pyright: ignore


def _get_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Schema bootstrap with Explicit Relational Columns
# ---------------------------------------------------------------------------

MYSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id VARCHAR(255) PRIMARY KEY,
    tool_type VARCHAR(64) NOT NULL,
    source_type VARCHAR(64) NOT NULL,
    source_account VARCHAR(255) NULL,
    source_username VARCHAR(255) NULL,
    source_password VARCHAR(255) NULL,
    source_warehouse VARCHAR(255) NULL,
    source_database VARCHAR(255) NULL,
    source_schema VARCHAR(255) NULL,
    source_table VARCHAR(255) NULL,
    source_host VARCHAR(255) NULL,
    source_port INT NULL,
    source_path VARCHAR(500) NULL,
    target_type VARCHAR(64) NOT NULL,
    target_account VARCHAR(255) NULL,
    target_username VARCHAR(255) NULL,
    target_password VARCHAR(255) NULL,
    target_warehouse VARCHAR(255) NULL,
    target_database VARCHAR(255) NULL,
    target_schema VARCHAR(255) NULL,
    target_table VARCHAR(255) NULL,
    target_host VARCHAR(255) NULL,
    target_port INT NULL,
    target_path VARCHAR(500) NULL,
    tool_account_id VARCHAR(255) NULL,
    tool_api_token VARCHAR(255) NULL,
    tool_base_url VARCHAR(500) NULL,
    webhook_url VARCHAR(500) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id        TEXT PRIMARY KEY,
    tool_type     TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    source_account TEXT, source_username TEXT, source_password TEXT, source_warehouse TEXT, source_database TEXT, source_schema TEXT, source_table TEXT, source_host TEXT, source_port INT, source_path TEXT,
    target_type   TEXT NOT NULL,
    target_account TEXT, target_username TEXT, target_password TEXT, target_warehouse TEXT, target_database TEXT, target_schema TEXT, target_table TEXT, target_host TEXT, target_port INT, target_path TEXT,
    tool_account_id TEXT, tool_api_token TEXT, tool_base_url TEXT,
    webhook_url   TEXT,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);
"""

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id        TEXT PRIMARY KEY,
    tool_type     TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    source_account TEXT, source_username TEXT, source_password TEXT, source_warehouse TEXT, source_database TEXT, source_schema TEXT, source_table TEXT, source_host TEXT, source_port INT, source_path TEXT,
    target_type   TEXT NOT NULL,
    target_account TEXT, target_username TEXT, target_password TEXT, target_warehouse TEXT, target_database TEXT, target_schema TEXT, target_table TEXT, target_host TEXT, target_port INT, target_path TEXT,
    tool_account_id TEXT, tool_api_token TEXT, tool_base_url TEXT,
    webhook_url   TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
"""


def _init_db() -> None:
    if is_mysql():
        try:
            conn = _get_mysql_conn()
            with conn.cursor() as cur:  # type: ignore # pyright: ignore
                cur.execute(MYSQL_SCHEMA)  # type: ignore # pyright: ignore
            conn.close()
            logger.info("AWS RDS MySQL pipelines table initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise MySQL DB: %s", exc)
            raise
    elif is_postgres():
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:  # type: ignore # pyright: ignore
                    cur.execute(PG_SCHEMA)  # type: ignore # pyright: ignore
                conn.commit()
            logger.info("Supabase / Postgres pipelines table initialised.")
        except Exception as exc:
            logger.exception("Failed to initialise Postgres DB: %s", exc)
            raise
    else:
        with _get_sqlite_conn() as conn:
            conn.execute(SQLITE_SCHEMA)
        logger.info("Local SQLite pipelines DB initialised at %s", _DB_PATH)


def init_db() -> None:
    _init_db()


_init_db()


# ---------------------------------------------------------------------------
# CRUD API with Explicit Relational Mapping & Multi-Tenant Lookup
# ---------------------------------------------------------------------------

def register_pipeline(
    job_id: str,
    tool_type: str,
    source_type: str,
    source_config: Dict[str, Any],
    target_type: str,
    target_config: Dict[str, Any],
    tool_config: Optional[Dict[str, Any]] = None,
    webhook_url: Optional[str] = None,
) -> None:
    """Upsert a pipeline configuration into explicit relational columns."""
    tool_cfg = tool_config or {}

    s_acc = source_config.get("account")
    s_usr = source_config.get("username") or source_config.get("user")
    s_pwd = source_config.get("password") or source_config.get("pass")
    s_wh  = source_config.get("warehouse")
    s_db  = source_config.get("database") or source_config.get("dbname")
    s_sch = source_config.get("schema") or source_config.get("schema_name")
    s_tbl = source_config.get("table") or source_config.get("table_name")
    s_hst = source_config.get("host")
    s_prt = source_config.get("port")
    s_pth = source_config.get("url_or_path") or source_config.get("path")

    t_acc = target_config.get("account")
    t_usr = target_config.get("username") or target_config.get("user")
    t_pwd = target_config.get("password") or target_config.get("pass")
    t_wh  = target_config.get("warehouse")
    t_db  = target_config.get("database") or target_config.get("dbname")
    t_sch = target_config.get("schema") or target_config.get("schema_name")
    t_tbl = target_config.get("table") or target_config.get("table_name")
    t_hst = target_config.get("host")
    t_prt = target_config.get("port")
    t_pth = target_config.get("url_or_path") or target_config.get("path")

    tl_acc = tool_cfg.get("account_id")
    tl_tok = tool_cfg.get("api_token")
    tl_url = tool_cfg.get("base_url") or tool_cfg.get("url")

    params = (
        job_id, tool_type, source_type,
        s_acc, s_usr, s_pwd, s_wh, s_db, s_sch, s_tbl, s_hst, s_prt, s_pth,
        target_type,
        t_acc, t_usr, t_pwd, t_wh, t_db, t_sch, t_tbl, t_hst, t_prt, t_pth,
        tl_acc, tl_tok, tl_url, webhook_url
    )

    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:  # type: ignore # pyright: ignore
            cur.execute("""
                INSERT INTO pipelines
                    (job_id, tool_type, source_type,
                     source_account, source_username, source_password, source_warehouse, source_database, source_schema, source_table, source_host, source_port, source_path,
                     target_type,
                     target_account, target_username, target_password, target_warehouse, target_database, target_schema, target_table, target_host, target_port, target_path,
                     tool_account_id, tool_api_token, tool_base_url, webhook_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tool_type        = VALUES(tool_type),
                    source_type      = VALUES(source_type),
                    source_account   = VALUES(source_account),
                    source_username  = VALUES(source_username),
                    source_password  = VALUES(source_password),
                    source_warehouse = VALUES(source_warehouse),
                    source_database  = VALUES(source_database),
                    source_schema    = VALUES(source_schema),
                    source_table     = VALUES(source_table),
                    source_host      = VALUES(source_host),
                    source_port      = VALUES(source_port),
                    source_path      = VALUES(source_path),
                    target_type      = VALUES(target_type),
                    target_account   = VALUES(target_account),
                    target_username  = VALUES(target_username),
                    target_password  = VALUES(target_password),
                    target_warehouse = VALUES(target_warehouse),
                    target_database  = VALUES(target_database),
                    target_schema    = VALUES(target_schema),
                    target_table     = VALUES(target_table),
                    target_host      = VALUES(target_host),
                    target_port      = VALUES(target_port),
                    target_path      = VALUES(target_path),
                    tool_account_id  = VALUES(tool_account_id),
                    tool_api_token   = VALUES(tool_api_token),
                    tool_base_url    = VALUES(tool_base_url),
                    webhook_url      = VALUES(webhook_url),
                    updated_at       = CURRENT_TIMESTAMP
            """, params)  # type: ignore # pyright: ignore
        conn.commit()
        conn.close()
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:  # type: ignore # pyright: ignore
                cur.execute("""
                    INSERT INTO pipelines
                        (job_id, tool_type, source_type,
                         source_account, source_username, source_password, source_warehouse, source_database, source_schema, source_table, source_host, source_port, source_path,
                         target_type,
                         target_account, target_username, target_password, target_warehouse, target_database, target_schema, target_table, target_host, target_port, target_path,
                         tool_account_id, tool_api_token, tool_base_url, webhook_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        tool_type        = EXCLUDED.tool_type,
                        source_type      = EXCLUDED.source_type,
                        source_account   = EXCLUDED.source_account,
                        source_username  = EXCLUDED.source_username,
                        source_password  = EXCLUDED.source_password,
                        source_warehouse = EXCLUDED.source_warehouse,
                        source_database  = EXCLUDED.source_database,
                        source_schema    = EXCLUDED.source_schema,
                        source_table     = EXCLUDED.source_table,
                        source_host      = EXCLUDED.source_host,
                        source_port      = EXCLUDED.source_port,
                        source_path      = EXCLUDED.source_path,
                        target_type      = EXCLUDED.target_type,
                        target_account   = EXCLUDED.target_account,
                        target_username  = EXCLUDED.target_username,
                        target_password  = EXCLUDED.target_password,
                        target_warehouse = EXCLUDED.target_warehouse,
                        target_database  = EXCLUDED.target_database,
                        target_schema    = EXCLUDED.target_schema,
                        target_table     = EXCLUDED.target_table,
                        target_host      = EXCLUDED.target_host,
                        target_port      = EXCLUDED.target_port,
                        target_path      = EXCLUDED.target_path,
                        tool_account_id  = EXCLUDED.tool_account_id,
                        tool_api_token   = EXCLUDED.tool_api_token,
                        tool_base_url    = EXCLUDED.tool_base_url,
                        webhook_url      = EXCLUDED.webhook_url,
                        updated_at       = now()
                """, params)  # type: ignore # pyright: ignore
            conn.commit()
    else:
        with _get_sqlite_conn() as conn:
            conn.execute("""
                INSERT INTO pipelines
                    (job_id, tool_type, source_type,
                     source_account, source_username, source_password, source_warehouse, source_database, source_schema, source_table, source_host, source_port, source_path,
                     target_type,
                     target_account, target_username, target_password, target_warehouse, target_database, target_schema, target_table, target_host, target_port, target_path,
                     tool_account_id, tool_api_token, tool_base_url, webhook_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    tool_type        = excluded.tool_type,
                    source_type      = excluded.source_type,
                    source_account   = excluded.source_account,
                    source_username  = excluded.source_username,
                    source_password  = excluded.source_password,
                    source_warehouse = excluded.source_warehouse,
                    source_database  = excluded.source_database,
                    source_schema    = excluded.source_schema,
                    source_table     = excluded.source_table,
                    source_host      = excluded.source_host,
                    source_port      = excluded.source_port,
                    source_path      = excluded.source_path,
                    target_type      = excluded.target_type,
                    target_account   = excluded.target_account,
                    target_username  = excluded.target_username,
                    target_password  = excluded.target_password,
                    target_warehouse = excluded.target_warehouse,
                    target_database  = excluded.target_database,
                    target_schema    = excluded.target_schema,
                    target_table     = excluded.target_table,
                    target_host      = excluded.target_host,
                    target_port      = excluded.target_port,
                    target_path      = excluded.target_path,
                    tool_account_id  = excluded.tool_account_id,
                    tool_api_token   = excluded.tool_api_token,
                    tool_base_url    = excluded.tool_base_url,
                    webhook_url      = excluded.webhook_url,
                    updated_at       = datetime('now')
            """, params)  # type: ignore # pyright: ignore
    logger.info("Pipeline registered with relational columns: job_id=%s", job_id)


def get_pipeline(job_id: str) -> Optional[Dict[str, Any]]:
    """Return a pipeline config dict, mapped from relational columns."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:  # type: ignore # pyright: ignore
            cur.execute("SELECT * FROM pipelines WHERE job_id = %s", (job_id,))  # type: ignore # pyright: ignore
            row = cur.fetchone()  # type: ignore # pyright: ignore
        conn.close()
        return _format_relational_row(dict(row)) if row else None  # type: ignore # pyright: ignore
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)  # type: ignore # pyright: ignore
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:  # type: ignore # pyright: ignore
                cur.execute("SELECT * FROM pipelines WHERE job_id = %s", (job_id,))  # type: ignore # pyright: ignore
                row = cur.fetchone()  # type: ignore # pyright: ignore
        return _format_relational_row(dict(row)) if row else None  # type: ignore # pyright: ignore
    else:
        with _get_sqlite_conn() as conn:
            row = conn.execute("SELECT * FROM pipelines WHERE job_id = ?", (job_id,)).fetchone()  # type: ignore # pyright: ignore
        return _format_relational_row(dict(row)) if row else None  # type: ignore # pyright: ignore


def find_pipeline(
    job_id: Optional[str] = None,
    run_id: Optional[str] = None,
    tool_account_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Multi-stage enterprise lookup across multi-tenant registered pipelines."""
    pipelines = list_pipelines()
    if not pipelines:
        return None

    target_jid = str(job_id or "").strip()
    target_rid = str(run_id or "").strip()
    target_aid = str(tool_account_id or "").strip()
    target_uid = str(user_id or "").strip()

    # 1. Exact job_id match
    if target_jid:
        for p in pipelines:
            if str(p.get("job_id") or "").strip() == target_jid:
                return p

    # 2. Exact run_id match
    if target_rid:
        for p in pipelines:
            if str(p.get("job_id") or "").strip() == target_rid:
                return p

    # 3. Match by tool_account_id (dbt Account ID)
    if target_aid:
        for p in pipelines:
            if str(p.get("tool_account_id") or "").strip() == target_aid:
                return p

    # 4. Match by user_id inside webhook_url
    if target_uid:
        for p in pipelines:
            url = str(p.get("webhook_url") or "")
            if target_uid and target_uid in url:
                return p

    # 5. Fallback: if single pipeline registered, use it
    if len(pipelines) == 1:
        return pipelines[0]

    return None


def list_pipelines() -> List[Dict[str, Any]]:
    """Return all registered pipelines."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:  # type: ignore # pyright: ignore
            cur.execute("SELECT * FROM pipelines ORDER BY created_at DESC")  # type: ignore # pyright: ignore
            rows = cur.fetchall()  # type: ignore # pyright: ignore
        conn.close()
        return [_format_relational_row(dict(r)) for r in rows]  # type: ignore # pyright: ignore
    elif is_postgres():
        dict_cursor = getattr(getattr(psycopg2, "extras", None), "DictCursor", None)  # type: ignore # pyright: ignore
        with _get_pg_conn() as conn:
            with conn.cursor(cursor_factory=dict_cursor) as cur:  # type: ignore # pyright: ignore
                cur.execute("SELECT * FROM pipelines ORDER BY created_at DESC")  # type: ignore # pyright: ignore
                rows = cur.fetchall()  # type: ignore # pyright: ignore
        return [_format_relational_row(dict(r)) for r in rows]  # type: ignore # pyright: ignore
    else:
        with _get_sqlite_conn() as conn:
            rows = conn.execute("SELECT * FROM pipelines ORDER BY created_at DESC").fetchall()  # type: ignore # pyright: ignore
        return [_format_relational_row(dict(r)) for r in rows]  # type: ignore # pyright: ignore


def delete_pipeline(job_id: str) -> bool:
    """Delete a pipeline configuration row."""
    if is_mysql():
        conn = _get_mysql_conn()
        with conn.cursor() as cur:  # type: ignore # pyright: ignore
            cur.execute("DELETE FROM pipelines WHERE job_id = %s", (job_id,))  # type: ignore # pyright: ignore
            count = cur.rowcount  # type: ignore # pyright: ignore
        conn.close()
        return count > 0
    elif is_postgres():
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:  # type: ignore # pyright: ignore
                cur.execute("DELETE FROM pipelines WHERE job_id = %s", (job_id,))  # type: ignore # pyright: ignore
                count = cur.rowcount  # type: ignore # pyright: ignore
            conn.commit()
        return count > 0
    else:
        with _get_sqlite_conn() as conn:
            sql_cur = conn.execute("DELETE FROM pipelines WHERE job_id = ?", (job_id,))  # type: ignore # pyright: ignore
            count = sql_cur.rowcount  # type: ignore # pyright: ignore
        return count > 0


def _format_relational_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """Assemble config dicts dynamically for adapters."""
    s_type = r.get("source_type")
    if s_type == "snowflake":
        src_cfg = {
            "account": r.get("source_account"),
            "username": r.get("source_username"),
            "password": r.get("source_password"),
            "warehouse": r.get("source_warehouse"),
            "database": r.get("source_database"),
            "schema": r.get("source_schema"),
            "table": r.get("source_table"),
        }
    elif s_type == "mysql":
        src_cfg = {
            "host": r.get("source_host"),
            "port": r.get("source_port"),
            "username": r.get("source_username"),
            "password": r.get("source_password"),
            "database": r.get("source_database"),
            "table_name": r.get("source_table"),
        }
    else:
        src_cfg = {"url_or_path": r.get("source_path")}

    t_type = r.get("target_type")
    if t_type == "snowflake":
        tgt_cfg = {
            "account": r.get("target_account"),
            "username": r.get("target_username"),
            "password": r.get("target_password"),
            "warehouse": r.get("target_warehouse"),
            "database": r.get("target_database"),
            "schema": r.get("target_schema"),
            "table": r.get("target_table"),
        }
    elif t_type == "mysql":
        tgt_cfg = {
            "host": r.get("target_host"),
            "port": r.get("target_port"),
            "username": r.get("target_username"),
            "password": r.get("target_password"),
            "database": r.get("target_database"),
            "table_name": r.get("target_table"),
        }
    else:
        tgt_cfg = {"url_or_path": r.get("target_path")}

    tool_cfg = {
        "account_id": r.get("tool_account_id"),
        "api_token": r.get("tool_api_token"),
        "base_url": r.get("tool_base_url"),
    }

    res = dict(r)
    res["source_config"] = src_cfg
    res["target_config"] = tgt_cfg
    res["tool_config"]   = tool_cfg
    return res
