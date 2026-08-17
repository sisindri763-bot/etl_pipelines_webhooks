"""
adapters/target/mysql.py
-------------------------
MySQL target adapter — supports single table, comma-separated table lists, or schema-wide table discovery.
"""

import datetime
import logging
from typing import Any, Dict, Optional, List

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, before_sleep_log

try:
    import pymysql  # type: ignore # pyright: ignore[reportMissingImports]
    import pymysql.cursors  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    pymysql = None

from adapters.target.base import DataAdapter

logger = logging.getLogger(__name__)


class MySQLTargetAdapter(DataAdapter):
    role = "target"

    @retry(
        retry=retry_if_exception_type((TimeoutError, ConnectionError)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=3),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        if pymysql is None:
            return _unavailable("MySQL", self.role, "pymysql not installed", run_id, config)

        host      = config["host"]
        port      = int(config.get("port", 3306))
        database  = config["database"]
        table_raw = str(config.get("table") or config.get("table_name") or "").strip()
        schema    = config.get("schema") or config.get("schema_name") or database
        username  = config.get("username") or config.get("user") or ""
        password  = config["password"]
        sample_n  = int(config.get("sample_rows", 10))
        charset   = config.get("charset", "utf8mb4")

        cursor_cls = getattr(getattr(pymysql, "cursors", None), "DictCursor", None)

        conn = pymysql.connect(  # type: ignore # pyright: ignore
            host=host, port=port, user=username, password=password,
            database=database, charset=charset,
            cursorclass=cursor_cls,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                # Discover table list
                if not table_raw or table_raw == "*":
                    cur.execute("""
                        SELECT TABLE_NAME
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """, (database,))
                    tbl_rows = cur.fetchall() or []
                    table_list = [r["TABLE_NAME"] for r in tbl_rows if isinstance(r, dict) and r.get("TABLE_NAME")]
                elif "," in table_raw:
                    table_list = [t.strip() for t in table_raw.split(",") if t.strip()]
                else:
                    table_list = [table_raw]

                total_rows = 0
                size_bytes = 0
                last_updated_at = None
                all_columns: List[str] = []
                sample_data: List[Dict[str, Any]] = []
                object_name = ", ".join(table_list) if table_list else (table_raw or database)

                if table_list:
                    in_clause = ", ".join(["%s"] * len(table_list))
                    query_tables = f"""
                        SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH AS BYTES, UPDATE_TIME
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name IN ({in_clause})
                    """
                    params_tbl = [database] + table_list
                    cur.execute(query_tables, params_tbl)
                    meta_rows = cur.fetchall() or []

                    for m in meta_rows:
                        if isinstance(m, dict):
                            rc = m.get("TABLE_ROWS")
                            if rc is not None:
                                total_rows += int(rc)
                            b = m.get("BYTES")
                            if b is not None:
                                size_bytes += int(b)
                            ut = m.get("UPDATE_TIME")
                            if ut and not last_updated_at:
                                last_updated_at = ut.isoformat() if hasattr(ut, "isoformat") else str(ut)

                    query_cols = f"""
                        SELECT TABLE_NAME, COLUMN_NAME
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name IN ({in_clause})
                        ORDER BY TABLE_NAME, ORDINAL_POSITION
                    """
                    cur.execute(query_cols, params_tbl)
                    col_rows = cur.fetchall() or []
                    for c in col_rows:
                        if isinstance(c, dict) and c.get("COLUMN_NAME"):
                            col_name = str(c["COLUMN_NAME"])
                            if col_name not in all_columns:
                                all_columns.append(col_name)

                    # Sample rows from first table
                    if table_list:
                        try:
                            t_sample = table_list[0]
                            cur.execute(f"SELECT * FROM `{t_sample}` LIMIT {sample_n}")
                            sample_rows = cur.fetchall() or []
                            sample_data = [dict(r) for r in sample_rows]
                            if not all_columns and cur.description:
                                all_columns = [desc[0] for desc in cur.description]
                        except Exception:
                            pass

        finally:
            conn.close()

        return {
            "run_id":          run_id,
            "asset_role":      "TARGET",
            "system_name":     "MySQL",
            "system_type":     "DATABASE",
            "database_name":   database,
            "schema_name":     schema,
            "object_name":     object_name,
            "object_type":     "TABLE",
            "row_count":       total_rows,
            "column_count":    len(all_columns),
            "size_bytes":      size_bytes if size_bytes > 0 else None,
            "last_updated_at": last_updated_at or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns":         all_columns,
            "sample":          sample_data,
        }


def _unavailable(system: str, role: str, reason: str, run_id: Optional[str], config: Dict[str, Any]) -> Dict[str, Any]:
    obj = config.get("table") or config.get("table_name") or config.get("database") or "UNKNOWN_TABLE"
    return {
        "run_id":          run_id,
        "asset_role":      "TARGET",
        "system_name":     system,
        "system_type":     "DATABASE",
        "database_name":   config.get("database"),
        "schema_name":     config.get("schema") or config.get("database"),
        "object_name":     obj,
        "object_type":     "TABLE",
        "row_count":       0,
        "column_count":    0,
        "size_bytes":      None,
        "last_updated_at": None,
        "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "columns":         [],
        "sample":          [],
        "error":           reason,
    }
