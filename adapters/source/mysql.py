"""
adapters/source/mysql.py
-------------------------
MySQL / MariaDB source adapter.
Produces the standard 15-field asset shape including size_bytes and last_updated_at
by querying information_schema alongside the data table.

Required config keys:
    host, port, database, table, username, password
Optional:
    schema_name  (default: same as database in MySQL)
    sample_rows  (default 10)
    charset      (default "utf8mb4")
"""

import datetime
import logging
from typing import Any, Dict, Optional

from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

try:
    import pymysql  # type: ignore # pyright: ignore[reportMissingImports]
    import pymysql.cursors  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    pymysql = None

from adapters.source.base import DataAdapter

logger = logging.getLogger(__name__)


class MySQLSourceAdapter(DataAdapter):
    role = "source"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        if pymysql is None:
            return _unavailable("mysql", self.role, "pymysql not installed", run_id, config)

        host      = config["host"]
        port      = int(config.get("port", 3306))
        database  = config["database"]
        table     = config["table"]
        schema    = config.get("schema_name", database)
        username  = config["username"]
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
                cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
                res = cur.fetchone()
                row_count = res["cnt"] if isinstance(res, dict) and "cnt" in res else 0

                # Sample rows + column list
                cur.execute(f"SELECT * FROM `{table}` LIMIT {sample_n}")
                rows    = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []

                # Size + last updated from information_schema
                cur.execute("""
                    SELECT
                        data_length + index_length AS size_bytes,
                        update_time
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (schema, table))
                meta_obj = cur.fetchone()
                meta_dict = meta_obj if isinstance(meta_obj, dict) else {}
                size_bytes      = meta_dict.get("size_bytes")
                last_updated_at = None
                raw_update_time = meta_dict.get("update_time")
                if raw_update_time:
                    if hasattr(raw_update_time, "isoformat"):
                        last_updated_at = raw_update_time.isoformat()
                    else:
                        last_updated_at = str(raw_update_time)
        finally:
            conn.close()

        return {
            "run_id":          run_id,
            "asset_role":      self.role.upper(),
            "system_name":     "MySQL",
            "system_type":     "DATABASE",
            "database_name":   database,
            "schema_name":     schema,
            "object_name":     table,
            "object_type":     "TABLE",
            "row_count":       row_count,
            "column_count":    len(columns),
            "size_bytes":      int(size_bytes) if size_bytes else None,
            "last_updated_at": last_updated_at,
            "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns":         columns,
            "sample":          [dict(r) for r in rows],
        }


def _unavailable(system, role, reason, run_id, config):
    return {
        "run_id":          run_id,
        "asset_role":      role.upper(),
        "system_name":     system,
        "system_type":     "DATABASE",
        "database_name":   config.get("database"),
        "schema_name":     config.get("schema_name", config.get("database")),
        "object_name":     config.get("table", "unknown"),
        "object_type":     "TABLE",
        "row_count":       -1,
        "column_count":    -1,
        "size_bytes":      None,
        "last_updated_at": None,
        "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "columns":         [],
        "sample":          [],
        "error":           reason,
    }
