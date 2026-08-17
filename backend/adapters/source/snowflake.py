"""
adapters/source/snowflake.py
-----------------------------
Snowflake source adapter — supports single table, comma-separated tables, or schema-wide discovery.
Produces standard 15-field asset shape with exact row counts, column counts, column names, and size bytes.
"""

import datetime
import logging
from typing import Any, Dict, Optional, List

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, before_sleep_log

try:
    import snowflake.connector  # type: ignore # pyright: ignore[reportMissingImports]
    from snowflake.connector import DictCursor  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    snowflake = None
    DictCursor = None  # type: ignore

from adapters.source.base import DataAdapter

logger = logging.getLogger(__name__)


class SnowflakeSourceAdapter(DataAdapter):
    role = "source"

    @retry(
        retry=retry_if_exception_type((TimeoutError, ConnectionError)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=3),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        if snowflake is None or not hasattr(snowflake, "connector"):
            return _unavailable("Snowflake", self.role, "snowflake-connector-python not installed", run_id, config)

        account   = config.get("account", "")
        warehouse = config.get("warehouse", "")
        database  = config.get("database", "")
        schema    = config.get("schema") or "PUBLIC"
        table_raw = str(config.get("table") or config.get("table_name") or "").strip()
        username  = config.get("username") or config.get("user") or ""
        password  = config.get("password", "")
        sf_role   = config.get("role")
        sample_n  = int(config.get("sample_rows", 10))

        conn_kwargs = dict(
            account=account, user=username, password=password,
            warehouse=warehouse, database=database, schema=schema,
            login_timeout=15, network_timeout=30,
        )
        if sf_role:
            conn_kwargs["role"] = sf_role

        conn = snowflake.connector.connect(**conn_kwargs)
        try:
            cur = conn.cursor(cursor_class=DictCursor)

            # Discover table list (both BASE TABLE and VIEW)
            if not table_raw or table_raw == "*":
                cur.execute("""
                    SELECT TABLE_NAME
                    FROM information_schema.tables
                    WHERE UPPER(table_catalog) = %s
                      AND UPPER(table_schema)  = %s
                """, (database.upper(), schema.upper()))
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
            object_name = ", ".join(table_list) if table_list else (table_raw or schema)

            if table_list:
                in_clause = ", ".join(["%s"] * len(table_list))
                query_tables = f"""
                    SELECT TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
                    FROM information_schema.tables
                    WHERE UPPER(table_catalog) = %s
                      AND UPPER(table_schema)  = %s
                      AND UPPER(table_name) IN ({in_clause})
                """
                params_tbl = [database.upper(), schema.upper()] + [t.upper() for t in table_list]
                cur.execute(query_tables, params_tbl)
                meta_rows = cur.fetchall() or []

                for m in meta_rows:
                    if isinstance(m, dict):
                        rc = m.get("ROW_COUNT")
                        if rc is not None:
                            total_rows += int(rc)
                        b = m.get("BYTES")
                        if b is not None:
                            size_bytes += int(b)
                        alt = m.get("LAST_ALTERED")
                        if alt and not last_updated_at:
                            last_updated_at = alt.isoformat() if hasattr(alt, "isoformat") else str(alt)

                query_cols = f"""
                    SELECT TABLE_NAME, COLUMN_NAME
                    FROM information_schema.columns
                    WHERE UPPER(table_catalog) = %s
                      AND UPPER(table_schema)  = %s
                      AND UPPER(table_name) IN ({in_clause})
                    ORDER BY TABLE_NAME, ORDINAL_POSITION
                """
                cur.execute(query_cols, params_tbl)
                col_rows = cur.fetchall() or []
                for c in col_rows:
                    if isinstance(c, dict) and c.get("COLUMN_NAME"):
                        col_name = str(c["COLUMN_NAME"])
                        if col_name not in all_columns:
                            all_columns.append(col_name)

                # Fallback for exact count if total_rows is 0 (e.g. for views or un-summarized tables)
                if total_rows == 0 and table_list:
                    for t_item in table_list[:5]:
                        try:
                            cur.execute(f'SELECT COUNT(*) AS CNT FROM "{database}"."{schema}"."{t_item}"')
                            res = cur.fetchone()
                            if isinstance(res, dict):
                                total_rows += int(res.get("CNT") or res.get("cnt") or 0)
                        except Exception:
                            pass

                # Fallback for columns across entire schema if still empty
                if not all_columns:
                    try:
                        cur.execute("""
                            SELECT COLUMN_NAME
                            FROM information_schema.columns
                            WHERE UPPER(table_catalog) = %s
                              AND UPPER(table_schema)  = %s
                            ORDER BY ORDINAL_POSITION
                        """, (database.upper(), schema.upper()))
                        c_rows = cur.fetchall() or []
                        for c in c_rows:
                            if isinstance(c, dict) and c.get("COLUMN_NAME"):
                                cn = str(c["COLUMN_NAME"])
                                if cn not in all_columns:
                                    all_columns.append(cn)
                    except Exception:
                        pass

                # Sample data from first table
                if table_list:
                    try:
                        t_sample = table_list[0]
                        cur.execute(f'SELECT * FROM "{database}"."{schema}"."{t_sample}" LIMIT {sample_n}')
                        sample_rows = cur.fetchall() or []
                        sample_data = [dict(r) for r in sample_rows]
                        if not all_columns and cur.description:
                            all_columns = [desc.name for desc in cur.description]
                    except Exception:
                        pass

        finally:
            conn.close()

        return {
            "run_id":          run_id,
            "asset_role":      self.role.upper(),
            "system_name":     "Snowflake",
            "system_type":     "DATA_WAREHOUSE",
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
    obj = config.get("table") or config.get("table_name") or config.get("schema") or "UNKNOWN_TABLE"
    return {
        "run_id":          run_id,
        "asset_role":      role.upper(),
        "system_name":     system,
        "system_type":     "DATA_WAREHOUSE",
        "database_name":   config.get("database"),
        "schema_name":     config.get("schema", "PUBLIC"),
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
