"""
adapters/target/snowflake.py
-----------------------------
Snowflake target adapter — role="TARGET".
Identical query logic to source; asset_role differs.
"""

import datetime
import logging
from typing import Any, Dict, Optional

from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

try:
    import snowflake.connector  # type: ignore # pyright: ignore[reportMissingImports]
    from snowflake.connector import DictCursor  # type: ignore # pyright: ignore[reportMissingImports]
except ImportError:
    snowflake = None
    DictCursor = None  # type: ignore

from adapters.target.base import DataAdapter

logger = logging.getLogger(__name__)


class SnowflakeTargetAdapter(DataAdapter):
    role = "target"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=15),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        if snowflake is None or not hasattr(snowflake, "connector"):
            return _unavailable("snowflake", self.role, "snowflake-connector-python not installed", run_id, config)

        account   = config["account"]
        warehouse = config["warehouse"]
        database  = config["database"]
        schema    = config.get("schema", "PUBLIC")
        table     = config["table"]
        username  = config.get("username") or config.get("user") or ""
        password  = config["password"]
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

            cur.execute(f'SELECT COUNT(*) AS CNT FROM "{database}"."{schema}"."{table}"')
            res = cur.fetchone()
            row_count = 0
            if isinstance(res, dict):
                row_count = int(res.get("CNT") or res.get("cnt") or 0)
            elif isinstance(res, (tuple, list)) and len(res) > 0:
                row_count = int(res[0])

            cur.execute(f'SELECT * FROM "{database}"."{schema}"."{table}" LIMIT {sample_n}')
            rows    = cur.fetchall()
            columns = [desc.name for desc in cur.description] if cur.description else []

            cur.execute("""
                SELECT BYTES, LAST_ALTERED
                FROM information_schema.tables
                WHERE table_catalog = %s AND table_schema = %s AND table_name = %s
            """, (database.upper(), schema.upper(), table.upper()))
            meta = cur.fetchone() or {}
            size_bytes      = meta.get("BYTES")
            last_updated_at = None
            raw_altered     = meta.get("LAST_ALTERED")
            if raw_altered:
                last_updated_at = raw_altered.isoformat() if hasattr(raw_altered, "isoformat") else str(raw_altered)
        finally:
            conn.close()

        return {
            "run_id":          run_id,
            "asset_role":      "TARGET",
            "system_name":     "Snowflake",
            "system_type":     "DATA_WAREHOUSE",
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
        "asset_role":      "TARGET",
        "system_name":     system,
        "system_type":     "DATA_WAREHOUSE",
        "database_name":   config.get("database"),
        "schema_name":     config.get("schema", "PUBLIC"),
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
