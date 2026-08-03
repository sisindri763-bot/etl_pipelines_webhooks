"""
adapters/target/__init__.py
-----------------------------
Registry: maps target type strings → adapter instances.

Usage:
    from adapters.target import TARGET_ADAPTERS
    result = TARGET_ADAPTERS["snowflake"].fetch_snapshot(config)
"""

from adapters.target.snowflake    import SnowflakeTargetAdapter
from adapters.target.mysql        import MySQLTargetAdapter
from adapters.target.csv_adapter  import CSVTargetAdapter
from adapters.target.excel_adapter import ExcelTargetAdapter
from adapters.target.api_adapter  import APITargetAdapter

TARGET_ADAPTERS = {
    "snowflake": SnowflakeTargetAdapter(),
    "mysql":     MySQLTargetAdapter(),
    "postgres":  MySQLTargetAdapter(),   # swap for PostgresTargetAdapter when added
    "csv":       CSVTargetAdapter(),
    "excel":     ExcelTargetAdapter(),
    "api":       APITargetAdapter(),
}

__all__ = ["TARGET_ADAPTERS"]
