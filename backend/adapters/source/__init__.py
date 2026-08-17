"""
adapters/source/__init__.py
----------------------------
Registry: maps source type strings → adapter instances.

Usage:
    from adapters.source import SOURCE_ADAPTERS
    result = SOURCE_ADAPTERS["mysql"].fetch_snapshot(config)
"""

from adapters.source.mysql        import MySQLSourceAdapter
from adapters.source.snowflake    import SnowflakeSourceAdapter
from adapters.source.csv_adapter  import CSVSourceAdapter
from adapters.source.excel_adapter import ExcelSourceAdapter
from adapters.source.api_adapter  import APISourceAdapter

SOURCE_ADAPTERS = {
    "mysql":    MySQLSourceAdapter(),
    "postgres": MySQLSourceAdapter(),   # swap for PostgresSourceAdapter when added
    "snowflake": SnowflakeSourceAdapter(),
    "csv":      CSVSourceAdapter(),
    "excel":    ExcelSourceAdapter(),
    "api":      APISourceAdapter(),
}

__all__ = ["SOURCE_ADAPTERS"]
