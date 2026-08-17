"""
adapters/source/base.py
-----------------------
Abstract base class shared by all source AND target adapters.

All subclasses must return this exact shape:

{
    "run_id":          str,          # correlation ID from the webhook
    "asset_role":      str,          # "SOURCE" | "TARGET"
    "system_name":     str,          # e.g. "MySQL", "Snowflake", "CSV"
    "system_type":     str,          # "DATABASE"|"DATA_WAREHOUSE"|"FILE"|"API"
    "database_name":   str | None,
    "schema_name":     str | None,
    "object_name":     str,          # table name, filename, endpoint path
    "object_type":     str,          # "TABLE"|"VIEW"|"FILE"|"ENDPOINT"
    "row_count":       int,
    "column_count":    int,
    "size_bytes":      int | None,
    "last_updated_at": str | None,   # ISO UTC — when the data itself last changed
    "observed_at":     str,          # ISO UTC — when we fetched this snapshot
    "columns":         list[str],    # column names (for data quality inspection)
    "sample":          list[dict],   # first N rows
}
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class DataAdapter(ABC):

    role: str = "data"   # overridden by subclasses: "source" or "target"

    @abstractmethod
    def fetch_snapshot(
        self,
        config: Dict[str, Any],
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Connect to the data store described by *config* and return a snapshot.

        Args:
            config: Adapter-specific config dict. Credentials inline or
                    pre-resolved from the secrets layer.
            run_id: Correlation ID from the webhook — stored in the result
                    so source/target rows can be joined to the log row.

        Returns:
            Dict matching the shape defined in the module docstring.
        """
        ...
