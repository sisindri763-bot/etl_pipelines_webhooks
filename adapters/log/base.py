"""
adapters/log/base.py
--------------------
Abstract base class for all log adapters.

All subclasses must return this exact shape:

{
    "id":                   str,          # UUID for this result record
    "pipeline_id":          str,          # tool-native job/connection ID
    "pipeline_name":        str | None,   # human-readable name from tool
    "status":               str,          # "success"|"error"|"running"|"cancelled"
    "start_time":           str | None,   # ISO UTC
    "end_time":             str | None,   # ISO UTC
    "duration":             float | None, # seconds
    "tool_name":            str,          # "dbt"|"airbyte"
    "rows_read":            int | None,
    "rows_written":         int | None,
    "error_message":        str | None,
    "raw_log":              str,          # JSON string of full API response
    "execution_mode":       str,          # "native"|"orchestrated"
    "triggered_by":         str | None,
    "orchestrator_tool":    str | None,
    "orchestrator_dag_id":  str | None,
    "orchestrator_task_id": str | None,
    "orchestrator_run_id":  str | None,
    "fetched_at":           str,          # ISO UTC
}
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LogAdapter(ABC):

    @abstractmethod
    def fetch_log(
        self,
        run_id: str,
        config: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch run metadata from a pipeline tool.

        Args:
            run_id:  Tool-native run/job identifier.
            config:  Adapter-specific config (account IDs, tokens, etc.).
            context: Optional orchestrator context from the webhook payload
                     (e.g. dag_id, task_id, run_id from Airflow).

        Returns:
            Dict matching the shape defined in the module docstring.
        """
        ...
