"""
adapters/target/api_adapter.py
--------------------------------
REST API target adapter — role="TARGET".
"""

import datetime
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from adapters.target.base import DataAdapter

logger = logging.getLogger(__name__)


class APITargetAdapter(DataAdapter):
    role = "target"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        url      = config["url"]
        method   = config.get("method", "GET").upper()
        headers  = config.get("headers", {})
        params   = config.get("params", {})
        body     = config.get("body")
        data_key = config.get("data_key")
        sample_n = int(config.get("sample_rows", 10))
        timeout  = int(config.get("timeout", 30))

        path_parts  = [p for p in urlparse(url).path.split("/") if p]
        object_name = path_parts[-1] if path_parts else url

        resp = requests.request(
            method, url, headers=headers, params=params,
            json=body, timeout=timeout,
        )
        resp.raise_for_status()

        size_bytes = int(resp.headers.get("Content-Length", len(resp.content)))
        raw        = resp.json()

        if data_key and isinstance(raw, dict):
            data = raw.get(data_key, raw)
        elif isinstance(raw, list):
            data = raw
        else:
            data = [raw] if isinstance(raw, dict) else []

        row_count = len(data)
        columns   = list(data[0].keys()) if data and isinstance(data[0], dict) else []

        return {
            "run_id":          run_id,
            "asset_role":      "TARGET",
            "system_name":     "API",
            "system_type":     "API",
            "database_name":   None,
            "schema_name":     None,
            "object_name":     object_name,
            "object_type":     "ENDPOINT",
            "row_count":       row_count,
            "column_count":    len(columns),
            "size_bytes":      size_bytes,
            "last_updated_at": resp.headers.get("Last-Modified"),
            "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns":         columns,
            "sample":          data[:sample_n],
        }
