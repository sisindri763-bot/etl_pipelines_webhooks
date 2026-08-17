"""
adapters/target/csv_adapter.py
--------------------------------
CSV target adapter — role="TARGET".
"""

import csv
import datetime
import io
import logging
import os
from typing import Any, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from adapters.target.base import DataAdapter

logger = logging.getLogger(__name__)


class CSVTargetAdapter(DataAdapter):
    role = "target"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def fetch_snapshot(self, config: Dict[str, Any], run_id: Optional[str] = None) -> Dict[str, Any]:
        url_or_path = config["url_or_path"]
        delimiter   = config.get("delimiter", ",")
        encoding    = config.get("encoding", "utf-8")
        sample_n    = int(config.get("sample_rows", 10))
        object_name = url_or_path.rstrip("/").split("/")[-1]

        if url_or_path.startswith("http://") or url_or_path.startswith("https://"):
            resp       = requests.get(url_or_path, timeout=30)
            resp.raise_for_status()
            size_bytes = int(resp.headers.get("Content-Length", len(resp.content)))
            text       = resp.text
        else:
            size_bytes = os.path.getsize(url_or_path)
            with open(url_or_path, "r", encoding=encoding) as f:
                text = f.read()

        reader    = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        columns   = list(reader.fieldnames or [])
        all_rows  = list(reader)

        return {
            "run_id":          run_id,
            "asset_role":      "TARGET",
            "system_name":     "CSV",
            "system_type":     "FILE",
            "database_name":   None,
            "schema_name":     None,
            "object_name":     object_name,
            "object_type":     "FILE",
            "row_count":       len(all_rows),
            "column_count":    len(columns),
            "size_bytes":      size_bytes,
            "last_updated_at": None,
            "observed_at":     datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns":         columns,
            "sample":          all_rows[:sample_n],
        }
