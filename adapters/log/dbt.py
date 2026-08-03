"""
adapters/log/dbt.py
--------------------
Fetches run metadata from the dbt Cloud Admin API v2.
Produces the standard 17-field log shape.

Required config keys:
    account_id  – dbt Cloud account numeric ID
    api_token   – Service Token or Personal Access Token
Optional:
    base_url    – defaults to "https://cloud.getdbt.com"
"""

import datetime
import json
import logging
import uuid

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from adapters.log.base import LogAdapter

from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_RETRY_STATUS = {429, 500, 502, 503, 504}

_DBT_STATUS_MAP = {
    1:  "queued",
    2:  "starting",
    3:  "running",
    10: "success",
    20: "error",
    30: "cancelled",
}


class DbtCloudLogAdapter(LogAdapter):
    """Fetches run details from dbt Cloud Admin API v2."""

    def fetch_log(
        self,
        run_id: str,
        config: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        account_id = str(config["account_id"])
        api_token  = config["api_token"]
        base_url   = config.get("base_url", "https://cloud.getdbt.com").rstrip("/")
        context    = context or {}

        headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type":  "application/json",
        }

        run_data  = self._fetch_run(base_url, account_id, run_id, headers)
        artifacts = self._fetch_artifacts(base_url, account_id, run_id, headers)

        data       = run_data.get("data", {})
        status_int = data.get("status", 0)
        status_str = _DBT_STATUS_MAP.get(status_int, f"unknown({status_int})")

        # Parse steps
        steps = []
        for step in data.get("run_steps", []):
            steps.append({
                "name":        step.get("name"),
                "status":      _DBT_STATUS_MAP.get(step.get("status"), "unknown"),
                "started_at":  step.get("started_at"),
                "finished_at": step.get("finished_at"),
                "duration_s":  step.get("duration"),
            })

        # Triggered_by — dbt API returns a cause string
        triggered_cause = None
        trigger_obj = data.get("trigger") or {}
        if isinstance(trigger_obj, dict):
            triggered_cause = trigger_obj.get("cause") or trigger_obj.get("github_pull_request_id")

        # Orchestrator context comes from the webhook payload passed as context
        orchestrator_tool    = context.get("orchestrator_tool")
        orchestrator_dag_id  = context.get("orchestrator_dag_id")
        orchestrator_task_id = context.get("orchestrator_task_id")
        orchestrator_run_id  = context.get("orchestrator_run_id")

        execution_mode = "orchestrated" if orchestrator_tool else "native"

        error_message = None
        if status_str == "error":
            error_message = data.get("status_message") or "Run failed — check dbt Cloud for details"

        job_obj = data.get("job") if isinstance(data.get("job"), dict) else {}
        pipeline_name = job_obj.get("name") if isinstance(job_obj, dict) else None

        return {
            "id":                   str(uuid.uuid4()),
            "pipeline_id":          str(data.get("job_id", run_id)),
            "pipeline_name":        pipeline_name,
            "status":               status_str,
            "start_time":           data.get("started_at"),
            "end_time":             data.get("finished_at"),
            "duration":             data.get("duration"),
            "tool_name":            "dbt",
            "rows_read":            None,    # dbt does not expose rows read
            "rows_written":         None,    # dbt does not expose rows written
            "error_message":        error_message,
            "raw_log":              json.dumps(run_data),
            "execution_mode":       execution_mode,
            "triggered_by":         context.get("triggered_by") or triggered_cause,
            "orchestrator_tool":    orchestrator_tool,
            "orchestrator_dag_id":  orchestrator_dag_id,
            "orchestrator_task_id": orchestrator_task_id,
            "orchestrator_run_id":  orchestrator_run_id,
            # dbt extras (beyond the standard shape)
            "git_branch":           data.get("git_branch"),
            "artifacts":            artifacts.get("data", []),
            "steps":                steps,
            "fetched_at":           datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _fetch_run(self, base_url, account_id, run_id, headers):
        url  = f"{base_url}/api/v2/accounts/{account_id}/runs/{run_id}/"
        resp = requests.get(url, headers=headers, timeout=15,
                            params={"include_related": '["run_steps","trigger","job"]'})
        if resp.status_code in _RETRY_STATUS:
            raise requests.ConnectionError(f"Retryable HTTP {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _fetch_artifacts(self, base_url, account_id, run_id, headers):
        url  = f"{base_url}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/"
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 404:
            return {"data": []}
        if resp.status_code in _RETRY_STATUS:
            raise requests.ConnectionError(f"Retryable HTTP {resp.status_code}")
        resp.raise_for_status()
        return resp.json()
