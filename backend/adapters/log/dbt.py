"""
adapters/log/dbt.py
-------------------
dbt Cloud log adapter.
Fetches run details, duration, status, and manifests from dbt Cloud Admin API v2.
Gracefully handles missing credentials, custom base URLs, and 401/404 API responses.
"""

import logging
import re
import uuid
from typing import Any, Dict, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, before_sleep_log

from adapters.log.base import LogAdapter

logger = logging.getLogger(__name__)

_RETRY_STATUS = {429, 500, 502, 503, 504}

_DBT_STATUS_MAP = {
    1:  "queued",
    2:  "starting",
    3:  "running",
    10: "success",
    20: "failed",
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
        account_id = str(config.get("account_id") or "")
        api_token  = str(config.get("api_token") or "")
        
        raw_url = config.get("base_url") or config.get("url") or "https://cloud.getdbt.com"
        if not raw_url or not str(raw_url).strip():
            raw_url = "https://cloud.getdbt.com"
        base_url = str(raw_url).rstrip("/")
        context  = context or {}

        headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type":  "application/json",
        }

        orchestrator_tool    = context.get("orchestrator_tool")
        orchestrator_dag_id  = context.get("orchestrator_dag_id")
        orchestrator_task_id = context.get("orchestrator_task_id")
        orchestrator_run_id  = context.get("orchestrator_run_id")

        try:
            if not account_id or not api_token or not run_id:
                raise ValueError("Missing account_id, api_token, or run_id")

            run_data  = self._fetch_run(base_url, account_id, run_id, headers)
            artifacts = self._fetch_artifacts(base_url, account_id, run_id, headers)

            data       = run_data.get("data", {})
            status_int = data.get("status", 0)
            status_str = _DBT_STATUS_MAP.get(status_int, "success")
            if status_str == "error":
                status_str = "failed"

            steps = []
            for step in data.get("run_steps", []):
                steps.append({
                    "name":        step.get("name"),
                    "status":      _DBT_STATUS_MAP.get(step.get("status"), "unknown"),
                    "started_at":  step.get("started_at"),
                    "finished_at": step.get("finished_at"),
                    "duration_s":  step.get("duration"),
                })

            triggered_cause = None
            trigger_obj = data.get("trigger") if isinstance(data.get("trigger"), dict) else {}
            if isinstance(trigger_obj, dict):
                triggered_cause = trigger_obj.get("cause") or trigger_obj.get("github_pull_request_id")
                cause_category  = str(trigger_obj.get("cause_category") or "").strip().lower()
            else:
                cause_category  = ""

            # Determine dynamic execution mode
            if orchestrator_tool:
                execution_mode = "orchestrated"
            elif cause_category:
                execution_mode = cause_category  # "ui", "schedule", "github", "api"
            else:
                execution_mode = "native"

            # Parse short, clean, human-readable error message
            error_message = None
            if status_str in ["failed", "error"]:
                status_str = "failed"
                model_name = None
                reason_str = None
                
                for step in data.get("run_steps", []):
                    if isinstance(step, dict):
                        step_logs = str(step.get("logs") or "")
                        for raw_line in step_logs.split("\n"):
                            line = re.sub(r'\x1b\[[0-9;]*[mGKB]', '', raw_line).strip()
                            line = re.sub(r'^\d{2}:\d{2}:\d{2}\s+', '', line).strip()

                            # Match model name
                            m_model = re.search(r'model\s+([A-Za-z0-9_\.]+)', line)
                            if m_model and not model_name and "target/run" not in line:
                                raw_mod = m_model.group(1).split(".")[-1]
                                if raw_mod and raw_mod not in ["sql", "view", "table"]:
                                    model_name = raw_mod

                            # Match specific error line
                            if "invalid identifier" in line.lower() or "syntax error" in line.lower() or "compilation error" in line.lower():
                                reason_str = line
                            elif "Database Error" in line and not reason_str:
                                reason_str = line

                if model_name and reason_str:
                    reason_clean = re.sub(r'\(target/run/[^)]+\)', '', reason_str).strip()
                    reason_clean = re.sub(r'^Error\s*\[[^\]]+\]:\s*', '', reason_clean).strip()
                    reason_clean = re.sub(r'^\s*\[Snowflake\]\s*\d+\s*\(\d+\):\s*', '', reason_clean).strip()
                    error_message = f"Model '{model_name}': {reason_clean}"
                elif reason_str:
                    reason_clean = re.sub(r'\(target/run/[^)]+\)', '', reason_str).strip()
                    reason_clean = re.sub(r'^Error\s*\[[^\]]+\]:\s*', '', reason_clean).strip()
                    reason_clean = re.sub(r'^\s*\[Snowflake\]\s*\d+\s*\(\d+\):\s*', '', reason_clean).strip()
                    error_message = reason_clean[:200]
                else:
                    error_message = data.get("status_message") or "dbt run failed"

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
                "rows_read":            None,
                "rows_written":         None,
                "error_message":        error_message,
                "raw_log":              run_data,
                "execution_mode":       execution_mode,
                "triggered_by":         context.get("triggered_by") or triggered_cause,
                "orchestrator_tool":    orchestrator_tool,
                "orchestrator_dag_id":  orchestrator_dag_id,
                "orchestrator_task_id": orchestrator_task_id,
                "orchestrator_run_id":  orchestrator_run_id,
                "artifacts":            artifacts,
            }
        except Exception as exc:
            logger.warning("dbt API fetch warning (using webhook fallback log): %s", exc)
            return {
                "id":                   str(uuid.uuid4()),
                "pipeline_id":          str(run_id or "unknown"),
                "pipeline_name":        "dbt_job_run",
                "status":               "failed",
                "start_time":           None,
                "end_time":             None,
                "duration":             None,
                "tool_name":            "dbt",
                "rows_read":            None,
                "rows_written":         None,
                "error_message":        str(exc),
                "raw_log":              {"api_warning": str(exc), "run_id": run_id},
                "execution_mode":       "native",
                "triggered_by":         context.get("triggered_by"),
                "orchestrator_tool":    orchestrator_tool,
                "orchestrator_dag_id":  orchestrator_dag_id,
                "orchestrator_task_id": orchestrator_task_id,
                "orchestrator_run_id":  orchestrator_run_id,
                "artifacts":            {},
            }

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
