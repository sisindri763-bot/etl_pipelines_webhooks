"""
app.py
------
Production webhook server — single-process Flask app.

Routes:
    POST   /webhooks/dbt/<user_id>        Receive dbt Cloud webhook, run all 3 adapters
    POST   /admin/register-config         Register a pipeline config
    GET    /admin/list-configs            List all registered pipeline configs
    DELETE /admin/delete-config/<job_id>  Remove a pipeline config
    GET    /admin/runs                    List recent pipeline runs from results DB
    GET    /admin/runs/<run_id>           Full detail for one run (log + source + target)
    GET    /health                        Health check + adapter registry

Environment variables (see .env):
    WEBHOOK_SECRET  HMAC secret from dbt Cloud (optional — skip for local dev)
    ADMIN_TOKEN     Bearer token protecting /admin routes (optional for local dev)
    LOG_LEVEL       DEBUG | INFO | WARNING | ERROR  (default: INFO)
"""

import datetime
import hashlib
import hmac
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv

from flask import Flask, jsonify, request, abort, send_file

# ---------------------------------------------------------------------------
# Bootstrap — must happen before adapter imports so the cwd is on sys.path
# ---------------------------------------------------------------------------
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from config.db import init_db, register_pipeline, get_pipeline, list_pipelines, delete_pipeline
from config.results_db import (
    init_results_db, save_pipeline_run, save_source_asset_metadata, save_target_asset_metadata,
    list_recent_runs, get_run_with_assets,
)
from adapters  import LOG_ADAPTERS, SOURCE_ADAPTERS, TARGET_ADAPTERS

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO").upper()
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "./results"))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    return response

@app.route("/", methods=["OPTIONS"])
@app.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path=None):
    return "", 200

# Initialise both DBs on startup
init_db()
init_results_db()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _verify_dbt_signature(payload_bytes: bytes, sig_header: str) -> bool:
    """
    Verify the dbt Cloud HMAC-SHA256 webhook signature.
    Skipped if WEBHOOK_SECRET is not set (useful for local dev).
    """
    secret = os.getenv("WEBHOOK_SECRET", "")
    if not secret:
        return True   # not configured — skip verification

    expected = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")


def _require_admin_token() -> None:
    """Abort with 401 if admin token is configured but not provided."""
    token = os.getenv("ADMIN_TOKEN", "")
    if not token:
        return   # not configured — open access (fine for local dev)

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != token:
        abort(401, description="Invalid or missing admin token")


# ---------------------------------------------------------------------------
# Webhook route
# ---------------------------------------------------------------------------

@app.route("/webhooks/dbt/<user_id>", methods=["POST"])
def dbt_webhook(user_id: str):
    """
    Receive a dbt Cloud webhook, look up the pipeline config,
    and run log + source + target fetches.

    dbt Cloud sends job_id in the payload; we use it as the config key.
    A failure in any individual fetch is captured and stored — it does
    NOT abort the whole request.
    """
    raw_body = request.get_data()

    # Signature verification
    sig = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not _verify_dbt_signature(raw_body, sig):
        logger.warning("Webhook signature mismatch for user_id=%s", user_id)
        abort(403, description="Invalid webhook signature")

    payload: Dict[str, Any] = {}
    try:
        raw_json = json.loads(raw_body)
        if isinstance(raw_json, dict):
            payload = raw_json
    except json.JSONDecodeError:
        abort(400, description="Invalid JSON payload")

    # dbt Cloud webhook payload fields
    data_obj  = payload.get("data")  # type: ignore # pyright: ignore
    data_dict = data_obj if isinstance(data_obj, dict) else {}

    job_id = str(
        payload.get("jobId")
        or payload.get("job_id")
        or data_dict.get("jobId")
        or ""
    )
    run_id = str(
        payload.get("runId")
        or payload.get("run_id")
        or data_dict.get("runId")
        or ""
    )

    if not job_id:
        abort(400, description="Could not extract job_id from webhook payload")

    # Extract orchestrator context — Airflow (or any orchestrator) may embed these
    # fields in the webhook payload so the worker knows how the run was triggered.
    orchestrator_context = {
        "triggered_by":         payload.get("triggered_by") or payload.get("triggeredBy"),
        "orchestrator_tool":    payload.get("orchestrator_tool") or payload.get("orchestratorTool"),
        "orchestrator_dag_id":  payload.get("orchestrator_dag_id") or payload.get("orchestratorDagId"),
        "orchestrator_task_id": payload.get("orchestrator_task_id") or payload.get("orchestratorTaskId"),
        "orchestrator_run_id":  payload.get("orchestrator_run_id") or payload.get("orchestratorRunId"),
    }

    logger.info("Webhook received — user_id=%s job_id=%s run_id=%s", user_id, job_id, run_id)

    # Look up pipeline config
    pipeline = get_pipeline(job_id)
    if pipeline is None:
        logger.warning("No config for job_id=%s — returning 200 to avoid dbt retries", job_id)
        return jsonify({
            "status":  "skipped",
            "reason":  f"No pipeline config registered for job_id={job_id}",
            "job_id":  job_id,
            "run_id":  run_id,
        }), 200

    # Build result bundle
    correlation_id = run_id or str(uuid.uuid4())
    result: Dict[str, Any] = {
        "correlation_id": correlation_id,
        "job_id":         job_id,
        "user_id":        user_id,
        "tool_type":      pipeline["tool_type"],
        "received_at":    datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "log":            None,
        "source":         None,
        "target":         None,
        "status":         "pending",
        "errors":         [],
    }

    # ── 1. Log adapter ──────────────────────────────────────────────────────
    try:
        tool_type    = pipeline["tool_type"]
        tool_config  = pipeline.get("tool_config", {})
        log_adapter  = LOG_ADAPTERS.get(tool_type)
        if log_adapter is None:
            raise ValueError(f"No log adapter registered for tool_type='{tool_type}'")
        log_dict = log_adapter.fetch_log(run_id, tool_config, context=orchestrator_context)
        result["log"] = log_dict
        logger.info("Log fetched — job_id=%s run_id=%s status=%s",
                    job_id, run_id, log_dict.get("status"))
    except Exception as exc:
        logger.exception("Log fetch failed — job_id=%s", job_id)
        result["errors"].append({"component": "log", "error": str(exc)})

    # ── 2. Source adapter ────────────────────────────────────────────────────
    try:
        source_type    = pipeline["source_type"]
        source_config  = pipeline.get("source_config", {})
        source_adapter = SOURCE_ADAPTERS.get(source_type)
        if source_adapter is None:
            raise ValueError(f"No source adapter registered for source_type='{source_type}'")
        source_dict = source_adapter.fetch_snapshot(source_config, run_id=correlation_id)
        result["source"] = source_dict
        logger.info("Source snapshot fetched — job_id=%s connector=%s rows=%s",
                    job_id, source_type, source_dict.get("row_count"))
    except Exception as exc:
        logger.exception("Source fetch failed — job_id=%s", job_id)
        result["errors"].append({"component": "source", "error": str(exc)})

    # ── 3. Target adapter ────────────────────────────────────────────────────
    try:
        target_type    = pipeline["target_type"]
        target_config  = pipeline.get("target_config", {})
        target_adapter = TARGET_ADAPTERS.get(target_type)
        if target_adapter is None:
            raise ValueError(f"No target adapter registered for target_type='{target_type}'")
        target_dict = target_adapter.fetch_snapshot(target_config, run_id=correlation_id)
        result["target"] = target_dict
        logger.info("Target snapshot fetched — job_id=%s connector=%s rows=%s",
                    job_id, target_type, target_dict.get("row_count"))
    except Exception as exc:
        logger.exception("Target fetch failed — job_id=%s", job_id)
        result["errors"].append({"component": "target", "error": str(exc)})

    # ── Final status ─────────────────────────────────────────────────────────
    n_errors = len(result["errors"])
    if n_errors == 0:
        result["status"] = "success"
    elif n_errors == 3:
        result["status"] = "failed"
    else:
        result["status"] = "partial"

    result["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # ── Save to Supabase / Postgres results DB ─────────────────────────────────────
    try:
        src_dict = result.get("source")
        tgt_dict = result.get("target")
        rows_read = src_dict.get("row_count") if isinstance(src_dict, dict) else None
        rows_written = tgt_dict.get("row_count") if isinstance(tgt_dict, dict) else None

        log_val = result.get("log")
        log_payload = log_val if isinstance(log_val, dict) else {
            "pipeline_id":          job_id,
            "pipeline_name":        None,
            "status":               result["status"],
            "start_time":           result["received_at"],
            "end_time":             result["completed_at"],
            "duration":             None,
            "tool_name":            pipeline.get("tool_type", "dbt"),
            "rows_read":            rows_read,
            "rows_written":         rows_written,
            "error_message":        json.dumps(result["errors"]) if result.get("errors") else None,
            "raw_log":              result,
            "execution_mode":       orchestrator_context.get("execution_mode") or ("orchestrated" if orchestrator_context.get("orchestrator_tool") else "native"),
            "triggered_by":         orchestrator_context.get("triggered_by"),
            "orchestrator_tool":    orchestrator_context.get("orchestrator_tool"),
            "orchestrator_dag_id":  orchestrator_context.get("orchestrator_dag_id"),
            "orchestrator_task_id": orchestrator_context.get("orchestrator_task_id"),
            "orchestrator_run_id":  orchestrator_context.get("orchestrator_run_id"),
        }

        # Save to pipeline_runs table
        save_pipeline_run(correlation_id, log_payload)

        # Save source & target to separate source_asset_metadata and target_asset_metadata tables
        if isinstance(src_dict, dict):
            save_source_asset_metadata(correlation_id, src_dict)
        if isinstance(tgt_dict, dict):
            save_target_asset_metadata(correlation_id, tgt_dict)

        logger.info("Results saved to Supabase / DB — run_id=%s status=%s", correlation_id, result["status"])
    except Exception as db_exc:
        logger.exception("Failed to save results to DB — %s", db_exc)

    # ── Save bundle (Disk) ────────────────────────────────────────────────────
    _save_result(job_id, correlation_id, result)

    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@app.route("/admin/register-config", methods=["POST"])
def admin_register_config():
    """
    Register or update a pipeline configuration.

    Expected JSON body:
    {
        "job_id":       "12345",
        "tool_type":    "dbt",
        "tool_config":  { "account_id": "...", "api_token": "..." },
        "source_type":  "mysql",
        "source_config": { "host": "...", "port": 3306, ... },
        "target_type":  "snowflake",
        "target_config": { "account": "...", ... }
    }

    Curl example:
        curl -X POST https://your-app.onrender.com/admin/register-config \\
             -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
             -H "Content-Type: application/json" \\
             -d @pipeline.json
    """
    _require_admin_token()

    data = request.get_json(silent=True)
    if not data:
        abort(400, description="JSON body required")

    required = ["job_id", "tool_type", "source_type", "source_config",
                "target_type", "target_config"]
    missing  = [k for k in required if k not in data]
    if missing:
        abort(400, description=f"Missing required fields: {missing}")

    register_pipeline(**data)

    return jsonify({
        "status":  "registered",
        "job_id":  data["job_id"],
        "message": f"Pipeline '{data['job_id']}' saved to config DB",
    }), 201


@app.route("/admin/list-configs", methods=["GET"])
def admin_list_configs():
    """
    Return all registered pipeline configs.

    Curl example:
        curl https://your-app.onrender.com/admin/list-configs \\
             -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
    """
    _require_admin_token()
    pipelines = list_pipelines()

    # Mask passwords in the response — never send them back
    safe = []
    for p in pipelines:
        p = _mask_credentials(p)
        safe.append(p)

    return jsonify({"count": len(safe), "pipelines": safe}), 200


@app.route("/admin/delete-config/<job_id>", methods=["DELETE"])
def admin_delete_config(job_id: str):
    """
    Delete a pipeline config by job_id.

    Curl example:
        curl -X DELETE https://your-app.onrender.com/admin/delete-config/12345 \\
             -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
    """
    _require_admin_token()
    deleted = delete_pipeline(job_id)
    if not deleted:
        abort(404, description=f"No pipeline found with job_id={job_id}")
    return jsonify({"status": "deleted", "job_id": job_id}), 200


# ---------------------------------------------------------------------------
# Health & Dashboard UI routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def serve_dashboard():
    return send_file(Path(__file__).parent / "index.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":   "ok",
        "service":  "webhook-server",
        "adapters": {
            "log":    list(LOG_ADAPTERS.keys()),
            "source": list(SOURCE_ADAPTERS.keys()),
            "target": list(TARGET_ADAPTERS.keys()),
        },
    }), 200


# ---------------------------------------------------------------------------
# Results query routes
# ---------------------------------------------------------------------------

@app.route("/admin/runs", methods=["GET"])
def admin_list_runs():
    """
    List recent pipeline runs stored in pipeline_runs.

    Query params:
        limit – max rows (default 50)
    """
    _require_admin_token()
    limit = int(request.args.get("limit", 50))
    runs  = list_recent_runs(limit=limit)
    return jsonify({"count": len(runs), "runs": runs}), 200


@app.route("/admin/runs/<pipeline_run_id>", methods=["GET"])
def admin_get_run(pipeline_run_id: str):
    """
    Full detail for one run from Supabase (pipeline_runs + asset_metadata).
    """
    _require_admin_token()
    data = get_run_with_assets(pipeline_run_id)
    if not data or data.get("run") is None:
        abort(404, description=f"No run found with id={pipeline_run_id}")
    return jsonify(data), 200


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _save_result(job_id: str, correlation_id: str, result: dict) -> None:
    """Write the result bundle to results/<job_id>/<correlation_id>.json"""
    job_dir = RESULTS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_path = job_dir / f"{correlation_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("Result saved — %s", out_path)


def _mask_credentials(pipeline: dict) -> dict:
    """Replace known credential fields with '***' before returning to caller."""
    SENSITIVE = {"password", "api_token", "secret", "token", "private_key"}
    import copy
    p = copy.deepcopy(pipeline)
    for cfg_key in ("source_config", "target_config", "tool_config"):
        cfg = p.get(cfg_key, {})
        if isinstance(cfg, dict):
            for k in list(cfg.keys()):
                if any(s in k.lower() for s in SENSITIVE):
                    cfg[k] = "***"
    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    logger.info("Starting webhook server on port %d (debug=%s)", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug)
