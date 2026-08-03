"""
shared/models.py
----------------
Dataclasses shared between the receiver and the worker.
No heavy dependencies here — plain Python only.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import datetime


# ---------------------------------------------------------------------------
# Pipeline configuration (loaded from config DB)
# ---------------------------------------------------------------------------

@dataclass
class AdapterConfig:
    """Generic config for one adapter (source or target)."""
    type: str                          # e.g. 'mysql', 'snowflake'
    config: Dict[str, Any]            # JSON blob from DB (secret refs, not passwords)
    secret_id: Optional[str] = None   # reference into the secrets layer


@dataclass
class PipelineConfig:
    """Full config for one pipeline, as stored in the config DB."""
    job_id: str
    tool_type: str                     # 'dbt', 'airflow'
    source: AdapterConfig
    target: AdapterConfig


# ---------------------------------------------------------------------------
# Job payload (what the receiver drops on the queue)
# ---------------------------------------------------------------------------

@dataclass
class WebhookJob:
    """Minimal payload enqueued by the receiver."""
    job_id: str
    run_id: str
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    received_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Adapter results
# ---------------------------------------------------------------------------

@dataclass
class LogResult:
    """Result from a log adapter fetch."""
    tool_type: str
    run_id: str
    status: str                        # 'success', 'error', 'running'
    rows: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)
    fetched_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )


@dataclass
class SnapshotResult:
    """Result from a source or target adapter fetch."""
    adapter_type: str                  # 'source' or 'target'
    connector: str                     # e.g. 'mysql', 'snowflake'
    row_count: int = 0
    columns: list = field(default_factory=list)
    sample: list = field(default_factory=list)   # first N rows as dicts
    fetched_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )


@dataclass
class JobResult:
    """Everything the worker produces for one webhook job."""
    job_id: str
    run_id: str
    log: Optional[LogResult] = None
    source: Optional[SnapshotResult] = None
    target: Optional[SnapshotResult] = None
    status: str = "pending"            # 'success', 'partial', 'failed'
    error: Optional[str] = None
    completed_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
