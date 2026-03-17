"""Pydantic data models.

Typed models for cluster info, Spark jobs, stages, executors,
SQL queries, and storage/RDD data returned by the APIs.
"""

from __future__ import annotations

import enum
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, Field, computed_field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ClusterState(str, enum.Enum):
    """Databricks cluster lifecycle states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RESTARTING = "RESTARTING"
    RESIZING = "RESIZING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class JobStatus(str, enum.Enum):
    """Spark job status."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class StageStatus(str, enum.Enum):
    """Spark stage status."""

    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"
    PENDING = "PENDING"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


# ---------------------------------------------------------------------------
# Format utilities
# ---------------------------------------------------------------------------

_BYTE_UNITS: Sequence[str] = ("B", "KB", "MB", "GB", "TB", "PB")


def format_bytes(n: int) -> str:
    """Format a byte count as a human-readable string.

    Args:
        n: Number of bytes.

    Returns:
        Formatted string such as ``'1.0 KB'`` or ``'3.2 GB'``.
    """
    if n is None:  # type: ignore[arg-type]
        return "--"
    if n < 0:
        return f"-{format_bytes(-n)}"
    if n == 0:
        return "0 B"
    exp = min(int(math.log(n, 1024)), len(_BYTE_UNITS) - 1)
    value = n / (1024**exp)
    # Drop the decimal for exact whole numbers
    if value == int(value) and exp == 0:
        return f"{int(value)} B"
    return f"{value:.1f} {_BYTE_UNITS[exp]}"


def format_duration(ms: int) -> str:
    """Format milliseconds into a compact human-readable duration.

    Args:
        ms: Duration in milliseconds.

    Returns:
        Formatted string such as ``'2m 15s'`` or ``'1d 3h 10m'``.
    """
    if ms is None:  # type: ignore[arg-type]
        return "--"
    if ms < 0:
        return f"-{format_duration(-ms)}"
    if ms == 0:
        return "0s"

    seconds = ms // 1000
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def format_timestamp(dt: Optional[datetime]) -> str:
    """Format a datetime as a compact timestamp string.

    Args:
        dt: UTC datetime to format, or ``None``.

    Returns:
        ``'HH:MM:SS'`` if today, ``'Mon DD HH:MM'`` otherwise, or ``'--'`` if None.
    """
    if dt is None:
        return "--"
    now = datetime.now(timezone.utc)
    if dt.date() == now.date():
        return dt.strftime("%H:%M:%S")
    return dt.strftime("%b %d %H:%M")


# ---------------------------------------------------------------------------
# Cluster models
# ---------------------------------------------------------------------------


class ClusterInfo(BaseModel):
    """Normalized Databricks cluster information."""

    cluster_id: str
    cluster_name: str = ""
    state: ClusterState = ClusterState.UNKNOWN
    state_message: str = ""
    start_time: Optional[datetime] = None
    driver_node_type: str = ""
    worker_node_type: str = ""
    num_workers: int = 0
    autoscale_min: Optional[int] = None
    autoscale_max: Optional[int] = None
    total_cores: int = 0
    total_memory_mb: int = 0
    spark_version: str = ""
    data_security_mode: str = ""
    runtime_engine: str = ""
    creator: str = ""
    autotermination_minutes: int = 0
    spark_conf: Dict[str, str] = Field(default_factory=dict)
    tags: Dict[str, str] = Field(default_factory=dict)
    spark_context_id: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def uptime_seconds(self) -> int:
        """Seconds since the cluster started, or 0 if not running."""
        if self.start_time is None:
            return 0
        delta = datetime.now(timezone.utc) - self.start_time
        return max(0, int(delta.total_seconds()))


class ClusterEvent(BaseModel):
    """A single cluster lifecycle event."""

    timestamp: datetime
    event_type: str = ""
    message: str = ""
    details: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Spark job / stage models
# ---------------------------------------------------------------------------


class SparkJob(BaseModel):
    """A Spark job as reported by the Spark REST API."""

    job_id: int
    name: str = ""
    status: JobStatus = JobStatus.UNKNOWN
    submission_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    num_tasks: int = 0
    num_active_tasks: int = 0
    num_completed_tasks: int = 0
    num_failed_tasks: int = 0
    num_stages: int = 0
    num_active_stages: int = 0
    num_completed_stages: int = 0
    num_failed_stages: int = 0
    stage_ids: List[int] = Field(default_factory=list)


class SparkStage(BaseModel):
    """A Spark stage as reported by the Spark REST API."""

    stage_id: int
    attempt_id: int = 0
    name: str = ""
    status: StageStatus = StageStatus.PENDING
    num_tasks: int = 0
    num_active_tasks: int = 0
    num_complete_tasks: int = 0
    num_failed_tasks: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    executor_run_time_ms: int = 0
    executor_cpu_time_ns: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0
    submission_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def spill_bytes(self) -> int:
        """Total spill (memory + disk)."""
        return self.memory_spill_bytes + self.disk_spill_bytes


# ---------------------------------------------------------------------------
# Executor model
# ---------------------------------------------------------------------------


class ExecutorInfo(BaseModel):
    """Spark executor metrics."""

    executor_id: str
    is_active: bool = True
    host_port: str = ""
    total_cores: int = 0
    max_tasks: int = 0
    active_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_duration_ms: int = 0
    total_gc_time_ms: int = 0
    max_memory: int = 0
    memory_used: int = 0
    disk_used: int = 0
    total_input_bytes: int = 0
    total_shuffle_read: int = 0
    total_shuffle_write: int = 0
    rdd_blocks: int = 0
    peak_jvm_heap: int = 0
    peak_jvm_off_heap: int = 0
    add_time: Optional[datetime] = None
    remove_time: Optional[datetime] = None
    remove_reason: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_driver(self) -> bool:
        """True if this is the driver executor."""
        return self.executor_id == "driver"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gc_ratio(self) -> float:
        """Fraction of total time spent in GC (0.0–1.0)."""
        if self.total_duration_ms <= 0:
            return 0.0
        return self.total_gc_time_ms / self.total_duration_ms

    @computed_field  # type: ignore[prop-decorator]
    @property
    def memory_used_pct(self) -> float:
        """Percentage of max memory currently used (0.0–100.0)."""
        if self.max_memory <= 0:
            return 0.0
        return (self.memory_used / self.max_memory) * 100.0


# ---------------------------------------------------------------------------
# SQL query model
# ---------------------------------------------------------------------------


class SQLQuery(BaseModel):
    """A Spark SQL query execution."""

    execution_id: int
    status: str = ""
    description: str = ""
    submission_time: Optional[datetime] = None
    duration_ms: int = 0
    running_jobs: int = 0
    success_jobs: int = 0
    failed_jobs: int = 0


# ---------------------------------------------------------------------------
# Storage / RDD model
# ---------------------------------------------------------------------------


class RDDInfo(BaseModel):
    """A cached RDD or DataFrame."""

    rdd_id: int
    name: str = ""
    num_partitions: int = 0
    num_cached_partitions: int = 0
    storage_level: str = ""
    memory_used: int = 0
    disk_used: int = 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fraction_cached(self) -> float:
        """Fraction of partitions that are cached (0.0–1.0)."""
        if self.num_partitions <= 0:
            return 0.0
        return self.num_cached_partitions / self.num_partitions


# ---------------------------------------------------------------------------
# Databricks job run model
# ---------------------------------------------------------------------------


class JobRun(BaseModel):
    """A Databricks job run (from the Jobs API)."""

    run_id: int
    job_id: int = 0
    run_name: str = ""
    state: str = ""
    result_state: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    setup_duration_ms: int = 0
    execution_duration_ms: int = 0
    creator: str = ""
    run_type: str = ""
    task_count: int = 0


# ---------------------------------------------------------------------------
# Library info model
# ---------------------------------------------------------------------------


class LibraryInfo(BaseModel):
    """Installed library status on a cluster."""

    name: str
    library_type: str = ""
    status: str = ""
    messages: List[str] = Field(default_factory=list)
