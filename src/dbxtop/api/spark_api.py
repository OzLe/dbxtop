"""Spark REST API client.

Communicates with the Spark UI REST API (proxied through Databricks)
to retrieve jobs, stages, executors, SQL queries, and storage data.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from urllib.parse import quote
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import httpx

from dbxtop.api.models import (
    ExecutorInfo,
    JobStatus,
    RDDInfo,
    SQLQuery,
    SparkJob,
    SparkStage,
    StageStatus,
)

logger = logging.getLogger(__name__)

_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class SparkRESTClient:
    """Async client for the Spark REST API v1 via the Databricks driver proxy.

    Args:
        workspace_url: Databricks workspace URL (no trailing slash).
        cluster_id: Target cluster ID.
        org_id: Databricks organisation ID.
        port: Spark UI proxy port on the driver node.
        token: Databricks bearer token.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        workspace_url: str,
        cluster_id: str,
        org_id: str,
        port: int = 40001,
        token: Optional[str] = None,
        token_provider: Optional[Union[Callable[[], str], Callable[[], Awaitable[str]]]] = None,
        timeout: float = 10.0,
    ) -> None:
        self._workspace_url = workspace_url.rstrip("/")
        self._cluster_id = cluster_id
        self._org_id = org_id
        self._port = port
        self._app_id: Optional[str] = None
        self._available = False
        self._rate_limit_backoff: float = 0.0
        self._rate_limited: bool = False
        self._rate_limit_until: float = 0.0
        self._available_checked_at: float = 0.0
        self._token_provider = token_provider

        headers: Dict[str, str] = {}
        if not token_provider and token:
            headers["Authorization"] = f"Bearer {token}"

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers=headers,
            follow_redirects=False,
        )

    async def _resolve_token(self) -> Optional[str]:
        """Resolve the token from the provider, handling both sync and async callables.

        Returns:
            The bearer token string, or ``None`` if no provider is configured
            or if token retrieval fails (logged as warning).
        """
        if not self._token_provider:
            return None
        try:
            result = self._token_provider()
            if hasattr(result, "__await__"):
                return await result
            return result
        except Exception:
            logger.warning("Token provider failed — request will be unauthenticated", exc_info=True)
            return None

    # -- URL construction ----------------------------------------------------

    @property
    def _base_url(self) -> str:
        """Driver proxy base URL (without trailing slash)."""
        org = quote(str(self._org_id), safe="")
        cid = quote(str(self._cluster_id), safe="")
        return f"{self._workspace_url}/driver-proxy-api/o/{org}/{cid}/{self._port}"

    def _app_url(self, endpoint: str) -> str:
        """Full URL for a Spark application endpoint."""
        app = quote(str(self._app_id), safe="")
        return f"{self._base_url}/api/v1/applications/{app}/{endpoint}"

    # -- lifecycle -----------------------------------------------------------

    async def discover_app_id(self) -> str:
        """Discover the Spark application ID.

        Queries ``GET /api/v1/applications`` and caches the first result.

        Returns:
            The Spark application ID.

        Raises:
            RuntimeError: If no Spark application is found.
        """
        url = f"{self._base_url}/api/v1/applications"
        try:
            headers: Optional[Dict[str, str]] = None
            token = await self._resolve_token()
            if token:
                headers = {"Authorization": f"Bearer {token}"}
            resp = await self._client.get(url, headers=headers)
            resp.raise_for_status()
            apps = resp.json()
            if not apps:
                self._available = False
                raise RuntimeError("No Spark applications found")
            self._app_id = apps[0]["id"]
            self._available = True
            logger.info("Discovered Spark app: %s", self._app_id)
            return self._app_id
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException, ValueError, KeyError) as exc:
            self._available = False
            raise RuntimeError(f"Could not discover Spark application: {exc}") from exc

    async def is_available(self) -> bool:
        """Check whether the Spark REST API is reachable.

        Uses the cached ``_available`` flag if checked within the last 30 s
        to avoid sending a network request on every fast poll cycle.

        Returns:
            ``True`` if a previous ``discover_app_id`` succeeded and the
            proxy is still responding; ``False`` otherwise.
        """
        if not self._app_id:
            return False
        # Return cached result if checked recently (30s)
        now = time.monotonic()
        if self._available and (now - self._available_checked_at) < 30.0:
            return True
        try:
            headers: Optional[Dict[str, str]] = None
            token = await self._resolve_token()
            if token:
                headers = {"Authorization": f"Bearer {token}"}
            resp = await self._client.get(f"{self._base_url}/api/v1/applications", headers=headers)
            self._available = resp.is_success
            self._available_checked_at = now
            return self._available
        except httpx.TransportError:
            self._available = False
            return False

    def reset_app_id(self) -> None:
        """Clear the cached Spark application ID to trigger re-discovery."""
        self._app_id = None
        self._available = False

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    # -- data fetchers -------------------------------------------------------

    async def get_jobs(self, status: Optional[str] = None) -> List[SparkJob]:
        """Fetch Spark jobs.

        Args:
            status: Optional status filter (e.g. ``'running'``).

        Returns:
            List of ``SparkJob`` models.
        """
        params: Dict[str, str] = {}
        if status:
            params["status"] = status
        data = await self._get("jobs", params=params)
        return [_map_spark_job(j) for j in data] if data else []

    async def get_stages(self, status: Optional[str] = None) -> List[SparkStage]:
        """Fetch Spark stages.

        Args:
            status: Optional status filter.

        Returns:
            List of ``SparkStage`` models.
        """
        params: Dict[str, str] = {}
        if status:
            params["status"] = status
        data = await self._get("stages", params=params)
        return [_map_spark_stage(s) for s in data] if data else []

    async def get_stage_detail(self, stage_id: int, attempt_id: int = 0) -> SparkStage:
        """Fetch detailed metrics for a specific stage attempt.

        Args:
            stage_id: Spark stage ID.
            attempt_id: Stage attempt ID (default 0).

        Returns:
            A ``SparkStage`` model with full metrics.
        """
        data = await self._get(f"stages/{stage_id}/{attempt_id}")
        if data is None:
            return SparkStage(stage_id=stage_id, attempt_id=attempt_id)
        return _map_spark_stage(data)

    async def get_executors(self) -> List[ExecutorInfo]:
        """Fetch all executor metrics.

        Returns:
            List of ``ExecutorInfo`` models.
        """
        data = await self._get("allexecutors")
        return [_map_executor(e) for e in data] if data else []

    async def get_sql(self) -> List[SQLQuery]:
        """Fetch SQL query executions.

        Returns:
            List of ``SQLQuery`` models.
        """
        data = await self._get("sql", params={"details": "true"})
        return [_map_sql(q) for q in data] if data else []

    async def get_storage(self) -> List[RDDInfo]:
        """Fetch cached RDD / DataFrame information.

        Returns:
            List of ``RDDInfo`` models.
        """
        data = await self._get("storage/rdd")
        return [_map_rdd(r) for r in data] if data else []

    # -- private transport ---------------------------------------------------

    @property
    def is_rate_limited(self) -> bool:
        """True if currently backing off due to rate limiting."""
        return self._rate_limited

    async def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Issue a GET request against the Spark REST API.

        Handles 404 (app restart), 429 (rate limit), timeouts,
        and connection errors gracefully.

        Returns:
            Parsed JSON response, or ``None`` on failure.
        """
        if not self._app_id:
            logger.warning("No Spark app_id — call discover_app_id() first")
            return None

        # Honour rate-limit backoff (skip request until cooldown expires)
        if self._rate_limit_until > 0:
            if time.monotonic() < self._rate_limit_until:
                return None
            # Backoff expired — clear the flag so the footer updates
            self._rate_limited = False
            self._rate_limit_until = 0.0

        url = self._app_url(endpoint)
        try:
            headers: Optional[Dict[str, str]] = None
            token = await self._resolve_token()
            if token:
                headers = {"Authorization": f"Bearer {token}"}
            resp = await self._client.get(url, params=params, headers=headers)

            # Rate limit handling
            if resp.status_code == 429:
                # Prefer server-provided Retry-After, fall back to exponential backoff
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        self._rate_limit_backoff = min(float(retry_after), 60.0)
                    except ValueError:
                        self._rate_limit_backoff = min((self._rate_limit_backoff or 1.0) * 2, 30.0)
                else:
                    self._rate_limit_backoff = min((self._rate_limit_backoff or 1.0) * 2, 30.0)
                self._rate_limited = True
                self._rate_limit_until = time.monotonic() + self._rate_limit_backoff
                logger.warning("Rate limited on %s — backing off %.1fs", endpoint, self._rate_limit_backoff)
                return None

            # Reset rate limit only on successful responses
            if self._rate_limit_backoff > 0 and resp.is_success:
                self._rate_limit_backoff = 0.0
                self._rate_limited = False
                self._rate_limit_until = 0.0

            if resp.status_code == 404 and self._available:
                logger.info("Got 404 — attempting app_id re-discovery")
                try:
                    await self.discover_app_id()
                    url = self._app_url(endpoint)
                    resp = await self._client.get(url, params=params, headers=headers)
                except RuntimeError:
                    self._available = False
                    return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("HTTP %d on %s: %s", exc.response.status_code, endpoint, exc)
            if exc.response.status_code == 404:
                self._available = False
            return None
        except httpx.TransportError as exc:
            # Covers TimeoutException, ConnectError, ReadError, WriteError, etc.
            logger.warning("Transport error fetching %s: %s", endpoint, exc)
            if isinstance(exc, httpx.ConnectError):
                self._available = False
            return None


# ---------------------------------------------------------------------------
# Mapping helpers  (JSON dicts → Pydantic models)
# ---------------------------------------------------------------------------


def _parse_spark_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse a Spark REST API timestamp string to UTC datetime."""
    if not value:
        return None
    try:
        return datetime.strptime(value, _ISO_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        # Some endpoints return epoch-ms as a string
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None


def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce a value to int, returning *default* on failure."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _map_spark_job(raw: Dict[str, Any]) -> SparkJob:
    status_str = str(raw.get("status", "UNKNOWN")).upper()
    try:
        status = JobStatus(status_str)
    except ValueError:
        status = JobStatus.UNKNOWN

    return SparkJob(
        job_id=_safe_int(raw.get("jobId")),
        name=raw.get("name", "") or raw.get("description", ""),
        status=status,
        submission_time=_parse_spark_ts(raw.get("submissionTime")),
        completion_time=_parse_spark_ts(raw.get("completionTime")),
        num_tasks=_safe_int(raw.get("numTasks")),
        num_active_tasks=_safe_int(raw.get("numActiveTasks")),
        num_completed_tasks=_safe_int(raw.get("numCompletedTasks")),
        num_failed_tasks=_safe_int(raw.get("numFailedTasks")),
        num_killed_tasks=_safe_int(raw.get("numKilledTasks")),
        killed_tasks_summary=raw.get("killedTasksSummary") or {},
        num_stages=_safe_int(raw.get("numStages")) or len(raw.get("stageIds", [])),
        num_active_stages=_safe_int(raw.get("numActiveStages")),
        num_completed_stages=_safe_int(raw.get("numCompletedStages")),
        num_failed_stages=_safe_int(raw.get("numFailedStages")),
        stage_ids=raw.get("stageIds", []),
    )


def _map_spark_stage(raw: Dict[str, Any]) -> SparkStage:
    status_str = str(raw.get("status", "PENDING")).upper()
    try:
        status = StageStatus(status_str)
    except ValueError:
        status = StageStatus.PENDING

    return SparkStage(
        stage_id=_safe_int(raw.get("stageId")),
        attempt_id=_safe_int(raw.get("attemptId")),
        name=raw.get("name", ""),
        status=status,
        num_tasks=_safe_int(raw.get("numTasks")),
        num_active_tasks=_safe_int(raw.get("numActiveTasks")),
        num_complete_tasks=_safe_int(raw.get("numCompleteTasks")),
        num_failed_tasks=_safe_int(raw.get("numFailedTasks")),
        input_bytes=_safe_int(raw.get("inputBytes")),
        input_records=_safe_int(raw.get("inputRecords")),
        output_bytes=_safe_int(raw.get("outputBytes")),
        output_records=_safe_int(raw.get("outputRecords")),
        shuffle_read_bytes=_safe_int(raw.get("shuffleReadBytes")),
        shuffle_write_bytes=_safe_int(raw.get("shuffleWriteBytes")),
        executor_run_time_ms=_safe_int(raw.get("executorRunTime")),
        executor_cpu_time_ns=_safe_int(raw.get("executorCpuTime")),
        memory_spill_bytes=_safe_int(raw.get("memoryBytesSpilled")),
        disk_spill_bytes=_safe_int(raw.get("diskBytesSpilled")),
        submission_time=_parse_spark_ts(raw.get("submissionTime")),
        completion_time=_parse_spark_ts(raw.get("completionTime")),
        failure_reason=raw.get("failureReason") or None,
        num_killed_tasks=_safe_int(raw.get("numKilledTasks")),
        killed_tasks_summary=raw.get("killedTasksSummary") or {},
        jvm_gc_time_ms=_safe_int(raw.get("jvmGCTime")),
        peak_execution_memory=_safe_int(raw.get("peakExecutionMemory")),
    )


def _map_executor(raw: Dict[str, Any]) -> ExecutorInfo:
    return ExecutorInfo(
        executor_id=str(raw.get("id", "")),
        is_active=raw.get("isActive", True),
        host_port=raw.get("hostPort", ""),
        total_cores=_safe_int(raw.get("totalCores")),
        max_tasks=_safe_int(raw.get("maxTasks")),
        active_tasks=_safe_int(raw.get("activeTasks")),
        completed_tasks=_safe_int(raw.get("completedTasks")),
        failed_tasks=_safe_int(raw.get("failedTasks")),
        total_duration_ms=_safe_int(raw.get("totalDuration")),
        total_gc_time_ms=_safe_int(raw.get("totalGCTime")),
        max_memory=_safe_int(raw.get("maxMemory")),
        memory_used=_safe_int(raw.get("memoryUsed")),
        disk_used=_safe_int(raw.get("diskUsed")),
        total_input_bytes=_safe_int(raw.get("totalInputBytes")),
        total_shuffle_read=_safe_int(raw.get("totalShuffleRead")),
        total_shuffle_write=_safe_int(raw.get("totalShuffleWrite")),
        rdd_blocks=_safe_int(raw.get("rddBlocks")),
        peak_jvm_heap=_safe_int((raw.get("peakMemoryMetrics") or {}).get("JVMHeapMemory")),
        peak_jvm_off_heap=_safe_int((raw.get("peakMemoryMetrics") or {}).get("JVMOffHeapMemory")),
        add_time=_parse_spark_ts(raw.get("addTime")),
        remove_time=_parse_spark_ts(raw.get("removeTime")),
        remove_reason=raw.get("removeReason", ""),
        is_excluded=bool(raw.get("isExcluded") or raw.get("isBlacklisted", False)),
        excluded_in_stages=raw.get("excludedInStages") or raw.get("blacklistedInStages") or [],
    )


def _map_sql(raw: Dict[str, Any]) -> SQLQuery:
    return SQLQuery(
        execution_id=_safe_int(raw.get("id")),
        status=raw.get("status", ""),
        description=raw.get("description", ""),
        submission_time=_parse_spark_ts(raw.get("submissionTime")),
        duration_ms=_safe_int(raw.get("duration")),
        running_jobs=len(raw.get("runningJobIds", [])),
        success_jobs=len(raw.get("successJobIds", [])),
        failed_job_ids=raw.get("failedJobIds") or [],
    )


def _map_rdd(raw: Dict[str, Any]) -> RDDInfo:
    return RDDInfo(
        rdd_id=_safe_int(raw.get("id")),
        name=raw.get("name", ""),
        num_partitions=_safe_int(raw.get("numPartitions")),
        num_cached_partitions=_safe_int(raw.get("numCachedPartitions")),
        storage_level=raw.get("storageLevel", ""),
        memory_used=_safe_int(raw.get("memoryUsed")),
        disk_used=_safe_int(raw.get("diskUsed")),
    )
