"""Databricks API client wrapper.

Wraps the databricks-sdk WorkspaceClient to provide cluster info,
event logs, and Spark UI proxy access for a target cluster.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import (
    ClusterDetails,
    ClusterEvent as SdkClusterEvent,
    Library,
    LibraryFullStatus,
)
from databricks.sdk.service.jobs import BaseRun

from dbxtop.api.models import (
    ClusterEvent,
    ClusterInfo,
    ClusterState,
    JobRun,
    LibraryInfo,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class AuthenticationError(Exception):
    """Raised when Databricks authentication fails (401/403)."""


class ClusterNotFoundError(Exception):
    """Raised when the target cluster does not exist."""


class DatabricksConnectionError(Exception):
    """Raised when the Databricks workspace is unreachable."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class DatabricksClient:
    """Async-friendly wrapper around the Databricks SDK WorkspaceClient.

    All public methods are async and use ``asyncio.to_thread`` internally
    because the SDK is synchronous.

    Args:
        profile: Databricks CLI profile name.
        cluster_id: Target cluster ID.
    """

    def __init__(self, profile: str, cluster_id: str) -> None:
        self._profile = profile
        self._cluster_id = cluster_id
        self._workspace = WorkspaceClient(profile=profile)
        self._org_id: Optional[str] = None

    # -- public async methods ------------------------------------------------

    async def get_cluster(self) -> ClusterInfo:
        """Fetch current cluster state and configuration.

        Returns:
            A ``ClusterInfo`` model with normalised fields.

        Raises:
            AuthenticationError: On 401/403.
            ClusterNotFoundError: If the cluster ID is invalid.
            ConnectionError: On network / timeout errors.
        """
        raw = await self._call(self._workspace.clusters.get, self._cluster_id)
        return _map_cluster(raw)

    async def get_events(self, since: Optional[datetime] = None) -> List[ClusterEvent]:
        """Fetch lifecycle events for the cluster.

        Args:
            since: Only return events after this timestamp (optional).

        Returns:
            Up to 50 most-recent events, newest first.
        """
        kwargs: Dict[str, Any] = {"cluster_id": self._cluster_id}
        if since is not None:
            kwargs["start_time"] = int(since.timestamp() * 1000)

        def _fetch_events() -> List[ClusterEvent]:
            raw_iter = self._workspace.clusters.events(**kwargs)
            events: List[ClusterEvent] = []
            for raw_event in raw_iter:
                events.append(_map_event(raw_event))
                if len(events) >= 50:
                    break
            return events

        return await self._call(_fetch_events)

    async def get_job_runs(self, active_only: bool = True) -> List[JobRun]:
        """Fetch Databricks job runs.

        Args:
            active_only: If True, only return active runs.

        Returns:
            Up to 20 job runs.
        """

        def _fetch_runs() -> List[JobRun]:
            raw_iter = self._workspace.jobs.list_runs(active_only=active_only)
            runs: List[JobRun] = []
            for raw_run in raw_iter:
                runs.append(_map_job_run(raw_run))
                if len(runs) >= 20:
                    break
            return runs

        return await self._call(_fetch_runs)

    async def get_library_status(self) -> List[LibraryInfo]:
        """Fetch installed library statuses for the cluster.

        Returns:
            List of library info models.
        """

        def _fetch_libs() -> List[LibraryInfo]:
            result = self._workspace.libraries.cluster_status(cluster_id=self._cluster_id)
            if result is None:
                return []
            # SDK returns ClusterLibraryStatuses — extract .library_statuses
            statuses = getattr(result, "library_statuses", None) or []
            return [_map_library(lib) for lib in statuses]

        return await self._call(_fetch_libs)

    async def get_workspace_url(self) -> str:
        """Return the workspace host URL (e.g. ``https://adb-123.1.azuredatabricks.net``)."""
        host = self._workspace.config.host
        return (host or "").rstrip("/")

    async def get_org_id(self) -> str:
        """Discover the Databricks organisation (workspace) ID.

        Tries three strategies:
        1. Extract from Azure-style URL: ``adb-{org_id}.{N}.azuredatabricks.net``
        2. Read ``x-databricks-org-id`` response header from a lightweight API call
        3. Fall back to empty string

        The result is cached after first successful resolution.
        """
        if self._org_id is not None:
            return self._org_id

        host = self._workspace.config.host or ""

        # Strategy 1: Azure URL pattern
        match = re.search(r"adb-(\d+)\.", host)
        if match:
            self._org_id = match.group(1)
            return self._org_id

        # Strategy 2: x-databricks-org-id response header (works for AWS/GCP)
        try:
            import httpx

            headers = self._workspace.config.authenticate()
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as http:
                resp = await http.get(
                    f"{host.rstrip('/')}/api/2.0/workspace/get-status",
                    params={"path": "/"},
                    headers=headers,
                )
                org = resp.headers.get("x-databricks-org-id", "")
                if org:
                    self._org_id = org
                    return self._org_id
        except Exception:
            logger.warning("org_id discovery via HTTP header failed", exc_info=True)

        self._org_id = ""
        return self._org_id

    def get_token(self) -> str:
        """Return the current authentication token (synchronous).

        Supports both PAT tokens and OAuth (via ``authenticate()`` headers).
        Used by ``SparkRESTClient`` for bearer-token auth.
        """
        # Try direct token first (PAT)
        if self._workspace.config.token:
            return self._workspace.config.token
        # Fall back to authenticate() which handles OAuth/U2M/etc.
        try:
            headers = self._workspace.config.authenticate()
            auth_header = headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                return auth_header[7:]
        except Exception:
            logger.warning("Token retrieval via authenticate() failed", exc_info=True)
        return ""

    # -- private helpers -----------------------------------------------------

    async def _call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a synchronous SDK call on a thread and translate errors."""
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except NotFound as exc:
            raise ClusterNotFoundError(f"Cluster {self._cluster_id} not found: {exc}") from exc
        except Exception as exc:
            # Check for specific SDK auth exceptions first
            exc_type_name = type(exc).__name__
            if exc_type_name in ("Unauthenticated", "PermissionDenied"):
                raise AuthenticationError(f"Authentication failed for profile '{self._profile}': {exc}") from exc
            # Fallback: check error message for auth indicators
            msg = str(exc).lower()
            if "401" in msg or "403" in msg or "unauthorized" in msg or "forbidden" in msg:
                raise AuthenticationError(f"Authentication failed for profile '{self._profile}': {exc}") from exc
            if "timeout" in msg or "connect" in msg:
                raise DatabricksConnectionError(f"Could not reach Databricks workspace: {exc}") from exc
            raise


# ---------------------------------------------------------------------------
# Mapping helpers  (SDK objects → Pydantic models)
# ---------------------------------------------------------------------------


def _ms_to_utc(ms: Optional[int]) -> Optional[datetime]:
    """Convert millisecond epoch timestamp to UTC datetime."""
    if ms is None or ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _safe_str(value: Any) -> str:
    """Coerce a value to str, returning '' for None."""
    if value is None:
        return ""
    return str(value)


def _map_cluster(raw: ClusterDetails) -> ClusterInfo:
    """Map an SDK ``ClusterDetails`` to our ``ClusterInfo`` model."""
    raw_state = getattr(raw, "state", None)
    # SDK returns an enum (e.g. State.RUNNING) — use .name to get "RUNNING"
    state_str = getattr(raw_state, "name", _safe_str(raw_state)).upper()
    try:
        state = ClusterState(state_str)
    except ValueError:
        state = ClusterState.UNKNOWN

    autoscale = getattr(raw, "autoscale", None)
    spark_conf = dict(raw.spark_conf) if raw.spark_conf else {}
    tags = dict(raw.custom_tags) if raw.custom_tags else {}

    return ClusterInfo(
        cluster_id=raw.cluster_id or "",
        cluster_name=raw.cluster_name or "",
        state=state,
        state_message=_safe_str(getattr(raw, "state_message", "")),
        start_time=_ms_to_utc(getattr(raw, "start_time", None)),
        driver_node_type=_safe_str(getattr(raw, "driver_node_type_id", "")),
        worker_node_type=_safe_str(getattr(raw, "node_type_id", "")),
        num_workers=getattr(raw, "num_workers", 0) or 0,
        autoscale_min=getattr(autoscale, "min_workers", None) if autoscale else None,
        autoscale_max=getattr(autoscale, "max_workers", None) if autoscale else None,
        total_cores=getattr(raw, "cluster_cores", 0) or 0,
        total_memory_mb=getattr(raw, "cluster_memory_mb", 0) or 0,
        spark_version=_safe_str(getattr(raw, "spark_version", "")),
        data_security_mode=_safe_str(getattr(raw, "data_security_mode", "")),
        runtime_engine=_safe_str(getattr(raw, "runtime_engine", "")),
        creator=_safe_str(getattr(raw, "creator_user_name", "")),
        autotermination_minutes=getattr(raw, "autotermination_minutes", 0) or 0,
        spark_conf=spark_conf,
        tags=tags,
        spark_context_id=_safe_str(getattr(raw, "spark_context_id", "")),
    )


def _map_event(raw: SdkClusterEvent) -> ClusterEvent:
    """Map an SDK ``ClusterEvent`` to our ``ClusterEvent`` model."""
    ts = _ms_to_utc(getattr(raw, "timestamp", None))
    details: Dict[str, Any] = {}
    raw_details = getattr(raw, "details", None)
    if raw_details is not None:
        try:
            details = {k: v for k, v in raw_details.as_dict().items() if v is not None}
        except (AttributeError, TypeError):
            pass

    return ClusterEvent(
        timestamp=ts or datetime.now(timezone.utc),
        event_type=_safe_str(getattr(raw, "type", "")),
        message=_safe_str(getattr(raw_details, "reason", "")),
        details=details,
    )


def _map_job_run(raw: BaseRun) -> JobRun:
    """Map an SDK ``BaseRun`` to our ``JobRun`` model."""
    state_obj = getattr(raw, "state", None)
    return JobRun(
        run_id=raw.run_id or 0,
        job_id=getattr(raw, "job_id", 0) or 0,
        run_name=_safe_str(getattr(raw, "run_name", "")),
        state=_safe_str(getattr(state_obj, "life_cycle_state", "")),
        result_state=_safe_str(getattr(state_obj, "result_state", "")),
        start_time=_ms_to_utc(getattr(raw, "start_time", None)),
        end_time=_ms_to_utc(getattr(raw, "end_time", None)),
        setup_duration_ms=getattr(raw, "setup_duration", 0) or 0,
        execution_duration_ms=getattr(raw, "execution_duration", 0) or 0,
        creator=_safe_str(getattr(raw, "creator_user_name", "")),
        run_type=_safe_str(getattr(raw, "run_type", "")),
        task_count=len(getattr(raw, "tasks", None) or []),
    )


def _map_library(raw: LibraryFullStatus) -> LibraryInfo:
    """Map an SDK ``LibraryFullStatus`` to our ``LibraryInfo`` model."""
    lib: Optional[Library] = getattr(raw, "library", None)
    name = ""
    lib_type = ""
    if lib is not None:
        # Library has one non-None attribute: pypi, maven, jar, egg, etc.
        for attr in ("pypi", "maven", "jar", "egg", "whl", "cran", "requirements"):
            val = getattr(lib, attr, None)
            if val is not None:
                lib_type = attr
                # pypi/maven have a .package attribute; jar/egg/whl are strings
                name = _safe_str(getattr(val, "package", val))
                break

    return LibraryInfo(
        name=name,
        library_type=lib_type,
        status=_safe_str(getattr(raw, "status", "")),
        messages=list(raw.messages) if raw.messages else [],
    )
