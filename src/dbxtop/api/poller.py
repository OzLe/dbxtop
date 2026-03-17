"""Polling orchestrator.

Manages periodic data fetching from the Databricks and Spark APIs,
supporting both fast (jobs/stages) and slow (cluster/events) refresh intervals.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, Set

from textual.app import App
from textual.message import Message
from textual.timer import Timer

from dbxtop.api.cache import DataCache
from dbxtop.api.client import DatabricksClient
from dbxtop.api.models import ClusterState
from dbxtop.api.spark_api import SparkRESTClient
from dbxtop.config import Settings

logger = logging.getLogger(__name__)

MAX_BACKOFF_MULTIPLIER: int = 4
"""Maximum backoff factor for consecutive errors on a single slot."""


class DataUpdated(Message):
    """Posted to the app when the cache receives fresh data.

    Attributes:
        updated_slots: Names of the cache slots that were updated.
    """

    def __init__(self, updated_slots: Set[str]) -> None:
        super().__init__()
        self.updated_slots = updated_slots


class MetricsPoller:
    """Two-tier async polling orchestrator.

    *Fast* polling (default 3 s) targets high-frequency Spark data:
    executors, jobs, stages.  *Slow* polling (default 15 s) targets
    cluster metadata, events, libraries, SQL, and storage.

    Args:
        dbx_client: Databricks SDK wrapper.
        spark_client: Spark REST API client (may be ``None`` if Spark is
            not yet available).
        cache: Shared data cache.
        app: The Textual ``App`` instance (used for timers and messaging).
        settings: Runtime configuration.
    """

    def __init__(
        self,
        dbx_client: DatabricksClient,
        spark_client: Optional[SparkRESTClient],
        cache: DataCache,
        app: App[Any],
        settings: Settings,
    ) -> None:
        self._dbx = dbx_client
        self._spark: Optional[SparkRESTClient] = spark_client
        self._cache = cache
        self._app = app
        self._settings = settings

        self._fast_timer: Optional[Timer] = None
        self._slow_timer: Optional[Timer] = None
        self._error_counts: dict[str, int] = {}
        self._poll_cycle: int = 0
        self._previous_state: Optional[ClusterState] = None
        self._previous_spark_context_id: str = ""
        self._poll_lock = asyncio.Lock()

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Create and start the fast and slow polling timers."""
        self._fast_timer = self._app.set_interval(self._settings.fast_poll_s, self._fast_poll, name="fast_poll")
        self._slow_timer = self._app.set_interval(self._settings.slow_poll_s, self._slow_poll, name="slow_poll")
        # Trigger an immediate slow poll so data appears on launch
        self._app.call_later(self._slow_poll)

    async def stop(self) -> None:
        """Cancel timers and close clients."""
        if self._fast_timer is not None:
            self._fast_timer.stop()
        if self._slow_timer is not None:
            self._slow_timer.stop()
        if self._spark is not None:
            await self._spark.close()

    async def set_spark_client(self, client: SparkRESTClient) -> None:
        """Attach or replace the Spark REST client at runtime.

        Closes the previous client (if any) before replacing it.

        Args:
            client: A newly initialised ``SparkRESTClient``.
        """
        if self._spark is not None:
            try:
                await self._spark.close()
            except Exception:
                logger.debug("Error closing old Spark client", exc_info=True)
        self._spark = client

    async def force_refresh(self) -> None:
        """Trigger an immediate fast *and* slow poll."""
        await self._fast_poll()
        await self._slow_poll()

    # -- fast poll (executors, jobs, stages) ---------------------------------

    async def _fast_poll(self) -> None:
        """Poll high-frequency Spark data."""
        if self._poll_lock.locked():
            return
        async with self._poll_lock:
            await self._fast_poll_inner()

    async def _fast_poll_inner(self) -> None:
        """Inner fast poll logic (called under lock)."""
        self._poll_cycle += 1
        if self._spark is None or not await self._spark.is_available():
            return

        updated: Set[str] = set()

        all_slots = ("executors", "spark_jobs", "stages")
        all_coros = (
            self._spark.get_executors,
            self._spark.get_jobs,
            self._spark.get_stages,
        )
        active_slots = []
        active_coros = []
        for slot_name, coro_fn in zip(all_slots, all_coros):
            if self._should_skip(slot_name):
                logger.debug("Skipping %s due to error backoff", slot_name)
            else:
                active_slots.append(slot_name)
                active_coros.append(coro_fn())

        if not active_coros:
            return

        results = await asyncio.gather(*active_coros, return_exceptions=True)

        for slot_name, result in zip(active_slots, results):
            if isinstance(result, BaseException):
                self._handle_error(slot_name, result)
            else:
                self._cache.update(slot_name, result)
                self._reset_backoff(slot_name)
                updated.add(slot_name)

        if updated:
            self._app.post_message(DataUpdated(updated))

    # -- slow poll (cluster, events, libraries, SQL, storage) ----------------

    async def _slow_poll(self) -> None:
        """Poll low-frequency SDK and Spark data."""
        if self._poll_lock.locked():
            return
        async with self._poll_lock:
            await self._slow_poll_inner()

    async def _slow_poll_inner(self) -> None:
        """Inner slow poll logic (called under lock)."""
        updated: Set[str] = set()

        # SDK calls (always available)
        all_sdk_slots = ("cluster", "events", "job_runs", "libraries")
        all_sdk_fns = (
            self._dbx.get_cluster,
            self._dbx.get_events,
            self._dbx.get_job_runs,
            self._dbx.get_library_status,
        )
        active_sdk_slots = []
        active_sdk_coros = []
        for slot_name, fn in zip(all_sdk_slots, all_sdk_fns):
            if self._should_skip(slot_name):
                logger.debug("Skipping %s due to error backoff", slot_name)
            else:
                active_sdk_slots.append(slot_name)
                active_sdk_coros.append(fn())

        if active_sdk_coros:
            sdk_results = await asyncio.gather(*active_sdk_coros, return_exceptions=True)
            for slot_name, result in zip(active_sdk_slots, sdk_results):
                if isinstance(result, BaseException):
                    self._handle_error(slot_name, result)
                else:
                    self._cache.update(slot_name, result)
                    self._reset_backoff(slot_name)
                    updated.add(slot_name)

        # Detect cluster state transitions and spark context changes
        cluster_slot = self._cache.get("cluster")
        if cluster_slot.data is not None:
            current_state = cluster_slot.data.state
            self._handle_state_transition(current_state)
            self._previous_state = current_state

            # Detect spark_context_id change (driver restart within RUNNING)
            ctx_id = cluster_slot.data.spark_context_id
            if ctx_id and self._previous_spark_context_id and ctx_id != self._previous_spark_context_id:
                logger.info(
                    "Spark context ID changed (%s → %s) — re-discovering app", self._previous_spark_context_id, ctx_id
                )
                if self._spark is not None:
                    self._spark._app_id = None  # noqa: SLF001
                    self._app.call_later(self._try_spark_reconnect)
            if ctx_id:
                self._previous_spark_context_id = ctx_id

        # Spark REST calls (only when available)
        if self._spark is not None and await self._spark.is_available():
            all_spark_slots = ("sql_queries", "storage")
            all_spark_fns = (self._spark.get_sql, self._spark.get_storage)
            active_spark_slots = []
            active_spark_coros = []
            for slot_name, fn in zip(all_spark_slots, all_spark_fns):
                if self._should_skip(slot_name):
                    logger.debug("Skipping %s due to error backoff", slot_name)
                else:
                    active_spark_slots.append(slot_name)
                    active_spark_coros.append(fn())

            if active_spark_coros:
                spark_results = await asyncio.gather(*active_spark_coros, return_exceptions=True)
                for slot_name, result in zip(active_spark_slots, spark_results):
                    if isinstance(result, BaseException):
                        self._handle_error(slot_name, result)
                    else:
                        self._cache.update(slot_name, result)
                        self._reset_backoff(slot_name)
                        updated.add(slot_name)

        if updated:
            self._app.post_message(DataUpdated(updated))

    # -- error / backoff handling --------------------------------------------

    def _handle_error(self, slot_name: str, exc: BaseException) -> None:
        """Record a polling error and increment the backoff counter."""
        count = self._error_counts.get(slot_name, 0) + 1
        self._error_counts[slot_name] = min(count, MAX_BACKOFF_MULTIPLIER)
        self._cache.mark_error(slot_name, str(exc))
        logger.warning("Poll error on %s (count=%d): %s", slot_name, count, exc)

    def _reset_backoff(self, slot_name: str) -> None:
        """Clear the backoff counter for a slot after a successful poll."""
        self._error_counts.pop(slot_name, None)

    def _should_skip(self, slot_name: str) -> bool:
        """Return True if the slot should be skipped due to error backoff.

        Slots with consecutive errors are polled less often: every 2^N cycles
        (up to MAX_BACKOFF_MULTIPLIER).
        """
        count = self._error_counts.get(slot_name, 0)
        if count == 0:
            return False
        skip_interval = min(2**count, 2**MAX_BACKOFF_MULTIPLIER)
        return (self._poll_cycle % skip_interval) != 0

    # -- cluster state transitions -------------------------------------------

    def _handle_state_transition(self, current: ClusterState) -> None:
        """React to cluster state changes.

        * TERMINATED/ERROR → mark all Spark-dependent slots as unavailable.
        * RUNNING after TERMINATED → attempt Spark REST re-discovery.
        """
        prev = self._previous_state
        if prev == current:
            return

        if current in (ClusterState.TERMINATED, ClusterState.ERROR):
            for slot in ("spark_jobs", "stages", "executors", "sql_queries", "storage"):
                self._cache.mark_error(slot, f"Cluster is {current.value}")
            logger.info("Cluster transitioned to %s — Spark data unavailable", current.value)

        elif current == ClusterState.RUNNING and prev in (
            ClusterState.TERMINATED,
            ClusterState.PENDING,
            None,
        ):
            if self._spark is not None:
                logger.info("Cluster now RUNNING — attempting Spark app discovery")
                self._app.call_later(self._try_spark_reconnect)

    async def _try_spark_reconnect(self) -> None:
        """Attempt to re-discover the Spark application after a cluster restart."""
        if self._spark is None:
            return
        try:
            await self._spark.discover_app_id()
            logger.info("Spark app re-discovered successfully")
        except RuntimeError:
            logger.info("Spark app not yet available — will retry on next poll")
