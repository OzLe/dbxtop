"""Data cache layer.

Provides in-memory caching and time-series history for polled data,
enabling sparkline rendering and stale-data detection.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Generic, List, Optional, TypeVar

from dbxtop.api.models import (
    ClusterEvent,
    ClusterInfo,
    ExecutorInfo,
    JobRun,
    LibraryInfo,
    RDDInfo,
    SQLQuery,
    SparkJob,
    SparkStage,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

MAX_RING_BUFFER_SIZE: int = 60
"""Number of historical samples retained per cache slot (≈3 min at 3 s poll)."""


@dataclass
class CacheSlot(Generic[T]):
    """A single cached data slot with staleness tracking and history ring buffer.

    Attributes:
        data: The most recent data value, or ``None`` if not yet populated.
        last_updated: UTC timestamp of the last successful update.
        error: Error message from the most recent failed poll, or ``None``.
        stale: ``True`` when the slot has experienced an error but retains old data.
        history: Ring buffer of recent data values for sparkline rendering.
    """

    data: Optional[T] = None
    last_updated: Optional[datetime] = None
    error: Optional[str] = None
    stale: bool = False
    history: Deque[T] = field(default_factory=lambda: deque(maxlen=MAX_RING_BUFFER_SIZE))


class DataCache:
    """Central data cache shared between the poller and views.

    The poller writes via :meth:`update` / :meth:`mark_error`, and views
    read via :meth:`get` / :meth:`get_history`.
    """

    def __init__(self) -> None:
        self._slots: Dict[str, CacheSlot[Any]] = {
            "cluster": CacheSlot[ClusterInfo](),
            "events": CacheSlot[List[ClusterEvent]](),
            "spark_jobs": CacheSlot[List[SparkJob]](),
            "stages": CacheSlot[List[SparkStage]](),
            "executors": CacheSlot[List[ExecutorInfo]](),
            "sql_queries": CacheSlot[List[SQLQuery]](),
            "storage": CacheSlot[List[RDDInfo]](),
            "job_runs": CacheSlot[List[JobRun]](),
            "libraries": CacheSlot[List[LibraryInfo]](),
        }

    # -- write API -----------------------------------------------------------

    def update(self, slot_name: str, data: Any) -> None:
        """Store fresh data in a cache slot.

        Clears any previous error/stale flags and appends to the ring buffer.

        Args:
            slot_name: Name of the slot (e.g. ``'cluster'``, ``'executors'``).
            data: The new data value.

        Raises:
            KeyError: If *slot_name* is not a recognised slot.
        """
        slot = self._slots[slot_name]
        slot.data = data
        slot.last_updated = datetime.now(timezone.utc)
        slot.error = None
        slot.stale = False
        slot.history.append(data)

    def mark_error(self, slot_name: str, error_msg: str) -> None:
        """Record a polling error on a slot without discarding existing data.

        Args:
            slot_name: Name of the slot.
            error_msg: Human-readable error description.
        """
        if slot_name not in self._slots:
            logger.warning("mark_error called for unknown slot '%s'", slot_name)
            return
        slot = self._slots[slot_name]
        slot.error = error_msg
        slot.stale = True

    # -- read API ------------------------------------------------------------

    def get(self, slot_name: str) -> CacheSlot[Any]:
        """Return the ``CacheSlot`` for the given name.

        Args:
            slot_name: Name of the slot.

        Raises:
            KeyError: If *slot_name* is not a recognised slot.
        """
        return self._slots[slot_name]

    def is_stale(self, slot_name: str, expected_interval: float) -> bool:
        """Check whether a slot's data is stale.

        A slot is considered stale if it has an explicit error flag **or** if
        its ``last_updated`` timestamp is more than ``2 × expected_interval``
        seconds in the past.

        Args:
            slot_name: Name of the slot.
            expected_interval: Expected polling interval in seconds.

        Returns:
            ``True`` if the data should be treated as stale.
        """
        slot = self._slots[slot_name]
        if slot.stale:
            return True
        if slot.last_updated is None:
            return True
        age = (datetime.now(timezone.utc) - slot.last_updated).total_seconds()
        return age > 2 * expected_interval

    def get_history(self, slot_name: str) -> Deque[Any]:
        """Return the ring buffer of historical values for a slot.

        Args:
            slot_name: Name of the slot.
        """
        return self._slots[slot_name].history

    @property
    def slot_names(self) -> List[str]:
        """All recognised slot names."""
        return list(self._slots.keys())
