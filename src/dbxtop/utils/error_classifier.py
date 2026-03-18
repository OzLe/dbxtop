"""Error classifier for Spark error patterns.

Regex-based classification of error messages, executor removal reasons,
and metric-based detection of data skew and GC pressure.
"""

from __future__ import annotations

import enum
import re
from typing import List, Tuple


class ErrorCategory(str, enum.Enum):
    """Broad classification of a Spark error or performance anti-pattern."""

    OOM = "OOM"
    SHUFFLE_FETCH = "SHUFFLE_FETCH"
    SERIALIZATION = "SERIALIZATION"
    TIMEOUT = "TIMEOUT"
    CONTAINER_KILLED = "CONTAINER_KILLED"
    DATA_SKEW = "DATA_SKEW"
    GC_PRESSURE = "GC_PRESSURE"
    DISK_SPILL = "DISK_SPILL"
    UNKNOWN = "UNKNOWN"


# -- regex patterns (compiled once) -----------------------------------------

_ERROR_PATTERNS: List[Tuple[re.Pattern[str], ErrorCategory]] = [
    (re.compile(r"OutOfMemoryError|Java heap space|java\.lang\.OutOfMemoryError", re.IGNORECASE), ErrorCategory.OOM),
    (re.compile(r"GC overhead limit exceeded", re.IGNORECASE), ErrorCategory.OOM),
    (
        re.compile(r"Container killed by YARN|Container was killed|killed by external signal", re.IGNORECASE),
        ErrorCategory.CONTAINER_KILLED,
    ),
    (
        re.compile(r"beyond the .* memory limit|exceeding memory limits|Cannot allocate memory", re.IGNORECASE),
        ErrorCategory.CONTAINER_KILLED,
    ),
    (
        re.compile(r"FetchFailed|FetchFailedException|shuffle fetch|Failed to connect", re.IGNORECASE),
        ErrorCategory.SHUFFLE_FETCH,
    ),
    (
        re.compile(r"Connection refused|Connection reset|connection was reset", re.IGNORECASE),
        ErrorCategory.SHUFFLE_FETCH,
    ),
    (
        re.compile(r"NotSerializableException|InvalidClassException|serializ", re.IGNORECASE),
        ErrorCategory.SERIALIZATION,
    ),
    (
        re.compile(r"TimeoutException|ReadTimeoutException|ConnectionTimeoutException", re.IGNORECASE),
        ErrorCategory.TIMEOUT,
    ),
    (re.compile(r"heartbeat.*timed out|Executor heartbeat timed out", re.IGNORECASE), ErrorCategory.TIMEOUT),
]

_REMOVAL_PATTERNS: List[Tuple[re.Pattern[str], ErrorCategory]] = [
    (
        re.compile(r"Container killed by YARN|Container was killed|killed by external signal", re.IGNORECASE),
        ErrorCategory.CONTAINER_KILLED,
    ),
    (re.compile(r"OutOfMemory|memory limit|exceeding memory", re.IGNORECASE), ErrorCategory.OOM),
    (re.compile(r"heartbeat.*timed out|lost|Executor heartbeat", re.IGNORECASE), ErrorCategory.TIMEOUT),
    (re.compile(r"FetchFailed|shuffle", re.IGNORECASE), ErrorCategory.SHUFFLE_FETCH),
]


# -- public API -------------------------------------------------------------


def classify_error(error_message: str) -> ErrorCategory:
    """Classify a Spark error message into a category.

    Args:
        error_message: The error text (e.g. task errorMessage, stage failureReason).

    Returns:
        The matched ``ErrorCategory``, or ``UNKNOWN`` if no pattern matches.
    """
    if not error_message:
        return ErrorCategory.UNKNOWN
    for pattern, category in _ERROR_PATTERNS:
        if pattern.search(error_message):
            return category
    return ErrorCategory.UNKNOWN


def classify_executor_removal(remove_reason: str) -> ErrorCategory:
    """Classify an executor removal reason.

    Args:
        remove_reason: The removeReason string from the Spark REST API.

    Returns:
        The matched ``ErrorCategory``, or ``UNKNOWN`` if no pattern matches.
    """
    if not remove_reason:
        return ErrorCategory.UNKNOWN
    for pattern, category in _REMOVAL_PATTERNS:
        if pattern.search(remove_reason):
            return category
    return ErrorCategory.UNKNOWN


def detect_skew(p50: float, max_val: float, threshold: float = 10.0) -> bool:
    """Detect data skew from quantile values.

    Args:
        p50: The median (50th percentile) value.
        max_val: The maximum (100th percentile) value.
        threshold: The max/median ratio above which skew is flagged.

    Returns:
        ``True`` if the max/median ratio exceeds the threshold.
    """
    if p50 <= 0:
        return max_val > 0
    return (max_val / p50) >= threshold


def detect_gc_pressure(gc_time_ms: int, run_time_ms: int, threshold: float = 0.10) -> bool:
    """Detect GC pressure from timing metrics.

    Args:
        gc_time_ms: Total JVM GC time in milliseconds.
        run_time_ms: Total executor run time in milliseconds.
        threshold: The GC/run ratio above which pressure is flagged.

    Returns:
        ``True`` if GC time exceeds the threshold fraction of run time.
    """
    if run_time_ms <= 0:
        return False
    return (gc_time_ms / run_time_ms) >= threshold
