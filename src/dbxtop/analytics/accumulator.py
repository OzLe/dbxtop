"""Sticky insight accumulator for the analytics engine.

Persists insights across refresh cycles so that transient performance issues
remain visible even after they resolve.  Each insight is keyed by
``(category.value, affected_entity, title)`` and tracks first-seen /
last-seen timestamps, peak severity, occurrence count, and resolution state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dbxtop.analytics.models import Insight, Severity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _severity_rank(severity: Severity) -> int:
    """Return a numeric rank for *severity* where lower = more severe.

    Args:
        severity: The severity level to rank.

    Returns:
        An integer rank (0 = CRITICAL, 1 = WARNING, 2 = INFO, 99 = unknown).
    """
    return {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}.get(severity, 99)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class AccumulatedInsight:
    """A single insight enriched with temporal and statistical metadata.

    Wraps the most recent :class:`Insight` object and adds tracking fields
    that survive across polling cycles.

    Attributes:
        insight: Most recent ``Insight`` snapshot for this key.
        first_seen: UTC timestamp of the first detection.
        last_seen: UTC timestamp of the most recent detection.
        resolved_at: UTC timestamp when the insight was no longer detected,
            or ``None`` if still active.
        peak_severity: Highest severity observed across all occurrences.
        occurrence_count: Number of polling cycles where this insight was
            detected.
        peak_metric_value: Highest ``metric_value`` observed across all
            occurrences.
    """

    insight: Insight
    first_seen: datetime
    last_seen: datetime
    resolved_at: Optional[datetime] = None
    peak_severity: Severity = Severity.INFO
    occurrence_count: int = 1
    peak_metric_value: float = 0.0

    @property
    def is_resolved(self) -> bool:
        """Return ``True`` if this insight is no longer actively detected."""
        return self.resolved_at is not None

    @property
    def duration_seconds(self) -> float:
        """Return elapsed seconds from *first_seen* to *last_seen*."""
        return (self.last_seen - self.first_seen).total_seconds()


# ---------------------------------------------------------------------------
# Accumulator
# ---------------------------------------------------------------------------

# Type alias for the compound key used to identify unique insights.
_InsightKey = Tuple[str, str, str]


class InsightAccumulator:
    """Accumulates insights across polling cycles.

    Each unique insight is identified by the compound key
    ``(category.value, affected_entity, title)``.  On every :meth:`update` call the
    accumulator upserts current insights, marks absent ones as resolved, and
    re-activates previously resolved insights that re-emerge.

    Example::

        acc = InsightAccumulator()
        acc.update(engine.analyze(cache), datetime.now(timezone.utc))
        for ai in acc.get_all():
            print(ai.insight.title, ai.occurrence_count)
    """

    def __init__(self) -> None:
        self._insights: Dict[_InsightKey, AccumulatedInsight] = {}

    # -- public API ---------------------------------------------------------

    def update(self, current_insights: List[Insight], timestamp: datetime) -> None:
        """Merge a fresh batch of insights into the accumulator.

        Args:
            current_insights: Insights produced by the latest engine run.
            timestamp: UTC timestamp of the current polling cycle.
        """
        current_keys: set[_InsightKey] = set()

        for insight in current_insights:
            key: _InsightKey = (insight.category.value, insight.affected_entity, insight.title)
            current_keys.add(key)

            existing = self._insights.get(key)
            if existing is None:
                # Brand-new insight.
                self._insights[key] = AccumulatedInsight(
                    insight=insight,
                    first_seen=timestamp,
                    last_seen=timestamp,
                    resolved_at=None,
                    peak_severity=insight.severity,
                    occurrence_count=1,
                    peak_metric_value=insight.metric_value,
                )
            else:
                # Update existing entry.
                existing.insight = insight
                existing.last_seen = timestamp
                existing.occurrence_count += 1

                # Track peak severity (lower rank = higher severity).
                if _severity_rank(insight.severity) < _severity_rank(existing.peak_severity):
                    existing.peak_severity = insight.severity

                # Track peak metric value.
                if insight.metric_value > existing.peak_metric_value:
                    existing.peak_metric_value = insight.metric_value

                # Re-emerged insight: clear resolved_at.
                if existing.resolved_at is not None:
                    existing.resolved_at = None

        # Mark absent insights as resolved.
        for key, accumulated in self._insights.items():
            if key not in current_keys and accumulated.resolved_at is None:
                accumulated.resolved_at = timestamp

    def get_all(self) -> List[AccumulatedInsight]:
        """Return all accumulated insights, active first then resolved.

        Active insights are sorted by severity (most severe first).
        Resolved insights are sorted by ``resolved_at`` descending (most
        recently resolved first).

        Returns:
            A new list of :class:`AccumulatedInsight` instances.
        """
        active: List[AccumulatedInsight] = []
        resolved: List[AccumulatedInsight] = []

        for accumulated in self._insights.values():
            if accumulated.is_resolved:
                resolved.append(accumulated)
            else:
                active.append(accumulated)

        active.sort(key=lambda a: _severity_rank(a.peak_severity))
        resolved.sort(key=lambda a: a.resolved_at or a.last_seen, reverse=True)

        return active + resolved

    def reset(self) -> None:
        """Remove all accumulated insights."""
        self._insights.clear()

    # -- properties ---------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Return the number of currently active (unresolved) insights."""
        return sum(1 for a in self._insights.values() if not a.is_resolved)

    @property
    def resolved_count(self) -> int:
        """Return the number of resolved insights."""
        return sum(1 for a in self._insights.values() if a.is_resolved)
