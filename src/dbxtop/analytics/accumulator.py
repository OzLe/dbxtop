"""Insight accumulator for tracking insights over time.

Tracks insights across multiple analytics cycles, recording first/last
seen times, occurrence counts, peak severity, and resolution status.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from dbxtop.analytics.models import Insight, Severity


@dataclass
class AccumulatedInsight:
    """An insight tracked over multiple analytics cycles."""

    insight: Insight
    """The most recent version of the insight."""

    first_seen: datetime
    """When this insight was first detected."""

    last_seen: datetime
    """When this insight was most recently detected."""

    resolved_at: Optional[datetime] = None
    """When this insight was no longer detected (None if still active)."""

    occurrence_count: int = 1
    """Number of analytics cycles where this insight was present."""

    peak_severity: Severity = Severity.INFO
    """Highest severity observed for this insight across all cycles."""

    peak_metric_value: float = 0.0
    """Highest metric value observed for this insight across all cycles."""


# Severity ordering for comparison
_SEVERITY_ORDER: Dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.CRITICAL: 2,
}


class InsightAccumulator:
    """Accumulates insights across multiple analytics cycles.

    Tracks which insights are active, when they first appeared, how many
    times they've been seen, and their peak severity and metric values.
    Insights that disappear from a cycle are marked as resolved.
    """

    def __init__(self) -> None:
        self._accumulated: Dict[str, AccumulatedInsight] = {}

    def update(self, insights: List[Insight], now: datetime) -> None:
        """Update accumulator with insights from the latest analytics cycle.

        Args:
            insights: Insights from the current analytics cycle.
            now: Current timestamp.
        """
        current_ids = {i.id for i in insights}

        # Update or create entries for current insights
        for insight in insights:
            if insight.id in self._accumulated:
                acc = self._accumulated[insight.id]
                acc.insight = insight
                acc.last_seen = now
                acc.occurrence_count += 1
                acc.resolved_at = None  # Re-activated if previously resolved
                if _SEVERITY_ORDER.get(insight.severity, 0) > _SEVERITY_ORDER.get(acc.peak_severity, 0):
                    acc.peak_severity = insight.severity
                if insight.metric_value > acc.peak_metric_value:
                    acc.peak_metric_value = insight.metric_value
            else:
                self._accumulated[insight.id] = AccumulatedInsight(
                    insight=insight,
                    first_seen=now,
                    last_seen=now,
                    peak_severity=insight.severity,
                    peak_metric_value=insight.metric_value,
                )

        # Mark missing insights as resolved
        for iid, acc in self._accumulated.items():
            if iid not in current_ids and acc.resolved_at is None:
                acc.resolved_at = now

    def get_all(self) -> List[AccumulatedInsight]:
        """Return all accumulated insights (active and resolved).

        Returns:
            List of all accumulated insights, sorted by last_seen descending.
        """
        return sorted(self._accumulated.values(), key=lambda a: a.last_seen, reverse=True)

    def get_active(self) -> List[AccumulatedInsight]:
        """Return only active (unresolved) accumulated insights.

        Returns:
            List of active accumulated insights, sorted by last_seen descending.
        """
        return sorted(
            (a for a in self._accumulated.values() if a.resolved_at is None),
            key=lambda a: a.last_seen,
            reverse=True,
        )
