"""Run session models for persistent analytics monitoring.

Pydantic models for user-managed monitoring sessions that capture
diagnostic data, accumulated insights, and cluster configuration
over time.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, computed_field

from dbxtop.analytics.models import DiagnosticReport


class AccumulatedInsightExport(BaseModel):
    """Serializable representation of an AccumulatedInsight for persistence."""

    category: str
    severity: str
    peak_severity: str
    title: str
    description: str
    recommendation: str
    affected_entity: str = ""
    first_seen: datetime
    last_seen: datetime
    resolved_at: Optional[datetime] = None
    occurrence_count: int = 1
    peak_metric_value: float = 0.0
    metric_value: float = 0.0
    threshold_value: float = 0.0


class RunSession(BaseModel):
    """A user-managed monitoring session with persistent state."""

    run_id: str
    name: str
    cluster_id: str
    started_at: datetime
    stopped_at: Optional[datetime] = None
    snapshots: List[DiagnosticReport] = Field(default_factory=list)
    accumulated_insights: List[AccumulatedInsightExport] = Field(default_factory=list)
    health_history: List[Tuple[datetime, int]] = Field(default_factory=list)
    config_snapshot: Dict[str, str] = Field(default_factory=dict)
    executor_count_history: List[Tuple[datetime, int]] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def duration_seconds(self) -> float:
        """Elapsed time in seconds since the run started."""
        end = self.stopped_at or datetime.now(timezone.utc)
        return (end - self.started_at).total_seconds()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_active(self) -> bool:
        """Whether the run is still recording."""
        return self.stopped_at is None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def avg_health_score(self) -> float:
        """Average health score across all recorded cycles."""
        if not self.health_history:
            return 0.0
        return sum(s for _, s in self.health_history) / len(self.health_history)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def worst_health_score(self) -> int:
        """Lowest health score observed during the run."""
        if not self.health_history:
            return 0
        return min(s for _, s in self.health_history)
