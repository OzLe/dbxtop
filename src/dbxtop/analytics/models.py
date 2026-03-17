"""Pydantic data models for the analytics engine.

Typed models for performance insights, health scores, recommendations,
and diagnostic reports produced by the analytics engine.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Dict, List

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Severity(str, enum.Enum):
    """Insight severity level."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class InsightCategory(str, enum.Enum):
    """Classification of performance insights."""

    GC = "GC"
    SPILL = "SPILL"
    SKEW = "SKEW"
    SHUFFLE = "SHUFFLE"
    UTILIZATION = "UTILIZATION"
    PARTITION = "PARTITION"
    STRAGGLER = "STRAGGLER"
    TASK_FAILURE = "TASK_FAILURE"
    MEMORY = "MEMORY"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Insight(BaseModel):
    """A single performance insight detected by the analytics engine."""

    id: str
    """Unique identifier, format: '{category.value}_{sequence}' e.g. 'GC_001'."""

    category: InsightCategory
    severity: Severity

    title: str
    """Short title, max 60 chars."""

    description: str
    """Plain-English explanation of what was detected, 1-3 sentences."""

    metric_value: float
    """The actual observed metric value that triggered this insight."""

    threshold_value: float
    """The threshold that was exceeded."""

    recommendation: str
    """Actionable fix suggestion in plain English."""

    affected_entity: str = ""
    """Identifier of the affected entity (executor ID, stage ID, etc.), or empty for cluster-wide."""


class HealthScore(BaseModel):
    """Overall cluster health score with component breakdown."""

    score: int
    """Aggregate health score, 0-100 where 100 = perfectly healthy."""

    label: str
    """Human-readable label: 'Excellent', 'Good', 'Fair', 'Poor', 'Critical', or 'N/A'."""

    color: str
    """Rich markup color string for the score display."""

    component_scores: Dict[str, int] = Field(default_factory=dict)
    """Per-component scores (0-100). Keys: 'gc', 'spill', 'skew', 'utilization', 'shuffle', 'task_failures'."""


class Recommendation(BaseModel):
    """An actionable recommendation derived from one or more insights."""

    title: str
    """Short title, max 80 chars."""

    description: str
    """Detailed suggestion with specific Spark config keys and values."""

    priority: int
    """1 = highest priority, 5 = lowest. Derived from severity of contributing insights."""

    category: InsightCategory
    """Primary category this recommendation addresses."""


class DiagnosticReport(BaseModel):
    """Complete diagnostic report produced by the analytics engine."""

    health: HealthScore
    insights: List[Insight] = Field(default_factory=list)
    recommendations: List[Recommendation] = Field(default_factory=list)
    timestamp: datetime
    """UTC timestamp when this report was generated."""

    executor_count: int = 0
    """Number of active executors analyzed."""

    active_stage_count: int = 0
    """Number of active stages analyzed."""

    active_job_count: int = 0
    """Number of active/running jobs analyzed."""
