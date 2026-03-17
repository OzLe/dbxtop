"""Unit tests for the InsightAccumulator."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dbxtop.analytics.accumulator import AccumulatedInsight, InsightAccumulator, _severity_rank
from dbxtop.analytics.models import Insight, InsightCategory, Severity


def _make_insight(
    category: InsightCategory = InsightCategory.GC,
    severity: Severity = Severity.WARNING,
    affected_entity: str = "",
    metric_value: float = 10.0,
    title: str = "Test insight",
) -> Insight:
    """Create a test Insight with defaults."""
    return Insight(
        id=f"{category.value}_001",
        category=category,
        severity=severity,
        title=title,
        description="Test description",
        metric_value=metric_value,
        threshold_value=5.0,
        recommendation="Test recommendation",
        affected_entity=affected_entity,
    )


class TestSeverityRank:
    def test_critical_is_lowest(self) -> None:
        assert _severity_rank(Severity.CRITICAL) == 0

    def test_warning_is_middle(self) -> None:
        assert _severity_rank(Severity.WARNING) == 1

    def test_info_is_highest(self) -> None:
        assert _severity_rank(Severity.INFO) == 2


class TestAccumulatedInsight:
    def test_is_resolved_false_when_no_resolved_at(self) -> None:
        acc = AccumulatedInsight(
            insight=_make_insight(),
            first_seen=datetime.now(timezone.utc),
            last_seen=datetime.now(timezone.utc),
            resolved_at=None,
            peak_severity=Severity.WARNING,
            occurrence_count=1,
            peak_metric_value=10.0,
        )
        assert not acc.is_resolved

    def test_is_resolved_true_when_resolved_at_set(self) -> None:
        acc = AccumulatedInsight(
            insight=_make_insight(),
            first_seen=datetime.now(timezone.utc),
            last_seen=datetime.now(timezone.utc),
            resolved_at=datetime.now(timezone.utc),
            peak_severity=Severity.WARNING,
            occurrence_count=1,
            peak_metric_value=10.0,
        )
        assert acc.is_resolved

    def test_duration_seconds_active(self) -> None:
        start = datetime.now(timezone.utc) - timedelta(seconds=60)
        acc = AccumulatedInsight(
            insight=_make_insight(),
            first_seen=start,
            last_seen=datetime.now(timezone.utc),
            resolved_at=None,
            peak_severity=Severity.WARNING,
            occurrence_count=1,
            peak_metric_value=10.0,
        )
        assert acc.duration_seconds >= 59

    def test_duration_seconds_resolved(self) -> None:
        start = datetime.now(timezone.utc) - timedelta(seconds=120)
        end = datetime.now(timezone.utc) - timedelta(seconds=60)
        acc = AccumulatedInsight(
            insight=_make_insight(),
            first_seen=start,
            last_seen=end,
            resolved_at=end,
            peak_severity=Severity.WARNING,
            occurrence_count=1,
            peak_metric_value=10.0,
        )
        assert 59 <= acc.duration_seconds <= 61


class TestInsightAccumulator:
    def test_empty_update(self) -> None:
        acc = InsightAccumulator()
        acc.update([], datetime.now(timezone.utc))
        assert acc.get_all() == []
        assert acc.active_count == 0
        assert acc.resolved_count == 0

    def test_new_insight_added(self) -> None:
        acc = InsightAccumulator()
        insight = _make_insight()
        now = datetime.now(timezone.utc)
        acc.update([insight], now)

        results = acc.get_all()
        assert len(results) == 1
        assert results[0].occurrence_count == 1
        assert results[0].first_seen == now
        assert results[0].last_seen == now
        assert results[0].resolved_at is None
        assert acc.active_count == 1

    def test_insight_updated_on_repeat(self) -> None:
        acc = InsightAccumulator()
        insight = _make_insight()
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=5)

        acc.update([insight], t1)
        acc.update([insight], t2)

        results = acc.get_all()
        assert len(results) == 1
        assert results[0].occurrence_count == 2
        assert results[0].first_seen == t1
        assert results[0].last_seen == t2

    def test_insight_resolved_when_absent(self) -> None:
        acc = InsightAccumulator()
        insight = _make_insight()
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=5)

        acc.update([insight], t1)
        acc.update([], t2)

        results = acc.get_all()
        assert len(results) == 1
        assert results[0].resolved_at == t2
        assert results[0].is_resolved
        assert acc.active_count == 0
        assert acc.resolved_count == 1

    def test_insight_re_emerged(self) -> None:
        acc = InsightAccumulator()
        insight = _make_insight()
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=5)
        t3 = t2 + timedelta(seconds=5)

        acc.update([insight], t1)  # detected
        acc.update([], t2)  # resolved
        acc.update([insight], t3)  # re-emerged

        results = acc.get_all()
        assert len(results) == 1
        assert results[0].resolved_at is None
        assert not results[0].is_resolved
        assert results[0].occurrence_count == 2

    def test_peak_severity_tracked(self) -> None:
        acc = InsightAccumulator()
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=5)

        warning = _make_insight(severity=Severity.WARNING)
        critical = _make_insight(severity=Severity.CRITICAL)

        acc.update([warning], t1)
        acc.update([critical], t2)

        results = acc.get_all()
        assert results[0].peak_severity == Severity.CRITICAL

    def test_peak_metric_tracked(self) -> None:
        acc = InsightAccumulator()
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=5)

        low = _make_insight(metric_value=5.0)
        high = _make_insight(metric_value=15.0)

        acc.update([low], t1)
        acc.update([high], t2)

        results = acc.get_all()
        assert results[0].peak_metric_value == 15.0

    def test_get_all_sort_order(self) -> None:
        acc = InsightAccumulator()
        now = datetime.now(timezone.utc)

        critical = _make_insight(category=InsightCategory.GC, severity=Severity.CRITICAL, affected_entity="1")
        warning = _make_insight(category=InsightCategory.SPILL, severity=Severity.WARNING, affected_entity="2")
        resolved = _make_insight(category=InsightCategory.SKEW, severity=Severity.WARNING, affected_entity="3")

        acc.update([critical, warning, resolved], now)
        acc.update([critical, warning], now + timedelta(seconds=5))  # resolved disappears

        results = acc.get_all()
        assert len(results) == 3
        # Active first: critical before warning
        assert not results[0].is_resolved
        assert results[0].insight.category == InsightCategory.GC
        assert not results[1].is_resolved
        assert results[1].insight.category == InsightCategory.SPILL
        # Resolved last
        assert results[2].is_resolved

    def test_reset_clears_state(self) -> None:
        acc = InsightAccumulator()
        acc.update([_make_insight()], datetime.now(timezone.utc))
        assert acc.active_count == 1
        acc.reset()
        assert acc.get_all() == []
        assert acc.active_count == 0
        assert acc.resolved_count == 0

    def test_multiple_categories(self) -> None:
        acc = InsightAccumulator()
        now = datetime.now(timezone.utc)
        insights = [
            _make_insight(category=InsightCategory.GC, affected_entity="exec1"),
            _make_insight(category=InsightCategory.SPILL, affected_entity="stage1"),
            _make_insight(category=InsightCategory.MEMORY, affected_entity="exec2"),
        ]
        acc.update(insights, now)
        assert acc.active_count == 3
        assert acc.resolved_count == 0
