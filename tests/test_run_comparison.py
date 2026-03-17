"""Unit tests for run comparison logic."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from dbxtop.analytics.models import DiagnosticReport, HealthScore
from dbxtop.analytics.run import AccumulatedInsightExport, RunSession
from dbxtop.views.run_comparison import compare_runs, _format_delta


def _make_run(
    name: str = "test",
    health_score: int = 75,
    component_scores: Optional[Dict[str, int]] = None,
    insights: Optional[List[AccumulatedInsightExport]] = None,
    config: Optional[Dict[str, str]] = None,
) -> RunSession:
    """Create a test RunSession with a single snapshot."""
    now = datetime.now(timezone.utc)
    if component_scores is None:
        component_scores = {"gc": 80, "spill": 90}
    snapshot = DiagnosticReport(
        health=HealthScore(
            score=health_score,
            label="Good",
            color="blue",
            component_scores=component_scores,
        ),
        timestamp=now,
    )
    return RunSession(
        run_id=f"run-{name}",
        name=name,
        cluster_id="cluster",
        started_at=now,
        stopped_at=now,
        snapshots=[snapshot],
        accumulated_insights=insights or [],
        health_history=[(now, health_score)],
        config_snapshot=config or {},
    )


def _make_insight_export(
    category: str = "GC",
    entity: str = "",
    severity: str = "WARNING",
) -> AccumulatedInsightExport:
    now = datetime.now(timezone.utc)
    return AccumulatedInsightExport(
        category=category,
        severity=severity,
        peak_severity=severity,
        title=f"Test {category}",
        description="Test",
        recommendation="Fix",
        affected_entity=entity,
        first_seen=now,
        last_seen=now,
        occurrence_count=1,
        peak_metric_value=10.0,
        metric_value=10.0,
        threshold_value=5.0,
    )


class TestCompareRuns:
    def test_health_delta_positive(self) -> None:
        a = _make_run("before", health_score=60)
        b = _make_run("after", health_score=85)
        comp = compare_runs(a, b)
        assert comp.health_delta == 25
        assert comp.health_score_a == 60
        assert comp.health_score_b == 85

    def test_health_delta_negative(self) -> None:
        a = _make_run("before", health_score=90)
        b = _make_run("after", health_score=70)
        comp = compare_runs(a, b)
        assert comp.health_delta == -20

    def test_component_deltas(self) -> None:
        a = _make_run("a", component_scores={"gc": 60, "spill": 80})
        b = _make_run("b", component_scores={"gc": 90, "spill": 70})
        comp = compare_runs(a, b)
        assert comp.component_deltas["gc"] == (60, 90, 30)
        assert comp.component_deltas["spill"] == (80, 70, -10)

    def test_resolved_insights(self) -> None:
        ins_a = _make_insight_export(category="GC", entity="exec1")
        a = _make_run("before", insights=[ins_a])
        b = _make_run("after", insights=[])
        comp = compare_runs(a, b)
        assert len(comp.resolved_insights) == 1
        assert comp.resolved_insights[0].category == "GC"

    def test_new_insights(self) -> None:
        ins_b = _make_insight_export(category="SKEW", entity="exec2")
        a = _make_run("before", insights=[])
        b = _make_run("after", insights=[ins_b])
        comp = compare_runs(a, b)
        assert len(comp.new_insights) == 1

    def test_persistent_insights(self) -> None:
        ins = _make_insight_export(category="GC", entity="exec1")
        a = _make_run("before", insights=[ins])
        b = _make_run("after", insights=[ins])
        comp = compare_runs(a, b)
        assert len(comp.persistent_insights) == 1

    def test_config_diff(self) -> None:
        a = _make_run("before", config={"spark.executor.memory": "4g", "spark.cores": "4"})
        b = _make_run("after", config={"spark.executor.memory": "8g", "spark.cores": "4"})
        comp = compare_runs(a, b)
        assert len(comp.config_changes) == 1
        assert comp.config_changes[0] == ("spark.executor.memory", "4g", "8g")

    def test_config_diff_added_key(self) -> None:
        a = _make_run("before", config={})
        b = _make_run("after", config={"spark.serializer": "kryo"})
        comp = compare_runs(a, b)
        assert len(comp.config_changes) == 1
        assert comp.config_changes[0][1] == "(unset)"

    def test_identical_runs(self) -> None:
        ins = _make_insight_export()
        a = _make_run("a", health_score=80, insights=[ins], config={"k": "v"})
        b = _make_run("b", health_score=80, insights=[ins], config={"k": "v"})
        comp = compare_runs(a, b)
        assert comp.health_delta == 0
        assert len(comp.config_changes) == 0
        assert len(comp.resolved_insights) == 0
        assert len(comp.new_insights) == 0

    def test_empty_runs(self) -> None:
        a = RunSession(run_id="a", name="a", cluster_id="c", started_at=datetime.now(timezone.utc))
        b = RunSession(run_id="b", name="b", cluster_id="c", started_at=datetime.now(timezone.utc))
        comp = compare_runs(a, b)
        assert comp.health_score_a == 0
        assert comp.health_score_b == 0


class TestFormatDelta:
    def test_positive(self) -> None:
        result = _format_delta(13.0)
        assert "+13" in result
        assert "\u2191" in result

    def test_negative(self) -> None:
        result = _format_delta(-5.0)
        assert "-5" in result
        assert "\u2193" in result

    def test_zero(self) -> None:
        result = _format_delta(0.0)
        assert "0" in result
        assert "=" in result
