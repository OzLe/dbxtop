"""Unit tests for run comparison logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

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


class TestOnRunListDismissed:
    """Tests for DbxTopApp._on_run_list_dismissed callback."""

    def _make_app(self) -> MagicMock:
        """Create a mock app with _on_run_list_dismissed from the real class."""
        from dbxtop.app import DbxTopApp

        app = MagicMock(spec=DbxTopApp)
        app._cluster_id = "cluster-123"
        app._on_run_list_dismissed = DbxTopApp._on_run_list_dismissed.__get__(app)
        return app

    def test_none_result_is_noop(self) -> None:
        app = self._make_app()
        app._on_run_list_dismissed(None)
        app.push_screen.assert_not_called()

    def test_empty_list_is_noop(self) -> None:
        app = self._make_app()
        app._on_run_list_dismissed([])
        app.push_screen.assert_not_called()

    def test_single_id_is_noop(self) -> None:
        app = self._make_app()
        app._on_run_list_dismissed(["run-1"])
        app.push_screen.assert_not_called()

    def test_two_ids_pushes_comparison_screen(self) -> None:
        from dbxtop.views.run_comparison import RunComparisonScreen

        app = self._make_app()

        now = datetime.now(timezone.utc)
        run_a = _make_run("before")
        run_a.run_id = "run-a"
        run_a.started_at = now - timedelta(hours=1)
        run_b = _make_run("after")
        run_b.run_id = "run-b"
        run_b.started_at = now

        with patch("dbxtop.analytics.run_manager.RunManager.load_run") as mock_load:
            mock_load.side_effect = lambda cid, rid: {"run-a": run_a, "run-b": run_b}[rid]
            app._on_run_list_dismissed(["run-a", "run-b"])

        app.push_screen.assert_called_once()
        screen_arg = app.push_screen.call_args[0][0]
        assert isinstance(screen_arg, RunComparisonScreen)

    def test_missing_run_shows_error(self) -> None:
        app = self._make_app()

        with patch("dbxtop.analytics.run_manager.RunManager.load_run", return_value=None):
            app._on_run_list_dismissed(["run-a", "run-b"])

        app.notify.assert_called_once()
        assert "Could not load" in app.notify.call_args[0][0]
        app.push_screen.assert_not_called()

    def test_orders_runs_by_start_time(self) -> None:
        """Earlier run should be run_a (before) in the comparison."""
        from dbxtop.app import DbxTopApp
        from dbxtop.views.run_comparison import RunComparisonScreen

        app = MagicMock(spec=DbxTopApp)
        app._cluster_id = "cluster-123"
        bound = DbxTopApp._on_run_list_dismissed.__get__(app)

        now = datetime.now(timezone.utc)
        run_early = _make_run("early")
        run_early.run_id = "run-early"
        run_early.started_at = now - timedelta(hours=2)
        run_late = _make_run("late")
        run_late.run_id = "run-late"
        run_late.started_at = now

        # Pass late first, early second — should still order correctly
        with patch("dbxtop.analytics.run_manager.RunManager.load_run") as mock_load:
            mock_load.side_effect = lambda cid, rid: {"run-late": run_late, "run-early": run_early}[rid]
            bound(["run-late", "run-early"])

        screen_arg = app.push_screen.call_args[0][0]
        assert isinstance(screen_arg, RunComparisonScreen)
        assert screen_arg._comp.run_a.run_id == "run-early"
        assert screen_arg._comp.run_b.run_id == "run-late"
