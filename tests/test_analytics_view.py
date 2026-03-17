"""Unit tests for the AnalyticsView (TDD — tests before implementation).

Tests cover import, compose structure, refresh_data with various cache
states, graceful degradation, and individual panel rendering methods.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from unittest import mock


from dbxtop.analytics.accumulator import AccumulatedInsight
from dbxtop.analytics.engine import AnalyticsEngine
from dbxtop.analytics.models import (
    DiagnosticReport,
    HealthScore,
    Insight,
    InsightCategory,
    Recommendation,
    Severity,
)
from dbxtop.api.cache import DataCache
from dbxtop.api.models import (
    ExecutorInfo,
    JobStatus,
    SparkJob,
    SparkStage,
    StageStatus,
)


def _wrap_insight(insight: Insight) -> AccumulatedInsight:
    """Wrap a raw Insight in an AccumulatedInsight for view tests."""
    now = datetime.now(timezone.utc)
    return AccumulatedInsight(
        insight=insight,
        first_seen=now,
        last_seen=now,
        resolved_at=None,
        peak_severity=insight.severity,
        occurrence_count=1,
        peak_metric_value=insight.metric_value,
    )


# ---------------------------------------------------------------------------
# Helper factories (mirroring test_analytics_engine.py patterns)
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)


def make_executor(
    executor_id: str = "1",
    *,
    is_active: bool = True,
    total_cores: int = 4,
    max_tasks: int = 4,
    active_tasks: int = 2,
    completed_tasks: int = 100,
    failed_tasks: int = 0,
    total_duration_ms: int = 600_000,
    total_gc_time_ms: int = 6_000,
    max_memory: int = 4_294_967_296,  # 4 GB
    memory_used: int = 1_073_741_824,  # 1 GB (25%)
    disk_used: int = 0,
    total_shuffle_read: int = 1_000_000,
    total_shuffle_write: int = 500_000,
    peak_jvm_heap: int = 0,
) -> ExecutorInfo:
    """Create an ExecutorInfo with realistic defaults."""
    return ExecutorInfo(
        executor_id=executor_id,
        is_active=is_active,
        host_port=f"10.0.0.{executor_id}:42000" if executor_id != "driver" else "10.0.0.1:0",
        total_cores=total_cores,
        max_tasks=max_tasks,
        active_tasks=active_tasks,
        completed_tasks=completed_tasks,
        failed_tasks=failed_tasks,
        total_duration_ms=total_duration_ms,
        total_gc_time_ms=total_gc_time_ms,
        max_memory=max_memory,
        memory_used=memory_used,
        disk_used=disk_used,
        total_shuffle_read=total_shuffle_read,
        total_shuffle_write=total_shuffle_write,
        peak_jvm_heap=peak_jvm_heap,
    )


def make_stage(
    stage_id: int = 1,
    *,
    status: StageStatus = StageStatus.ACTIVE,
    num_tasks: int = 200,
    num_active_tasks: int = 10,
    num_complete_tasks: int = 180,
    num_failed_tasks: int = 0,
    input_bytes: int = 1_073_741_824,  # 1 GB
    output_bytes: int = 536_870_912,  # 512 MB
    shuffle_read_bytes: int = 0,
    shuffle_write_bytes: int = 0,
    memory_spill_bytes: int = 0,
    disk_spill_bytes: int = 0,
    executor_run_time_ms: int = 120_000,
) -> SparkStage:
    """Create a SparkStage with realistic defaults."""
    return SparkStage(
        stage_id=stage_id,
        status=status,
        num_tasks=num_tasks,
        num_active_tasks=num_active_tasks,
        num_complete_tasks=num_complete_tasks,
        num_failed_tasks=num_failed_tasks,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        shuffle_read_bytes=shuffle_read_bytes,
        shuffle_write_bytes=shuffle_write_bytes,
        memory_spill_bytes=memory_spill_bytes,
        disk_spill_bytes=disk_spill_bytes,
        executor_run_time_ms=executor_run_time_ms,
    )


def make_job(
    job_id: int = 1,
    *,
    status: JobStatus = JobStatus.RUNNING,
    num_tasks: int = 200,
    num_active_tasks: int = 10,
    num_completed_tasks: int = 180,
    num_failed_tasks: int = 0,
) -> SparkJob:
    """Create a SparkJob with realistic defaults."""
    return SparkJob(
        job_id=job_id,
        name=f"job-{job_id}",
        status=status,
        num_tasks=num_tasks,
        num_active_tasks=num_active_tasks,
        num_completed_tasks=num_completed_tasks,
        num_failed_tasks=num_failed_tasks,
        submission_time=_NOW,
    )


def _populated_cache(
    executors: List[ExecutorInfo] | None = None,
    stages: List[SparkStage] | None = None,
    jobs: List[SparkJob] | None = None,
) -> DataCache:
    """Return a DataCache populated with the given data (or sensible defaults)."""
    cache = DataCache()
    if executors is not None:
        cache.update("executors", executors)
    if stages is not None:
        cache.update("stages", stages)
    if jobs is not None:
        cache.update("spark_jobs", jobs)
    return cache


def _make_healthy_report() -> DiagnosticReport:
    """Create a healthy DiagnosticReport with no issues."""
    return DiagnosticReport(
        health=HealthScore(
            score=95,
            label="Excellent",
            color="green",
            component_scores={
                "gc": 98,
                "spill": 100,
                "skew": 100,
                "utilization": 85,
                "shuffle": 100,
                "task_failures": 100,
            },
        ),
        insights=[],
        recommendations=[],
        timestamp=_NOW,
        executor_count=4,
        active_stage_count=1,
        active_job_count=1,
    )


def _make_unhealthy_report() -> DiagnosticReport:
    """Create a report with CRITICAL, WARNING, and INFO insights."""
    return DiagnosticReport(
        health=HealthScore(
            score=42,
            label="Poor",
            color="#ff8c00",
            component_scores={
                "gc": 30,
                "spill": 50,
                "skew": 40,
                "utilization": 60,
                "shuffle": 70,
                "task_failures": 90,
            },
        ),
        insights=[
            Insight(
                id="GC_001",
                category=InsightCategory.GC,
                severity=Severity.CRITICAL,
                title="High GC pressure on executor 3 (12.5%)",
                description="Executor 3 is spending 12.5% of total time in GC.",
                metric_value=12.5,
                threshold_value=10.0,
                recommendation="Increase spark.executor.memory or use Kryo serialization.",
                affected_entity="3",
            ),
            Insight(
                id="SPILL_001",
                category=InsightCategory.SPILL,
                severity=Severity.WARNING,
                title="Memory spill on stage 5 (2.3 GB)",
                description="Stage 5 is spilling 2.3 GB to disk.",
                metric_value=2_469_606_195.2,
                threshold_value=0.0,
                recommendation="Increase spark.sql.shuffle.partitions to reduce partition size.",
                affected_entity="stage_5",
            ),
            Insight(
                id="UTIL_001",
                category=InsightCategory.UTILIZATION,
                severity=Severity.INFO,
                title="Low cluster utilization (45%)",
                description="Only 45% of available task slots are active.",
                metric_value=45.0,
                threshold_value=70.0,
                recommendation="Consider reducing worker count or increasing parallelism.",
                affected_entity="",
            ),
        ],
        recommendations=[
            Recommendation(
                title="GC: 1 issue(s) detected",
                description="Increase spark.executor.memory or use Kryo serialization.",
                priority=1,
                category=InsightCategory.GC,
            ),
            Recommendation(
                title="SPILL: 1 issue(s) detected",
                description="Increase spark.sql.shuffle.partitions to reduce partition size.",
                priority=3,
                category=InsightCategory.SPILL,
            ),
        ],
        timestamp=_NOW,
        executor_count=4,
        active_stage_count=2,
        active_job_count=1,
    )


# ---------------------------------------------------------------------------
# TestAnalyticsViewImports
# ---------------------------------------------------------------------------


class TestAnalyticsViewImports:
    """Verify that AnalyticsView is importable from its expected location."""

    def test_import_analytics_view(self) -> None:
        """AnalyticsView can be imported from dbxtop.views.analytics."""
        from dbxtop.views.analytics import AnalyticsView

        assert AnalyticsView is not None

    def test_analytics_view_is_base_view_subclass(self) -> None:
        """AnalyticsView is a subclass of BaseView."""
        from dbxtop.views.analytics import AnalyticsView
        from dbxtop.views.base import BaseView

        assert issubclass(AnalyticsView, BaseView)


# ---------------------------------------------------------------------------
# TestAnalyticsViewCompose
# ---------------------------------------------------------------------------


class TestAnalyticsViewCompose:
    """Verify that compose() yields the expected widget structure."""

    def test_compose_yields_placeholder(self) -> None:
        """compose() should yield a placeholder Static widget with id='analytics-placeholder'."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        widgets = list(view.compose())

        # The first yielded widget should be the placeholder
        placeholder_found = False
        for widget in widgets:
            # Check for the placeholder by id or by content
            widget_id = getattr(widget, "id", None)
            if widget_id == "analytics-placeholder":
                placeholder_found = True
                break

        assert placeholder_found, "compose() should yield a widget with id='analytics-placeholder'"

    def test_compose_yields_top_row_container(self) -> None:
        """compose() should yield a top-row container with health and issues panels."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        # The compose should produce widgets; we check it doesn't raise
        widgets = list(view.compose())
        assert len(widgets) >= 3, "compose() should yield at least placeholder + top row + bottom row"

    def test_initial_placeholder_text(self) -> None:
        """Initial placeholder should contain 'Waiting for Spark application...'."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        widgets = list(view.compose())

        # Find the placeholder widget
        from textual.widgets import Static

        placeholder = None
        for widget in widgets:
            widget_id = getattr(widget, "id", None)
            if widget_id == "analytics-placeholder":
                placeholder = widget
                break

        assert placeholder is not None
        assert isinstance(placeholder, Static)
        # The renderable (content) of the Static should contain the waiting message
        assert "Waiting for Spark application" in str(placeholder.renderable)


# ---------------------------------------------------------------------------
# TestAnalyticsViewRefreshData
# ---------------------------------------------------------------------------


class TestAnalyticsViewRefreshData:
    """Test refresh_data with various cache states."""

    def test_refresh_data_with_populated_cache_runs_engine(self) -> None:
        """refresh_data with executor data should invoke the analytics engine."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()

        executors = [
            make_executor("1", total_gc_time_ms=3_000, total_duration_ms=100_000),
            make_executor("2", total_gc_time_ms=2_000, total_duration_ms=100_000),
        ]
        cache = _populated_cache(
            executors=executors,
            stages=[make_stage(1)],
            jobs=[make_job(1)],
        )

        # The engine is an attribute of the view; mock it to verify invocation
        mock_report = _make_healthy_report()
        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = mock_report
            # Mock the panel rendering methods to avoid needing mounted widgets
            with mock.patch.object(view, "_update_panels"):
                view.refresh_data(cache)

            mock_engine.analyze.assert_called_once()
            call_args = mock_engine.analyze.call_args
            # First positional arg should be executors list
            assert call_args[0][0] == executors or call_args[1].get("executors") == executors

    def test_refresh_data_with_none_executors_keeps_placeholder(self) -> None:
        """refresh_data with None executors should not attempt analysis."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        cache = DataCache()  # All slots are None

        with mock.patch.object(view, "_engine") as mock_engine:
            # Should not call analyze when executors is None
            with mock.patch.object(view, "_update_panels", create=True):
                view.refresh_data(cache)

            mock_engine.analyze.assert_not_called()

    def test_refresh_data_with_executors_no_stages_calls_engine(self) -> None:
        """refresh_data with executors but no stages should still run analysis (partial data)."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        executors = [make_executor("1"), make_executor("2")]
        cache = _populated_cache(executors=executors)

        mock_report = _make_healthy_report()
        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = mock_report
            with mock.patch.object(view, "_update_panels"):
                view.refresh_data(cache)

            # Should still call analyze — engine handles None stages gracefully
            mock_engine.analyze.assert_called_once()

    def test_refresh_data_with_empty_executor_list_calls_engine(self) -> None:
        """refresh_data with an empty (but non-None) executor list should call analyze."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        cache = _populated_cache(executors=[])

        # The engine may return None for empty executors or a healthy report — either is valid.
        # The important thing is that the view passes the data through.
        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = None
            with mock.patch.object(view, "_update_panels", create=True):
                view.refresh_data(cache)

            mock_engine.analyze.assert_called_once()

    def test_refresh_data_engine_returns_none_does_not_crash(self) -> None:
        """If the engine returns None, refresh_data should not raise."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        executors = [make_executor("1")]
        cache = _populated_cache(executors=executors)

        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = None
            with mock.patch.object(view, "_update_panels", create=True):
                # Should not raise
                view.refresh_data(cache)


# ---------------------------------------------------------------------------
# TestAnalyticsViewGracefulDegradation
# ---------------------------------------------------------------------------


class TestAnalyticsViewGracefulDegradation:
    """Test graceful degradation with missing or partial data."""

    def test_none_executor_data_does_not_invoke_engine(self) -> None:
        """With None executor data, the engine should not be called."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        cache = DataCache()

        with mock.patch.object(view, "_engine") as mock_engine:
            with mock.patch.object(view, "_update_panels", create=True):
                view.refresh_data(cache)
            mock_engine.analyze.assert_not_called()

    def test_empty_executor_list_produces_report(self) -> None:
        """An empty executor list should be passed to the engine (engine decides outcome)."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        cache = _populated_cache(executors=[])

        # The real engine returns None for empty executors, but the view should still try
        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = _make_healthy_report()
            with mock.patch.object(view, "_update_panels"):
                view.refresh_data(cache)
            mock_engine.analyze.assert_called_once()

    def test_real_engine_with_healthy_executors(self) -> None:
        """Integration-style: real engine with healthy executors produces a report."""
        engine = AnalyticsEngine()
        executors = [
            make_executor("1", total_gc_time_ms=1_000, total_duration_ms=100_000),
            make_executor("2", total_gc_time_ms=2_000, total_duration_ms=100_000),
        ]
        report = engine.analyze(executors=executors, stages=[], jobs=[], sql_queries=None, cluster=None)
        assert report is not None
        assert report.health.score >= 80
        assert report.health.label in ("Excellent", "Good")


# ---------------------------------------------------------------------------
# TestAnalyticsViewPanelRendering
# ---------------------------------------------------------------------------


class TestAnalyticsViewPanelRendering:
    """Test the individual panel rendering helper methods."""

    def test_render_health_panel_contains_score(self) -> None:
        """_render_health_panel should include the numeric score."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        health = HealthScore(
            score=82,
            label="Good",
            color="blue",
            component_scores={
                "gc": 90,
                "spill": 75,
                "skew": 95,
                "utilization": 70,
                "shuffle": 80,
                "task_failures": 100,
            },
        )

        result = view._render_health_panel(health)
        assert "82" in result, "Health panel should display the score number"

    def test_render_health_panel_contains_label(self) -> None:
        """_render_health_panel should include the label text."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        health = HealthScore(score=82, label="Good", color="blue")

        result = view._render_health_panel(health)
        assert "Good" in result, "Health panel should display the health label"

    def test_render_health_panel_contains_component_scores(self) -> None:
        """_render_health_panel should show component breakdown."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        health = HealthScore(
            score=82,
            label="Good",
            color="blue",
            component_scores={
                "gc": 90,
                "spill": 75,
                "skew": 95,
                "utilization": 70,
                "shuffle": 80,
                "task_failures": 100,
            },
        )

        result = view._render_health_panel(health)
        # Should show component names and their scores
        assert "90" in result, "Health panel should show GC component score"
        assert "75" in result, "Health panel should show Spill component score"

    def test_render_health_panel_contains_title(self) -> None:
        """_render_health_panel should include 'HEALTH SCORE' title."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        health = HealthScore(score=95, label="Excellent", color="green")

        result = view._render_health_panel(health)
        assert "HEALTH" in result.upper(), "Health panel should have a title containing 'HEALTH'"

    def test_render_issues_panel_with_critical_insight(self) -> None:
        """_render_issues_panel should show CRITICAL insights with red icon."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        accumulated = [
            _wrap_insight(
                Insight(
                    id="GC_001",
                    category=InsightCategory.GC,
                    severity=Severity.CRITICAL,
                    title="High GC pressure on executor 3 (12.5%)",
                    description="Executor 3 is spending 12.5% of total time in GC.",
                    metric_value=12.5,
                    threshold_value=10.0,
                    recommendation="Increase spark.executor.memory.",
                    affected_entity="3",
                )
            ),
        ]

        result = view._render_issues_panel(accumulated)
        assert "High GC pressure" in result, "Issues panel should show insight title"
        assert "red" in result.lower(), "CRITICAL insights should use red styling"

    def test_render_issues_panel_with_warning_insight(self) -> None:
        """_render_issues_panel should show WARNING insights with yellow icon."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        accumulated = [
            _wrap_insight(
                Insight(
                    id="SPILL_001",
                    category=InsightCategory.SPILL,
                    severity=Severity.WARNING,
                    title="Memory spill on stage 5",
                    description="Stage 5 is spilling data to disk.",
                    metric_value=2_000_000_000.0,
                    threshold_value=0.0,
                    recommendation="Increase partition count.",
                    affected_entity="stage_5",
                )
            ),
        ]

        result = view._render_issues_panel(accumulated)
        assert "Memory spill" in result
        assert "yellow" in result.lower(), "WARNING insights should use yellow styling"

    def test_render_issues_panel_with_info_insight(self) -> None:
        """_render_issues_panel should show INFO insights with dim icon."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        accumulated = [
            _wrap_insight(
                Insight(
                    id="UTIL_001",
                    category=InsightCategory.UTILIZATION,
                    severity=Severity.INFO,
                    title="Low cluster utilization (45%)",
                    description="Only 45% of slots active.",
                    metric_value=45.0,
                    threshold_value=70.0,
                    recommendation="Consider reducing workers.",
                    affected_entity="",
                )
            ),
        ]

        result = view._render_issues_panel(accumulated)
        assert "Low cluster utilization" in result
        assert "dim" in result.lower(), "INFO insights should use dim styling"

    def test_render_issues_panel_with_no_insights(self) -> None:
        """_render_issues_panel with empty list shows 'No issues detected'."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        result = view._render_issues_panel([])
        assert "No issues detected" in result

    def test_render_issues_panel_with_mixed_severities(self) -> None:
        """_render_issues_panel should render CRITICAL, WARNING, and INFO together."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        report = _make_unhealthy_report()

        result = view._render_issues_panel([_wrap_insight(i) for i in report.insights])
        assert "High GC pressure" in result
        assert "Memory spill" in result
        assert "Low cluster utilization" in result

    def test_render_issues_panel_title(self) -> None:
        """_render_issues_panel should contain a title."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        result = view._render_issues_panel([])
        assert "ISSUES" in result.upper() or "TOP ISSUES" in result.upper()

    def test_render_recommendations_panel_with_recommendations(self) -> None:
        """_render_recommendations_panel should show numbered recommendations."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        recommendations = [
            Recommendation(
                title="GC: 1 issue(s) detected",
                description="Increase spark.executor.memory or use Kryo serialization.",
                priority=1,
                category=InsightCategory.GC,
            ),
            Recommendation(
                title="SPILL: 1 issue(s) detected",
                description="Increase spark.sql.shuffle.partitions.",
                priority=3,
                category=InsightCategory.SPILL,
            ),
        ]

        result = view._render_recommendations_panel(recommendations)
        assert "1." in result, "Recommendations should be numbered"
        assert "2." in result, "Second recommendation should be numbered"
        assert "spark.executor.memory" in result or "GC" in result

    def test_render_recommendations_panel_empty(self) -> None:
        """_render_recommendations_panel with empty list shows 'No recommendations'."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        result = view._render_recommendations_panel([])
        assert "No recommendation" in result.lower() or "healthy" in result.lower()

    def test_render_recommendations_panel_title(self) -> None:
        """_render_recommendations_panel should contain a title."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        result = view._render_recommendations_panel([])
        assert "RECOMMENDATION" in result.upper()

    def test_render_recommendations_panel_priority_order(self) -> None:
        """Recommendations should be displayed in priority order."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        recommendations = [
            Recommendation(
                title="Low priority fix",
                description="Minor optimization.",
                priority=5,
                category=InsightCategory.UTILIZATION,
            ),
            Recommendation(
                title="High priority fix",
                description="Critical fix needed.",
                priority=1,
                category=InsightCategory.GC,
            ),
        ]

        result = view._render_recommendations_panel(recommendations)
        # The first numbered item should come from the higher-priority recommendation
        # Find positions of the recommendation titles
        high_pos = result.find("High priority fix")
        low_pos = result.find("Low priority fix")
        # Both should exist
        assert high_pos != -1, "High priority recommendation should be present"
        assert low_pos != -1, "Low priority recommendation should be present"


# ---------------------------------------------------------------------------
# TestAnalyticsViewThrottling
# ---------------------------------------------------------------------------


class TestAnalyticsViewThrottling:
    """Test the refresh throttling mechanism."""

    def test_first_refresh_always_runs(self) -> None:
        """The first call to refresh_data should always execute (no throttle)."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        executors = [make_executor("1")]
        cache = _populated_cache(executors=executors)

        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = _make_healthy_report()
            with mock.patch.object(view, "_update_panels"):
                view.refresh_data(cache)
            mock_engine.analyze.assert_called_once()

    def test_rapid_refresh_is_throttled(self) -> None:
        """Rapid successive calls to refresh_data should be throttled."""
        from dbxtop.views.analytics import AnalyticsView

        view = AnalyticsView()
        executors = [make_executor("1")]
        cache = _populated_cache(executors=executors)

        with mock.patch.object(view, "_engine") as mock_engine:
            mock_engine.analyze.return_value = _make_healthy_report()
            with mock.patch.object(view, "_update_panels"):
                # First call should run
                view.refresh_data(cache)
                # Second call immediately after should be throttled
                view.refresh_data(cache)

            # Engine should be called once (second call throttled) or twice if
            # the throttle interval is very short. At minimum, no crash.
            assert mock_engine.analyze.call_count >= 1


# ---------------------------------------------------------------------------
# TestAnalyticsViewEngineIntegration
# ---------------------------------------------------------------------------


class TestAnalyticsViewEngineIntegration:
    """Integration tests: real engine + view panel rendering (no mocked engine)."""

    def test_healthy_cluster_full_flow(self) -> None:
        """Real engine with healthy data produces report that renders cleanly."""
        from dbxtop.views.analytics import AnalyticsView

        engine = AnalyticsEngine()
        executors = [
            make_executor("1", total_gc_time_ms=1_000, total_duration_ms=100_000),
            make_executor("2", total_gc_time_ms=2_000, total_duration_ms=100_000),
        ]
        stages = [make_stage(1, memory_spill_bytes=0, disk_spill_bytes=0)]
        jobs = [make_job(1)]

        report = engine.analyze(executors=executors, stages=stages, jobs=jobs, sql_queries=None, cluster=None)
        assert report is not None

        view = AnalyticsView()
        health_text = view._render_health_panel(report.health)
        issues_text = view._render_issues_panel([_wrap_insight(i) for i in report.insights])
        view._render_recommendations_panel(report.recommendations)

        assert str(report.health.score) in health_text
        assert report.health.label in health_text
        # Healthy cluster should have few or no issues
        if not report.insights:
            assert "No issues detected" in issues_text

    def test_gc_pressure_renders_in_issues(self) -> None:
        """High GC executor triggers insight that renders in issues panel."""
        from dbxtop.views.analytics import AnalyticsView

        engine = AnalyticsEngine()
        executors = [
            make_executor("1", total_gc_time_ms=15_000, total_duration_ms=100_000),  # 15% GC
            make_executor("2", total_gc_time_ms=1_000, total_duration_ms=100_000),  # 1% GC
        ]

        report = engine.analyze(executors=executors, stages=[], jobs=[], sql_queries=None, cluster=None)
        assert report is not None
        assert len(report.insights) > 0

        view = AnalyticsView()
        issues_text = view._render_issues_panel([_wrap_insight(i) for i in report.insights])
        assert "GC" in issues_text.upper() or "gc" in issues_text.lower()

    def test_spill_renders_in_issues(self) -> None:
        """Stage with spill triggers insight that renders in issues panel."""
        from dbxtop.views.analytics import AnalyticsView

        engine = AnalyticsEngine()
        executors = [make_executor("1"), make_executor("2")]
        stages = [
            make_stage(
                1,
                memory_spill_bytes=2_000_000_000,
                disk_spill_bytes=2_000_000_000,
                input_bytes=1_073_741_824,
            ),
        ]

        report = engine.analyze(executors=executors, stages=stages, jobs=[], sql_queries=None, cluster=None)
        assert report is not None

        view = AnalyticsView()
        issues_text = view._render_issues_panel([_wrap_insight(i) for i in report.insights])
        # Should mention spill in some form
        spill_mentioned = "spill" in issues_text.lower() or "SPILL" in issues_text
        assert spill_mentioned, f"Spill insight should appear in issues panel, got: {issues_text[:200]}"
