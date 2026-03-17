"""Unit tests for RunSession model and run comparison."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dbxtop.analytics.models import DiagnosticReport, HealthScore
from dbxtop.analytics.run import AccumulatedInsightExport, RunSession
from dbxtop.views.run_comparison import compare_runs, _format_delta


class TestRunSession:
    def test_serialization_roundtrip(self) -> None:
        run = RunSession(
            run_id="test-123",
            name="test-run",
            cluster_id="cluster-abc",
            started_at=datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc),
            stopped_at=datetime(2026, 3, 17, 12, 30, 0, tzinfo=timezone.utc),
            config_snapshot={"spark.executor.memory": "4g"},
            health_history=[(datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc), 72)],
        )
        json_str = run.model_dump_json()
        loaded = RunSession.model_validate_json(json_str)
        assert loaded.run_id == "test-123"
        assert loaded.name == "test-run"
        assert loaded.stopped_at is not None
        assert loaded.config_snapshot["spark.executor.memory"] == "4g"
        assert len(loaded.health_history) == 1

    def test_duration_active(self) -> None:
        run = RunSession(
            run_id="t",
            name="t",
            cluster_id="c",
            started_at=datetime.now(timezone.utc) - timedelta(seconds=60),
        )
        assert run.duration_seconds >= 59

    def test_duration_stopped(self) -> None:
        start = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
        stop = datetime(2026, 3, 17, 12, 30, 0, tzinfo=timezone.utc)
        run = RunSession(run_id="t", name="t", cluster_id="c", started_at=start, stopped_at=stop)
        assert run.duration_seconds == 1800.0

    def test_is_active(self) -> None:
        run = RunSession(run_id="t", name="t", cluster_id="c", started_at=datetime.now(timezone.utc))
        assert run.is_active

    def test_is_not_active_when_stopped(self) -> None:
        now = datetime.now(timezone.utc)
        run = RunSession(run_id="t", name="t", cluster_id="c", started_at=now, stopped_at=now)
        assert not run.is_active

    def test_avg_health_score(self) -> None:
        now = datetime.now(timezone.utc)
        run = RunSession(
            run_id="t",
            name="t",
            cluster_id="c",
            started_at=now,
            health_history=[(now, 60), (now, 80), (now, 100)],
        )
        assert run.avg_health_score == 80.0

    def test_avg_health_score_empty(self) -> None:
        run = RunSession(run_id="t", name="t", cluster_id="c", started_at=datetime.now(timezone.utc))
        assert run.avg_health_score == 0.0

    def test_worst_health_score(self) -> None:
        now = datetime.now(timezone.utc)
        run = RunSession(
            run_id="t",
            name="t",
            cluster_id="c",
            started_at=now,
            health_history=[(now, 60), (now, 80), (now, 40)],
        )
        assert run.worst_health_score == 40

    def test_empty_run_serialization(self) -> None:
        run = RunSession(run_id="t", name="t", cluster_id="c", started_at=datetime.now(timezone.utc))
        loaded = RunSession.model_validate_json(run.model_dump_json())
        assert loaded.snapshots == []
        assert loaded.accumulated_insights == []


class TestAccumulatedInsightExport:
    def test_serialization(self) -> None:
        now = datetime.now(timezone.utc)
        export = AccumulatedInsightExport(
            category="GC",
            severity="WARNING",
            peak_severity="CRITICAL",
            title="Test",
            description="Desc",
            recommendation="Fix",
            affected_entity="e1",
            first_seen=now,
            last_seen=now,
            occurrence_count=5,
            peak_metric_value=12.5,
            metric_value=10.0,
            threshold_value=5.0,
        )
        loaded = AccumulatedInsightExport.model_validate_json(export.model_dump_json())
        assert loaded.category == "GC"
        assert loaded.peak_severity == "CRITICAL"
        assert loaded.occurrence_count == 5


# ---------------------------------------------------------------------------
# Run comparison tests
# ---------------------------------------------------------------------------


def _make_run(
    name: str = "test",
    health_score: int = 75,
    component_scores: dict[str, int] | None = None,
    insights: list[AccumulatedInsightExport] | None = None,
    config: dict[str, str] | None = None,
) -> RunSession:
    now = datetime.now(timezone.utc)
    if component_scores is None:
        component_scores = {"gc": 80, "spill": 90}
    snapshot = DiagnosticReport(
        health=HealthScore(score=health_score, label="Good", color="blue", component_scores=component_scores),
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


def _make_export(category: str = "GC", entity: str = "", severity: str = "WARNING") -> AccumulatedInsightExport:
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
        comp = compare_runs(_make_run("a", 60), _make_run("b", 85))
        assert comp.health_delta == 25

    def test_health_delta_negative(self) -> None:
        comp = compare_runs(_make_run("a", 90), _make_run("b", 70))
        assert comp.health_delta == -20

    def test_component_deltas(self) -> None:
        a = _make_run("a", component_scores={"gc": 60, "spill": 80})
        b = _make_run("b", component_scores={"gc": 90, "spill": 70})
        comp = compare_runs(a, b)
        assert comp.component_deltas["gc"] == (60, 90, 30)
        assert comp.component_deltas["spill"] == (80, 70, -10)

    def test_resolved_insights(self) -> None:
        comp = compare_runs(
            _make_run("a", insights=[_make_export("GC", "e1")]),
            _make_run("b", insights=[]),
        )
        assert len(comp.resolved_insights) == 1

    def test_new_insights(self) -> None:
        comp = compare_runs(
            _make_run("a", insights=[]),
            _make_run("b", insights=[_make_export("SKEW", "e2")]),
        )
        assert len(comp.new_insights) == 1

    def test_persistent_insights(self) -> None:
        ins = _make_export("GC", "e1")
        comp = compare_runs(_make_run("a", insights=[ins]), _make_run("b", insights=[ins]))
        assert len(comp.persistent_insights) == 1

    def test_config_diff(self) -> None:
        comp = compare_runs(
            _make_run("a", config={"spark.executor.memory": "4g", "spark.cores": "4"}),
            _make_run("b", config={"spark.executor.memory": "8g", "spark.cores": "4"}),
        )
        assert len(comp.config_changes) == 1
        assert comp.config_changes[0] == ("spark.executor.memory", "4g", "8g")

    def test_config_diff_added_key(self) -> None:
        comp = compare_runs(_make_run("a", config={}), _make_run("b", config={"spark.serializer": "kryo"}))
        assert comp.config_changes[0][1] == "(unset)"

    def test_identical_runs(self) -> None:
        ins = _make_export()
        comp = compare_runs(
            _make_run("a", 80, insights=[ins], config={"k": "v"}), _make_run("b", 80, insights=[ins], config={"k": "v"})
        )
        assert comp.health_delta == 0
        assert len(comp.config_changes) == 0
        assert len(comp.resolved_insights) == 0
        assert len(comp.new_insights) == 0

    def test_empty_runs(self) -> None:
        now = datetime.now(timezone.utc)
        a = RunSession(run_id="a", name="a", cluster_id="c", started_at=now)
        b = RunSession(run_id="b", name="b", cluster_id="c", started_at=now)
        comp = compare_runs(a, b)
        assert comp.health_score_a == 0
        assert comp.health_score_b == 0


class TestFormatDelta:
    def test_positive(self) -> None:
        assert "+" in _format_delta(13.0) and "↑" in _format_delta(13.0)

    def test_negative(self) -> None:
        assert "-" in _format_delta(-5.0) and "↓" in _format_delta(-5.0)

    def test_zero(self) -> None:
        assert "0" in _format_delta(0.0)
