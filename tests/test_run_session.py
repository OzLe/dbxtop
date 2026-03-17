"""Unit tests for RunSession model and RunManager."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dbxtop.analytics.run import AccumulatedInsightExport, RunSession


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

    def test_duration_seconds_active(self) -> None:
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=datetime.now(timezone.utc) - timedelta(seconds=60),
        )
        assert run.duration_seconds >= 59

    def test_duration_seconds_stopped(self) -> None:
        start = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
        stop = datetime(2026, 3, 17, 12, 30, 0, tzinfo=timezone.utc)
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=start,
            stopped_at=stop,
        )
        assert run.duration_seconds == 1800.0

    def test_is_active(self) -> None:
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=datetime.now(timezone.utc),
        )
        assert run.is_active

    def test_is_not_active_when_stopped(self) -> None:
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=datetime.now(timezone.utc),
            stopped_at=datetime.now(timezone.utc),
        )
        assert not run.is_active

    def test_avg_health_score(self) -> None:
        now = datetime.now(timezone.utc)
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=now,
            health_history=[(now, 60), (now, 80), (now, 100)],
        )
        assert run.avg_health_score == 80.0

    def test_avg_health_score_empty(self) -> None:
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=datetime.now(timezone.utc),
        )
        assert run.avg_health_score == 0.0

    def test_worst_health_score(self) -> None:
        now = datetime.now(timezone.utc)
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=now,
            health_history=[(now, 60), (now, 80), (now, 40)],
        )
        assert run.worst_health_score == 40

    def test_empty_run_serialization(self) -> None:
        run = RunSession(
            run_id="test",
            name="test",
            cluster_id="cluster",
            started_at=datetime.now(timezone.utc),
        )
        json_str = run.model_dump_json()
        loaded = RunSession.model_validate_json(json_str)
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
            description="Test desc",
            recommendation="Fix it",
            affected_entity="exec1",
            first_seen=now,
            last_seen=now,
            occurrence_count=5,
            peak_metric_value=12.5,
            metric_value=10.0,
            threshold_value=5.0,
        )
        json_str = export.model_dump_json()
        loaded = AccumulatedInsightExport.model_validate_json(json_str)
        assert loaded.category == "GC"
        assert loaded.peak_severity == "CRITICAL"
        assert loaded.occurrence_count == 5
