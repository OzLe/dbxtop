"""Unit tests for analytics Pydantic models and enums."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dbxtop.analytics.models import (
    DiagnosticReport,
    HealthScore,
    Insight,
    InsightCategory,
    Recommendation,
    Severity,
)


# ---------------------------------------------------------------------------
# Severity enum
# ---------------------------------------------------------------------------


class TestSeverity:
    def test_info_value(self) -> None:
        assert Severity.INFO == "INFO"
        assert Severity.INFO.value == "INFO"

    def test_warning_value(self) -> None:
        assert Severity.WARNING == "WARNING"
        assert Severity.WARNING.value == "WARNING"

    def test_critical_value(self) -> None:
        assert Severity.CRITICAL == "CRITICAL"
        assert Severity.CRITICAL.value == "CRITICAL"

    def test_all_members(self) -> None:
        members = {s.value for s in Severity}
        assert members == {"INFO", "WARNING", "CRITICAL"}

    def test_is_str_enum(self) -> None:
        """Severity members should be usable as plain strings."""
        assert isinstance(Severity.WARNING, str)

    def test_ordering_by_value(self) -> None:
        """Severity values should support string comparison for sorting."""
        severities = [Severity.WARNING, Severity.CRITICAL, Severity.INFO]
        sorted_names = sorted(severities, key=lambda s: s.value)
        assert sorted_names[0] == Severity.CRITICAL
        assert sorted_names[1] == Severity.INFO
        assert sorted_names[2] == Severity.WARNING


# ---------------------------------------------------------------------------
# InsightCategory enum
# ---------------------------------------------------------------------------


class TestInsightCategory:
    def test_gc_value(self) -> None:
        assert InsightCategory.GC == "GC"

    def test_spill_value(self) -> None:
        assert InsightCategory.SPILL == "SPILL"

    def test_skew_value(self) -> None:
        assert InsightCategory.SKEW == "SKEW"

    def test_shuffle_value(self) -> None:
        assert InsightCategory.SHUFFLE == "SHUFFLE"

    def test_utilization_value(self) -> None:
        assert InsightCategory.UTILIZATION == "UTILIZATION"

    def test_partition_value(self) -> None:
        assert InsightCategory.PARTITION == "PARTITION"

    def test_straggler_value(self) -> None:
        assert InsightCategory.STRAGGLER == "STRAGGLER"

    def test_task_failure_value(self) -> None:
        assert InsightCategory.TASK_FAILURE == "TASK_FAILURE"

    def test_memory_value(self) -> None:
        assert InsightCategory.MEMORY == "MEMORY"

    def test_all_members(self) -> None:
        expected = {
            "GC",
            "SPILL",
            "SKEW",
            "SHUFFLE",
            "UTILIZATION",
            "PARTITION",
            "STRAGGLER",
            "TASK_FAILURE",
            "MEMORY",
        }
        actual = {c.value for c in InsightCategory}
        assert actual == expected

    def test_member_count(self) -> None:
        assert len(InsightCategory) == 9

    def test_is_str_enum(self) -> None:
        assert isinstance(InsightCategory.GC, str)


# ---------------------------------------------------------------------------
# Insight model
# ---------------------------------------------------------------------------


class TestInsight:
    @pytest.fixture()
    def sample_insight(self) -> Insight:
        return Insight(
            id="GC_001",
            category=InsightCategory.GC,
            severity=Severity.WARNING,
            title="High GC pressure on executor 3",
            description="Executor 3 is spending 7.2% of its time in garbage collection.",
            metric_value=7.2,
            threshold_value=5.0,
            recommendation="Increase spark.executor.memory or enable Kryo serialization.",
            affected_entity="3",
        )

    def test_creation(self, sample_insight: Insight) -> None:
        assert sample_insight.id == "GC_001"
        assert sample_insight.category == InsightCategory.GC
        assert sample_insight.severity == Severity.WARNING
        assert sample_insight.title == "High GC pressure on executor 3"
        assert sample_insight.metric_value == pytest.approx(7.2)
        assert sample_insight.threshold_value == pytest.approx(5.0)
        assert sample_insight.affected_entity == "3"

    def test_default_affected_entity(self) -> None:
        insight = Insight(
            id="UTIL_001",
            category=InsightCategory.UTILIZATION,
            severity=Severity.WARNING,
            title="Low cluster utilization",
            description="Only 35% of available task slots are in use.",
            metric_value=35.0,
            threshold_value=70.0,
            recommendation="Reduce cluster size.",
        )
        assert insight.affected_entity == ""

    def test_serialization_round_trip(self, sample_insight: Insight) -> None:
        data = sample_insight.model_dump()
        restored = Insight.model_validate(data)
        assert restored.id == sample_insight.id
        assert restored.category == sample_insight.category
        assert restored.severity == sample_insight.severity
        assert restored.title == sample_insight.title
        assert restored.metric_value == pytest.approx(sample_insight.metric_value)
        assert restored.threshold_value == pytest.approx(sample_insight.threshold_value)
        assert restored.recommendation == sample_insight.recommendation
        assert restored.affected_entity == sample_insight.affected_entity

    def test_json_round_trip(self, sample_insight: Insight) -> None:
        json_str = sample_insight.model_dump_json()
        restored = Insight.model_validate_json(json_str)
        assert restored == sample_insight

    def test_model_dump_contains_all_fields(self, sample_insight: Insight) -> None:
        data = sample_insight.model_dump()
        expected_keys = {
            "id",
            "category",
            "severity",
            "title",
            "description",
            "metric_value",
            "threshold_value",
            "recommendation",
            "affected_entity",
        }
        assert set(data.keys()) == expected_keys

    def test_critical_insight(self) -> None:
        insight = Insight(
            id="GC_002",
            category=InsightCategory.GC,
            severity=Severity.CRITICAL,
            title="Severe GC on executor 5 (15.3%)",
            description="Executor 5 is critically impacted by GC.",
            metric_value=15.3,
            threshold_value=10.0,
            recommendation="Increase memory.",
        )
        assert insight.severity == Severity.CRITICAL
        assert insight.metric_value > insight.threshold_value

    def test_info_insight(self) -> None:
        insight = Insight(
            id="PARTITION_001",
            category=InsightCategory.PARTITION,
            severity=Severity.INFO,
            title="Partition sizing note",
            description="Partitions are within optimal range.",
            metric_value=128_000_000.0,
            threshold_value=268_435_456.0,
            recommendation="No action needed.",
        )
        assert insight.severity == Severity.INFO


# ---------------------------------------------------------------------------
# HealthScore model
# ---------------------------------------------------------------------------


class TestHealthScore:
    def test_excellent_score(self) -> None:
        hs = HealthScore(score=95, label="Excellent", color="green")
        assert hs.score == 95
        assert hs.label == "Excellent"
        assert hs.color == "green"

    def test_good_score(self) -> None:
        hs = HealthScore(score=75, label="Good", color="blue")
        assert hs.score == 75
        assert hs.label == "Good"
        assert hs.color == "blue"

    def test_fair_score(self) -> None:
        hs = HealthScore(score=55, label="Fair", color="yellow")
        assert hs.score == 55
        assert hs.label == "Fair"
        assert hs.color == "yellow"

    def test_poor_score(self) -> None:
        hs = HealthScore(score=35, label="Poor", color="#ff8c00")
        assert hs.score == 35
        assert hs.label == "Poor"
        assert hs.color == "#ff8c00"

    def test_critical_score(self) -> None:
        hs = HealthScore(score=15, label="Critical", color="red")
        assert hs.score == 15
        assert hs.label == "Critical"
        assert hs.color == "red"

    def test_default_component_scores(self) -> None:
        hs = HealthScore(score=80, label="Good", color="blue")
        assert hs.component_scores == {}

    def test_with_component_scores(self) -> None:
        components = {
            "gc": 90,
            "spill": 75,
            "skew": 100,
            "utilization": 60,
            "shuffle": 85,
            "task_failures": 100,
        }
        hs = HealthScore(score=82, label="Good", color="blue", component_scores=components)
        assert hs.component_scores["gc"] == 90
        assert hs.component_scores["spill"] == 75
        assert hs.component_scores["skew"] == 100
        assert len(hs.component_scores) == 6

    def test_boundary_score_90(self) -> None:
        """Score of exactly 90 should map to Excellent."""
        hs = HealthScore(score=90, label="Excellent", color="green")
        assert hs.label == "Excellent"

    def test_boundary_score_70(self) -> None:
        """Score of exactly 70 should map to Good."""
        hs = HealthScore(score=70, label="Good", color="blue")
        assert hs.label == "Good"

    def test_boundary_score_50(self) -> None:
        """Score of exactly 50 should map to Fair."""
        hs = HealthScore(score=50, label="Fair", color="yellow")
        assert hs.label == "Fair"

    def test_boundary_score_30(self) -> None:
        """Score of exactly 30 should map to Poor."""
        hs = HealthScore(score=30, label="Poor", color="#ff8c00")
        assert hs.label == "Poor"

    def test_zero_score(self) -> None:
        hs = HealthScore(score=0, label="Critical", color="red")
        assert hs.score == 0

    def test_perfect_score(self) -> None:
        hs = HealthScore(score=100, label="Excellent", color="green")
        assert hs.score == 100

    def test_na_label(self) -> None:
        """N/A label used when no executors are available."""
        hs = HealthScore(score=0, label="N/A", color="dim")
        assert hs.label == "N/A"
        assert hs.color == "dim"

    def test_serialization_round_trip(self) -> None:
        components = {"gc": 90, "spill": 75, "skew": 100}
        hs = HealthScore(score=85, label="Good", color="blue", component_scores=components)
        data = hs.model_dump()
        restored = HealthScore.model_validate(data)
        assert restored.score == hs.score
        assert restored.label == hs.label
        assert restored.color == hs.color
        assert restored.component_scores == hs.component_scores


# ---------------------------------------------------------------------------
# Recommendation model
# ---------------------------------------------------------------------------


class TestRecommendation:
    def test_creation(self) -> None:
        rec = Recommendation(
            title="Reduce GC Pressure",
            description="Increase spark.executor.memory or enable Kryo serialization.",
            priority=1,
            category=InsightCategory.GC,
        )
        assert rec.title == "Reduce GC Pressure"
        assert rec.priority == 1
        assert rec.category == InsightCategory.GC

    def test_low_priority(self) -> None:
        rec = Recommendation(
            title="Consider partition tuning",
            description="Adjust spark.sql.shuffle.partitions for better parallelism.",
            priority=5,
            category=InsightCategory.PARTITION,
        )
        assert rec.priority == 5

    def test_serialization_round_trip(self) -> None:
        rec = Recommendation(
            title="Fix Memory Spill",
            description="Increase spark.sql.shuffle.partitions to reduce partition sizes.",
            priority=1,
            category=InsightCategory.SPILL,
        )
        data = rec.model_dump()
        restored = Recommendation.model_validate(data)
        assert restored.title == rec.title
        assert restored.description == rec.description
        assert restored.priority == rec.priority
        assert restored.category == rec.category

    def test_all_categories_allowed(self) -> None:
        """Every InsightCategory should be valid for a Recommendation."""
        for cat in InsightCategory:
            rec = Recommendation(
                title=f"Test {cat.value}",
                description="Test description.",
                priority=3,
                category=cat,
            )
            assert rec.category == cat


# ---------------------------------------------------------------------------
# DiagnosticReport model
# ---------------------------------------------------------------------------


class TestDiagnosticReport:
    @pytest.fixture()
    def empty_report(self) -> DiagnosticReport:
        return DiagnosticReport(
            health=HealthScore(score=100, label="Excellent", color="green"),
            timestamp=datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc),
        )

    @pytest.fixture()
    def populated_report(self) -> DiagnosticReport:
        insight = Insight(
            id="GC_001",
            category=InsightCategory.GC,
            severity=Severity.WARNING,
            title="High GC on executor 3",
            description="GC is at 7.2%.",
            metric_value=7.2,
            threshold_value=5.0,
            recommendation="Increase memory.",
            affected_entity="3",
        )
        rec = Recommendation(
            title="Reduce GC Pressure",
            description="Increase spark.executor.memory.",
            priority=3,
            category=InsightCategory.GC,
        )
        return DiagnosticReport(
            health=HealthScore(
                score=78,
                label="Good",
                color="blue",
                component_scores={
                    "gc": 60,
                    "spill": 100,
                    "skew": 100,
                    "utilization": 80,
                    "shuffle": 100,
                    "task_failures": 100,
                },
            ),
            insights=[insight],
            recommendations=[rec],
            timestamp=datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc),
            executor_count=4,
            active_stage_count=2,
            active_job_count=1,
        )

    def test_empty_report_defaults(self, empty_report: DiagnosticReport) -> None:
        assert empty_report.insights == []
        assert empty_report.recommendations == []
        assert empty_report.executor_count == 0
        assert empty_report.active_stage_count == 0
        assert empty_report.active_job_count == 0

    def test_empty_report_health(self, empty_report: DiagnosticReport) -> None:
        assert empty_report.health.score == 100
        assert empty_report.health.label == "Excellent"

    def test_empty_report_timestamp(self, empty_report: DiagnosticReport) -> None:
        assert empty_report.timestamp.year == 2026
        assert empty_report.timestamp.tzinfo == timezone.utc

    def test_populated_report_insights(self, populated_report: DiagnosticReport) -> None:
        assert len(populated_report.insights) == 1
        assert populated_report.insights[0].id == "GC_001"
        assert populated_report.insights[0].severity == Severity.WARNING

    def test_populated_report_recommendations(self, populated_report: DiagnosticReport) -> None:
        assert len(populated_report.recommendations) == 1
        assert populated_report.recommendations[0].priority == 3

    def test_populated_report_counts(self, populated_report: DiagnosticReport) -> None:
        assert populated_report.executor_count == 4
        assert populated_report.active_stage_count == 2
        assert populated_report.active_job_count == 1

    def test_populated_report_health_components(self, populated_report: DiagnosticReport) -> None:
        assert populated_report.health.component_scores["gc"] == 60
        assert populated_report.health.component_scores["spill"] == 100

    def test_serialization_round_trip(self, populated_report: DiagnosticReport) -> None:
        data = populated_report.model_dump()
        restored = DiagnosticReport.model_validate(data)
        assert restored.health.score == populated_report.health.score
        assert len(restored.insights) == len(populated_report.insights)
        assert len(restored.recommendations) == len(populated_report.recommendations)
        assert restored.executor_count == populated_report.executor_count
        assert restored.timestamp == populated_report.timestamp

    def test_multiple_insights_and_recommendations(self) -> None:
        insights = [
            Insight(
                id=f"GC_{i:03d}",
                category=InsightCategory.GC,
                severity=Severity.WARNING,
                title=f"GC issue {i}",
                description=f"Executor {i} has high GC.",
                metric_value=float(5 + i),
                threshold_value=5.0,
                recommendation="Increase memory.",
                affected_entity=str(i),
            )
            for i in range(1, 6)
        ]
        recs = [
            Recommendation(
                title=f"Rec {i}",
                description=f"Description {i}.",
                priority=i,
                category=InsightCategory.GC,
            )
            for i in range(1, 4)
        ]
        report = DiagnosticReport(
            health=HealthScore(score=55, label="Fair", color="yellow"),
            insights=insights,
            recommendations=recs,
            timestamp=datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc),
            executor_count=8,
            active_stage_count=5,
            active_job_count=3,
        )
        assert len(report.insights) == 5
        assert len(report.recommendations) == 3
        assert report.executor_count == 8
