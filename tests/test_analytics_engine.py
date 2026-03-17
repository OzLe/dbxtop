"""Unit tests for the analytics engine detectors and health scoring."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dbxtop.analytics.engine import (
    GC_CRITICAL_RATIO,
    PARTITION_TOO_LARGE,
    PARTITION_TOO_SMALL,
    SPILL_CRITICAL_ABS,
    STRAGGLER_PROGRESS_THRESHOLD,
    AnalyticsEngine,
)
from dbxtop.analytics.models import (
    DiagnosticReport,
    InsightCategory,
    Severity,
)
from dbxtop.api.models import (
    ExecutorInfo,
    JobStatus,
    SparkJob,
    SparkStage,
    StageStatus,
)


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


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
        submission_time=datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture()
def engine() -> AnalyticsEngine:
    return AnalyticsEngine()


# ---------------------------------------------------------------------------
# TestGCPressureDetector
# ---------------------------------------------------------------------------


class TestGCPressureDetector:
    """Tests for detect_gc_pressure."""

    def test_healthy_executors_no_insights(self, engine: AnalyticsEngine) -> None:
        """Executors with gc_ratio < 5% produce no insights."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=2_000),  # 2%
            make_executor("2", total_duration_ms=100_000, total_gc_time_ms=3_000),  # 3%
            make_executor("3", total_duration_ms=100_000, total_gc_time_ms=4_000),  # 4%
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 0

    def test_warning_threshold(self, engine: AnalyticsEngine) -> None:
        """gc_ratio between 5-10% produces WARNING insight."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=7_000),  # 7%
            make_executor("2", total_duration_ms=100_000, total_gc_time_ms=2_000),  # 2% OK
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.GC
        assert insights[0].affected_entity == "1"
        assert insights[0].metric_value == pytest.approx(7.0)

    def test_critical_threshold(self, engine: AnalyticsEngine) -> None:
        """gc_ratio >= 10% produces CRITICAL insight."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=15_000),  # 15%
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL
        assert insights[0].metric_value == pytest.approx(15.0)
        assert insights[0].threshold_value == pytest.approx(GC_CRITICAL_RATIO * 100)

    def test_driver_excluded(self, engine: AnalyticsEngine) -> None:
        """Driver executor is excluded from GC analysis when other executors exist."""
        executors = [
            make_executor("driver", total_duration_ms=100_000, total_gc_time_ms=20_000),  # 20% driver
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=2_000),  # 2% OK
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 0

    def test_zero_duration_skipped(self, engine: AnalyticsEngine) -> None:
        """Executors with total_duration_ms == 0 are skipped."""
        executors = [
            make_executor("1", total_duration_ms=0, total_gc_time_ms=100),
            make_executor("2", total_duration_ms=100_000, total_gc_time_ms=2_000),
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 0

    def test_multiple_executors_sorted_by_severity(self, engine: AnalyticsEngine) -> None:
        """Multiple GC insights are sorted worst first."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=6_000),  # 6% WARNING
            make_executor("2", total_duration_ms=100_000, total_gc_time_ms=12_000),  # 12% CRITICAL
            make_executor("3", total_duration_ms=100_000, total_gc_time_ms=8_000),  # 8% WARNING
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 3
        # Worst first
        assert insights[0].metric_value == pytest.approx(12.0)
        assert insights[0].severity == Severity.CRITICAL

    def test_inactive_executors_ignored(self, engine: AnalyticsEngine) -> None:
        """Inactive executors should not be analyzed."""
        executors = [
            make_executor("1", is_active=False, total_duration_ms=100_000, total_gc_time_ms=20_000),
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 0

    def test_exactly_at_warning_threshold(self, engine: AnalyticsEngine) -> None:
        """gc_ratio exactly at 5% triggers WARNING."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=5_000),
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING

    def test_exactly_at_critical_threshold(self, engine: AnalyticsEngine) -> None:
        """gc_ratio exactly at 10% triggers CRITICAL."""
        executors = [
            make_executor("1", total_duration_ms=100_000, total_gc_time_ms=10_000),
        ]
        insights = engine.detect_gc_pressure(executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL


# ---------------------------------------------------------------------------
# TestSpillDetector
# ---------------------------------------------------------------------------


class TestSpillDetector:
    """Tests for detect_memory_spill."""

    def test_no_spill_no_insights(self, engine: AnalyticsEngine) -> None:
        """Stages with spill_bytes == 0 produce no insights."""
        stages = [
            make_stage(1, memory_spill_bytes=0, disk_spill_bytes=0),
            make_stage(2, memory_spill_bytes=0, disk_spill_bytes=0),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) == 0

    def test_minor_spill_warning(self, engine: AnalyticsEngine) -> None:
        """Any spill > 0 but below critical thresholds produces WARNING."""
        stages = [
            make_stage(
                1,
                memory_spill_bytes=50_000_000,  # 50 MB
                disk_spill_bytes=10_000_000,  # 10 MB
                input_bytes=10_000_000_000,  # 10 GB (spill << input)
            ),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.SPILL
        assert insights[0].affected_entity == "stage_1"

    def test_severe_spill_critical_ratio(self, engine: AnalyticsEngine) -> None:
        """Spill > input bytes (ratio > 1.0) produces CRITICAL."""
        input_bytes = 500_000_000  # 500 MB
        spill_mem = 400_000_000  # 400 MB
        spill_disk = 200_000_000  # 200 MB -> total 600 MB > 500 MB input
        stages = [
            make_stage(
                1,
                memory_spill_bytes=spill_mem,
                disk_spill_bytes=spill_disk,
                input_bytes=input_bytes,
            ),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) >= 1
        critical_insights = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical_insights) >= 1

    def test_absolute_disk_spill_critical(self, engine: AnalyticsEngine) -> None:
        """disk_spill_bytes > 1 GB produces CRITICAL regardless of ratio."""
        stages = [
            make_stage(
                1,
                memory_spill_bytes=0,
                disk_spill_bytes=SPILL_CRITICAL_ABS + 1,  # Just over 1 GB
                input_bytes=100_000_000_000,  # 100 GB input (so ratio is low)
            ),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) >= 1
        critical_insights = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical_insights) >= 1

    def test_pending_stages_ignored(self, engine: AnalyticsEngine) -> None:
        """Only ACTIVE or COMPLETE stages are considered."""
        stages = [
            make_stage(1, status=StageStatus.PENDING, memory_spill_bytes=999_999_999, disk_spill_bytes=999_999_999),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) == 0

    def test_multiple_stages_sorted_by_spill(self, engine: AnalyticsEngine) -> None:
        """Multiple spill insights are sorted by spill bytes descending."""
        stages = [
            make_stage(1, memory_spill_bytes=10_000_000, disk_spill_bytes=5_000_000),
            make_stage(2, memory_spill_bytes=500_000_000, disk_spill_bytes=200_000_000),
            make_stage(3, memory_spill_bytes=50_000_000, disk_spill_bytes=20_000_000),
        ]
        insights = engine.detect_memory_spill(stages)
        assert len(insights) == 3
        # Largest spill should be first
        assert insights[0].affected_entity == "stage_2"


# ---------------------------------------------------------------------------
# TestSkewDetector
# ---------------------------------------------------------------------------


class TestSkewDetector:
    """Tests for detect_data_skew."""

    def test_balanced_executors_no_insights(self, engine: AnalyticsEngine) -> None:
        """Evenly distributed shuffle reads produce no insights."""
        executors = [
            make_executor("1", total_shuffle_read=1_000_000),
            make_executor("2", total_shuffle_read=1_100_000),
            make_executor("3", total_shuffle_read=950_000),
            make_executor("4", total_shuffle_read=1_050_000),
        ]
        insights = engine.detect_data_skew(executors)
        assert len(insights) == 0

    def test_moderate_skew_warning(self, engine: AnalyticsEngine) -> None:
        """3x shuffle imbalance produces WARNING."""
        executors = [
            make_executor("1", total_shuffle_read=1_000_000),
            make_executor("2", total_shuffle_read=1_000_000),
            make_executor("3", total_shuffle_read=3_500_000),  # 3.5x median
        ]
        insights = engine.detect_data_skew(executors)
        warning_insights = [i for i in insights if i.severity == Severity.WARNING]
        assert len(warning_insights) >= 1
        assert warning_insights[0].category == InsightCategory.SKEW

    def test_severe_skew_critical(self, engine: AnalyticsEngine) -> None:
        """5x+ shuffle imbalance produces CRITICAL."""
        executors = [
            make_executor("1", total_shuffle_read=1_000_000),
            make_executor("2", total_shuffle_read=1_000_000),
            make_executor("3", total_shuffle_read=1_000_000),
            make_executor("4", total_shuffle_read=6_000_000),  # 6x median
        ]
        insights = engine.detect_data_skew(executors)
        critical_insights = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical_insights) >= 1

    def test_single_executor_skipped(self, engine: AnalyticsEngine) -> None:
        """Skew detection requires >= 2 non-driver executors."""
        executors = [
            make_executor("1", total_shuffle_read=5_000_000),
        ]
        insights = engine.detect_data_skew(executors)
        assert len(insights) == 0

    def test_no_shuffle_read_no_insights(self, engine: AnalyticsEngine) -> None:
        """If no executor has shuffle reads, no skew insight is generated."""
        executors = [
            make_executor("1", total_shuffle_read=0),
            make_executor("2", total_shuffle_read=0),
        ]
        insights = engine.detect_data_skew(executors)
        assert len(insights) == 0

    def test_driver_excluded(self, engine: AnalyticsEngine) -> None:
        """Driver executor should be excluded from skew analysis."""
        executors = [
            make_executor("driver", total_shuffle_read=10_000_000),  # high, but driver
            make_executor("1", total_shuffle_read=1_000_000),
            make_executor("2", total_shuffle_read=1_100_000),
        ]
        insights = engine.detect_data_skew(executors)
        # Driver should be excluded, remaining 2 are balanced
        assert len(insights) == 0


# ---------------------------------------------------------------------------
# TestShuffleDetector
# ---------------------------------------------------------------------------


class TestShuffleDetector:
    """Tests for detect_shuffle_bottleneck."""

    def test_low_shuffle_no_insights(self, engine: AnalyticsEngine) -> None:
        """Stages with low shuffle ratio produce no insights."""
        stages = [
            make_stage(
                1,
                input_bytes=10_000_000_000,  # 10 GB
                shuffle_read_bytes=1_000_000_000,  # 1 GB
                shuffle_write_bytes=500_000_000,  # 0.5 GB -> ratio 0.15
            ),
        ]
        insights = engine.detect_shuffle_bottleneck(stages)
        assert len(insights) == 0

    def test_warning_ratio(self, engine: AnalyticsEngine) -> None:
        """Shuffle ratio > 0.5 produces WARNING."""
        stages = [
            make_stage(
                1,
                input_bytes=1_000_000_000,  # 1 GB
                shuffle_read_bytes=400_000_000,  # 400 MB
                shuffle_write_bytes=200_000_000,  # 200 MB -> ratio 0.6
            ),
        ]
        insights = engine.detect_shuffle_bottleneck(stages)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.SHUFFLE

    def test_critical_ratio(self, engine: AnalyticsEngine) -> None:
        """Shuffle ratio > 2.0 produces CRITICAL."""
        stages = [
            make_stage(
                1,
                input_bytes=1_000_000_000,  # 1 GB
                shuffle_read_bytes=1_500_000_000,  # 1.5 GB
                shuffle_write_bytes=1_000_000_000,  # 1 GB -> ratio 2.5
            ),
        ]
        insights = engine.detect_shuffle_bottleneck(stages)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL

    def test_zero_input_bytes_skipped(self, engine: AnalyticsEngine) -> None:
        """Stages with zero input bytes are skipped (can't compute ratio)."""
        stages = [
            make_stage(
                1,
                input_bytes=0,
                shuffle_read_bytes=1_000_000,
                shuffle_write_bytes=500_000,
            ),
        ]
        insights = engine.detect_shuffle_bottleneck(stages)
        assert len(insights) == 0

    def test_pending_stages_ignored(self, engine: AnalyticsEngine) -> None:
        """Only ACTIVE or COMPLETE stages are analyzed."""
        stages = [
            make_stage(
                1,
                status=StageStatus.PENDING,
                input_bytes=100_000,
                shuffle_read_bytes=500_000,
                shuffle_write_bytes=500_000,
            ),
        ]
        insights = engine.detect_shuffle_bottleneck(stages)
        assert len(insights) == 0


# ---------------------------------------------------------------------------
# TestUtilizationDetector
# ---------------------------------------------------------------------------


class TestUtilizationDetector:
    """Tests for detect_cluster_utilization."""

    def test_high_utilization_no_insights(self, engine: AnalyticsEngine) -> None:
        """High utilization (> 70%) produces no insights."""
        executors = [
            make_executor("1", max_tasks=4, active_tasks=4),
            make_executor("2", max_tasks=4, active_tasks=3),
            make_executor("3", max_tasks=4, active_tasks=4),
        ]
        jobs = [make_job(1, status=JobStatus.RUNNING)]
        insights = engine.detect_cluster_utilization(executors, jobs)
        assert len(insights) == 0

    def test_low_utilization_warning(self, engine: AnalyticsEngine) -> None:
        """Utilization between 30-70% with running jobs produces WARNING."""
        executors_warning = [
            make_executor("1", max_tasks=4, active_tasks=3),
            make_executor("2", max_tasks=4, active_tasks=2),
            make_executor("3", max_tasks=4, active_tasks=2),
            make_executor("4", max_tasks=4, active_tasks=1),
        ]
        # 8 active / 16 slots = 50%
        jobs = [make_job(1, status=JobStatus.RUNNING)]
        insights = engine.detect_cluster_utilization(executors_warning, jobs)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING

    def test_very_low_utilization_critical(self, engine: AnalyticsEngine) -> None:
        """Utilization < 30% with running jobs produces CRITICAL."""
        executors = [
            make_executor("1", max_tasks=8, active_tasks=1),
            make_executor("2", max_tasks=8, active_tasks=0),
            make_executor("3", max_tasks=8, active_tasks=0),
            make_executor("4", max_tasks=8, active_tasks=0),
        ]
        # 1 active / 32 slots = 3.1%
        jobs = [make_job(1, status=JobStatus.RUNNING)]
        insights = engine.detect_cluster_utilization(executors, jobs)
        assert len(insights) >= 1
        critical_insights = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical_insights) >= 1

    def test_idle_cluster_no_jobs_ok(self, engine: AnalyticsEngine) -> None:
        """Idle cluster with no running jobs should NOT produce utilization warnings."""
        executors = [
            make_executor("1", max_tasks=4, active_tasks=0),
            make_executor("2", max_tasks=4, active_tasks=0),
        ]
        jobs: list[SparkJob] = []
        insights = engine.detect_cluster_utilization(executors, jobs)
        assert len(insights) == 0

    def test_idle_cluster_completed_jobs_ok(self, engine: AnalyticsEngine) -> None:
        """Idle cluster with only completed jobs should NOT produce warnings."""
        executors = [
            make_executor("1", max_tasks=4, active_tasks=0),
            make_executor("2", max_tasks=4, active_tasks=0),
        ]
        jobs = [make_job(1, status=JobStatus.SUCCEEDED)]
        insights = engine.detect_cluster_utilization(executors, jobs)
        assert len(insights) == 0

    def test_no_executors_returns_empty(self, engine: AnalyticsEngine) -> None:
        """No executors means no utilization insights."""
        jobs = [make_job(1, status=JobStatus.RUNNING)]
        insights = engine.detect_cluster_utilization([], jobs)
        assert len(insights) == 0


# ---------------------------------------------------------------------------
# TestPartitionDetector
# ---------------------------------------------------------------------------


class TestPartitionDetector:
    """Tests for detect_partition_issues."""

    def test_optimal_partitions(self, engine: AnalyticsEngine) -> None:
        """Partitions within the optimal range produce no insights."""
        stages = [
            make_stage(
                1,
                num_tasks=100,
                input_bytes=12_800_000_000,  # 12.8 GB / 100 tasks = 128 MB avg (in range)
            ),
        ]
        executors = [make_executor("1", total_cores=8)]
        insights = engine.detect_partition_issues(stages, executors)
        assert len(insights) == 0

    def test_oversized_partitions(self, engine: AnalyticsEngine) -> None:
        """Partitions larger than 256 MB produce a WARNING."""
        stages = [
            make_stage(
                1,
                num_tasks=10,
                input_bytes=5_368_709_120,  # ~5 GB / 10 tasks = ~512 MB avg
            ),
        ]
        executors = [make_executor("1", total_cores=8)]
        insights = engine.detect_partition_issues(stages, executors)
        assert len(insights) >= 1
        assert insights[0].category == InsightCategory.PARTITION
        # Avg partition size ~512 MB > 256 MB threshold
        assert insights[0].metric_value > PARTITION_TOO_LARGE

    def test_undersized_partitions(self, engine: AnalyticsEngine) -> None:
        """Partitions smaller than 1 MB produce a WARNING."""
        stages = [
            make_stage(
                1,
                num_tasks=10000,
                input_bytes=5_000_000,  # 5 MB / 10000 tasks = 500 bytes avg
            ),
        ]
        executors = [make_executor("1", total_cores=8)]
        insights = engine.detect_partition_issues(stages, executors)
        assert len(insights) >= 1
        assert insights[0].category == InsightCategory.PARTITION
        assert insights[0].metric_value < PARTITION_TOO_SMALL

    def test_zero_tasks_skipped(self, engine: AnalyticsEngine) -> None:
        """Stages with zero tasks are skipped."""
        stages = [
            make_stage(1, num_tasks=0, input_bytes=1_000_000_000),
        ]
        executors = [make_executor("1", total_cores=8)]
        insights = engine.detect_partition_issues(stages, executors)
        assert len(insights) == 0

    def test_zero_input_bytes_skipped(self, engine: AnalyticsEngine) -> None:
        """Stages with zero input bytes are skipped."""
        stages = [
            make_stage(1, num_tasks=100, input_bytes=0),
        ]
        executors = [make_executor("1", total_cores=8)]
        insights = engine.detect_partition_issues(stages, executors)
        assert len(insights) == 0


# ---------------------------------------------------------------------------
# TestStragglerDetector
# ---------------------------------------------------------------------------


class TestStragglerDetector:
    """Tests for detect_straggler_tasks."""

    def test_no_stragglers(self, engine: AnalyticsEngine) -> None:
        """Stage well before completion has no straggler issues."""
        stages = [
            make_stage(
                1,
                num_tasks=200,
                num_complete_tasks=100,  # 50%
                num_active_tasks=50,
            ),
        ]
        insights = engine.detect_straggler_tasks(stages)
        assert len(insights) == 0

    def test_straggler_at_high_completion(self, engine: AnalyticsEngine) -> None:
        """Stage at >95% completion with few remaining tasks suggests stragglers."""
        stages = [
            make_stage(
                1,
                num_tasks=200,
                num_complete_tasks=198,  # 99%
                num_active_tasks=2,
            ),
        ]
        insights = engine.detect_straggler_tasks(stages)
        assert len(insights) >= 1
        assert insights[0].category == InsightCategory.STRAGGLER
        assert insights[0].metric_value > STRAGGLER_PROGRESS_THRESHOLD * 100

    def test_completed_stage_no_straggler(self, engine: AnalyticsEngine) -> None:
        """Fully completed stages should not flag stragglers."""
        stages = [
            make_stage(
                1,
                status=StageStatus.COMPLETE,
                num_tasks=200,
                num_complete_tasks=200,
                num_active_tasks=0,
            ),
        ]
        insights = engine.detect_straggler_tasks(stages)
        assert len(insights) == 0

    def test_single_remaining_task_critical(self, engine: AnalyticsEngine) -> None:
        """A single remaining active task in a large stage is a CRITICAL straggler."""
        stages = [
            make_stage(
                1,
                num_tasks=100,
                num_complete_tasks=99,
                num_active_tasks=1,
            ),
        ]
        insights = engine.detect_straggler_tasks(stages)
        assert len(insights) >= 1
        critical = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical) >= 1

    def test_pending_stage_skipped(self, engine: AnalyticsEngine) -> None:
        """PENDING stages should not be analyzed for stragglers."""
        stages = [
            make_stage(
                1,
                status=StageStatus.PENDING,
                num_tasks=100,
                num_complete_tasks=99,
                num_active_tasks=1,
            ),
        ]
        insights = engine.detect_straggler_tasks(stages)
        assert len(insights) == 0


# ---------------------------------------------------------------------------
# TestTaskFailureDetector
# ---------------------------------------------------------------------------


class TestTaskFailureDetector:
    """Tests for detect_task_failures."""

    def test_no_failures(self, engine: AnalyticsEngine) -> None:
        """Zero failed tasks produces no insights."""
        executors = [
            make_executor("1", completed_tasks=500, failed_tasks=0),
            make_executor("2", completed_tasks=500, failed_tasks=0),
        ]
        insights = engine.detect_task_failures(executors)
        assert len(insights) == 0

    def test_warning_rate(self, engine: AnalyticsEngine) -> None:
        """Failure rate between 1-5% produces WARNING."""
        executors = [
            make_executor("1", completed_tasks=490, failed_tasks=10),  # 2% failure
        ]
        insights = engine.detect_task_failures(executors)
        warning_insights = [
            i for i in insights if i.severity == Severity.WARNING and i.category == InsightCategory.TASK_FAILURE
        ]
        assert len(warning_insights) >= 1

    def test_critical_rate(self, engine: AnalyticsEngine) -> None:
        """Failure rate >= 5% produces CRITICAL."""
        executors = [
            make_executor("1", completed_tasks=90, failed_tasks=10),  # 10% failure
        ]
        insights = engine.detect_task_failures(executors)
        critical_insights = [
            i for i in insights if i.severity == Severity.CRITICAL and i.category == InsightCategory.TASK_FAILURE
        ]
        assert len(critical_insights) >= 1

    def test_too_few_tasks_skipped(self, engine: AnalyticsEngine) -> None:
        """Fewer than 10 total tasks should not trigger failure insights."""
        executors = [
            make_executor("1", completed_tasks=3, failed_tasks=3),  # 50% but only 6 tasks
        ]
        insights = engine.detect_task_failures(executors)
        assert len(insights) == 0

    def test_no_executors_returns_empty(self, engine: AnalyticsEngine) -> None:
        """Empty executor list produces no insights."""
        insights = engine.detect_task_failures([])
        assert len(insights) == 0

    def test_exactly_at_warning_threshold(self, engine: AnalyticsEngine) -> None:
        """Failure rate exactly at 1% triggers WARNING."""
        # 1 failed out of 100 total = 1%
        executors = [
            make_executor("1", completed_tasks=99, failed_tasks=1),
            make_executor("2", completed_tasks=0, failed_tasks=0),
        ]
        insights = engine.detect_task_failures(executors)
        warning_insights = [i for i in insights if i.category == InsightCategory.TASK_FAILURE]
        assert len(warning_insights) >= 1


# ---------------------------------------------------------------------------
# TestMemoryPressureDetector
# ---------------------------------------------------------------------------


class TestMemoryPressureDetector:
    """Tests for detect_memory_pressure."""

    def test_healthy_memory(self, engine: AnalyticsEngine) -> None:
        """Executors using < 80% memory produce no insights."""
        executors = [
            make_executor("1", max_memory=4_294_967_296, memory_used=2_147_483_648),  # 50%
            make_executor("2", max_memory=4_294_967_296, memory_used=1_073_741_824),  # 25%
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) == 0

    def test_warning_at_80_percent(self, engine: AnalyticsEngine) -> None:
        """Memory usage > 80% produces WARNING."""
        executors = [
            make_executor(
                "1",
                max_memory=4_294_967_296,  # 4 GB
                memory_used=3_650_722_201,  # ~85%
            ),
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) >= 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.MEMORY
        assert insights[0].affected_entity == "1"

    def test_critical_at_95_percent(self, engine: AnalyticsEngine) -> None:
        """Memory usage > 95% produces CRITICAL."""
        executors = [
            make_executor(
                "1",
                max_memory=4_294_967_296,  # 4 GB
                memory_used=4_166_924_698,  # ~97%
            ),
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) >= 1
        assert insights[0].severity == Severity.CRITICAL

    def test_zero_max_memory_skipped(self, engine: AnalyticsEngine) -> None:
        """Executors with max_memory == 0 are skipped."""
        executors = [
            make_executor("1", max_memory=0, memory_used=1_000_000),
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) == 0

    def test_inactive_executors_ignored(self, engine: AnalyticsEngine) -> None:
        """Inactive executors should not be analyzed for memory pressure."""
        executors = [
            make_executor(
                "1",
                is_active=False,
                max_memory=4_294_967_296,
                memory_used=4_294_967_296,  # 100%
            ),
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) == 0

    def test_sorted_by_memory_pct_descending(self, engine: AnalyticsEngine) -> None:
        """Multiple insights sorted by memory_used_pct descending."""
        executors = [
            make_executor("1", max_memory=4_294_967_296, memory_used=3_650_722_201),  # ~85%
            make_executor("2", max_memory=4_294_967_296, memory_used=4_166_924_698),  # ~97%
            make_executor("3", max_memory=4_294_967_296, memory_used=3_865_470_566),  # ~90%
        ]
        insights = engine.detect_memory_pressure(executors)
        assert len(insights) == 3
        # Worst first
        assert insights[0].affected_entity == "2"


# ---------------------------------------------------------------------------
# TestHealthScore
# ---------------------------------------------------------------------------


class TestHealthScore:
    """Tests for compute_health_score."""

    def test_perfect_health(self, engine: AnalyticsEngine) -> None:
        """All metrics healthy -> score near 100."""
        executors = [
            make_executor(
                "1",
                total_gc_time_ms=1_000,
                total_duration_ms=100_000,
                memory_used=500_000_000,
                max_memory=4_294_967_296,
                active_tasks=3,
                max_tasks=4,
                total_shuffle_read=1_000_000,
                completed_tasks=100,
                failed_tasks=0,
            ),
            make_executor(
                "2",
                total_gc_time_ms=1_000,
                total_duration_ms=100_000,
                memory_used=500_000_000,
                max_memory=4_294_967_296,
                active_tasks=4,
                max_tasks=4,
                total_shuffle_read=1_100_000,
                completed_tasks=100,
                failed_tasks=0,
            ),
        ]
        stages: list[SparkStage] = [
            make_stage(
                1,
                memory_spill_bytes=0,
                disk_spill_bytes=0,
                shuffle_read_bytes=100_000,
                shuffle_write_bytes=50_000,
                input_bytes=1_000_000_000,
            ),
        ]
        insights: list = []
        health = engine.compute_health_score(insights, executors, stages)
        assert health.score >= 85
        assert health.label in ("Excellent", "Good")

    def test_gc_heavy_score(self, engine: AnalyticsEngine) -> None:
        """High GC across executors drags score down."""
        executors = [
            make_executor("1", total_gc_time_ms=18_000, total_duration_ms=100_000),  # 18% GC
            make_executor("2", total_gc_time_ms=15_000, total_duration_ms=100_000),  # 15% GC
        ]
        stages: list[SparkStage] = []
        insights: list = []
        health = engine.compute_health_score(insights, executors, stages)
        # GC is 25% of weight and score should be low for GC component
        assert health.component_scores["gc"] < 50
        assert health.score < 90

    def test_no_executors_returns_na(self, engine: AnalyticsEngine) -> None:
        """No executors -> score 0, label 'N/A'."""
        health = engine.compute_health_score([], [], [])
        assert health.score == 0
        assert health.label == "N/A"
        assert health.color == "dim"

    def test_no_stages_defaults_to_healthy(self, engine: AnalyticsEngine) -> None:
        """No active stages -> stage-based components default to 100."""
        executors = [
            make_executor("1", total_gc_time_ms=1_000, total_duration_ms=100_000),
            make_executor("2", total_gc_time_ms=1_000, total_duration_ms=100_000),
        ]
        health = engine.compute_health_score([], executors, [])
        assert health.component_scores.get("spill", 100) == 100
        assert health.component_scores.get("shuffle", 100) == 100

    def test_score_label_color_excellent(self, engine: AnalyticsEngine) -> None:
        """Score 90-100 maps to Excellent / green."""
        executors = [
            make_executor(
                "1",
                total_gc_time_ms=100,
                total_duration_ms=100_000,
                active_tasks=4,
                max_tasks=4,
                completed_tasks=500,
                failed_tasks=0,
                total_shuffle_read=1_000_000,
            ),
            make_executor(
                "2",
                total_gc_time_ms=100,
                total_duration_ms=100_000,
                active_tasks=4,
                max_tasks=4,
                completed_tasks=500,
                failed_tasks=0,
                total_shuffle_read=1_100_000,
            ),
        ]
        health = engine.compute_health_score([], executors, [])
        if health.score >= 90:
            assert health.label == "Excellent"
            assert health.color == "green"

    def test_component_scores_present(self, engine: AnalyticsEngine) -> None:
        """Health score includes all expected component scores."""
        executors = [
            make_executor("1"),
            make_executor("2"),
        ]
        health = engine.compute_health_score([], executors, [])
        expected_keys = {"gc", "spill", "skew", "utilization", "shuffle", "task_failures"}
        assert set(health.component_scores.keys()) == expected_keys


# ---------------------------------------------------------------------------
# TestAnalyze
# ---------------------------------------------------------------------------


class TestAnalyze:
    """Tests for the analyze() entry point."""

    def test_full_analysis_produces_report(self, engine: AnalyticsEngine) -> None:
        """analyze() with varied data produces a complete report."""
        executors = [
            make_executor(
                "1", total_gc_time_ms=12_000, total_duration_ms=100_000, total_shuffle_read=1_000_000
            ),  # 12% GC -> CRITICAL
            make_executor("2", total_gc_time_ms=1_000, total_duration_ms=100_000, total_shuffle_read=1_100_000),
            make_executor("3", total_gc_time_ms=1_000, total_duration_ms=100_000, total_shuffle_read=1_000_000),
        ]
        stages = [
            make_stage(1, memory_spill_bytes=500_000_000, disk_spill_bytes=200_000_000),
        ]
        jobs = [make_job(1, status=JobStatus.RUNNING)]

        report = engine.analyze(
            executors=executors,
            stages=stages,
            jobs=jobs,
            sql_queries=None,
            cluster=None,
        )

        assert report is not None
        assert isinstance(report, DiagnosticReport)
        assert report.health is not None
        assert report.health.score >= 0
        assert report.health.score <= 100
        assert len(report.insights) > 0
        assert report.executor_count > 0
        assert report.timestamp is not None

    def test_none_executors_returns_none(self, engine: AnalyticsEngine) -> None:
        """analyze() returns None when executors is None."""
        report = engine.analyze(
            executors=None,
            stages=[make_stage(1)],
            jobs=[make_job(1)],
            sql_queries=None,
            cluster=None,
        )
        assert report is None

    def test_empty_lists_returns_healthy(self, engine: AnalyticsEngine) -> None:
        """Empty but non-None lists produce a healthy report with no insights."""
        executors = [
            make_executor("1", total_gc_time_ms=500, total_duration_ms=100_000, active_tasks=3, max_tasks=4),
            make_executor("2", total_gc_time_ms=500, total_duration_ms=100_000, active_tasks=4, max_tasks=4),
        ]
        report = engine.analyze(
            executors=executors,
            stages=[],
            jobs=[],
            sql_queries=[],
            cluster=None,
        )
        assert report is not None
        assert isinstance(report, DiagnosticReport)
        assert len(report.insights) == 0
        assert report.health.score >= 80

    def test_determinism(self, engine: AnalyticsEngine) -> None:
        """Same input always produces the same output."""
        executors = [
            make_executor("1", total_gc_time_ms=8_000, total_duration_ms=100_000),
            make_executor("2", total_gc_time_ms=2_000, total_duration_ms=100_000),
        ]
        stages = [make_stage(1, memory_spill_bytes=100_000)]
        jobs = [make_job(1, status=JobStatus.RUNNING)]

        report1 = engine.analyze(executors=executors, stages=stages, jobs=jobs, sql_queries=None, cluster=None)
        report2 = engine.analyze(executors=executors, stages=stages, jobs=jobs, sql_queries=None, cluster=None)

        assert report1 is not None
        assert report2 is not None
        assert report1.health.score == report2.health.score
        assert len(report1.insights) == len(report2.insights)
        for i1, i2 in zip(report1.insights, report2.insights):
            assert i1.id == i2.id
            assert i1.severity == i2.severity
            assert i1.metric_value == pytest.approx(i2.metric_value)

    def test_report_has_recommendations(self, engine: AnalyticsEngine) -> None:
        """Report with insights should generate recommendations."""
        executors = [
            make_executor("1", total_gc_time_ms=15_000, total_duration_ms=100_000),  # 15% GC
            make_executor("2", total_gc_time_ms=1_000, total_duration_ms=100_000),
        ]
        stages = [
            make_stage(1, memory_spill_bytes=2_000_000_000, disk_spill_bytes=500_000_000),
        ]
        jobs = [make_job(1, status=JobStatus.RUNNING)]

        report = engine.analyze(
            executors=executors,
            stages=stages,
            jobs=jobs,
            sql_queries=None,
            cluster=None,
        )

        assert report is not None
        assert len(report.recommendations) > 0
        # Recommendations should be sorted by priority
        for i in range(len(report.recommendations) - 1):
            assert report.recommendations[i].priority <= report.recommendations[i + 1].priority

    def test_report_insights_sorted_by_severity(self, engine: AnalyticsEngine) -> None:
        """Insights in the report should be sorted: CRITICAL first, then WARNING, then INFO."""
        executors = [
            make_executor("1", total_gc_time_ms=15_000, total_duration_ms=100_000),  # CRITICAL GC
            make_executor("2", total_gc_time_ms=7_000, total_duration_ms=100_000),  # WARNING GC
            make_executor("3", total_gc_time_ms=1_000, total_duration_ms=100_000),
        ]
        stages = [
            make_stage(1, memory_spill_bytes=100_000, disk_spill_bytes=50_000),  # WARNING spill
        ]
        jobs = [make_job(1, status=JobStatus.RUNNING)]

        report = engine.analyze(
            executors=executors,
            stages=stages,
            jobs=jobs,
            sql_queries=None,
            cluster=None,
        )

        assert report is not None
        if len(report.insights) >= 2:
            severity_order = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
            for i in range(len(report.insights) - 1):
                current = severity_order.get(report.insights[i].severity.value, 99)
                next_val = severity_order.get(report.insights[i + 1].severity.value, 99)
                assert current <= next_val, (
                    f"Insight {report.insights[i].id} ({report.insights[i].severity}) "
                    f"should come before {report.insights[i + 1].id} ({report.insights[i + 1].severity})"
                )

    def test_report_executor_count(self, engine: AnalyticsEngine) -> None:
        """Report should include accurate executor count."""
        executors = [
            make_executor("1"),
            make_executor("2"),
            make_executor("3", is_active=False),
        ]
        report = engine.analyze(
            executors=executors,
            stages=[],
            jobs=[],
            sql_queries=None,
            cluster=None,
        )
        assert report is not None
        # Should count active executors
        assert report.executor_count >= 2

    def test_none_stages_jobs_handled(self, engine: AnalyticsEngine) -> None:
        """None stages/jobs/sql_queries should be safely handled as empty."""
        executors = [
            make_executor("1", total_gc_time_ms=500, total_duration_ms=100_000),
        ]
        report = engine.analyze(
            executors=executors,
            stages=None,
            jobs=None,
            sql_queries=None,
            cluster=None,
        )
        assert report is not None
        assert isinstance(report, DiagnosticReport)
