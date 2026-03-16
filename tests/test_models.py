"""Unit tests for Pydantic models and format utilities."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dbxtop.api.models import (
    ClusterInfo,
    ClusterState,
    ExecutorInfo,
    RDDInfo,
    SparkJob,
    SparkStage,
    format_bytes,
    format_duration,
    format_timestamp,
)


# ---------------------------------------------------------------------------
# format_bytes
# ---------------------------------------------------------------------------


class TestFormatBytes:
    def test_zero(self) -> None:
        assert format_bytes(0) == "0 B"

    def test_small_bytes(self) -> None:
        assert format_bytes(512) == "512 B"

    def test_one_kb(self) -> None:
        assert format_bytes(1024) == "1.0 KB"

    def test_one_mb(self) -> None:
        assert format_bytes(1024 * 1024) == "1.0 MB"

    def test_one_gb(self) -> None:
        assert format_bytes(1024**3) == "1.0 GB"

    def test_one_tb(self) -> None:
        assert format_bytes(1024**4) == "1.0 TB"

    def test_fractional_mb(self) -> None:
        assert format_bytes(int(2.5 * 1024 * 1024)) == "2.5 MB"

    def test_negative(self) -> None:
        assert format_bytes(-1024) == "-1.0 KB"

    def test_very_large(self) -> None:
        result = format_bytes(1024**5)
        assert "PB" in result


# ---------------------------------------------------------------------------
# format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_zero(self) -> None:
        assert format_duration(0) == "0s"

    def test_seconds(self) -> None:
        assert format_duration(5000) == "5s"

    def test_minutes_seconds(self) -> None:
        assert format_duration(125_000) == "2m 5s"

    def test_hours_minutes(self) -> None:
        assert format_duration(3_660_000) == "1h 1m"

    def test_days_hours_minutes(self) -> None:
        assert format_duration(90_060_000) == "1d 1h 1m"

    def test_negative(self) -> None:
        assert format_duration(-5000) == "-5s"

    def test_exactly_one_minute(self) -> None:
        assert format_duration(60_000) == "1m"

    def test_sub_second_rounds_down(self) -> None:
        assert format_duration(999) == "0s"


# ---------------------------------------------------------------------------
# format_timestamp
# ---------------------------------------------------------------------------


class TestFormatTimestamp:
    def test_none(self) -> None:
        assert format_timestamp(None) == "--"

    def test_today(self) -> None:
        now = datetime.now(timezone.utc)
        result = format_timestamp(now)
        assert ":" in result  # HH:MM:SS format

    def test_old_date(self) -> None:
        old = datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc)
        result = format_timestamp(old)
        assert "Jan" in result
        assert "15" in result


# ---------------------------------------------------------------------------
# ClusterInfo computed fields
# ---------------------------------------------------------------------------


class TestClusterInfo:
    def test_uptime_no_start_time(self) -> None:
        info = ClusterInfo(cluster_id="abc")
        assert info.uptime_seconds == 0

    def test_uptime_with_start_time(self) -> None:
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        info = ClusterInfo(cluster_id="abc", start_time=start)
        assert info.uptime_seconds > 0

    def test_default_state(self) -> None:
        info = ClusterInfo(cluster_id="abc")
        assert info.state == ClusterState.UNKNOWN

    def test_round_trip(self) -> None:
        info = ClusterInfo(
            cluster_id="0311-160038-vjuv0ah9",
            cluster_name="test-cluster",
            state=ClusterState.RUNNING,
            num_workers=4,
            total_cores=16,
            total_memory_mb=65536,
        )
        d = info.model_dump()
        restored = ClusterInfo.model_validate(d)
        assert restored.cluster_id == info.cluster_id
        assert restored.state == ClusterState.RUNNING
        assert restored.num_workers == 4


# ---------------------------------------------------------------------------
# ExecutorInfo computed fields
# ---------------------------------------------------------------------------


class TestExecutorInfo:
    def test_is_driver(self) -> None:
        exe = ExecutorInfo(executor_id="driver")
        assert exe.is_driver is True

    def test_is_not_driver(self) -> None:
        exe = ExecutorInfo(executor_id="1")
        assert exe.is_driver is False

    def test_gc_ratio_zero_duration(self) -> None:
        exe = ExecutorInfo(executor_id="1", total_duration_ms=0, total_gc_time_ms=100)
        assert exe.gc_ratio == 0.0

    def test_gc_ratio_normal(self) -> None:
        exe = ExecutorInfo(executor_id="1", total_duration_ms=1000, total_gc_time_ms=100)
        assert exe.gc_ratio == pytest.approx(0.1)

    def test_memory_used_pct_zero_max(self) -> None:
        exe = ExecutorInfo(executor_id="1", max_memory=0, memory_used=100)
        assert exe.memory_used_pct == 0.0

    def test_memory_used_pct_normal(self) -> None:
        exe = ExecutorInfo(executor_id="1", max_memory=1000, memory_used=250)
        assert exe.memory_used_pct == pytest.approx(25.0)


# ---------------------------------------------------------------------------
# SparkStage computed field
# ---------------------------------------------------------------------------


class TestSparkStage:
    def test_spill_bytes(self) -> None:
        stage = SparkStage(stage_id=1, memory_spill_bytes=100, disk_spill_bytes=200)
        assert stage.spill_bytes == 300


# ---------------------------------------------------------------------------
# RDDInfo computed field
# ---------------------------------------------------------------------------


class TestRDDInfo:
    def test_fraction_cached_zero_partitions(self) -> None:
        rdd = RDDInfo(rdd_id=1, num_partitions=0, num_cached_partitions=0)
        assert rdd.fraction_cached == 0.0

    def test_fraction_cached_normal(self) -> None:
        rdd = RDDInfo(rdd_id=1, num_partitions=10, num_cached_partitions=7)
        assert rdd.fraction_cached == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# SparkJob round-trip
# ---------------------------------------------------------------------------


class TestSparkJob:
    def test_round_trip(self) -> None:
        job = SparkJob(job_id=42, name="test-job", num_tasks=100, num_completed_tasks=50)
        d = job.model_dump()
        restored = SparkJob.model_validate(d)
        assert restored.job_id == 42
        assert restored.num_tasks == 100
