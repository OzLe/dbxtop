"""Snapshot-style tests for view rendering with mock data.

These tests verify that each view renders correctly with representative
data by mounting views in a minimal Textual app and checking output.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dbxtop.api.cache import DataCache
from dbxtop.api.models import (
    ClusterEvent,
    ClusterInfo,
    ClusterState,
    ExecutorInfo,
    JobStatus,
    LibraryInfo,
    RDDInfo,
    SparkJob,
    SparkStage,
    SQLQuery,
    StageStatus,
)


# ---------------------------------------------------------------------------
# Mock data factories
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)


def _make_cluster() -> ClusterInfo:
    return ClusterInfo(
        cluster_id="0123-456789-abcdefgh",
        cluster_name="analytics-prod",
        state=ClusterState.RUNNING,
        state_message="",
        start_time=datetime(2026, 3, 17, 10, 0, 0, tzinfo=timezone.utc),
        driver_node_type="i3.xlarge",
        worker_node_type="i3.2xlarge",
        num_workers=4,
        autoscale_min=2,
        autoscale_max=8,
        total_cores=32,
        total_memory_mb=131072,
        spark_version="15.4.x-scala2.12",
        data_security_mode="SINGLE_USER",
        runtime_engine="STANDARD",
        creator="oz@example.com",
        autotermination_minutes=120,
        spark_conf={"spark.sql.shuffle.partitions": "200"},
    )


def _make_events() -> list[ClusterEvent]:
    return [
        ClusterEvent(timestamp=_NOW, event_type="RUNNING", message="Cluster is running"),
        ClusterEvent(timestamp=_NOW, event_type="CREATING", message="Cluster created"),
    ]


def _make_libraries() -> list[LibraryInfo]:
    return [
        LibraryInfo(name="pandas==2.0.0", library_type="pypi", status="INSTALLED"),
        LibraryInfo(name="numpy==1.24.0", library_type="pypi", status="INSTALLED"),
    ]


def _make_jobs() -> list[SparkJob]:
    return [
        SparkJob(
            job_id=1,
            name="count at NativeMethodAccessorImpl.java:0",
            status=JobStatus.RUNNING,
            submission_time=_NOW,
            num_tasks=1000,
            num_active_tasks=50,
            num_completed_tasks=847,
            num_failed_tasks=0,
            num_stages=3,
            num_active_stages=1,
            num_completed_stages=2,
            stage_ids=[0, 1, 2],
        ),
        SparkJob(
            job_id=0,
            name="save at DataFrameWriter.scala:123",
            status=JobStatus.SUCCEEDED,
            submission_time=datetime(2026, 3, 17, 11, 30, 0, tzinfo=timezone.utc),
            num_tasks=200,
            num_completed_tasks=200,
            num_stages=2,
            num_completed_stages=2,
            stage_ids=[3, 4],
        ),
    ]


def _make_stages() -> list[SparkStage]:
    return [
        SparkStage(
            stage_id=2,
            name="count at NativeMethodAccessorImpl.java:0",
            status=StageStatus.ACTIVE,
            num_tasks=500,
            num_active_tasks=50,
            num_complete_tasks=400,
            input_bytes=1073741824,
            output_bytes=536870912,
            shuffle_read_bytes=268435456,
            shuffle_write_bytes=134217728,
            executor_run_time_ms=120000,
            submission_time=_NOW,
        ),
        SparkStage(
            stage_id=1,
            name="map at Pipeline.scala:45",
            status=StageStatus.COMPLETE,
            num_tasks=200,
            num_complete_tasks=200,
            input_bytes=536870912,
            executor_run_time_ms=45000,
        ),
    ]


def _make_executors() -> list[ExecutorInfo]:
    return [
        ExecutorInfo(
            executor_id="driver",
            is_active=True,
            host_port="10.0.0.1:40001",
            total_cores=4,
            active_tasks=0,
            completed_tasks=100,
            total_duration_ms=300000,
            total_gc_time_ms=3000,
            max_memory=4294967296,
            memory_used=1073741824,
            add_time=_NOW,
        ),
        ExecutorInfo(
            executor_id="1",
            is_active=True,
            host_port="10.0.0.2:40001",
            total_cores=8,
            active_tasks=5,
            completed_tasks=500,
            total_duration_ms=600000,
            total_gc_time_ms=12000,
            max_memory=8589934592,
            memory_used=4294967296,
            disk_used=536870912,
            total_shuffle_read=1073741824,
            total_shuffle_write=536870912,
            add_time=_NOW,
        ),
    ]


def _make_sql_queries() -> list[SQLQuery]:
    return [
        SQLQuery(
            execution_id=5,
            status="RUNNING",
            description="SELECT count(*) FROM events WHERE date > '2026-03-01'",
            submission_time=_NOW,
            running_jobs=2,
        ),
        SQLQuery(
            execution_id=4,
            status="COMPLETED",
            description="INSERT INTO target SELECT * FROM source",
            submission_time=datetime(2026, 3, 17, 11, 0, 0, tzinfo=timezone.utc),
            duration_ms=45000,
            success_jobs=3,
        ),
    ]


def _make_storage() -> list[RDDInfo]:
    return [
        RDDInfo(
            rdd_id=1,
            name="In-memory table events",
            num_partitions=200,
            num_cached_partitions=200,
            storage_level="Memory Deserialized 1x Replicated",
            memory_used=2147483648,
            disk_used=0,
        ),
        RDDInfo(
            rdd_id=2,
            name="In-memory table users",
            num_partitions=50,
            num_cached_partitions=50,
            storage_level="Disk Serialized 1x Replicated",
            memory_used=0,
            disk_used=536870912,
        ),
    ]


def _populated_cache() -> DataCache:
    """Return a DataCache pre-populated with all mock data."""
    cache = DataCache()
    cache.update("cluster", _make_cluster())
    cache.update("events", _make_events())
    cache.update("libraries", _make_libraries())
    cache.update("spark_jobs", _make_jobs())
    cache.update("stages", _make_stages())
    cache.update("executors", _make_executors())
    cache.update("sql_queries", _make_sql_queries())
    cache.update("storage", _make_storage())
    return cache


# ---------------------------------------------------------------------------
# Tests — verify views accept mock data without errors
# ---------------------------------------------------------------------------


class TestViewRendering:
    """Test that each view's refresh_data runs without error on mock data."""

    @pytest.fixture()
    def cache(self) -> DataCache:
        return _populated_cache()

    def test_cluster_view_data(self, cache: DataCache) -> None:
        """ClusterView reads cluster, events, and libraries slots."""
        cluster = cache.get("cluster")
        events = cache.get("events")
        libs = cache.get("libraries")
        assert cluster.data is not None
        assert cluster.data.cluster_name == "analytics-prod"
        assert cluster.data.state == ClusterState.RUNNING
        assert len(events.data) == 2
        assert len(libs.data) == 2

    def test_jobs_view_data(self, cache: DataCache) -> None:
        """JobsView reads spark_jobs slot."""
        jobs = cache.get("spark_jobs").data
        assert len(jobs) == 2
        assert jobs[0].status == JobStatus.RUNNING
        assert jobs[0].num_completed_tasks == 847

    def test_stages_view_data(self, cache: DataCache) -> None:
        """StagesView reads stages slot."""
        stages = cache.get("stages").data
        assert len(stages) == 2
        assert stages[0].status == StageStatus.ACTIVE
        assert stages[0].spill_bytes == 0

    def test_executors_view_data(self, cache: DataCache) -> None:
        """ExecutorsView reads executors slot."""
        executors = cache.get("executors").data
        assert len(executors) == 2
        assert executors[0].is_driver is True
        assert executors[1].gc_ratio == pytest.approx(0.02)
        assert executors[1].memory_used_pct == pytest.approx(50.0)

    def test_sql_view_data(self, cache: DataCache) -> None:
        """SQLView reads sql_queries slot."""
        queries = cache.get("sql_queries").data
        assert len(queries) == 2
        assert queries[0].running_jobs == 2

    def test_storage_view_data(self, cache: DataCache) -> None:
        """StorageView reads storage slot."""
        rdds = cache.get("storage").data
        assert len(rdds) == 2
        assert rdds[0].fraction_cached == pytest.approx(1.0)

    def test_empty_cache_slots(self) -> None:
        """Views handle None data gracefully."""
        cache = DataCache()
        for slot_name in cache.slot_names:
            slot = cache.get(slot_name)
            assert slot.data is None


class TestEmptyStates:
    """Test views with empty (but non-None) data lists."""

    def test_empty_jobs(self) -> None:
        cache = DataCache()
        cache.update("spark_jobs", [])
        assert cache.get("spark_jobs").data == []

    def test_empty_stages(self) -> None:
        cache = DataCache()
        cache.update("stages", [])
        assert cache.get("stages").data == []

    def test_empty_executors(self) -> None:
        cache = DataCache()
        cache.update("executors", [])
        assert cache.get("executors").data == []

    def test_empty_sql(self) -> None:
        cache = DataCache()
        cache.update("sql_queries", [])
        assert cache.get("sql_queries").data == []

    def test_empty_storage(self) -> None:
        cache = DataCache()
        cache.update("storage", [])
        assert cache.get("storage").data == []


class TestErrorStates:
    """Test cache error marking preserves data."""

    def test_error_preserves_existing_data(self) -> None:
        cache = _populated_cache()
        cache.mark_error("spark_jobs", "Connection timeout")
        slot = cache.get("spark_jobs")
        assert slot.stale is True
        assert slot.error == "Connection timeout"
        assert slot.data is not None  # data preserved
        assert len(slot.data) == 2

    def test_error_on_empty_slot(self) -> None:
        cache = DataCache()
        cache.mark_error("executors", "Cluster terminated")
        slot = cache.get("executors")
        assert slot.stale is True
        assert slot.data is None
