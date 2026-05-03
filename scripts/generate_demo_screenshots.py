"""Generate demo SVG screenshots of every dbxtop view.

Mounts ``DbxTopApp`` with a pre-populated ``DataCache`` (no live cluster
required), switches to each tab, and saves a Textual SVG screenshot to
``docs/screenshots/``.

Usage:
    python scripts/generate_demo_screenshots.py [--out docs/screenshots] [--size 180x50]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

# Make the in-tree package importable when running from the repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"
if SRC.is_dir():
    sys.path.insert(0, str(SRC))

from dbxtop.api.cache import DataCache  # noqa: E402
from dbxtop.api.models import (  # noqa: E402
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
from dbxtop.app import DbxTopApp  # noqa: E402

logger = logging.getLogger("dbxtop.demo")


# ---------------------------------------------------------------------------
# Mock data — rich enough to fill each view convincingly
# ---------------------------------------------------------------------------

# Anchor "now" relative to the real clock so uptimes / "Submitted" stamps look
# fresh rather than historical artefacts. Cluster started 4h 23m ago; the
# active job started 8 minutes ago.
_NOW = datetime.now(timezone.utc)
_CLUSTER_START = _NOW - timedelta(hours=4, minutes=23)
_JOB_RUNNING_START = _NOW - timedelta(minutes=8, seconds=12)
_JOB_COMPLETED_START = _NOW - timedelta(minutes=22)


def _make_cluster() -> ClusterInfo:
    return ClusterInfo(
        cluster_id="0626-094521-runt7m23",
        cluster_name="analytics-prod-cluster",
        state=ClusterState.RUNNING,
        state_message="",
        start_time=_CLUSTER_START,
        driver_node_type="i3.2xlarge",
        worker_node_type="i3.4xlarge",
        num_workers=8,
        autoscale_min=4,
        autoscale_max=16,
        total_cores=128,
        total_memory_mb=262144,  # 256 GB
        spark_version="15.4.x-scala2.12 (Databricks Runtime 15.4 LTS)",
        data_security_mode="SINGLE_USER",
        runtime_engine="PHOTON",
        creator="oz@example.com",
        autotermination_minutes=120,
        spark_conf={
            "spark.databricks.delta.preview.enabled": "true",
            "spark.databricks.io.cache.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.statistics.histogram.enabled": "true",
        },
        tags={"team": "data-platform", "env": "production"},
    )


def _make_events() -> List[ClusterEvent]:
    return [
        ClusterEvent(
            timestamp=_NOW - timedelta(minutes=2),
            event_type="UPSIZE_COMPLETED",
            message="Cluster successfully resized from 6 to 8 workers",
        ),
        ClusterEvent(
            timestamp=_NOW - timedelta(minutes=4),
            event_type="AUTOSCALING_STATS_REPORT",
            message="Autoscaling: target 8 workers based on load (cpu_pressure=0.78)",
        ),
        ClusterEvent(
            timestamp=_NOW - timedelta(minutes=12),
            event_type="RUNNING",
            message="Cluster is running",
        ),
        ClusterEvent(
            timestamp=_NOW - timedelta(minutes=14),
            event_type="INIT_SCRIPTS_FINISHED",
            message="Init scripts finished in 18s",
        ),
        ClusterEvent(
            timestamp=_NOW - timedelta(minutes=15),
            event_type="STARTING",
            message="Driver is starting",
        ),
        ClusterEvent(
            timestamp=_CLUSTER_START,
            event_type="CREATING",
            message="Cluster created by oz@example.com (8 workers, i3.4xlarge)",
        ),
    ]


def _make_libraries() -> List[LibraryInfo]:
    return [
        LibraryInfo(name="pandas==2.2.2", library_type="pypi", status="INSTALLED"),
        LibraryInfo(name="numpy==1.26.4", library_type="pypi", status="INSTALLED"),
        LibraryInfo(name="scikit-learn==1.5.0", library_type="pypi", status="INSTALLED"),
        LibraryInfo(name="mlflow==2.14.1", library_type="pypi", status="INSTALLED"),
        LibraryInfo(name="delta-spark==3.2.0", library_type="maven", status="INSTALLED"),
        LibraryInfo(name="pyarrow==16.1.0", library_type="pypi", status="INSTALLING"),
    ]


def _make_jobs() -> List[SparkJob]:
    """Mix of running, succeeded, and failed jobs across stage counts."""
    return [
        SparkJob(
            job_id=234,
            name="aggregate at SilverToGold.scala:128",
            status=JobStatus.RUNNING,
            submission_time=_JOB_RUNNING_START,
            num_tasks=1600,
            num_active_tasks=64,
            num_completed_tasks=1247,
            num_failed_tasks=0,
            num_stages=5,
            num_active_stages=2,
            num_completed_stages=3,
            stage_ids=[14, 15, 16, 17, 18],
        ),
        SparkJob(
            job_id=233,
            name="join at FactBuilder.scala:88",
            status=JobStatus.RUNNING,
            submission_time=_NOW - timedelta(minutes=3, seconds=22),
            num_tasks=800,
            num_active_tasks=32,
            num_completed_tasks=612,
            num_failed_tasks=0,
            num_stages=3,
            num_active_stages=1,
            num_completed_stages=2,
            stage_ids=[19, 20, 21],
        ),
        SparkJob(
            job_id=232,
            name="count at NativeMethodAccessorImpl.java:0",
            status=JobStatus.SUCCEEDED,
            submission_time=_NOW - timedelta(minutes=11),
            completion_time=_NOW - timedelta(minutes=10, seconds=18),
            num_tasks=200,
            num_completed_tasks=200,
            num_stages=2,
            num_completed_stages=2,
            stage_ids=[10, 11],
        ),
        SparkJob(
            job_id=231,
            name="save at DataFrameWriter.scala:412",
            status=JobStatus.SUCCEEDED,
            submission_time=_NOW - timedelta(minutes=18),
            completion_time=_NOW - timedelta(minutes=14, seconds=5),
            num_tasks=400,
            num_completed_tasks=400,
            num_stages=4,
            num_completed_stages=4,
            stage_ids=[6, 7, 8, 9],
        ),
        SparkJob(
            job_id=230,
            name="write at GoldDelta.scala:201",
            status=JobStatus.FAILED,
            submission_time=_NOW - timedelta(minutes=22),
            completion_time=_NOW - timedelta(minutes=20, seconds=44),
            num_tasks=600,
            num_completed_tasks=587,
            num_failed_tasks=13,
            num_stages=3,
            num_completed_stages=2,
            num_failed_stages=1,
            stage_ids=[3, 4, 5],
        ),
        SparkJob(
            job_id=229,
            name="collect at NativeMethodAccessorImpl.java:0",
            status=JobStatus.SUCCEEDED,
            submission_time=_JOB_COMPLETED_START,
            completion_time=_NOW - timedelta(minutes=21, seconds=38),
            num_tasks=120,
            num_completed_tasks=120,
            num_stages=1,
            num_completed_stages=1,
            stage_ids=[0],
        ),
    ]


def _make_stages() -> List[SparkStage]:
    """Stages spanning ACTIVE / COMPLETE / FAILED with shuffle and spill."""
    return [
        SparkStage(
            stage_id=18,
            name="aggregate at SilverToGold.scala:128",
            status=StageStatus.ACTIVE,
            num_tasks=400,
            num_active_tasks=32,
            num_complete_tasks=287,
            input_bytes=42 * 1024**3,
            input_records=128_472_991,
            output_bytes=8 * 1024**3,
            output_records=24_180_004,
            shuffle_read_bytes=12 * 1024**3,
            shuffle_write_bytes=6 * 1024**3,
            executor_run_time_ms=4 * 60 * 1000 + 22 * 1000,
            executor_cpu_time_ns=180 * 1_000_000_000,
            memory_spill_bytes=512 * 1024**2,
            disk_spill_bytes=128 * 1024**2,
            jvm_gc_time_ms=12_400,
            peak_execution_memory=2_400_000_000,
            submission_time=_NOW - timedelta(minutes=2, seconds=15),
        ),
        SparkStage(
            stage_id=17,
            name="exchange at SilverToGold.scala:121",
            status=StageStatus.ACTIVE,
            num_tasks=400,
            num_active_tasks=32,
            num_complete_tasks=341,
            input_bytes=18 * 1024**3,
            shuffle_read_bytes=8 * 1024**3,
            shuffle_write_bytes=12 * 1024**3,
            executor_run_time_ms=2 * 60 * 1000 + 47 * 1000,
            executor_cpu_time_ns=140 * 1_000_000_000,
            jvm_gc_time_ms=4_200,
            submission_time=_NOW - timedelta(minutes=4),
        ),
        SparkStage(
            stage_id=20,
            name="map at FactBuilder.scala:74",
            status=StageStatus.ACTIVE,
            num_tasks=200,
            num_active_tasks=24,
            num_complete_tasks=158,
            input_bytes=6 * 1024**3,
            output_bytes=4 * 1024**3,
            executor_run_time_ms=98_000,
            jvm_gc_time_ms=1_800,
            submission_time=_NOW - timedelta(minutes=2),
        ),
        SparkStage(
            stage_id=16,
            name="scan parquet silver.events",
            status=StageStatus.COMPLETE,
            num_tasks=400,
            num_complete_tasks=400,
            input_bytes=64 * 1024**3,
            input_records=210_184_992,
            output_bytes=14 * 1024**3,
            executor_run_time_ms=180_000,
            jvm_gc_time_ms=3_400,
            submission_time=_NOW - timedelta(minutes=7, seconds=12),
            completion_time=_NOW - timedelta(minutes=4, seconds=12),
        ),
        SparkStage(
            stage_id=15,
            name="filter at SilverToGold.scala:101",
            status=StageStatus.COMPLETE,
            num_tasks=400,
            num_complete_tasks=400,
            input_bytes=14 * 1024**3,
            output_bytes=12 * 1024**3,
            shuffle_write_bytes=10 * 1024**3,
            executor_run_time_ms=120_000,
            submission_time=_NOW - timedelta(minutes=4, seconds=14),
            completion_time=_NOW - timedelta(minutes=2, seconds=15),
        ),
        SparkStage(
            stage_id=11,
            name="count at NativeMethodAccessorImpl.java:0",
            status=StageStatus.COMPLETE,
            num_tasks=200,
            num_complete_tasks=200,
            input_bytes=4 * 1024**3,
            executor_run_time_ms=42_000,
            submission_time=_NOW - timedelta(minutes=11),
            completion_time=_NOW - timedelta(minutes=10, seconds=18),
        ),
        SparkStage(
            stage_id=5,
            name="write at GoldDelta.scala:201",
            status=StageStatus.FAILED,
            num_tasks=200,
            num_complete_tasks=187,
            num_failed_tasks=13,
            input_bytes=6 * 1024**3,
            output_bytes=2 * 1024**3,
            shuffle_read_bytes=4 * 1024**3,
            memory_spill_bytes=1_500_000_000,
            disk_spill_bytes=400 * 1024**2,
            executor_run_time_ms=72_000,
            jvm_gc_time_ms=8_100,
            submission_time=_NOW - timedelta(minutes=22),
            completion_time=_NOW - timedelta(minutes=20, seconds=44),
            failure_reason=(
                "Job aborted due to stage failure: Task 142 in stage 5.0 failed 4 times, "
                "most recent failure: ExecutorLostFailure (executor 4 exited caused by one "
                "of the running tasks). Reason: Container killed by YARN for exceeding "
                "memory limits. 11.2 GB of 11 GB physical memory used."
            ),
        ),
        SparkStage(
            stage_id=4,
            name="exchange at GoldDelta.scala:189",
            status=StageStatus.COMPLETE,
            num_tasks=200,
            num_complete_tasks=200,
            input_bytes=10 * 1024**3,
            shuffle_write_bytes=4 * 1024**3,
            executor_run_time_ms=58_000,
            submission_time=_NOW - timedelta(minutes=22, seconds=10),
            completion_time=_NOW - timedelta(minutes=21, seconds=12),
        ),
        SparkStage(
            stage_id=3,
            name="scan parquet bronze.raw_events",
            status=StageStatus.COMPLETE,
            num_tasks=200,
            num_complete_tasks=200,
            input_bytes=24 * 1024**3,
            input_records=92_111_004,
            output_bytes=10 * 1024**3,
            executor_run_time_ms=68_000,
            submission_time=_NOW - timedelta(minutes=22, seconds=44),
            completion_time=_NOW - timedelta(minutes=22, seconds=10),
        ),
        SparkStage(
            stage_id=0,
            name="collect at NativeMethodAccessorImpl.java:0",
            status=StageStatus.COMPLETE,
            num_tasks=120,
            num_complete_tasks=120,
            input_bytes=512 * 1024**2,
            executor_run_time_ms=24_000,
            submission_time=_JOB_COMPLETED_START,
            completion_time=_NOW - timedelta(minutes=21, seconds=38),
        ),
    ]


def _make_executors() -> List[ExecutorInfo]:
    """Driver + 8 workers, varying memory/GC, plus one excluded and one dead."""
    GB = 1024**3
    executors: List[ExecutorInfo] = [
        ExecutorInfo(
            executor_id="driver",
            is_active=True,
            host_port="10.0.4.12:40001",
            total_cores=8,
            max_tasks=8,
            active_tasks=0,
            completed_tasks=412,
            total_duration_ms=14 * 60 * 1000,
            total_gc_time_ms=11_200,
            max_memory=int(8 * GB),
            memory_used=int(2.6 * GB),
            disk_used=0,
            peak_jvm_heap=int(11.4 * GB),
            peak_jvm_off_heap=int(1.2 * GB),
            add_time=_CLUSTER_START + timedelta(seconds=22),
        ),
        ExecutorInfo(
            executor_id="1",
            is_active=True,
            host_port="10.0.4.31:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=12,
            completed_tasks=2_847,
            failed_tasks=0,
            total_duration_ms=2_400_000,
            total_gc_time_ms=120_000,
            max_memory=int(12 * GB),
            memory_used=int(7.4 * GB),
            disk_used=int(1.2 * GB),
            total_input_bytes=int(82 * GB),
            total_shuffle_read=int(34 * GB),
            total_shuffle_write=int(18 * GB),
            peak_jvm_heap=int(28 * GB),
            peak_jvm_off_heap=int(2.4 * GB),
            rdd_blocks=412,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="2",
            is_active=True,
            host_port="10.0.4.32:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=14,
            completed_tasks=3_104,
            failed_tasks=2,
            total_duration_ms=2_580_000,
            total_gc_time_ms=210_000,  # ~8% gc (yellow)
            max_memory=int(12 * GB),
            memory_used=int(9.8 * GB),
            disk_used=int(2.4 * GB),
            total_input_bytes=int(94 * GB),
            total_shuffle_read=int(42 * GB),
            total_shuffle_write=int(22 * GB),
            peak_jvm_heap=int(34 * GB),
            peak_jvm_off_heap=int(2.4 * GB),
            rdd_blocks=502,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="3",
            is_active=True,
            host_port="10.0.4.33:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=10,
            completed_tasks=2_512,
            total_duration_ms=2_140_000,
            total_gc_time_ms=68_000,
            max_memory=int(12 * GB),
            memory_used=int(5.2 * GB),
            disk_used=int(800 * 1024**2),
            total_input_bytes=int(70 * GB),
            total_shuffle_read=int(28 * GB),
            total_shuffle_write=int(14 * GB),
            peak_jvm_heap=int(24 * GB),
            peak_jvm_off_heap=int(2.0 * GB),
            rdd_blocks=380,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="4",
            is_active=True,
            host_port="10.0.4.34:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=16,
            completed_tasks=3_402,
            total_duration_ms=2_700_000,
            total_gc_time_ms=340_000,  # ~12% gc (red)
            max_memory=int(12 * GB),
            memory_used=int(11.1 * GB),
            disk_used=int(3.4 * GB),
            total_input_bytes=int(102 * GB),
            total_shuffle_read=int(48 * GB),
            total_shuffle_write=int(28 * GB),
            peak_jvm_heap=int(38 * GB),
            peak_jvm_off_heap=int(2.4 * GB),
            rdd_blocks=540,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="5",
            is_active=True,
            host_port="10.0.4.35:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=8,
            completed_tasks=2_104,
            total_duration_ms=1_980_000,
            total_gc_time_ms=44_000,
            max_memory=int(12 * GB),
            memory_used=int(4.6 * GB),
            disk_used=int(420 * 1024**2),
            total_input_bytes=int(58 * GB),
            total_shuffle_read=int(22 * GB),
            total_shuffle_write=int(12 * GB),
            peak_jvm_heap=int(20 * GB),
            peak_jvm_off_heap=int(1.6 * GB),
            rdd_blocks=304,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="6",
            is_active=True,
            is_excluded=True,
            excluded_in_stages=[5],
            host_port="10.0.4.36:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=0,
            completed_tasks=1_802,
            failed_tasks=8,
            total_duration_ms=1_600_000,
            total_gc_time_ms=82_000,
            max_memory=int(12 * GB),
            memory_used=int(6.0 * GB),
            disk_used=int(1.0 * GB),
            total_input_bytes=int(48 * GB),
            total_shuffle_read=int(20 * GB),
            total_shuffle_write=int(10 * GB),
            peak_jvm_heap=int(22 * GB),
            peak_jvm_off_heap=int(1.8 * GB),
            rdd_blocks=270,
            add_time=_CLUSTER_START + timedelta(minutes=1),
        ),
        ExecutorInfo(
            executor_id="7",
            is_active=True,
            host_port="10.0.4.37:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=11,
            completed_tasks=2_604,
            total_duration_ms=2_320_000,
            total_gc_time_ms=98_000,
            max_memory=int(12 * GB),
            memory_used=int(7.0 * GB),
            disk_used=int(1.4 * GB),
            total_input_bytes=int(76 * GB),
            total_shuffle_read=int(30 * GB),
            total_shuffle_write=int(16 * GB),
            peak_jvm_heap=int(26 * GB),
            peak_jvm_off_heap=int(2.0 * GB),
            rdd_blocks=410,
            add_time=_CLUSTER_START + timedelta(minutes=2),
        ),
        ExecutorInfo(
            executor_id="8",
            is_active=True,
            host_port="10.0.4.38:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=9,
            completed_tasks=1_204,
            total_duration_ms=1_180_000,
            total_gc_time_ms=42_000,
            max_memory=int(12 * GB),
            memory_used=int(5.8 * GB),
            disk_used=int(640 * 1024**2),
            total_input_bytes=int(46 * GB),
            total_shuffle_read=int(18 * GB),
            total_shuffle_write=int(8 * GB),
            peak_jvm_heap=int(20 * GB),
            peak_jvm_off_heap=int(1.6 * GB),
            rdd_blocks=240,
            add_time=_NOW - timedelta(minutes=14),
        ),
        # Dead executor — was lost during the failed write stage.
        ExecutorInfo(
            executor_id="0",
            is_active=False,
            host_port="10.0.4.30:40001",
            total_cores=16,
            max_tasks=16,
            active_tasks=0,
            completed_tasks=802,
            failed_tasks=6,
            total_duration_ms=920_000,
            total_gc_time_ms=64_000,
            max_memory=int(12 * GB),
            memory_used=0,
            disk_used=0,
            total_input_bytes=int(28 * GB),
            total_shuffle_read=int(8 * GB),
            total_shuffle_write=int(4 * GB),
            peak_jvm_heap=int(12 * GB),
            peak_jvm_off_heap=int(1.2 * GB),
            add_time=_CLUSTER_START + timedelta(minutes=1),
            remove_time=_NOW - timedelta(minutes=20, seconds=44),
            remove_reason="Container killed by YARN for exceeding memory limits.",
        ),
    ]
    return executors


def _make_executor_history(snapshots: int = 40) -> List[List[ExecutorInfo]]:
    """Generate historical executor snapshots for sparkline rendering.

    Each snapshot mutates memory_used and total_gc_time_ms slightly to give
    realistic-looking sparkline shapes. Earlier snapshots show lower load,
    later snapshots ramp up.
    """
    import math

    base = _make_executors()
    history: List[List[ExecutorInfo]] = []

    for i in range(snapshots):
        progress = i / max(1, snapshots - 1)  # 0.0 → 1.0
        snapshot: List[ExecutorInfo] = []
        for idx, exe in enumerate(base):
            phase = (idx * 0.7) + (progress * math.pi * 1.5)
            wave = (math.sin(phase) + 1) / 2  # 0..1

            # Drop dead executor from earlier history (it was alive then)
            if not exe.is_active and progress < 0.6:
                continue

            mem_factor = 0.45 + 0.45 * wave * (0.5 + progress)
            gc_factor = 0.3 + 0.7 * wave * progress
            data = exe.model_dump()
            data["memory_used"] = int(exe.max_memory * min(0.95, mem_factor))
            data["total_gc_time_ms"] = int(exe.total_gc_time_ms * (0.4 + gc_factor))
            data["active_tasks"] = max(0, int(exe.active_tasks * (0.4 + 0.6 * (1.0 - abs(wave - 0.5) * 1.4))))
            snapshot.append(ExecutorInfo(**data))
        history.append(snapshot)

    # Ensure the last snapshot is the canonical "current" state
    history.append(base)
    return history


def _make_sql_queries() -> List[SQLQuery]:
    return [
        SQLQuery(
            execution_id=84,
            status="RUNNING",
            description=(
                "WITH recent AS (SELECT user_id, MAX(event_ts) AS last_seen FROM silver.events "
                "WHERE event_date >= '2026-04-25' GROUP BY user_id) SELECT u.country, COUNT(*) "
                "AS active_users FROM gold.users u JOIN recent r ON u.user_id = r.user_id GROUP BY u.country"
            ),
            submission_time=_JOB_RUNNING_START,
            running_jobs=2,
        ),
        SQLQuery(
            execution_id=83,
            status="COMPLETED",
            description="MERGE INTO gold.fact_orders USING staging.orders ON ...",
            submission_time=_NOW - timedelta(minutes=11),
            duration_ms=72_000,
            success_jobs=3,
        ),
        SQLQuery(
            execution_id=82,
            status="COMPLETED",
            description="SELECT COUNT(*), AVG(amount) FROM gold.fact_orders WHERE order_date = current_date()",
            submission_time=_NOW - timedelta(minutes=14),
            duration_ms=4_200,
            success_jobs=1,
        ),
        SQLQuery(
            execution_id=81,
            status="FAILED",
            description="INSERT OVERWRITE gold.daily_revenue SELECT date, SUM(amount) FROM ... GROUP BY date",
            submission_time=_NOW - timedelta(minutes=22),
            duration_ms=84_000,
            failed_job_ids=[230],
        ),
        SQLQuery(
            execution_id=80,
            status="COMPLETED",
            description="OPTIMIZE silver.events ZORDER BY (event_date, user_id)",
            submission_time=_NOW - timedelta(minutes=34),
            duration_ms=180_000,
            success_jobs=4,
        ),
    ]


def _make_storage() -> List[RDDInfo]:
    GB = 1024**3
    return [
        RDDInfo(
            rdd_id=42,
            name="In-memory table silver.events",
            num_partitions=400,
            num_cached_partitions=400,
            storage_level="Memory Deserialized 1x Replicated",
            memory_used=int(18 * GB),
            disk_used=0,
        ),
        RDDInfo(
            rdd_id=41,
            name="In-memory table gold.users",
            num_partitions=200,
            num_cached_partitions=200,
            storage_level="Memory Deserialized 1x Replicated",
            memory_used=int(6 * GB),
            disk_used=0,
        ),
        RDDInfo(
            rdd_id=40,
            name="In-memory table gold.sessions",
            num_partitions=400,
            num_cached_partitions=312,
            storage_level="Memory and Disk Serialized 1x Replicated",
            memory_used=int(8 * GB),
            disk_used=int(2 * GB),
        ),
        RDDInfo(
            rdd_id=39,
            name="*Project [user_id, country] +- *Filter (event_ts > ...) - cached",
            num_partitions=200,
            num_cached_partitions=200,
            storage_level="Memory Deserialized 1x Replicated",
            memory_used=int(2 * GB),
            disk_used=0,
        ),
        RDDInfo(
            rdd_id=38,
            name="In-memory table bronze.raw_events_sample",
            num_partitions=64,
            num_cached_partitions=48,
            storage_level="Disk Serialized 1x Replicated",
            memory_used=0,
            disk_used=int(1 * GB),
        ),
    ]


def build_demo_cache() -> DataCache:
    """Build a fully-populated DataCache for demo screenshots.

    Populates the executors history ring buffer with 40 synthetic snapshots so
    sparklines render with realistic shapes.
    """
    cache = DataCache()
    cache.update("cluster", _make_cluster())
    cache.update("events", _make_events())
    cache.update("libraries", _make_libraries())
    cache.update("spark_jobs", _make_jobs())
    cache.update("stages", _make_stages())

    # Replay executor history so the ring buffer populates
    for snapshot in _make_executor_history():
        cache.update("executors", snapshot)

    cache.update("sql_queries", _make_sql_queries())
    cache.update("storage", _make_storage())
    return cache


# ---------------------------------------------------------------------------
# Demo App — DbxTopApp wired up with the pre-built cache (no live polling)
# ---------------------------------------------------------------------------


class _StubDatabricksClient:
    """No-op stand-in for DatabricksClient — parent on_mount calls only its constructor."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class _StubPoller:
    """No-op stand-in for MetricsPoller — never polls, never overwrites the demo cache."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def force_refresh(self) -> None:
        pass

    async def set_spark_client(self, *args: Any, **kwargs: Any) -> None:
        pass


class DemoDbxTopApp(DbxTopApp):
    """DbxTopApp variant that skips real client setup and renders a static cache.

    Textual dispatches ``on_mount`` to every class in the MRO, so we cannot just
    override the method — the parent's body still runs. Instead we monkey-patch
    ``DatabricksClient`` and ``MetricsPoller`` to harmless stubs at import time
    in this module's scope, then re-set ``self._cache`` to the demo cache after
    the parent finishes.
    """

    # CSS_PATH on the parent class is resolved relative to where the *subclass*
    # is defined, so re-pin it to the canonical location next to ``app.py``.
    CSS_PATH = str(SRC / "dbxtop" / "styles" / "default.tcss")

    def __init__(self, cache: DataCache, profile: str = "prod") -> None:
        super().__init__(profile=profile, cluster_id="0626-094521-runt7m23")
        self._demo_cache = cache

    async def on_mount(self) -> None:  # type: ignore[override]
        from textual.widgets import TabbedContent

        from dbxtop.views.base import BaseView

        # Parent's on_mount has already run (Textual dispatches to MRO), and
        # set up empty cache + stub clients via the module-level patches. Now
        # we replace the cache with the demo data and refresh the UI.
        self._cache = self._demo_cache

        # Stand-in Spark client: header / footer only check truthiness and a
        # rate-limit flag. We never actually call its methods.
        self._spark_client = SimpleNamespace(is_rate_limited=False)  # type: ignore[assignment]

        # Header: set spark availability *before* rendering so the bar shows
        # "Spark: connected" rather than the default "disconnected".
        if self._header is not None:
            self._header.spark_available = True
            self._header.update_from_cache(self._cache)
        self._update_spark_status(True)
        self._update_footer_status()
        self._update_footer_error_counts(set(self._cache.slot_names))

        # Refresh every view eagerly so each tab is fully populated before
        # we screenshot it. Without this, only the initially-active tab
        # ("cluster") gets a refresh_data call.
        try:
            tabbed = self.query_one(TabbedContent)
            for pane_id in ("cluster", "jobs", "stages", "executors", "sql", "storage", "analytics"):
                pane = tabbed.get_pane(pane_id)
                for child in pane.walk_children():
                    if isinstance(child, BaseView):
                        child.refresh_data(self._cache)
                        break
        except Exception:
            logger.debug("Failed to pre-refresh all views", exc_info=True)

    def _show_error(self, message: str) -> None:  # type: ignore[override]
        # Suppress the parent's error reporting — in demo mode there is no
        # client to fail, but we keep the stubs in place defensively.
        logger.debug("suppressed demo error: %s", message)

    async def _try_init_spark_client(self) -> None:  # type: ignore[override]
        # Parent schedules this via call_later in its on_mount. The default
        # implementation calls workspace lookup methods on the stub client and
        # then resets ``self._spark_client`` to None on failure, which would
        # flip the header/footer back to "Spark: disconnected" right after our
        # override has set it to connected. Make it a no-op for demo mode.
        return None


def _install_demo_stubs() -> None:
    """Replace network-touching dependencies with no-op stubs.

    Run before instantiating ``DemoDbxTopApp`` so that the inherited
    ``on_mount`` does not attempt to contact Databricks.
    """
    import dbxtop.app as app_module

    app_module.DatabricksClient = _StubDatabricksClient  # type: ignore[assignment]
    app_module.MetricsPoller = _StubPoller  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Screenshot driver
# ---------------------------------------------------------------------------

# Tab id → output filename stem
_TABS: list[tuple[str, str]] = [
    ("cluster", "01-cluster"),
    ("jobs", "02-jobs"),
    ("stages", "03-stages"),
    ("executors", "04-executors"),
    ("sql", "05-sql"),
    ("storage", "06-storage"),
    ("analytics", "07-analytics"),
]


async def _capture(app: DemoDbxTopApp, out_dir: Path, size: tuple[int, int]) -> list[Path]:
    from textual.widgets import TabbedContent

    from dbxtop.views.base import BaseView

    saved: list[Path] = []
    async with app.run_test(size=size) as pilot:
        # Let layout + initial cache render settle.
        await pilot.pause()
        await pilot.pause()

        for tab_id, stem in _TABS:
            # Activate the tab via TabbedContent and let Textual emit the
            # TabActivated event before we screenshot.
            tabbed = app.query_one(TabbedContent)
            tabbed.active = tab_id
            await pilot.pause()
            await pilot.pause()

            # Belt-and-braces: explicitly drive refresh_data on the now-active
            # view in case the TabActivated handler raced ahead of us.
            assert app._cache is not None
            active_pane = tabbed.get_pane(tab_id)
            for child in active_pane.walk_children():
                if isinstance(child, BaseView):
                    child.refresh_data(app._cache)
                    break
            if app._footer is not None:
                app._footer.active_tab = tab_id
            await pilot.pause()

            filename = f"{stem}.svg"
            app.save_screenshot(filename=filename, path=str(out_dir))
            saved.append(out_dir / filename)
            logger.info("saved %s", saved[-1])

    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "docs" / "screenshots",
        help="Output directory for SVG screenshots",
    )
    parser.add_argument(
        "--size",
        default="180x50",
        help="Terminal size as WIDTHxHEIGHT (default: 180x50)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    try:
        w_str, h_str = args.size.lower().split("x")
        size = (int(w_str), int(h_str))
    except ValueError:
        parser.error(f"--size must be WIDTHxHEIGHT, got {args.size!r}")
        return 2

    args.out.mkdir(parents=True, exist_ok=True)

    _install_demo_stubs()
    cache = build_demo_cache()
    app = DemoDbxTopApp(cache=cache)
    saved = asyncio.run(_capture(app, args.out, size))

    print(f"Wrote {len(saved)} screenshots to {args.out}")
    for path in saved:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
