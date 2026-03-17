"""Stateless analytics engine for computing performance insights.

The engine takes lists of Pydantic model instances from the cache and
returns Insight, HealthScore, and DiagnosticReport objects. All methods
are pure functions with no side effects, no API calls, and no state
mutation.
"""

from __future__ import annotations

import statistics
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

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
    ClusterInfo,
    ExecutorInfo,
    JobStatus,
    RDDInfo,
    SQLQuery,
    SparkJob,
    SparkStage,
    StageStatus,
    format_bytes,
)

# ---------------------------------------------------------------------------
# Threshold constants
# ---------------------------------------------------------------------------

# GC Pressure thresholds
GC_WARNING_RATIO: float = 0.05  # 5% GC time -> WARNING
GC_CRITICAL_RATIO: float = 0.10  # 10% GC time -> CRITICAL

# Memory Spill thresholds
SPILL_WARNING_BYTES: int = 0  # Any spill -> WARNING (spill_bytes > 0)
SPILL_CRITICAL_RATIO: float = 1.0  # spill > input -> CRITICAL
SPILL_CRITICAL_ABS: int = 1_073_741_824  # 1 GB disk spill on single stage -> CRITICAL

# Data Skew thresholds (based on AQE defaults)
SKEW_WARNING_FACTOR: float = 3.0  # max shuffle_read > 3x median -> WARNING
SKEW_CRITICAL_FACTOR: float = 5.0  # max shuffle_read > 5x median -> CRITICAL (AQE default)
SKEW_COV_WARNING: float = 0.5  # coefficient of variation > 0.5 -> WARNING
SKEW_COV_CRITICAL: float = 1.0  # coefficient of variation > 1.0 -> CRITICAL

# Shuffle Overhead thresholds
SHUFFLE_WARNING_RATIO: float = 0.5  # shuffle bytes / input bytes > 0.5 -> WARNING
SHUFFLE_CRITICAL_RATIO: float = 2.0  # shuffle bytes / input bytes > 2.0 -> CRITICAL

# Utilization thresholds
UTIL_WARNING_PCT: float = 70.0  # utilization < 70% -> WARNING
UTIL_CRITICAL_PCT: float = 30.0  # utilization < 30% -> CRITICAL

# Partition sizing thresholds (from Spark docs and AQE defaults)
PARTITION_TOO_LARGE: int = 268_435_456  # 256 MB per task -> WARNING
PARTITION_TOO_SMALL: int = 1_048_576  # 1 MB per task -> WARNING
PARTITION_OPTIMAL_LOW: int = 67_108_864  # 64 MB (AQE advisory size)
PARTITION_OPTIMAL_HIGH: int = 268_435_456  # 256 MB

# Straggler thresholds
STRAGGLER_PROGRESS_THRESHOLD: float = 0.95  # Stage > 95% complete
STRAGGLER_WARNING_FACTOR: float = 2.0  # remaining time > 2x avg task time -> WARNING
STRAGGLER_CRITICAL_FACTOR: float = 5.0  # remaining time > 5x avg task time -> CRITICAL

# Task failure thresholds
FAILURE_WARNING_RATE: float = 0.01  # 1% failure rate -> WARNING
FAILURE_CRITICAL_RATE: float = 0.05  # 5% failure rate -> CRITICAL

# Memory pressure thresholds
MEMORY_WARNING_PCT: float = 80.0  # > 80% memory used -> WARNING
MEMORY_CRITICAL_PCT: float = 95.0  # > 95% memory used -> CRITICAL
PEAK_HEAP_WARNING_PCT: float = 85.0  # peak_jvm_heap > 85% max_memory -> WARNING
PEAK_HEAP_CRITICAL_PCT: float = 95.0  # peak_jvm_heap > 95% max_memory -> CRITICAL

# -- Phase A thresholds --
EXECUTOR_HEAP_WARNING_BYTES: int = 64 * 1024**3  # 64 GB
EXECUTOR_CORES_WARNING: int = 8
MEMORY_PER_CORE_WARNING_BYTES: int = 2 * 1024**3  # 2 GB per core
DRIVER_GC_RATIO_FACTOR: float = 2.0  # driver GC > 2x avg worker
DRIVER_MEMORY_CRITICAL_PCT: float = 95.0

# -- Phase B thresholds --
SMALL_FILE_BYTES_PER_TASK: int = 1_048_576  # 1 MB
SMALL_FILE_MIN_TASKS: int = 100
CPU_EFFICIENCY_IO_BOUND: float = 0.3
CPU_EFFICIENCY_CPU_BOUND: float = 0.7
SQL_ANOMALY_WARNING_FACTOR: float = 5.0
SQL_ANOMALY_CRITICAL_FACTOR: float = 10.0
SQL_ANOMALY_MIN_DURATION_MS: int = 60_000
SQL_LONG_RUNNING_MS: int = 600_000

# -- Phase C thresholds --
JOIN_SHUFFLE_INPUT_RATIO: float = 5.0
JOIN_SMALL_INPUT_BYTES: int = 104_857_600  # 100 MB
CACHE_PARTIAL_THRESHOLD: float = 0.5
CACHE_MEMORY_WARNING_PCT: float = 60.0
EXECUTOR_COUNT_COV_WARNING: float = 0.3

# Health score weights
HEALTH_WEIGHTS: Dict[str, float] = {
    "gc": 0.18,
    "spill": 0.15,
    "skew": 0.15,
    "utilization": 0.12,
    "shuffle": 0.08,
    "task_failures": 0.08,
    "config": 0.08,
    "driver": 0.06,
    "io_efficiency": 0.05,
    "stability": 0.05,
}


def _parse_byte_string(s: str) -> Optional[int]:
    """Parse Spark byte config strings like '64MB', '1g', '256m' to bytes."""
    s = s.strip().lower()
    multipliers = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
    }
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            try:
                return int(float(s[: -len(suffix)]) * mult)
            except ValueError:
                return None
    try:
        return int(s)
    except ValueError:
        return None


class AnalyticsEngine:
    """Stateless engine that computes performance insights from cached metrics.

    All methods are pure functions (no side effects, no API calls).
    The engine reads from Pydantic model lists and returns Insight/HealthScore objects.
    """

    # Counters for generating unique insight IDs per category per analyze() call
    _insight_counters: Dict[str, int]

    def __init__(self) -> None:
        self._insight_counters = {}

    def _next_id(self, category: InsightCategory) -> str:
        """Generate the next unique insight ID for a category.

        Args:
            category: The insight category.

        Returns:
            A unique ID string like 'GC_001'.
        """
        key = category.value
        count = self._insight_counters.get(key, 0) + 1
        self._insight_counters[key] = count
        return f"{key}_{count:03d}"

    # ------------------------------------------------------------------
    # Detector: GC Pressure
    # ------------------------------------------------------------------

    def detect_gc_pressure(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect excessive garbage collection on individual executors.

        Args:
            executors: List of executor info objects.

        Returns:
            Insights sorted by gc_ratio descending (worst first).
        """
        # Determine if we should skip the driver
        active_non_driver = [e for e in executors if e.is_active and not e.is_driver]
        has_non_driver = len(active_non_driver) > 0

        candidates: List[ExecutorInfo] = []
        for e in executors:
            if not e.is_active:
                continue
            if e.total_duration_ms <= 0:
                continue
            if e.is_driver and has_non_driver:
                continue
            candidates.append(e)

        insights: List[Insight] = []
        for e in candidates:
            gc_ratio = e.gc_ratio
            gc_pct = gc_ratio * 100.0

            if gc_ratio >= GC_CRITICAL_RATIO:
                severity = Severity.CRITICAL
                threshold_pct = GC_CRITICAL_RATIO * 100.0
            elif gc_ratio >= GC_WARNING_RATIO:
                severity = Severity.WARNING
                threshold_pct = GC_WARNING_RATIO * 100.0
            else:
                continue

            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.GC),
                    category=InsightCategory.GC,
                    severity=severity,
                    title=f"High GC pressure on executor {e.executor_id} ({gc_pct:.1f}%)",
                    description=(
                        f"Executor {e.executor_id} is spending {gc_pct:.1f}% of its time "
                        f"in garbage collection, exceeding the {threshold_pct:.0f}% threshold."
                    ),
                    metric_value=gc_pct,
                    threshold_value=threshold_pct,
                    recommendation=(
                        "Increase spark.executor.memory, enable Kryo serialization "
                        "(spark.serializer=org.apache.spark.serializer.KryoSerializer), "
                        "or adjust spark.memory.fraction to reduce GC pressure."
                    ),
                    affected_entity=e.executor_id,
                )
            )

        # Sort by gc_ratio descending (worst first)
        insights.sort(key=lambda i: i.metric_value, reverse=True)
        return insights

    # ------------------------------------------------------------------
    # Detector: Memory Spill
    # ------------------------------------------------------------------

    def detect_memory_spill(self, stages: List[SparkStage]) -> List[Insight]:
        """Detect memory-to-disk spill on active and recently completed stages.

        Args:
            stages: List of Spark stage objects.

        Returns:
            Insights sorted by spill_bytes descending.
        """
        insights: List[Insight] = []

        for s in stages:
            if s.status not in (StageStatus.ACTIVE, StageStatus.COMPLETE):
                continue

            total_spill = s.spill_bytes
            if total_spill <= 0:
                continue

            # Determine severity
            severity = Severity.WARNING
            threshold: float = 0.0

            if s.disk_spill_bytes > SPILL_CRITICAL_ABS:
                severity = Severity.CRITICAL
                threshold = float(SPILL_CRITICAL_ABS)
            elif s.input_bytes > 0 and total_spill / s.input_bytes > SPILL_CRITICAL_RATIO:
                severity = Severity.CRITICAL
                threshold = float(s.input_bytes) * SPILL_CRITICAL_RATIO

            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.SPILL),
                    category=InsightCategory.SPILL,
                    severity=severity,
                    title=f"Memory spill on stage {s.stage_id} ({format_bytes(total_spill)})",
                    description=(
                        f"Stage {s.stage_id} has spilled {format_bytes(total_spill)} "
                        f"({format_bytes(s.memory_spill_bytes)} memory, "
                        f"{format_bytes(s.disk_spill_bytes)} disk)."
                    ),
                    metric_value=float(total_spill),
                    threshold_value=threshold,
                    recommendation=(
                        "Increase spark.executor.memory or increase "
                        "spark.sql.shuffle.partitions to reduce partition sizes. "
                        "Consider using broadcast joins for small tables."
                    ),
                    affected_entity=f"stage_{s.stage_id}",
                )
            )

        # Sort by spill bytes descending
        insights.sort(key=lambda i: i.metric_value, reverse=True)
        return insights

    # ------------------------------------------------------------------
    # Detector: Data Skew
    # ------------------------------------------------------------------

    def detect_data_skew(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect data skew via shuffle read imbalance across executors.

        Args:
            executors: List of executor info objects.

        Returns:
            Up to 2 insights (shuffle imbalance + task imbalance).
        """
        # Filter to active, non-driver executors with shuffle reads
        candidates = [e for e in executors if e.is_active and not e.is_driver and e.total_shuffle_read > 0]

        if len(candidates) < 2:
            return []

        insights: List[Insight] = []

        # Shuffle read imbalance
        shuffle_reads = [e.total_shuffle_read for e in candidates]
        median_read = statistics.median(shuffle_reads)

        if median_read > 0:
            max_read = max(shuffle_reads)
            ratio = max_read / median_read
            max_executor = max(candidates, key=lambda e: e.total_shuffle_read)

            if ratio >= SKEW_CRITICAL_FACTOR:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SKEW),
                        category=InsightCategory.SKEW,
                        severity=Severity.CRITICAL,
                        title=f"Data skew detected: {ratio:.1f}x shuffle read imbalance",
                        description=(
                            f"Executor {max_executor.executor_id} has {ratio:.1f}x the median "
                            f"shuffle read ({format_bytes(max_read)} vs median "
                            f"{format_bytes(int(median_read))}), indicating severe data skew."
                        ),
                        metric_value=ratio,
                        threshold_value=SKEW_CRITICAL_FACTOR,
                        recommendation=(
                            "Enable AQE skew join optimization: "
                            "spark.sql.adaptive.skewJoin.enabled=true. "
                            "Consider salting skewed keys or repartitioning data."
                        ),
                        affected_entity=max_executor.executor_id,
                    )
                )
            elif ratio >= SKEW_WARNING_FACTOR:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SKEW),
                        category=InsightCategory.SKEW,
                        severity=Severity.WARNING,
                        title=f"Data skew detected: {ratio:.1f}x shuffle read imbalance",
                        description=(
                            f"Executor {max_executor.executor_id} has {ratio:.1f}x the median "
                            f"shuffle read ({format_bytes(max_read)} vs median "
                            f"{format_bytes(int(median_read))}), indicating data skew."
                        ),
                        metric_value=ratio,
                        threshold_value=SKEW_WARNING_FACTOR,
                        recommendation=(
                            "Enable AQE skew join optimization: "
                            "spark.sql.adaptive.skewJoin.enabled=true. "
                            "Consider salting skewed keys or repartitioning data."
                        ),
                        affected_entity=max_executor.executor_id,
                    )
                )

        # Task imbalance via coefficient of variation
        task_counts = [float(e.completed_tasks) for e in candidates]
        mean_tasks = statistics.mean(task_counts)
        if mean_tasks > 0 and len(task_counts) >= 2:
            stdev_tasks = statistics.stdev(task_counts)
            cov = stdev_tasks / mean_tasks

            if cov >= SKEW_COV_CRITICAL:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SKEW),
                        category=InsightCategory.SKEW,
                        severity=Severity.CRITICAL,
                        title=f"Task distribution skew (CoV={cov:.2f})",
                        description=(
                            f"The coefficient of variation for completed tasks across "
                            f"executors is {cov:.2f}, indicating highly uneven task distribution."
                        ),
                        metric_value=cov,
                        threshold_value=SKEW_COV_CRITICAL,
                        recommendation=(
                            "Enable AQE skew join optimization: "
                            "spark.sql.adaptive.skewJoin.enabled=true. "
                            "Consider repartitioning data for better balance."
                        ),
                        affected_entity="",
                    )
                )
            elif cov >= SKEW_COV_WARNING:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SKEW),
                        category=InsightCategory.SKEW,
                        severity=Severity.WARNING,
                        title=f"Task distribution skew (CoV={cov:.2f})",
                        description=(
                            f"The coefficient of variation for completed tasks across "
                            f"executors is {cov:.2f}, indicating uneven task distribution."
                        ),
                        metric_value=cov,
                        threshold_value=SKEW_COV_WARNING,
                        recommendation=(
                            "Enable AQE skew join optimization: "
                            "spark.sql.adaptive.skewJoin.enabled=true. "
                            "Consider repartitioning data for better balance."
                        ),
                        affected_entity="",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Shuffle Bottleneck
    # ------------------------------------------------------------------

    def detect_shuffle_bottleneck(self, stages: List[SparkStage]) -> List[Insight]:
        """Detect excessive shuffle overhead per stage.

        Args:
            stages: List of Spark stage objects.

        Returns:
            Insights sorted by shuffle_ratio descending.
        """
        insights: List[Insight] = []

        for s in stages:
            if s.status not in (StageStatus.ACTIVE, StageStatus.COMPLETE):
                continue
            if s.input_bytes <= 0:
                continue

            shuffle_total = s.shuffle_read_bytes + s.shuffle_write_bytes
            shuffle_ratio = shuffle_total / s.input_bytes

            if shuffle_ratio >= SHUFFLE_CRITICAL_RATIO:
                severity = Severity.CRITICAL
                threshold = SHUFFLE_CRITICAL_RATIO
            elif shuffle_ratio >= SHUFFLE_WARNING_RATIO:
                severity = Severity.WARNING
                threshold = SHUFFLE_WARNING_RATIO
            else:
                continue

            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.SHUFFLE),
                    category=InsightCategory.SHUFFLE,
                    severity=severity,
                    title=f"Heavy shuffle on stage {s.stage_id} ({shuffle_ratio:.1f}x input)",
                    description=(
                        f"Stage {s.stage_id} has a shuffle-to-input ratio of {shuffle_ratio:.1f}x "
                        f"({format_bytes(shuffle_total)} shuffled vs {format_bytes(s.input_bytes)} input)."
                    ),
                    metric_value=shuffle_ratio,
                    threshold_value=threshold,
                    recommendation=(
                        "Increase spark.sql.autoBroadcastJoinThreshold for small table joins. "
                        "Use coalesce() instead of repartition() where possible. "
                        "Consider pre-partitioning data by join keys."
                    ),
                    affected_entity=f"stage_{s.stage_id}",
                )
            )

        insights.sort(key=lambda i: i.metric_value, reverse=True)
        return insights

    # ------------------------------------------------------------------
    # Detector: Cluster Utilization
    # ------------------------------------------------------------------

    def detect_cluster_utilization(
        self,
        executors: List[ExecutorInfo],
        jobs: List[SparkJob],
    ) -> List[Insight]:
        """Detect underutilization of cluster resources.

        Args:
            executors: List of executor info objects.
            jobs: List of Spark job objects.

        Returns:
            At most one insight for cluster-wide utilization.
        """
        active_executors = [e for e in executors if e.is_active]
        if not active_executors:
            return []

        # Only flag underutilization if there are active/running jobs
        has_running_jobs = any(j.status == JobStatus.RUNNING for j in jobs)
        if not has_running_jobs:
            return []

        total_active_tasks = sum(e.active_tasks for e in active_executors)
        total_slots = sum(e.max_tasks if e.max_tasks > 0 else e.total_cores for e in active_executors)

        if total_slots == 0:
            return []

        utilization_pct = (total_active_tasks / total_slots) * 100.0
        idle_count = sum(1 for e in active_executors if e.active_tasks == 0)

        if utilization_pct < UTIL_CRITICAL_PCT:
            severity = Severity.CRITICAL
            threshold = UTIL_CRITICAL_PCT
        elif utilization_pct < UTIL_WARNING_PCT:
            severity = Severity.WARNING
            threshold = UTIL_WARNING_PCT
        else:
            return []

        return [
            Insight(
                id=self._next_id(InsightCategory.UTILIZATION),
                category=InsightCategory.UTILIZATION,
                severity=severity,
                title=f"Low cluster utilization ({utilization_pct:.0f}%, {idle_count} idle executors)",
                description=(
                    f"Only {utilization_pct:.1f}% of available task slots are in use "
                    f"({total_active_tasks}/{total_slots} slots). "
                    f"{idle_count} executor(s) are completely idle."
                ),
                metric_value=utilization_pct,
                threshold_value=threshold,
                recommendation=(
                    "Reduce the number of workers, increase spark.sql.shuffle.partitions "
                    "for better parallelism, repartition data, or enable auto-termination."
                ),
                affected_entity="",
            )
        ]

    # ------------------------------------------------------------------
    # Detector: Partition Issues
    # ------------------------------------------------------------------

    def detect_partition_issues(
        self,
        stages: List[SparkStage],
        executors: List[ExecutorInfo],
    ) -> List[Insight]:
        """Detect suboptimal partition sizes.

        Args:
            stages: List of Spark stage objects.
            executors: List of executor info objects.

        Returns:
            Insights for stages with oversized or undersized partitions.
        """
        active_executors = [e for e in executors if e.is_active]
        total_cores = sum(e.total_cores for e in active_executors)

        insights: List[Insight] = []

        for s in stages:
            if s.status != StageStatus.ACTIVE:
                continue
            if s.num_tasks <= 0 or s.input_bytes <= 0:
                continue

            avg_partition_bytes = s.input_bytes / s.num_tasks

            if avg_partition_bytes > PARTITION_TOO_LARGE:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.PARTITION),
                        category=InsightCategory.PARTITION,
                        severity=Severity.WARNING,
                        title=f"Large partitions on stage {s.stage_id} ({format_bytes(int(avg_partition_bytes))} avg)",
                        description=(
                            f"Stage {s.stage_id} has an average partition size of "
                            f"{format_bytes(int(avg_partition_bytes))}, which exceeds the "
                            f"recommended maximum of {format_bytes(PARTITION_TOO_LARGE)}."
                        ),
                        metric_value=avg_partition_bytes,
                        threshold_value=float(PARTITION_TOO_LARGE),
                        recommendation=(
                            "Increase spark.sql.shuffle.partitions or use repartition() "
                            "to create more, smaller partitions."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )
            elif avg_partition_bytes < PARTITION_TOO_SMALL:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.PARTITION),
                        category=InsightCategory.PARTITION,
                        severity=Severity.WARNING,
                        title=(
                            f"Tiny partitions on stage {s.stage_id} "
                            f"({format_bytes(int(avg_partition_bytes))} avg, {s.num_tasks} tasks)"
                        ),
                        description=(
                            f"Stage {s.stage_id} has an average partition size of "
                            f"{format_bytes(int(avg_partition_bytes))} with {s.num_tasks} tasks, "
                            f"which is below the recommended minimum of {format_bytes(PARTITION_TOO_SMALL)}."
                        ),
                        metric_value=avg_partition_bytes,
                        threshold_value=float(PARTITION_TOO_SMALL),
                        recommendation=(
                            "Decrease spark.sql.shuffle.partitions or use coalesce() "
                            "to reduce the number of partitions."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )

            # Check task-to-core ratio
            if total_cores > 0 and s.num_tasks < total_cores:
                task_core_ratio = s.num_tasks / total_cores
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.PARTITION),
                        category=InsightCategory.PARTITION,
                        severity=Severity.WARNING,
                        title=(
                            f"Too few partitions on stage {s.stage_id} ({s.num_tasks} tasks for {total_cores} cores)"
                        ),
                        description=(
                            f"Stage {s.stage_id} has only {s.num_tasks} tasks but there are "
                            f"{total_cores} cores available. This limits parallelism."
                        ),
                        metric_value=task_core_ratio,
                        threshold_value=1.0,
                        recommendation=(
                            "Increase spark.sql.shuffle.partitions or use repartition() "
                            "to increase the number of partitions to at least match the core count."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Straggler Tasks
    # ------------------------------------------------------------------

    def detect_straggler_tasks(self, stages: List[SparkStage]) -> List[Insight]:
        """Detect straggler tasks via stage progress stalling near completion.

        Args:
            stages: List of Spark stage objects.

        Returns:
            Insights for stages with potential straggler tasks.
        """
        insights: List[Insight] = []

        for s in stages:
            if s.status != StageStatus.ACTIVE:
                continue
            if s.num_tasks <= 0:
                continue

            progress = s.num_complete_tasks / s.num_tasks
            remaining_tasks = s.num_tasks - s.num_complete_tasks

            if progress > STRAGGLER_PROGRESS_THRESHOLD and remaining_tasks <= 3 and s.num_active_tasks > 0:
                if remaining_tasks == 1:
                    severity = Severity.CRITICAL
                else:
                    severity = Severity.WARNING

                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.STRAGGLER),
                        category=InsightCategory.STRAGGLER,
                        severity=severity,
                        title=(f"Straggler tasks on stage {s.stage_id} ({remaining_tasks} of {s.num_tasks} remaining)"),
                        description=(
                            f"Stage {s.stage_id} is {progress * 100:.1f}% complete with "
                            f"{remaining_tasks} task(s) still running. This pattern suggests "
                            f"straggler tasks that are slower than the rest."
                        ),
                        metric_value=progress * 100.0,
                        threshold_value=STRAGGLER_PROGRESS_THRESHOLD * 100.0,
                        recommendation=(
                            "Enable speculative execution (spark.speculation=true) to "
                            "re-launch slow tasks. Check for data skew and consider "
                            "AQE skew join optimization."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )
            elif s.num_active_tasks == 1 and s.num_complete_tasks > 10 and progress > STRAGGLER_PROGRESS_THRESHOLD:
                # Additional straggler pattern: single active task with many completed
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.STRAGGLER),
                        category=InsightCategory.STRAGGLER,
                        severity=Severity.WARNING,
                        title=(f"Straggler tasks on stage {s.stage_id} ({remaining_tasks} of {s.num_tasks} remaining)"),
                        description=(
                            f"Stage {s.stage_id} has 1 active task with {s.num_complete_tasks} "
                            f"completed, suggesting a straggler task pattern."
                        ),
                        metric_value=progress * 100.0,
                        threshold_value=STRAGGLER_PROGRESS_THRESHOLD * 100.0,
                        recommendation=(
                            "Enable speculative execution (spark.speculation=true) to "
                            "re-launch slow tasks. Check for data skew."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Task Failures
    # ------------------------------------------------------------------

    def detect_task_failures(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect high task failure rates across the cluster.

        Args:
            executors: List of executor info objects.

        Returns:
            Insights for cluster-wide and per-executor failure rates.
        """
        active_executors = [e for e in executors if e.is_active]
        if not active_executors:
            return []

        total_failed = sum(e.failed_tasks for e in active_executors)
        total_completed = sum(e.completed_tasks for e in active_executors)
        total_tasks = total_completed + total_failed

        if total_tasks < 10:
            return []

        failure_rate = total_failed / total_tasks
        failure_pct = failure_rate * 100.0

        insights: List[Insight] = []

        if failure_rate >= FAILURE_CRITICAL_RATE:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.TASK_FAILURE),
                    category=InsightCategory.TASK_FAILURE,
                    severity=Severity.CRITICAL,
                    title=f"Task failure rate {failure_pct:.1f}% ({total_failed}/{total_tasks} tasks)",
                    description=(
                        f"The cluster-wide task failure rate is {failure_pct:.1f}% "
                        f"({total_failed} failed out of {total_tasks} total tasks)."
                    ),
                    metric_value=failure_pct,
                    threshold_value=FAILURE_CRITICAL_RATE * 100.0,
                    recommendation=(
                        "Check executor logs for OOM errors or data integrity issues. "
                        "Consider increasing spark.executor.memory or spark.task.maxFailures."
                    ),
                    affected_entity="",
                )
            )
        elif failure_rate >= FAILURE_WARNING_RATE:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.TASK_FAILURE),
                    category=InsightCategory.TASK_FAILURE,
                    severity=Severity.WARNING,
                    title=f"Task failure rate {failure_pct:.1f}% ({total_failed}/{total_tasks} tasks)",
                    description=(
                        f"The cluster-wide task failure rate is {failure_pct:.1f}% "
                        f"({total_failed} failed out of {total_tasks} total tasks)."
                    ),
                    metric_value=failure_pct,
                    threshold_value=FAILURE_WARNING_RATE * 100.0,
                    recommendation=(
                        "Check executor logs for errors. Consider increasing "
                        "spark.task.maxFailures for transient failures."
                    ),
                    affected_entity="",
                )
            )

        # Detect per-executor failure hotspots
        if total_failed > 0:
            for e in active_executors:
                if e.failed_tasks > 0 and e.failed_tasks > total_failed * 0.5:
                    hotspot_pct = (e.failed_tasks / total_failed) * 100.0
                    insights.append(
                        Insight(
                            id=self._next_id(InsightCategory.TASK_FAILURE),
                            category=InsightCategory.TASK_FAILURE,
                            severity=Severity.WARNING,
                            title=f"Executor {e.executor_id} has {hotspot_pct:.0f}% of all task failures",
                            description=(
                                f"Executor {e.executor_id} accounts for {e.failed_tasks} of "
                                f"{total_failed} total task failures ({hotspot_pct:.0f}%)."
                            ),
                            metric_value=hotspot_pct,
                            threshold_value=50.0,
                            recommendation=(
                                "Check this executor's logs for specific error patterns. "
                                "Consider blacklisting the node if failures persist."
                            ),
                            affected_entity=e.executor_id,
                        )
                    )

        return insights

    # ------------------------------------------------------------------
    # Detector: Memory Pressure
    # ------------------------------------------------------------------

    def detect_memory_pressure(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect executors approaching memory limits.

        Args:
            executors: List of executor info objects.

        Returns:
            Insights sorted by memory_used_pct descending.
        """
        insights: List[Insight] = []

        for e in executors:
            if not e.is_active:
                continue
            if e.max_memory <= 0:
                continue

            mem_pct = e.memory_used_pct

            if mem_pct >= MEMORY_CRITICAL_PCT:
                severity = Severity.CRITICAL
                threshold = MEMORY_CRITICAL_PCT
            elif mem_pct >= MEMORY_WARNING_PCT:
                severity = Severity.WARNING
                threshold = MEMORY_WARNING_PCT
            else:
                # Also check peak JVM heap if available
                if e.peak_jvm_heap > 0:
                    peak_pct = (e.peak_jvm_heap / e.max_memory) * 100.0
                    if peak_pct >= PEAK_HEAP_CRITICAL_PCT:
                        severity = Severity.CRITICAL
                        threshold = PEAK_HEAP_CRITICAL_PCT
                        insights.append(
                            Insight(
                                id=self._next_id(InsightCategory.MEMORY),
                                category=InsightCategory.MEMORY,
                                severity=severity,
                                title=f"High peak JVM heap on executor {e.executor_id} ({peak_pct:.1f}%)",
                                description=(
                                    f"Executor {e.executor_id} peak JVM heap is {peak_pct:.1f}% of max memory."
                                ),
                                metric_value=peak_pct,
                                threshold_value=threshold,
                                recommendation=(
                                    "Increase spark.executor.memory, reduce spark.memory.fraction, "
                                    "or use Kryo serialization to reduce memory footprint."
                                ),
                                affected_entity=e.executor_id,
                            )
                        )
                    elif peak_pct >= PEAK_HEAP_WARNING_PCT:
                        severity = Severity.WARNING
                        threshold = PEAK_HEAP_WARNING_PCT
                        insights.append(
                            Insight(
                                id=self._next_id(InsightCategory.MEMORY),
                                category=InsightCategory.MEMORY,
                                severity=severity,
                                title=f"High peak JVM heap on executor {e.executor_id} ({peak_pct:.1f}%)",
                                description=(
                                    f"Executor {e.executor_id} peak JVM heap is {peak_pct:.1f}% of max memory."
                                ),
                                metric_value=peak_pct,
                                threshold_value=threshold,
                                recommendation=(
                                    "Increase spark.executor.memory, reduce spark.memory.fraction, "
                                    "or use Kryo serialization to reduce memory footprint."
                                ),
                                affected_entity=e.executor_id,
                            )
                        )
                continue

            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.MEMORY),
                    category=InsightCategory.MEMORY,
                    severity=severity,
                    title=f"High memory usage on executor {e.executor_id} ({mem_pct:.1f}%)",
                    description=(
                        f"Executor {e.executor_id} is using {mem_pct:.1f}% of its "
                        f"maximum memory ({format_bytes(e.memory_used)} / "
                        f"{format_bytes(e.max_memory)})."
                    ),
                    metric_value=mem_pct,
                    threshold_value=threshold,
                    recommendation=(
                        "Increase spark.executor.memory, reduce spark.memory.fraction, "
                        "or use Kryo serialization to reduce memory footprint."
                    ),
                    affected_entity=e.executor_id,
                )
            )

        # Sort by metric_value descending (worst first)
        insights.sort(key=lambda i: i.metric_value, reverse=True)
        return insights

    # ------------------------------------------------------------------
    # Detector: AQE Configuration (Phase A)
    # ------------------------------------------------------------------

    def detect_aqe_config(self, cluster: Optional[ClusterInfo]) -> List[Insight]:
        """Detect suboptimal Adaptive Query Execution configuration.

        Args:
            cluster: Cluster info, or None if unavailable.

        Returns:
            Insights for AQE misconfigurations.
        """
        if cluster is None:
            return []

        insights: List[Insight] = []
        conf = cluster.spark_conf

        aqe_enabled = conf.get("spark.sql.adaptive.enabled", "true").lower()
        if aqe_enabled == "false":
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.AQE_CONFIG),
                    category=InsightCategory.AQE_CONFIG,
                    severity=Severity.CRITICAL,
                    title="AQE is disabled",
                    description=(
                        "Adaptive Query Execution is disabled. AQE dynamically optimizes "
                        "query plans at runtime, including partition coalescing and skew handling."
                    ),
                    metric_value=0.0,
                    threshold_value=1.0,
                    recommendation=(
                        "Enable AQE: set spark.sql.adaptive.enabled=true. "
                        "AQE is enabled by default since Spark 3.2 and provides significant "
                        "performance improvements for most workloads."
                    ),
                    affected_entity="",
                )
            )
            return insights

        # AQE is enabled — check sub-features
        coalesce_enabled = conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true").lower()
        if coalesce_enabled == "false":
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.AQE_CONFIG),
                    category=InsightCategory.AQE_CONFIG,
                    severity=Severity.WARNING,
                    title="AQE partition coalescing disabled",
                    description=(
                        "AQE partition coalescing is disabled. This feature merges small "
                        "post-shuffle partitions to reduce task overhead."
                    ),
                    metric_value=0.0,
                    threshold_value=1.0,
                    recommendation=(
                        "Enable partition coalescing: set spark.sql.adaptive.coalescePartitions.enabled=true."
                    ),
                    affected_entity="",
                )
            )

        skew_enabled = conf.get("spark.sql.adaptive.skewJoin.enabled", "true").lower()
        if skew_enabled == "false":
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.AQE_CONFIG),
                    category=InsightCategory.AQE_CONFIG,
                    severity=Severity.WARNING,
                    title="AQE skew join optimization disabled",
                    description=(
                        "AQE skew join handling is disabled. This feature splits skewed "
                        "partitions to prevent straggler tasks during joins."
                    ),
                    metric_value=0.0,
                    threshold_value=1.0,
                    recommendation=("Enable skew join optimization: set spark.sql.adaptive.skewJoin.enabled=true."),
                    affected_entity="",
                )
            )

        # Check advisory partition size
        advisory_raw = conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes", "")
        if advisory_raw:
            advisory_bytes = _parse_byte_string(advisory_raw)
            if advisory_bytes is not None and advisory_bytes < 32 * 1024**2:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.AQE_CONFIG),
                        category=InsightCategory.AQE_CONFIG,
                        severity=Severity.INFO,
                        title=f"AQE advisory partition size is small ({format_bytes(advisory_bytes)})",
                        description=(
                            f"The AQE advisory partition size is {format_bytes(advisory_bytes)}, "
                            f"below the recommended minimum of 32 MB. This may create too many "
                            f"small partitions."
                        ),
                        metric_value=float(advisory_bytes),
                        threshold_value=float(32 * 1024**2),
                        recommendation=(
                            "Increase spark.sql.adaptive.advisoryPartitionSizeInBytes to at least "
                            "32MB (e.g. '64MB') for better partition sizing."
                        ),
                        affected_entity="",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Serialization (Phase A)
    # ------------------------------------------------------------------

    def detect_serialization(
        self,
        cluster: Optional[ClusterInfo],
        executors: List[ExecutorInfo],
    ) -> List[Insight]:
        """Detect suboptimal serialization configuration.

        Args:
            cluster: Cluster info, or None if unavailable.
            executors: List of executor info objects.

        Returns:
            Insights for serialization issues.
        """
        if cluster is None:
            return []

        serializer = cluster.spark_conf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        if "kryo" in serializer.lower():
            return []

        active_non_driver = [e for e in executors if e.is_active and not e.is_driver and e.total_duration_ms > 0]
        if not active_non_driver:
            return []

        avg_gc = statistics.mean([e.gc_ratio for e in active_non_driver])
        total_shuffle = sum(e.total_shuffle_read + e.total_shuffle_write for e in active_non_driver)

        if avg_gc >= GC_CRITICAL_RATIO:
            severity = Severity.CRITICAL
        elif avg_gc >= GC_WARNING_RATIO or total_shuffle > 10 * 1024**3:
            severity = Severity.WARNING
        else:
            return []

        return [
            Insight(
                id=self._next_id(InsightCategory.SERIALIZATION),
                category=InsightCategory.SERIALIZATION,
                severity=severity,
                title="Java serialization in use (Kryo recommended)",
                description=(
                    f"The cluster is using Java serialization with {avg_gc * 100:.1f}% avg GC "
                    f"and {format_bytes(total_shuffle)} total shuffle. Kryo serialization is "
                    f"typically 2-10x faster and more compact."
                ),
                metric_value=avg_gc * 100.0,
                threshold_value=GC_WARNING_RATIO * 100.0,
                recommendation=(
                    "Switch to Kryo serialization: set "
                    "spark.serializer=org.apache.spark.serializer.KryoSerializer. "
                    "Register custom classes with spark.kryo.classesToRegister for best results."
                ),
                affected_entity="",
            )
        ]

    # ------------------------------------------------------------------
    # Detector: Executor Sizing (Phase A)
    # ------------------------------------------------------------------

    def detect_executor_sizing(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect suboptimal executor memory and core sizing.

        Args:
            executors: List of executor info objects.

        Returns:
            Insights for oversized or poorly balanced executors.
        """
        active_non_driver = [e for e in executors if e.is_active and not e.is_driver]
        if not active_non_driver:
            return []

        # All executors typically have the same sizing; use the first one
        e = active_non_driver[0]
        insights: List[Insight] = []

        if e.max_memory > EXECUTOR_HEAP_WARNING_BYTES:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.EXECUTOR_SIZING),
                    category=InsightCategory.EXECUTOR_SIZING,
                    severity=Severity.WARNING,
                    title=f"Large executor heap ({format_bytes(e.max_memory)})",
                    description=(
                        f"Executor heap is {format_bytes(e.max_memory)}, exceeding the "
                        f"recommended maximum of {format_bytes(EXECUTOR_HEAP_WARNING_BYTES)}. "
                        f"Large heaps increase GC pause times."
                    ),
                    metric_value=float(e.max_memory),
                    threshold_value=float(EXECUTOR_HEAP_WARNING_BYTES),
                    recommendation=(
                        "Use more executors with smaller heaps (e.g. 16-32 GB each) instead "
                        "of fewer large executors. This reduces GC pause times."
                    ),
                    affected_entity=e.executor_id,
                )
            )

        if e.total_cores > EXECUTOR_CORES_WARNING:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.EXECUTOR_SIZING),
                    category=InsightCategory.EXECUTOR_SIZING,
                    severity=Severity.WARNING,
                    title=f"High core count per executor ({e.total_cores} cores)",
                    description=(
                        f"Each executor has {e.total_cores} cores, exceeding the recommended "
                        f"maximum of {EXECUTOR_CORES_WARNING}. Too many cores per executor "
                        f"increases HDFS throughput contention."
                    ),
                    metric_value=float(e.total_cores),
                    threshold_value=float(EXECUTOR_CORES_WARNING),
                    recommendation=(
                        "Reduce spark.executor.cores to 4-5 and increase the number of "
                        "executors for better parallelism and reduced contention."
                    ),
                    affected_entity=e.executor_id,
                )
            )

        if e.total_cores > 0 and e.max_memory / e.total_cores < MEMORY_PER_CORE_WARNING_BYTES:
            mem_per_core = e.max_memory / e.total_cores
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.EXECUTOR_SIZING),
                    category=InsightCategory.EXECUTOR_SIZING,
                    severity=Severity.WARNING,
                    title=f"Low memory per core ({format_bytes(int(mem_per_core))}/core)",
                    description=(
                        f"Memory per core is {format_bytes(int(mem_per_core))}, below the "
                        f"recommended minimum of {format_bytes(MEMORY_PER_CORE_WARNING_BYTES)}. "
                        f"This may cause excessive spill to disk."
                    ),
                    metric_value=mem_per_core,
                    threshold_value=float(MEMORY_PER_CORE_WARNING_BYTES),
                    recommendation=(
                        "Increase spark.executor.memory or decrease spark.executor.cores "
                        "to provide at least 2 GB of memory per core."
                    ),
                    affected_entity=e.executor_id,
                )
            )

        return insights

    # ------------------------------------------------------------------
    # Detector: Driver Bottleneck (Phase A)
    # ------------------------------------------------------------------

    def detect_driver_bottleneck(self, executors: List[ExecutorInfo]) -> List[Insight]:
        """Detect driver resource bottlenecks relative to workers.

        Args:
            executors: List of executor info objects.

        Returns:
            Insights for driver GC or memory issues.
        """
        driver = None
        workers: List[ExecutorInfo] = []
        for e in executors:
            if not e.is_active:
                continue
            if e.is_driver:
                driver = e
            else:
                workers.append(e)

        if driver is None or not workers:
            return []

        insights: List[Insight] = []

        # Driver GC check
        worker_gc_ratios = [w.gc_ratio for w in workers if w.total_duration_ms > 0]
        if worker_gc_ratios and driver.total_duration_ms > 0:
            avg_worker_gc = statistics.mean(worker_gc_ratios)
            if driver.gc_ratio > DRIVER_GC_RATIO_FACTOR * avg_worker_gc and driver.gc_ratio > 0.05:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.DRIVER_BOTTLENECK),
                        category=InsightCategory.DRIVER_BOTTLENECK,
                        severity=Severity.WARNING,
                        title=f"Driver GC pressure ({driver.gc_ratio * 100:.1f}% vs {avg_worker_gc * 100:.1f}% avg worker)",
                        description=(
                            f"The driver GC ratio ({driver.gc_ratio * 100:.1f}%) is more than "
                            f"{DRIVER_GC_RATIO_FACTOR:.0f}x the average worker GC ratio "
                            f"({avg_worker_gc * 100:.1f}%). This may indicate the driver is "
                            f"collecting too much data or holding large broadcast variables."
                        ),
                        metric_value=driver.gc_ratio * 100.0,
                        threshold_value=avg_worker_gc * DRIVER_GC_RATIO_FACTOR * 100.0,
                        recommendation=(
                            "Increase spark.driver.memory. Avoid collecting large datasets "
                            "to the driver. Use .show() or .take() instead of .collect(). "
                            "Reduce broadcast variable sizes."
                        ),
                        affected_entity="driver",
                    )
                )

        # Driver memory check
        if driver.memory_used_pct > DRIVER_MEMORY_CRITICAL_PCT:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.DRIVER_BOTTLENECK),
                    category=InsightCategory.DRIVER_BOTTLENECK,
                    severity=Severity.CRITICAL,
                    title=f"Driver memory critical ({driver.memory_used_pct:.1f}%)",
                    description=(
                        f"The driver is using {driver.memory_used_pct:.1f}% of its available "
                        f"memory ({format_bytes(driver.memory_used)} / "
                        f"{format_bytes(driver.max_memory)}). This may cause OOM errors."
                    ),
                    metric_value=driver.memory_used_pct,
                    threshold_value=DRIVER_MEMORY_CRITICAL_PCT,
                    recommendation=(
                        "Increase spark.driver.memory. Avoid collecting large datasets to "
                        "the driver with .collect(). Use .show(), .take(), or write results "
                        "to storage instead."
                    ),
                    affected_entity="driver",
                )
            )

        return insights

    # ------------------------------------------------------------------
    # Detector: Config Anti-Patterns (Phase A)
    # ------------------------------------------------------------------

    def detect_config_anti_patterns(
        self,
        cluster: Optional[ClusterInfo],
        executors: List[ExecutorInfo],
    ) -> List[Insight]:
        """Detect common Spark configuration anti-patterns.

        Args:
            cluster: Cluster info, or None if unavailable.
            executors: List of executor info objects.

        Returns:
            Insights for configuration anti-patterns.
        """
        if cluster is None:
            return []

        insights: List[Insight] = []
        conf = cluster.spark_conf

        # Check shuffle.partitions vs total cores
        total_cores = sum(e.total_cores for e in executors if e.is_active)
        shuffle_partitions_str = conf.get("spark.sql.shuffle.partitions", "200")
        try:
            shuffle_partitions = int(shuffle_partitions_str)
        except ValueError:
            shuffle_partitions = 200

        coalesce_enabled = conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true").lower()
        if total_cores > 0 and shuffle_partitions > 10 * total_cores and coalesce_enabled == "false":
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.CONFIG_ANTI_PATTERN),
                    category=InsightCategory.CONFIG_ANTI_PATTERN,
                    severity=Severity.WARNING,
                    title=f"Excessive shuffle partitions ({shuffle_partitions} vs {total_cores} cores)",
                    description=(
                        f"spark.sql.shuffle.partitions={shuffle_partitions} is more than 10x "
                        f"the total core count ({total_cores}) and AQE coalescing is disabled. "
                        f"This creates excessive task scheduling overhead."
                    ),
                    metric_value=float(shuffle_partitions),
                    threshold_value=float(10 * total_cores),
                    recommendation=(
                        "Either reduce spark.sql.shuffle.partitions to 2-3x core count, "
                        "or enable AQE partition coalescing: "
                        "spark.sql.adaptive.coalescePartitions.enabled=true."
                    ),
                    affected_entity="",
                )
            )

        # Check memory.fraction out of [0.4, 0.8]
        mem_fraction_str = conf.get("spark.memory.fraction", "0.6")
        try:
            mem_fraction = float(mem_fraction_str)
        except ValueError:
            mem_fraction = 0.6

        if mem_fraction < 0.4 or mem_fraction > 0.8:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.CONFIG_ANTI_PATTERN),
                    category=InsightCategory.CONFIG_ANTI_PATTERN,
                    severity=Severity.INFO,
                    title=f"Unusual memory fraction ({mem_fraction:.2f})",
                    description=(
                        f"spark.memory.fraction={mem_fraction:.2f} is outside the typical "
                        f"range of [0.4, 0.8]. The default is 0.6."
                    ),
                    metric_value=mem_fraction,
                    threshold_value=0.6,
                    recommendation=(
                        "Consider using the default spark.memory.fraction=0.6 unless you "
                        "have specific requirements for more user memory or execution memory."
                    ),
                    affected_entity="",
                )
            )

        # Check memory.storageFraction > 0.7
        storage_fraction_str = conf.get("spark.memory.storageFraction", "0.5")
        try:
            storage_fraction = float(storage_fraction_str)
        except ValueError:
            storage_fraction = 0.5

        if storage_fraction > 0.7:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.CONFIG_ANTI_PATTERN),
                    category=InsightCategory.CONFIG_ANTI_PATTERN,
                    severity=Severity.WARNING,
                    title=f"High storage fraction ({storage_fraction:.2f})",
                    description=(
                        f"spark.memory.storageFraction={storage_fraction:.2f} reserves more "
                        f"than 70% of unified memory for caching, leaving little room for "
                        f"execution (shuffles, joins, sorts)."
                    ),
                    metric_value=storage_fraction,
                    threshold_value=0.7,
                    recommendation=(
                        "Reduce spark.memory.storageFraction to 0.5 (default) unless you "
                        "have a caching-heavy workload. High storage fractions can cause "
                        "execution spill to disk."
                    ),
                    affected_entity="",
                )
            )

        # Check autoBroadcastJoinThreshold == "-1"
        broadcast_threshold = conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
        if broadcast_threshold.strip() == "-1":
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.CONFIG_ANTI_PATTERN),
                    category=InsightCategory.CONFIG_ANTI_PATTERN,
                    severity=Severity.INFO,
                    title="Auto broadcast join disabled",
                    description=(
                        "spark.sql.autoBroadcastJoinThreshold is set to -1, disabling "
                        "automatic broadcast joins. This forces all joins to use shuffle, "
                        "which may be slower for small-to-large table joins."
                    ),
                    metric_value=-1.0,
                    threshold_value=0.0,
                    recommendation=(
                        "Re-enable auto broadcast joins: set "
                        "spark.sql.autoBroadcastJoinThreshold=10485760 (10 MB default) "
                        "to allow the optimizer to broadcast small tables automatically."
                    ),
                    affected_entity="",
                )
            )

        return insights

    # ------------------------------------------------------------------
    # Detector: Photon Opportunity (Phase A)
    # ------------------------------------------------------------------

    def detect_photon_opportunity(
        self,
        cluster: Optional[ClusterInfo],
        sql_queries: Optional[List[SQLQuery]],
    ) -> List[Insight]:
        """Detect opportunities to use Photon runtime for SQL-heavy workloads.

        Args:
            cluster: Cluster info, or None if unavailable.
            sql_queries: List of SQL queries, or None.

        Returns:
            Insights for Photon upgrade opportunities.
        """
        if cluster is None:
            return []

        # If already using Photon, nothing to suggest
        if cluster.runtime_engine and "PHOTON" in cluster.runtime_engine.upper():
            return []

        queries = sql_queries or []
        if not queries:
            return []

        completed = [q for q in queries if q.duration_ms > 0]
        if not completed:
            return []

        query_count = len(completed)
        avg_duration_s = statistics.mean([q.duration_ms for q in completed]) / 1000.0

        if query_count > 20 and avg_duration_s > 60.0:
            severity = Severity.WARNING
        elif query_count > 5:
            severity = Severity.INFO
        else:
            return []

        return [
            Insight(
                id=self._next_id(InsightCategory.PHOTON_OPPORTUNITY),
                category=InsightCategory.PHOTON_OPPORTUNITY,
                severity=severity,
                title=f"Photon runtime may improve SQL performance ({query_count} queries)",
                description=(
                    f"This cluster is running {query_count} SQL queries with an average "
                    f"duration of {avg_duration_s:.1f}s on the standard runtime. Photon "
                    f"can accelerate SQL and DataFrame workloads by 2-8x."
                ),
                metric_value=float(query_count),
                threshold_value=5.0,
                recommendation=(
                    "Switch to a Photon-enabled runtime (e.g. Photon Runtime). "
                    "Photon provides native vectorized execution for SQL, joins, "
                    "aggregations, and file I/O."
                ),
                affected_entity="",
            )
        ]

    # ------------------------------------------------------------------
    # Detector: I/O Patterns (Phase B)
    # ------------------------------------------------------------------

    def detect_io_patterns(self, stages: List[SparkStage]) -> List[Insight]:
        """Detect small file / partition problems and I/O-bound stages.

        Args:
            stages: List of Spark stage objects.

        Returns:
            Insights for I/O pattern issues.
        """
        insights: List[Insight] = []

        for s in stages:
            if s.status not in (StageStatus.ACTIVE, StageStatus.COMPLETE):
                continue
            if s.input_bytes <= 0 or s.num_tasks <= 0:
                continue

            bytes_per_task = s.input_bytes / s.num_tasks

            # Small file/partition detection
            if bytes_per_task < SMALL_FILE_BYTES_PER_TASK and s.num_tasks > SMALL_FILE_MIN_TASKS:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.IO_PATTERN),
                        category=InsightCategory.IO_PATTERN,
                        severity=Severity.WARNING,
                        title=f"Small file/partition problem on stage {s.stage_id}",
                        description=(
                            f"Stage {s.stage_id} has {s.num_tasks} tasks processing only "
                            f"{format_bytes(int(bytes_per_task))} each "
                            f"({format_bytes(s.input_bytes)} total). This pattern indicates "
                            f"too many small files or partitions."
                        ),
                        metric_value=bytes_per_task,
                        threshold_value=float(SMALL_FILE_BYTES_PER_TASK),
                        recommendation=(
                            "Compact small files using OPTIMIZE or coalesce(). "
                            "Enable AQE partition coalescing: "
                            "spark.sql.adaptive.coalescePartitions.enabled=true."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )

            # I/O bound detection via CPU efficiency
            if s.executor_run_time_ms > 0:
                cpu_efficiency = s.executor_cpu_time_ns / (s.executor_run_time_ms * 1_000_000)
                if cpu_efficiency < CPU_EFFICIENCY_IO_BOUND and s.input_bytes > 0:
                    insights.append(
                        Insight(
                            id=self._next_id(InsightCategory.IO_PATTERN),
                            category=InsightCategory.IO_PATTERN,
                            severity=Severity.WARNING,
                            title=f"I/O bound stage {s.stage_id} (CPU efficiency {cpu_efficiency:.0%})",
                            description=(
                                f"Stage {s.stage_id} has {cpu_efficiency:.0%} CPU efficiency, "
                                f"meaning most time is spent waiting for I/O rather than computing."
                            ),
                            metric_value=cpu_efficiency,
                            threshold_value=CPU_EFFICIENCY_IO_BOUND,
                            recommendation=(
                                "Consider caching frequently read data, using columnar formats "
                                "(Parquet/Delta), or increasing I/O parallelism. "
                                "Check for remote storage latency."
                            ),
                            affected_entity=f"stage_{s.stage_id}",
                        )
                    )

        return insights

    # ------------------------------------------------------------------
    # Detector: CPU/IO Classification (Phase B)
    # ------------------------------------------------------------------

    def detect_cpu_io_classification(self, stages: List[SparkStage]) -> List[Insight]:
        """Classify the overall workload as CPU-bound or I/O-bound.

        Args:
            stages: List of Spark stage objects.

        Returns:
            At most one insight classifying the workload.
        """
        active = [s for s in stages if s.status == StageStatus.ACTIVE and s.executor_run_time_ms > 0]
        if not active:
            return []

        efficiencies = [s.executor_cpu_time_ns / (s.executor_run_time_ms * 1_000_000) for s in active]
        avg_efficiency = statistics.mean(efficiencies)

        if avg_efficiency > CPU_EFFICIENCY_CPU_BOUND:
            return [
                Insight(
                    id=self._next_id(InsightCategory.CPU_IO_BOUND),
                    category=InsightCategory.CPU_IO_BOUND,
                    severity=Severity.INFO,
                    title=f"CPU-bound workload (avg CPU efficiency {avg_efficiency:.0%})",
                    description=(
                        f"Active stages have an average CPU efficiency of {avg_efficiency:.0%}, "
                        f"indicating a compute-intensive workload."
                    ),
                    metric_value=avg_efficiency,
                    threshold_value=CPU_EFFICIENCY_CPU_BOUND,
                    recommendation=(
                        "For CPU-bound workloads, add more cores or enable Photon runtime. "
                        "Consider optimizing UDFs or switching to built-in Spark functions."
                    ),
                    affected_entity="",
                )
            ]
        elif avg_efficiency < CPU_EFFICIENCY_IO_BOUND:
            return [
                Insight(
                    id=self._next_id(InsightCategory.CPU_IO_BOUND),
                    category=InsightCategory.CPU_IO_BOUND,
                    severity=Severity.INFO,
                    title=f"I/O-bound workload (avg CPU efficiency {avg_efficiency:.0%})",
                    description=(
                        f"Active stages have an average CPU efficiency of {avg_efficiency:.0%}, "
                        f"indicating most time is spent on I/O rather than computation."
                    ),
                    metric_value=avg_efficiency,
                    threshold_value=CPU_EFFICIENCY_IO_BOUND,
                    recommendation=(
                        "For I/O-bound workloads, consider caching hot data, using columnar "
                        "formats (Parquet/Delta), compacting small files, or upgrading "
                        "to faster storage."
                    ),
                    affected_entity="",
                )
            ]

        return []

    # ------------------------------------------------------------------
    # Detector: Stage Retries (Phase B)
    # ------------------------------------------------------------------

    def detect_stage_retries(self, stages: List[SparkStage]) -> List[Insight]:
        """Detect stages that have been retried.

        Args:
            stages: List of Spark stage objects.

        Returns:
            Insights for retried stages.
        """
        insights: List[Insight] = []
        retried_count = 0

        for s in stages:
            if s.attempt_id <= 0:
                continue

            retried_count += 1
            if s.attempt_id >= 3:
                severity = Severity.CRITICAL
            else:
                severity = Severity.WARNING

            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.STAGE_RETRY),
                    category=InsightCategory.STAGE_RETRY,
                    severity=severity,
                    title=f"Stage {s.stage_id} retried (attempt {s.attempt_id})",
                    description=(
                        f"Stage {s.stage_id} is on attempt {s.attempt_id}, indicating "
                        f"previous attempts failed. Repeated retries waste compute and "
                        f"delay job completion."
                    ),
                    metric_value=float(s.attempt_id),
                    threshold_value=1.0,
                    recommendation=(
                        "Check executor logs for the root cause of stage failures. "
                        "Common causes: OOM errors, fetch failures, node decommissioning. "
                        "Consider increasing spark.executor.memory or spark.task.maxFailures."
                    ),
                    affected_entity=f"stage_{s.stage_id}",
                )
            )

        # Escalate if multiple stages are being retried
        if retried_count > 1:
            # Find the highest severity insight and promote if needed
            for insight in insights:
                if insight.severity == Severity.WARNING:
                    insights.append(
                        Insight(
                            id=self._next_id(InsightCategory.STAGE_RETRY),
                            category=InsightCategory.STAGE_RETRY,
                            severity=Severity.CRITICAL,
                            title=f"Multiple stages retried ({retried_count} stages)",
                            description=(
                                f"{retried_count} stages have been retried, indicating "
                                f"a systemic issue such as unstable nodes or memory pressure."
                            ),
                            metric_value=float(retried_count),
                            threshold_value=1.0,
                            recommendation=(
                                "Multiple stage retries suggest a cluster-wide issue. "
                                "Check for unstable nodes, insufficient memory, or network "
                                "problems. Consider increasing executor memory or using "
                                "larger instance types."
                            ),
                            affected_entity="",
                        )
                    )
                    break

        return insights

    # ------------------------------------------------------------------
    # Detector: SQL Anomalies (Phase B)
    # ------------------------------------------------------------------

    def detect_sql_anomalies(self, sql_queries: Optional[List[SQLQuery]]) -> List[Insight]:
        """Detect anomalous SQL query patterns.

        Args:
            sql_queries: List of SQL queries, or None.

        Returns:
            Insights for SQL anomalies.
        """
        if not sql_queries:
            return []

        insights: List[Insight] = []

        # Compute median duration of completed queries
        completed = [q for q in sql_queries if q.status != "RUNNING" and q.duration_ms > 0]
        if len(completed) >= 2:
            durations = [q.duration_ms for q in completed]
            median_duration = statistics.median(durations)
        else:
            median_duration = 0.0

        for q in sql_queries:
            # Flag long-running queries
            if q.status == "RUNNING" and q.duration_ms > SQL_LONG_RUNNING_MS:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SQL_ANOMALY),
                        category=InsightCategory.SQL_ANOMALY,
                        severity=Severity.INFO,
                        title=f"Long-running SQL query #{q.execution_id} ({q.duration_ms / 1000:.0f}s)",
                        description=(
                            f"SQL query #{q.execution_id} has been running for "
                            f"{q.duration_ms / 1000:.0f}s, exceeding the "
                            f"{SQL_LONG_RUNNING_MS / 1000:.0f}s threshold."
                        ),
                        metric_value=float(q.duration_ms),
                        threshold_value=float(SQL_LONG_RUNNING_MS),
                        recommendation=(
                            "Review the query execution plan for optimization opportunities. "
                            "Check for missing partition pruning, inefficient joins, or "
                            "full table scans."
                        ),
                        affected_entity=f"sql_{q.execution_id}",
                    )
                )

            # Flag duration outliers
            if median_duration > 0 and q.duration_ms > SQL_ANOMALY_MIN_DURATION_MS:
                ratio = q.duration_ms / median_duration
                if ratio >= SQL_ANOMALY_CRITICAL_FACTOR:
                    insights.append(
                        Insight(
                            id=self._next_id(InsightCategory.SQL_ANOMALY),
                            category=InsightCategory.SQL_ANOMALY,
                            severity=Severity.CRITICAL,
                            title=f"SQL query #{q.execution_id} is {ratio:.0f}x slower than median",
                            description=(
                                f"SQL query #{q.execution_id} took {q.duration_ms / 1000:.0f}s, "
                                f"which is {ratio:.0f}x the median duration of "
                                f"{median_duration / 1000:.0f}s."
                            ),
                            metric_value=ratio,
                            threshold_value=SQL_ANOMALY_CRITICAL_FACTOR,
                            recommendation=(
                                "This query is a significant outlier. Review its execution plan, "
                                "check for data skew or missing statistics, and consider "
                                "adding partition pruning or indexing."
                            ),
                            affected_entity=f"sql_{q.execution_id}",
                        )
                    )
                elif ratio >= SQL_ANOMALY_WARNING_FACTOR:
                    insights.append(
                        Insight(
                            id=self._next_id(InsightCategory.SQL_ANOMALY),
                            category=InsightCategory.SQL_ANOMALY,
                            severity=Severity.WARNING,
                            title=f"SQL query #{q.execution_id} is {ratio:.0f}x slower than median",
                            description=(
                                f"SQL query #{q.execution_id} took {q.duration_ms / 1000:.0f}s, "
                                f"which is {ratio:.0f}x the median duration of "
                                f"{median_duration / 1000:.0f}s."
                            ),
                            metric_value=ratio,
                            threshold_value=SQL_ANOMALY_WARNING_FACTOR,
                            recommendation=(
                                "Review this query's execution plan for optimization "
                                "opportunities. Check for data skew or missing statistics."
                            ),
                            affected_entity=f"sql_{q.execution_id}",
                        )
                    )

            # Flag queries with failed jobs
            if q.failed_jobs > 0:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.SQL_ANOMALY),
                        category=InsightCategory.SQL_ANOMALY,
                        severity=Severity.WARNING,
                        title=f"SQL query #{q.execution_id} has {q.failed_jobs} failed job(s)",
                        description=(
                            f"SQL query #{q.execution_id} has {q.failed_jobs} failed job(s) "
                            f"out of {q.success_jobs + q.failed_jobs + q.running_jobs} total. "
                            f"Failed jobs may indicate data issues or resource constraints."
                        ),
                        metric_value=float(q.failed_jobs),
                        threshold_value=0.0,
                        recommendation=(
                            "Check the Spark UI for detailed error messages on the failed jobs. "
                            "Common causes: OOM, data format issues, or permission errors."
                        ),
                        affected_entity=f"sql_{q.execution_id}",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Join Strategy (Phase C)
    # ------------------------------------------------------------------

    def detect_join_strategy(
        self,
        stages: List[SparkStage],
        cluster: Optional[ClusterInfo],
    ) -> List[Insight]:
        """Detect stages that may benefit from broadcast joins.

        Args:
            stages: List of Spark stage objects.
            cluster: Cluster info, or None if unavailable.

        Returns:
            Insights for potential broadcast join candidates.
        """
        insights: List[Insight] = []

        for s in stages:
            if s.status not in (StageStatus.ACTIVE, StageStatus.COMPLETE):
                continue
            if s.input_bytes <= 0:
                continue

            shuffle_total = s.shuffle_read_bytes + s.shuffle_write_bytes
            if shuffle_total > JOIN_SHUFFLE_INPUT_RATIO * s.input_bytes and s.input_bytes < JOIN_SMALL_INPUT_BYTES:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.JOIN_STRATEGY),
                        category=InsightCategory.JOIN_STRATEGY,
                        severity=Severity.WARNING,
                        title=f"Possible broadcast join candidate (stage {s.stage_id})",
                        description=(
                            f"Stage {s.stage_id} has {format_bytes(shuffle_total)} shuffle "
                            f"for only {format_bytes(s.input_bytes)} input "
                            f"({shuffle_total / s.input_bytes:.1f}x ratio). The small input "
                            f"size suggests a broadcast join could eliminate the shuffle."
                        ),
                        metric_value=shuffle_total / s.input_bytes,
                        threshold_value=JOIN_SHUFFLE_INPUT_RATIO,
                        recommendation=(
                            "Use a broadcast join hint (/*+ BROADCAST(small_table) */) or "
                            "increase spark.sql.autoBroadcastJoinThreshold to cover this "
                            "table's size."
                        ),
                        affected_entity=f"stage_{s.stage_id}",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Caching Issues (Phase C)
    # ------------------------------------------------------------------

    def detect_caching_issues(
        self,
        storage: Optional[List[RDDInfo]],
        executors: List[ExecutorInfo],
        cluster: Optional[ClusterInfo],
    ) -> List[Insight]:
        """Detect caching inefficiencies.

        Args:
            storage: List of cached RDDs/DataFrames, or None.
            executors: List of executor info objects.
            cluster: Cluster info, or None if unavailable.

        Returns:
            Insights for caching issues.
        """
        if not storage:
            return []

        insights: List[Insight] = []

        # Check for partially cached RDDs
        for rdd in storage:
            if rdd.fraction_cached < CACHE_PARTIAL_THRESHOLD:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.CACHING),
                        category=InsightCategory.CACHING,
                        severity=Severity.WARNING,
                        title=f"Partially cached: {rdd.name or f'RDD {rdd.rdd_id}'} ({rdd.fraction_cached:.0%})",
                        description=(
                            f"'{rdd.name or f'RDD {rdd.rdd_id}'}' has only "
                            f"{rdd.num_cached_partitions}/{rdd.num_partitions} partitions cached "
                            f"({rdd.fraction_cached:.0%}). Partial caching may cause recomputation."
                        ),
                        metric_value=rdd.fraction_cached,
                        threshold_value=CACHE_PARTIAL_THRESHOLD,
                        recommendation=(
                            "Ensure sufficient memory for full caching, or switch to "
                            "MEMORY_AND_DISK storage level to prevent evictions. "
                            "Consider unpersisting unused cached data."
                        ),
                        affected_entity=f"rdd_{rdd.rdd_id}",
                    )
                )

        # Check total cache memory usage
        active_executors = [e for e in executors if e.is_active and not e.is_driver]
        total_executor_memory = sum(e.max_memory for e in active_executors)
        total_cache_memory = sum(rdd.memory_used for rdd in storage)

        if total_executor_memory > 0:
            cache_pct = (total_cache_memory / total_executor_memory) * 100.0
            if cache_pct > CACHE_MEMORY_WARNING_PCT:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.CACHING),
                        category=InsightCategory.CACHING,
                        severity=Severity.WARNING,
                        title=f"High cache memory usage ({cache_pct:.0f}% of executor memory)",
                        description=(
                            f"Cached data is using {format_bytes(total_cache_memory)} "
                            f"({cache_pct:.0f}% of {format_bytes(total_executor_memory)} "
                            f"total executor memory). This leaves limited memory for execution."
                        ),
                        metric_value=cache_pct,
                        threshold_value=CACHE_MEMORY_WARNING_PCT,
                        recommendation=(
                            "Unpersist unused cached data with .unpersist(). "
                            "Use MEMORY_AND_DISK storage level to spill to disk instead of "
                            "evicting. Consider increasing executor memory."
                        ),
                        affected_entity="",
                    )
                )

        return insights

    # ------------------------------------------------------------------
    # Detector: Auto-Termination (Phase C)
    # ------------------------------------------------------------------

    def detect_auto_termination(
        self,
        cluster: Optional[ClusterInfo],
        jobs: List[SparkJob],
        executors: List[ExecutorInfo],
    ) -> List[Insight]:
        """Detect auto-termination configuration issues.

        Args:
            cluster: Cluster info, or None if unavailable.
            jobs: List of Spark jobs.
            executors: List of executor info objects.

        Returns:
            Insights for auto-termination configuration.
        """
        if cluster is None:
            return []

        insights: List[Insight] = []
        auto_term_mins = cluster.autotermination_minutes

        if auto_term_mins == 0:
            has_active_jobs = any(j.status == JobStatus.RUNNING for j in jobs)
            has_active_tasks = any(e.active_tasks > 0 for e in executors if e.is_active)

            if not has_active_jobs and not has_active_tasks:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.AUTO_TERMINATION),
                        category=InsightCategory.AUTO_TERMINATION,
                        severity=Severity.CRITICAL,
                        title="Idle cluster with no auto-termination",
                        description=(
                            "The cluster has no active jobs or tasks and auto-termination "
                            "is disabled. This will incur unnecessary costs."
                        ),
                        metric_value=0.0,
                        threshold_value=1.0,
                        recommendation=(
                            "Enable auto-termination (e.g. 30-60 minutes) to avoid paying "
                            "for idle clusters. Set autotermination_minutes in the cluster "
                            "configuration."
                        ),
                        affected_entity="",
                    )
                )
            else:
                insights.append(
                    Insight(
                        id=self._next_id(InsightCategory.AUTO_TERMINATION),
                        category=InsightCategory.AUTO_TERMINATION,
                        severity=Severity.WARNING,
                        title="Auto-termination disabled",
                        description=(
                            "Auto-termination is disabled on this cluster. The cluster "
                            "will continue running (and incurring costs) indefinitely."
                        ),
                        metric_value=0.0,
                        threshold_value=1.0,
                        recommendation=(
                            "Enable auto-termination with a reasonable timeout (e.g. 30-120 "
                            "minutes) to prevent cost overruns from forgotten clusters."
                        ),
                        affected_entity="",
                    )
                )
        elif auto_term_mins > 120:
            insights.append(
                Insight(
                    id=self._next_id(InsightCategory.AUTO_TERMINATION),
                    category=InsightCategory.AUTO_TERMINATION,
                    severity=Severity.INFO,
                    title=f"Long auto-termination timeout ({auto_term_mins} min)",
                    description=(
                        f"Auto-termination is set to {auto_term_mins} minutes. Consider "
                        f"a shorter timeout to reduce costs during idle periods."
                    ),
                    metric_value=float(auto_term_mins),
                    threshold_value=120.0,
                    recommendation=(
                        "Consider reducing auto-termination to 30-60 minutes for "
                        "development clusters, or 60-120 minutes for production workloads."
                    ),
                    affected_entity="",
                )
            )

        return insights

    # ------------------------------------------------------------------
    # Detector: Dynamic Allocation (Phase C)
    # ------------------------------------------------------------------

    def detect_dynamic_allocation(
        self,
        executors: List[ExecutorInfo],
        cache: Optional[DataCache],
    ) -> List[Insight]:
        """Detect executor count instability from dynamic allocation.

        Args:
            executors: Current executor list.
            cache: Data cache for historical executor snapshots, or None.

        Returns:
            Insights for executor count instability.
        """
        if cache is None:
            return []

        history: deque[object] = cache.get_history("executors")
        if len(history) < 3:
            return []

        # Count active executors in each snapshot
        counts: List[float] = []
        for snapshot in history:
            if isinstance(snapshot, list):
                active_count = sum(1 for e in snapshot if isinstance(e, ExecutorInfo) and e.is_active)
                counts.append(float(active_count))

        if len(counts) < 3:
            return []

        mean_count = statistics.mean(counts)
        if mean_count <= 0:
            return []

        stdev_count = statistics.stdev(counts)
        cov = stdev_count / mean_count

        if cov > EXECUTOR_COUNT_COV_WARNING:
            return [
                Insight(
                    id=self._next_id(InsightCategory.DYNAMIC_ALLOCATION),
                    category=InsightCategory.DYNAMIC_ALLOCATION,
                    severity=Severity.WARNING,
                    title=f"Executor count unstable (CoV={cov:.2f})",
                    description=(
                        f"Executor count has varied significantly over recent polls "
                        f"(mean={mean_count:.1f}, stdev={stdev_count:.1f}, CoV={cov:.2f}). "
                        f"Frequent scaling may cause shuffle fetch failures and performance "
                        f"degradation."
                    ),
                    metric_value=cov,
                    threshold_value=EXECUTOR_COUNT_COV_WARNING,
                    recommendation=(
                        "If using dynamic allocation, increase "
                        "spark.dynamicAllocation.executorIdleTimeout or set "
                        "spark.dynamicAllocation.minExecutors closer to the typical count. "
                        "Consider using a fixed executor count for stable workloads."
                    ),
                    affected_entity="",
                )
            ]

        return []

    # ------------------------------------------------------------------
    # Health Score
    # ------------------------------------------------------------------

    def compute_health_score(
        self,
        insights: List[Insight],
        executors: List[ExecutorInfo],
        stages: List[SparkStage],
    ) -> HealthScore:
        """Compute the overall health score from insights and raw data.

        Args:
            insights: List of insights from detectors.
            executors: List of executor info objects.
            stages: List of Spark stage objects.

        Returns:
            A HealthScore with component breakdown.
        """
        active_executors = [e for e in executors if e.is_active]

        if not active_executors:
            return HealthScore(score=0, label="N/A", color="dim")

        # Non-driver active executors for GC and skew calculations
        non_driver = [e for e in active_executors if not e.is_driver]
        if not non_driver:
            non_driver = active_executors

        # -- GC Score --
        gc_ratios = [e.gc_ratio for e in non_driver if e.total_duration_ms > 0]
        if gc_ratios:
            max_gc_pct = max(gc_ratios) * 100.0
            gc_score = max(0, min(100, int(100 - (max_gc_pct - 2) * (100 / 18))))
        else:
            gc_score = 100

        # -- Spill Score --
        active_stages = [s for s in stages if s.status == StageStatus.ACTIVE]
        if active_stages:
            total_spill = sum(s.spill_bytes for s in active_stages)
            spill_score = max(0, min(100, int(100 - (total_spill / 10_737_418_240) * 100)))
        else:
            spill_score = 100

        # -- Skew Score --
        shuffle_candidates = [e for e in non_driver if e.total_shuffle_read > 0]
        if len(shuffle_candidates) >= 2:
            shuffle_reads = [float(e.total_shuffle_read) for e in shuffle_candidates]
            mean_sr = statistics.mean(shuffle_reads)
            if mean_sr > 0:
                stdev_sr = statistics.stdev(shuffle_reads)
                cov = stdev_sr / mean_sr
                skew_score = max(0, min(100, int(100 - (cov - 0.3) * (100 / 1.7))))
            else:
                skew_score = 100
        else:
            skew_score = 100

        # -- Utilization Score --
        total_active_tasks = sum(e.active_tasks for e in active_executors)
        total_slots = sum(e.max_tasks if e.max_tasks > 0 else e.total_cores for e in active_executors)
        if total_slots > 0:
            utilization_pct = (total_active_tasks / total_slots) * 100.0
            util_score = min(100, int(utilization_pct * (100 / 80)))
        else:
            util_score = 100

        # -- Shuffle Score --
        active_input_stages = [s for s in active_stages if s.input_bytes > 0]
        if active_input_stages:
            ratios = [(s.shuffle_read_bytes + s.shuffle_write_bytes) / s.input_bytes for s in active_input_stages]
            avg_ratio = sum(ratios) / len(ratios)
            shuffle_score = max(0, min(100, int(100 - (avg_ratio - 0.5) * (100 / 4.5))))
        else:
            shuffle_score = 100

        # -- Task Failures Score --
        total_failed = sum(e.failed_tasks for e in active_executors)
        total_completed = sum(e.completed_tasks for e in active_executors)
        total_tasks = total_completed + total_failed
        if total_tasks > 0:
            failure_rate_pct = (total_failed / total_tasks) * 100.0
            task_failures_score = max(0, min(100, int(100 - (failure_rate_pct / 5) * 100)))
        else:
            task_failures_score = 100

        # -- Config Score (from Phase A insight counts/severity) --
        config_categories = {
            InsightCategory.AQE_CONFIG,
            InsightCategory.SERIALIZATION,
            InsightCategory.CONFIG_ANTI_PATTERN,
            InsightCategory.PHOTON_OPPORTUNITY,
        }
        config_score = 100
        for i in insights:
            if i.category in config_categories:
                if i.severity == Severity.CRITICAL:
                    config_score -= 30
                elif i.severity == Severity.WARNING:
                    config_score -= 15
        config_score = max(0, config_score)

        # -- Driver Score (from DRIVER_BOTTLENECK insights) --
        driver_score = 100
        for i in insights:
            if i.category == InsightCategory.DRIVER_BOTTLENECK:
                if i.severity == Severity.CRITICAL:
                    driver_score -= 30
                elif i.severity == Severity.WARNING:
                    driver_score -= 15
        driver_score = max(0, driver_score)

        # -- I/O Efficiency Score (from IO_PATTERN and CPU_IO_BOUND WARNING+) --
        io_efficiency_score = 100
        for i in insights:
            if i.category in (InsightCategory.IO_PATTERN, InsightCategory.CPU_IO_BOUND):
                if i.severity == Severity.CRITICAL:
                    io_efficiency_score -= 30
                elif i.severity == Severity.WARNING:
                    io_efficiency_score -= 15
        io_efficiency_score = max(0, io_efficiency_score)

        # -- Stability Score (from STAGE_RETRY and DYNAMIC_ALLOCATION insights) --
        stability_score = 100
        for i in insights:
            if i.category in (InsightCategory.STAGE_RETRY, InsightCategory.DYNAMIC_ALLOCATION):
                if i.severity == Severity.CRITICAL:
                    stability_score -= 30
                elif i.severity == Severity.WARNING:
                    stability_score -= 15
        stability_score = max(0, stability_score)

        component_scores = {
            "gc": gc_score,
            "spill": spill_score,
            "skew": skew_score,
            "utilization": util_score,
            "shuffle": shuffle_score,
            "task_failures": task_failures_score,
            "config": config_score,
            "driver": driver_score,
            "io_efficiency": io_efficiency_score,
            "stability": stability_score,
        }

        # Weighted sum
        weighted_score = sum(component_scores[k] * HEALTH_WEIGHTS[k] for k in HEALTH_WEIGHTS)
        score = max(0, min(100, int(weighted_score)))

        # Map score to label and color
        label, color = self._score_to_label_color(score)

        return HealthScore(
            score=score,
            label=label,
            color=color,
            component_scores=component_scores,
        )

    @staticmethod
    def _score_to_label_color(score: int) -> tuple[str, str]:
        """Map a numeric score to a human label and color.

        Args:
            score: Health score 0-100.

        Returns:
            Tuple of (label, color).
        """
        if score >= 90:
            return "Excellent", "green"
        elif score >= 70:
            return "Good", "blue"
        elif score >= 50:
            return "Fair", "yellow"
        elif score >= 30:
            return "Poor", "#ff8c00"
        else:
            return "Critical", "red"

    # ------------------------------------------------------------------
    # Recommendations
    # ------------------------------------------------------------------

    def _generate_recommendations(self, insights: List[Insight]) -> List[Recommendation]:
        """Group insights by category and produce one recommendation per category.

        Args:
            insights: List of insights from detectors.

        Returns:
            Up to 5 recommendations sorted by priority ascending.
        """
        if not insights:
            return []

        # Group insights by category
        by_category: Dict[InsightCategory, List[Insight]] = {}
        for insight in insights:
            by_category.setdefault(insight.category, []).append(insight)

        severity_priority = {
            Severity.CRITICAL: 1,
            Severity.WARNING: 3,
            Severity.INFO: 5,
        }

        recommendations: List[Recommendation] = []
        for category, category_insights in by_category.items():
            # Find worst severity
            worst_severity = min(
                (i.severity for i in category_insights),
                key=lambda s: severity_priority.get(s, 5),
            )
            # Use worst insight's recommendation as description
            worst_insight = min(
                category_insights,
                key=lambda i: severity_priority.get(i.severity, 5),
            )

            count = len(category_insights)
            category_display = category.value.replace("_", " ").title()

            recommendations.append(
                Recommendation(
                    title=f"{category_display}: {count} issue(s) detected",
                    description=worst_insight.recommendation,
                    priority=severity_priority.get(worst_severity, 5),
                    category=category,
                )
            )

        # Sort by priority ascending
        recommendations.sort(key=lambda r: r.priority)

        # Return top 5
        return recommendations[:5]

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------

    def analyze(
        self,
        executors: Optional[List[ExecutorInfo]],
        stages: Optional[List[SparkStage]],
        jobs: Optional[List[SparkJob]],
        sql_queries: Optional[List[SQLQuery]],
        cluster: Optional[ClusterInfo],
        storage: Optional[List[RDDInfo]] = None,
        cache: Optional[DataCache] = None,
    ) -> Optional[DiagnosticReport]:
        """Run all detectors and produce a complete diagnostic report.

        Args:
            executors: List of executor info, or None if unavailable.
            stages: List of Spark stages, or None.
            jobs: List of Spark jobs, or None.
            sql_queries: List of SQL queries, or None.
            cluster: Cluster info, or None.
            storage: List of cached RDDs/DataFrames, or None.
            cache: Data cache for historical data, or None.

        Returns:
            A DiagnosticReport, or None if executors is None (insufficient data).
        """
        if executors is None:
            return None

        # Reset insight counters for each analyze call
        self._insight_counters = {}

        safe_stages: List[SparkStage] = stages if stages is not None else []
        safe_jobs: List[SparkJob] = jobs if jobs is not None else []

        # Run all detectors
        all_insights: List[Insight] = []
        all_insights.extend(self.detect_gc_pressure(executors))
        all_insights.extend(self.detect_memory_spill(safe_stages))
        all_insights.extend(self.detect_data_skew(executors))
        all_insights.extend(self.detect_shuffle_bottleneck(safe_stages))
        all_insights.extend(self.detect_cluster_utilization(executors, safe_jobs))
        all_insights.extend(self.detect_partition_issues(safe_stages, executors))
        all_insights.extend(self.detect_straggler_tasks(safe_stages))
        all_insights.extend(self.detect_task_failures(executors))
        all_insights.extend(self.detect_memory_pressure(executors))

        # Phase A — Config / model checks
        all_insights.extend(self.detect_aqe_config(cluster))
        all_insights.extend(self.detect_serialization(cluster, executors))
        all_insights.extend(self.detect_executor_sizing(executors))
        all_insights.extend(self.detect_driver_bottleneck(executors))
        all_insights.extend(self.detect_config_anti_patterns(cluster, executors))
        all_insights.extend(self.detect_photon_opportunity(cluster, sql_queries))

        # Phase B — Metric-derived detections
        all_insights.extend(self.detect_io_patterns(safe_stages))
        all_insights.extend(self.detect_cpu_io_classification(safe_stages))
        all_insights.extend(self.detect_stage_retries(safe_stages))
        all_insights.extend(self.detect_sql_anomalies(sql_queries))

        # Phase C — Cross-reference detections
        all_insights.extend(self.detect_join_strategy(safe_stages, cluster))
        all_insights.extend(self.detect_caching_issues(storage, executors, cluster))
        all_insights.extend(self.detect_auto_termination(cluster, safe_jobs, executors))
        all_insights.extend(self.detect_dynamic_allocation(executors, cache))

        # Deduplicate by id
        seen_ids: set[str] = set()
        unique_insights: List[Insight] = []
        for insight in all_insights:
            if insight.id not in seen_ids:
                seen_ids.add(insight.id)
                unique_insights.append(insight)

        # Sort: CRITICAL first, then WARNING, then INFO
        severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
        unique_insights.sort(key=lambda i: severity_order.get(i.severity, 99))

        # Compute health score
        health = self.compute_health_score(unique_insights, executors, safe_stages)

        # Generate recommendations
        recommendations = self._generate_recommendations(unique_insights)

        # Count active entities
        active_executors = [e for e in executors if e.is_active]
        active_stages = [s for s in safe_stages if s.status == StageStatus.ACTIVE]
        active_jobs = [j for j in safe_jobs if j.status == JobStatus.RUNNING]

        return DiagnosticReport(
            health=health,
            insights=unique_insights,
            recommendations=recommendations,
            timestamp=datetime.now(timezone.utc),
            executor_count=len(active_executors),
            active_stage_count=len(active_stages),
            active_job_count=len(active_jobs),
        )
