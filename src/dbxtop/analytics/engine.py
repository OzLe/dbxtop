"""Stateless analytics engine for computing performance insights.

The engine takes lists of Pydantic model instances from the cache and
returns Insight, HealthScore, and DiagnosticReport objects. All methods
are pure functions with no side effects, no API calls, and no state
mutation.
"""

from __future__ import annotations

import statistics
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
from dbxtop.api.models import (
    ClusterInfo,
    ExecutorInfo,
    JobStatus,
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

# Health score weights
HEALTH_WEIGHTS: Dict[str, float] = {
    "gc": 0.25,
    "spill": 0.20,
    "skew": 0.20,
    "utilization": 0.15,
    "shuffle": 0.10,
    "task_failures": 0.10,
}


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

        component_scores = {
            "gc": gc_score,
            "spill": spill_score,
            "skew": skew_score,
            "utilization": util_score,
            "shuffle": shuffle_score,
            "task_failures": task_failures_score,
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
    ) -> Optional[DiagnosticReport]:
        """Run all detectors and produce a complete diagnostic report.

        Args:
            executors: List of executor info, or None if unavailable.
            stages: List of Spark stages, or None.
            jobs: List of Spark jobs, or None.
            sql_queries: List of SQL queries, or None.
            cluster: Cluster info, or None.

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
