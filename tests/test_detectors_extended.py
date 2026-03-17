"""Unit tests for the 14 extended optimization detectors (SEM2-106).

Covers Phase A (config/model checks), Phase B (metric-derived), and
Phase C (cross-reference) detectors added in SEM2-100.
"""

from __future__ import annotations

from collections import deque
from typing import Dict, Optional
from unittest.mock import MagicMock


from dbxtop.analytics.engine import (
    AnalyticsEngine,
    _parse_byte_string,
)
from dbxtop.analytics.models import InsightCategory, Severity
from dbxtop.api.models import (
    ClusterInfo,
    ClusterState,
    ExecutorInfo,
    JobStatus,
    RDDInfo,
    SparkJob,
    SparkStage,
    SQLQuery,
    StageStatus,
)

# ---------------------------------------------------------------------------
# Shared test data factories
# ---------------------------------------------------------------------------


def _make_cluster(spark_conf: Optional[Dict[str, str]] = None, **kwargs: object) -> ClusterInfo:
    """Create a test ClusterInfo with sensible defaults."""
    defaults: Dict[str, object] = {
        "cluster_id": "test-cluster",
        "cluster_name": "test",
        "state": ClusterState.RUNNING,
        "spark_version": "14.3.x-scala2.12",
        "runtime_engine": "STANDARD",
        "autotermination_minutes": 60,
        "total_cores": 32,
        "total_memory_mb": 65536,
        "spark_conf": spark_conf or {},
    }
    defaults.update(kwargs)
    return ClusterInfo(**defaults)  # type: ignore[arg-type]


def _make_executor(
    executor_id: str = "1",
    is_active: bool = True,
    total_cores: int = 4,
    max_memory: int = 8 * 1024**3,  # 8 GB
    memory_used: int = 4 * 1024**3,
    total_duration_ms: int = 100_000,
    total_gc_time_ms: int = 2_000,
    active_tasks: int = 2,
    total_shuffle_read: int = 0,
    total_shuffle_write: int = 0,
    **kwargs: object,
) -> ExecutorInfo:
    """Create a test ExecutorInfo with sensible defaults."""
    defaults: Dict[str, object] = {
        "executor_id": executor_id,
        "is_active": is_active,
        "total_cores": total_cores,
        "max_memory": max_memory,
        "memory_used": memory_used,
        "total_duration_ms": total_duration_ms,
        "total_gc_time_ms": total_gc_time_ms,
        "active_tasks": active_tasks,
        "total_shuffle_read": total_shuffle_read,
        "total_shuffle_write": total_shuffle_write,
        "max_tasks": total_cores,
        "completed_tasks": 100,
    }
    defaults.update(kwargs)
    return ExecutorInfo(**defaults)  # type: ignore[arg-type]


def _make_stage(
    stage_id: int = 0,
    status: StageStatus = StageStatus.ACTIVE,
    num_tasks: int = 100,
    input_bytes: int = 1024**3,  # 1 GB
    shuffle_read_bytes: int = 0,
    shuffle_write_bytes: int = 0,
    executor_run_time_ms: int = 60_000,
    executor_cpu_time_ns: int = 30_000_000_000,  # 30s in ns
    attempt_id: int = 0,
    **kwargs: object,
) -> SparkStage:
    """Create a test SparkStage with sensible defaults."""
    defaults: Dict[str, object] = {
        "stage_id": stage_id,
        "status": status,
        "num_tasks": num_tasks,
        "input_bytes": input_bytes,
        "shuffle_read_bytes": shuffle_read_bytes,
        "shuffle_write_bytes": shuffle_write_bytes,
        "executor_run_time_ms": executor_run_time_ms,
        "executor_cpu_time_ns": executor_cpu_time_ns,
        "attempt_id": attempt_id,
        "num_complete_tasks": num_tasks if status == StageStatus.COMPLETE else 0,
    }
    defaults.update(kwargs)
    return SparkStage(**defaults)  # type: ignore[arg-type]


def _make_sql(
    execution_id: int = 1,
    duration_ms: int = 30_000,
    status: str = "COMPLETED",
    failed_jobs: int = 0,
    **kwargs: object,
) -> SQLQuery:
    """Create a test SQLQuery with sensible defaults."""
    defaults: Dict[str, object] = {
        "execution_id": execution_id,
        "duration_ms": duration_ms,
        "status": status,
        "failed_jobs": failed_jobs,
        "success_jobs": 1,
    }
    defaults.update(kwargs)
    return SQLQuery(**defaults)  # type: ignore[arg-type]


# ===================================================================
# TestParseByteString
# ===================================================================


class TestParseByteString:
    """Tests for the _parse_byte_string helper."""

    def test_megabyte_short(self) -> None:
        assert _parse_byte_string("64m") == 64 * 1024**2

    def test_gigabyte_short(self) -> None:
        assert _parse_byte_string("1g") == 1 * 1024**3

    def test_megabyte_long(self) -> None:
        assert _parse_byte_string("256MB") == 256 * 1024**2

    def test_gigabyte_long(self) -> None:
        assert _parse_byte_string("4GB") == 4 * 1024**3

    def test_kilobyte_short(self) -> None:
        assert _parse_byte_string("512k") == 512 * 1024

    def test_kilobyte_long(self) -> None:
        assert _parse_byte_string("512KB") == 512 * 1024

    def test_terabyte(self) -> None:
        assert _parse_byte_string("1t") == 1 * 1024**4

    def test_bytes_suffix(self) -> None:
        assert _parse_byte_string("100b") == 100

    def test_plain_int(self) -> None:
        assert _parse_byte_string("1024") == 1024

    def test_plain_int_with_whitespace(self) -> None:
        assert _parse_byte_string("  2048  ") == 2048

    def test_invalid_returns_none(self) -> None:
        assert _parse_byte_string("abc") is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_byte_string("") is None

    def test_case_insensitive(self) -> None:
        assert _parse_byte_string("64M") == _parse_byte_string("64m")
        assert _parse_byte_string("1G") == _parse_byte_string("1g")


# ===================================================================
# TestDetectAqeConfig
# ===================================================================


class TestDetectAqeConfig:
    """Tests for the AQE configuration detector."""

    def test_aqe_disabled(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.sql.adaptive.enabled": "false"})
        insights = engine.detect_aqe_config(cluster)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL
        assert insights[0].category == InsightCategory.AQE_CONFIG
        assert "disabled" in insights[0].title.lower()

    def test_aqe_sub_features_disabled_coalesce(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "false",
            }
        )
        insights = engine.detect_aqe_config(cluster)
        coalesce = [i for i in insights if "coalesce" in i.title.lower() or "coalescing" in i.title.lower()]
        assert len(coalesce) == 1
        assert coalesce[0].severity == Severity.WARNING

    def test_aqe_sub_features_disabled_skew(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "false",
            }
        )
        insights = engine.detect_aqe_config(cluster)
        skew = [i for i in insights if "skew" in i.title.lower()]
        assert len(skew) == 1
        assert skew[0].severity == Severity.WARNING

    def test_aqe_small_advisory_partition(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16m",
            }
        )
        insights = engine.detect_aqe_config(cluster)
        advisory = [i for i in insights if "advisory" in i.title.lower()]
        assert len(advisory) == 1
        assert advisory[0].severity == Severity.INFO

    def test_aqe_all_enabled_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        insights = engine.detect_aqe_config(cluster)
        assert len(insights) == 0

    def test_no_cluster_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_aqe_config(None) == []


# ===================================================================
# TestDetectSerialization
# ===================================================================


class TestDetectSerialization:
    """Tests for the serialization detector."""

    def test_java_serializer_with_gc_critical(self) -> None:
        """Java serializer + avg GC > 10% => CRITICAL."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        executors = [
            _make_executor("1", total_duration_ms=100_000, total_gc_time_ms=12_000),
            _make_executor("2", total_duration_ms=100_000, total_gc_time_ms=11_000),
        ]
        insights = engine.detect_serialization(cluster, executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL
        assert insights[0].category == InsightCategory.SERIALIZATION

    def test_java_serializer_with_gc_warning(self) -> None:
        """Java serializer + avg GC between 5-10% => WARNING."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        executors = [
            _make_executor("1", total_duration_ms=100_000, total_gc_time_ms=6_000),
            _make_executor("2", total_duration_ms=100_000, total_gc_time_ms=7_000),
        ]
        insights = engine.detect_serialization(cluster, executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING

    def test_java_serializer_with_high_shuffle(self) -> None:
        """Java serializer + low GC but shuffle > 10 GB => WARNING."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        executors = [
            _make_executor(
                "1",
                total_duration_ms=100_000,
                total_gc_time_ms=1_000,
                total_shuffle_read=6 * 1024**3,
                total_shuffle_write=6 * 1024**3,
            ),
        ]
        insights = engine.detect_serialization(cluster, executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING

    def test_kryo_serializer_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.serializer": "org.apache.spark.serializer.KryoSerializer"})
        executors = [
            _make_executor("1", total_duration_ms=100_000, total_gc_time_ms=15_000),
        ]
        assert engine.detect_serialization(cluster, executors) == []

    def test_java_low_gc_low_shuffle_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        executors = [
            _make_executor(
                "1",
                total_duration_ms=100_000,
                total_gc_time_ms=1_000,
                total_shuffle_read=100,
                total_shuffle_write=100,
            ),
        ]
        assert engine.detect_serialization(cluster, executors) == []

    def test_no_cluster_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_serialization(None, [_make_executor("1")]) == []


# ===================================================================
# TestDetectExecutorSizing
# ===================================================================


class TestDetectExecutorSizing:
    """Tests for the executor sizing detector."""

    def test_large_heap(self) -> None:
        engine = AnalyticsEngine()
        executors = [_make_executor("1", max_memory=70 * 1024**3)]
        insights = engine.detect_executor_sizing(executors)
        heap = [i for i in insights if "heap" in i.title.lower() or "large" in i.title.lower()]
        assert len(heap) == 1
        assert heap[0].severity == Severity.WARNING
        assert heap[0].category == InsightCategory.EXECUTOR_SIZING

    def test_many_cores(self) -> None:
        engine = AnalyticsEngine()
        executors = [_make_executor("1", total_cores=16, max_memory=64 * 1024**3)]
        insights = engine.detect_executor_sizing(executors)
        core = [i for i in insights if "core" in i.title.lower()]
        assert len(core) == 1
        assert core[0].severity == Severity.WARNING

    def test_low_mem_per_core(self) -> None:
        engine = AnalyticsEngine()
        # 4 GB / 8 cores = 0.5 GB/core < 2 GB threshold
        executors = [_make_executor("1", total_cores=8, max_memory=4 * 1024**3, max_tasks=8)]
        insights = engine.detect_executor_sizing(executors)
        mem = [i for i in insights if "memory per core" in i.title.lower()]
        assert len(mem) == 1
        assert mem[0].severity == Severity.WARNING

    def test_normal_sizing_no_insights(self) -> None:
        engine = AnalyticsEngine()
        # 16 GB / 4 cores = 4 GB/core; 16 GB < 64 GB; 4 <= 8 cores
        executors = [_make_executor("1", total_cores=4, max_memory=16 * 1024**3)]
        assert engine.detect_executor_sizing(executors) == []

    def test_driver_only_no_insights(self) -> None:
        engine = AnalyticsEngine()
        executors = [_make_executor("driver", max_memory=100 * 1024**3, total_cores=16)]
        assert engine.detect_executor_sizing(executors) == []

    def test_all_three_issues(self) -> None:
        """Executor with large heap, many cores, AND low mem/core triggers all."""
        engine = AnalyticsEngine()
        # 70 GB / 16 cores = ~4.4 GB/core (above 2 GB threshold)
        executors = [_make_executor("1", total_cores=16, max_memory=70 * 1024**3)]
        insights = engine.detect_executor_sizing(executors)
        assert len(insights) >= 2  # at least large heap + many cores


# ===================================================================
# TestDetectDriverBottleneck
# ===================================================================


class TestDetectDriverBottleneck:
    """Tests for the driver bottleneck detector."""

    def test_driver_high_gc(self) -> None:
        """Driver GC 2x+ workers => WARNING."""
        engine = AnalyticsEngine()
        executors = [
            _make_executor("driver", total_duration_ms=100_000, total_gc_time_ms=20_000),
            _make_executor("1", total_duration_ms=100_000, total_gc_time_ms=3_000),
            _make_executor("2", total_duration_ms=100_000, total_gc_time_ms=3_000),
        ]
        insights = engine.detect_driver_bottleneck(executors)
        gc = [i for i in insights if "gc" in i.title.lower()]
        assert len(gc) == 1
        assert gc[0].severity == Severity.WARNING
        assert gc[0].category == InsightCategory.DRIVER_BOTTLENECK
        assert gc[0].affected_entity == "driver"

    def test_driver_memory_critical(self) -> None:
        """Driver >95% memory => CRITICAL."""
        engine = AnalyticsEngine()
        max_mem = 10 * 1024**3
        executors = [
            _make_executor("driver", max_memory=max_mem, memory_used=int(max_mem * 0.96)),
            _make_executor("1"),
        ]
        insights = engine.detect_driver_bottleneck(executors)
        mem = [i for i in insights if "memory" in i.title.lower()]
        assert len(mem) == 1
        assert mem[0].severity == Severity.CRITICAL

    def test_no_driver_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        executors = [_make_executor("1"), _make_executor("2")]
        assert engine.detect_driver_bottleneck(executors) == []

    def test_no_workers_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        executors = [_make_executor("driver", total_gc_time_ms=20_000)]
        assert engine.detect_driver_bottleneck(executors) == []

    def test_driver_gc_below_threshold_no_insight(self) -> None:
        engine = AnalyticsEngine()
        executors = [
            _make_executor("driver", total_duration_ms=100_000, total_gc_time_ms=4_000),
            _make_executor("1", total_duration_ms=100_000, total_gc_time_ms=3_000),
            _make_executor("2", total_duration_ms=100_000, total_gc_time_ms=3_000),
        ]
        insights = engine.detect_driver_bottleneck(executors)
        gc = [i for i in insights if "gc" in i.title.lower()]
        assert gc == []


# ===================================================================
# TestDetectConfigAntiPatterns
# ===================================================================


class TestDetectConfigAntiPatterns:
    """Tests for the configuration anti-patterns detector."""

    def test_shuffle_partitions_mismatch(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(
            spark_conf={
                "spark.sql.shuffle.partitions": "10000",
                "spark.sql.adaptive.coalescePartitions.enabled": "false",
            }
        )
        # 4 executors * 4 cores = 16 total cores; 10000 > 10*16
        executors = [_make_executor(str(i)) for i in range(4)]
        insights = engine.detect_config_anti_patterns(cluster, executors)
        shuffle = [i for i in insights if "shuffle" in i.title.lower() and "partition" in i.title.lower()]
        assert len(shuffle) == 1
        assert shuffle[0].severity == Severity.WARNING

    def test_shuffle_partitions_with_coalesce_ok(self) -> None:
        """High shuffle.partitions is fine when coalescing is enabled (default)."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.sql.shuffle.partitions": "10000"})
        executors = [_make_executor(str(i)) for i in range(4)]
        insights = engine.detect_config_anti_patterns(cluster, executors)
        shuffle = [i for i in insights if "shuffle" in i.title.lower() and "partition" in i.title.lower()]
        assert shuffle == []

    def test_memory_fraction_below_range(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.memory.fraction": "0.3"})
        insights = engine.detect_config_anti_patterns(cluster, [_make_executor("1")])
        frac = [i for i in insights if "memory fraction" in i.title.lower()]
        assert len(frac) == 1
        assert frac[0].severity == Severity.INFO

    def test_memory_fraction_above_range(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.memory.fraction": "0.9"})
        insights = engine.detect_config_anti_patterns(cluster, [_make_executor("1")])
        frac = [i for i in insights if "memory fraction" in i.title.lower()]
        assert len(frac) == 1

    def test_high_storage_fraction(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.memory.storageFraction": "0.8"})
        insights = engine.detect_config_anti_patterns(cluster, [_make_executor("1")])
        sf = [i for i in insights if "storage fraction" in i.title.lower()]
        assert len(sf) == 1
        assert sf[0].severity == Severity.WARNING

    def test_broadcast_disabled(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={"spark.sql.autoBroadcastJoinThreshold": "-1"})
        insights = engine.detect_config_anti_patterns(cluster, [_make_executor("1")])
        bc = [i for i in insights if "broadcast" in i.title.lower()]
        assert len(bc) == 1
        assert bc[0].severity == Severity.INFO

    def test_default_config_no_issues(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(spark_conf={})
        assert engine.detect_config_anti_patterns(cluster, [_make_executor("1")]) == []

    def test_no_cluster_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_config_anti_patterns(None, [_make_executor("1")]) == []


# ===================================================================
# TestDetectPhotonOpportunity
# ===================================================================


class TestDetectPhotonOpportunity:
    """Tests for the Photon runtime opportunity detector."""

    def test_non_photon_with_sql_info(self) -> None:
        """Non-Photon + >5 SQL queries => INFO."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(runtime_engine="STANDARD")
        queries = [_make_sql(execution_id=i, duration_ms=30_000) for i in range(10)]
        insights = engine.detect_photon_opportunity(cluster, queries)
        assert len(insights) == 1
        assert insights[0].severity == Severity.INFO
        assert insights[0].category == InsightCategory.PHOTON_OPPORTUNITY

    def test_non_photon_heavy_sql_warning(self) -> None:
        """>20 queries with avg >60s => WARNING."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(runtime_engine="STANDARD")
        queries = [_make_sql(execution_id=i, duration_ms=120_000) for i in range(25)]
        insights = engine.detect_photon_opportunity(cluster, queries)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING

    def test_photon_runtime_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(runtime_engine="PHOTON")
        queries = [_make_sql(execution_id=i, duration_ms=30_000) for i in range(10)]
        assert engine.detect_photon_opportunity(cluster, queries) == []

    def test_no_sql_queries_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(runtime_engine="STANDARD")
        assert engine.detect_photon_opportunity(cluster, []) == []

    def test_few_queries_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _make_cluster(runtime_engine="STANDARD")
        queries = [_make_sql(execution_id=i, duration_ms=30_000) for i in range(3)]
        assert engine.detect_photon_opportunity(cluster, queries) == []

    def test_no_cluster_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_photon_opportunity(None, [_make_sql()]) == []


# ===================================================================
# TestDetectIoPatterns
# ===================================================================


class TestDetectIoPatterns:
    """Tests for the I/O patterns detector."""

    def test_small_files(self) -> None:
        """input_bytes/num_tasks < 1MB with >100 tasks => WARNING."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            num_tasks=200,
            input_bytes=100 * 1024**2,  # 512 KB/task
        )
        insights = engine.detect_io_patterns([stage])
        small = [i for i in insights if "small file" in i.title.lower()]
        assert len(small) == 1
        assert small[0].severity == Severity.WARNING
        assert small[0].category == InsightCategory.IO_PATTERN

    def test_small_files_below_task_threshold(self) -> None:
        """Small bytes/task but <100 tasks => no small-file insight."""
        engine = AnalyticsEngine()
        stage = _make_stage(stage_id=0, num_tasks=50, input_bytes=10 * 1024**2)
        insights = engine.detect_io_patterns([stage])
        small = [i for i in insights if "small file" in i.title.lower()]
        assert small == []

    def test_io_bound_stage(self) -> None:
        """CPU efficiency < 0.3 => WARNING."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            executor_run_time_ms=100_000,
            executor_cpu_time_ns=10_000_000_000,  # 10%
        )
        insights = engine.detect_io_patterns([stage])
        io = [i for i in insights if "i/o bound" in i.title.lower()]
        assert len(io) == 1
        assert io[0].severity == Severity.WARNING

    def test_normal_io_no_insights(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            num_tasks=10,
            input_bytes=1024**3,  # 100 MB/task
            executor_run_time_ms=60_000,
            executor_cpu_time_ns=30_000_000_000,  # 50%
        )
        assert engine.detect_io_patterns([stage]) == []

    def test_skipped_stages_ignored(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(status=StageStatus.SKIPPED, num_tasks=200, input_bytes=100 * 1024**2)
        assert engine.detect_io_patterns([stage]) == []


# ===================================================================
# TestDetectCpuIoClassification
# ===================================================================


class TestDetectCpuIoClassification:
    """Tests for the CPU/IO classification detector."""

    def test_cpu_bound(self) -> None:
        """avg CPU efficiency > 0.7 => INFO cpu-bound."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            status=StageStatus.ACTIVE,
            executor_run_time_ms=100_000,
            executor_cpu_time_ns=80_000_000_000,  # 80%
        )
        insights = engine.detect_cpu_io_classification([stage])
        assert len(insights) == 1
        assert insights[0].severity == Severity.INFO
        assert "cpu-bound" in insights[0].title.lower()

    def test_io_bound(self) -> None:
        """avg CPU efficiency < 0.3 => INFO io-bound."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            status=StageStatus.ACTIVE,
            executor_run_time_ms=100_000,
            executor_cpu_time_ns=20_000_000_000,  # 20%
        )
        insights = engine.detect_cpu_io_classification([stage])
        assert len(insights) == 1
        assert insights[0].severity == Severity.INFO
        assert "i/o-bound" in insights[0].title.lower()

    def test_mixed_workload_no_insight(self) -> None:
        """CPU efficiency ~0.5 => no insight."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            status=StageStatus.ACTIVE,
            executor_run_time_ms=100_000,
            executor_cpu_time_ns=50_000_000_000,
        )
        assert engine.detect_cpu_io_classification([stage]) == []

    def test_no_active_stages(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(status=StageStatus.COMPLETE)
        assert engine.detect_cpu_io_classification([stage]) == []

    def test_empty_stages(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_cpu_io_classification([]) == []


# ===================================================================
# TestDetectStageRetries
# ===================================================================


class TestDetectStageRetries:
    """Tests for the stage retries detector."""

    def test_single_retry(self) -> None:
        """attempt_id=1 => WARNING."""
        engine = AnalyticsEngine()
        stage = _make_stage(stage_id=5, attempt_id=1)
        insights = engine.detect_stage_retries([stage])
        assert len(insights) >= 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.STAGE_RETRY
        assert "stage 5" in insights[0].title.lower()

    def test_multiple_retries_critical(self) -> None:
        """attempt_id >= 3 => CRITICAL."""
        engine = AnalyticsEngine()
        stage = _make_stage(stage_id=3, attempt_id=3)
        insights = engine.detect_stage_retries([stage])
        assert len(insights) >= 1
        assert insights[0].severity == Severity.CRITICAL
        assert insights[0].metric_value == 3.0

    def test_no_retries_empty(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(stage_id=0, attempt_id=0)
        assert engine.detect_stage_retries([stage]) == []

    def test_multiple_stages_retried_escalation(self) -> None:
        engine = AnalyticsEngine()
        stages = [
            _make_stage(stage_id=1, attempt_id=1),
            _make_stage(stage_id=2, attempt_id=1),
        ]
        insights = engine.detect_stage_retries(stages)
        # Per-stage WARNINGs + escalation CRITICAL
        assert len(insights) >= 3
        critical = [i for i in insights if i.severity == Severity.CRITICAL]
        assert len(critical) >= 1


# ===================================================================
# TestDetectSqlAnomalies
# ===================================================================


class TestDetectSqlAnomalies:
    """Tests for the SQL anomalies detector."""

    def test_outlier_query(self) -> None:
        """Query >= 5x median duration => WARNING."""
        engine = AnalyticsEngine()
        queries = [
            _make_sql(execution_id=1, duration_ms=10_000),
            _make_sql(execution_id=2, duration_ms=10_000),
            _make_sql(execution_id=3, duration_ms=10_000),
            _make_sql(execution_id=4, duration_ms=300_000),  # ~30x median
        ]
        insights = engine.detect_sql_anomalies(queries)
        outlier = [i for i in insights if "slower than median" in i.title.lower()]
        assert len(outlier) >= 1

    def test_failed_jobs(self) -> None:
        """failed_jobs > 0 => WARNING."""
        engine = AnalyticsEngine()
        queries = [_make_sql(execution_id=1, duration_ms=30_000, failed_jobs=2)]
        insights = engine.detect_sql_anomalies(queries)
        failures = [i for i in insights if "failed" in i.title.lower()]
        assert len(failures) == 1
        assert failures[0].severity == Severity.WARNING

    def test_long_running(self) -> None:
        """>10 min running query => INFO."""
        engine = AnalyticsEngine()
        queries = [_make_sql(execution_id=1, duration_ms=700_000, status="RUNNING")]
        insights = engine.detect_sql_anomalies(queries)
        long = [i for i in insights if "long-running" in i.title.lower()]
        assert len(long) == 1
        assert long[0].severity == Severity.INFO

    def test_no_queries_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_sql_anomalies([]) == []

    def test_none_queries_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_sql_anomalies(None) == []

    def test_normal_queries_no_anomalies(self) -> None:
        engine = AnalyticsEngine()
        queries = [_make_sql(execution_id=i, duration_ms=10_000) for i in range(5)]
        insights = engine.detect_sql_anomalies(queries)
        assert not any("slower" in i.title.lower() for i in insights)
        assert not any("failed" in i.title.lower() for i in insights)
        assert not any("long-running" in i.title.lower() for i in insights)


# ===================================================================
# TestDetectJoinStrategy
# ===================================================================


class TestDetectJoinStrategy:
    """Tests for the join strategy detector."""

    def test_broadcast_candidate(self) -> None:
        """shuffle > 5x input, input < 100 MB => WARNING."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            input_bytes=50 * 1024**2,
            shuffle_read_bytes=300 * 1024**2,
            shuffle_write_bytes=0,
        )
        insights = engine.detect_join_strategy([stage], _make_cluster())
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.JOIN_STRATEGY

    def test_normal_join_no_insight(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            input_bytes=200 * 1024**2,
            shuffle_read_bytes=100 * 1024**2,
        )
        assert engine.detect_join_strategy([stage], None) == []

    def test_large_input_not_broadcast_candidate(self) -> None:
        """Input >= 100 MB disqualifies broadcast even with high shuffle ratio."""
        engine = AnalyticsEngine()
        stage = _make_stage(
            stage_id=0,
            input_bytes=200 * 1024**2,
            shuffle_read_bytes=2000 * 1024**2,
        )
        assert engine.detect_join_strategy([stage], None) == []

    def test_pending_stages_ignored(self) -> None:
        engine = AnalyticsEngine()
        stage = _make_stage(status=StageStatus.PENDING, input_bytes=10 * 1024**2, shuffle_read_bytes=200 * 1024**2)
        assert engine.detect_join_strategy([stage], None) == []


# ===================================================================
# TestDetectCachingIssues
# ===================================================================


class TestDetectCachingIssues:
    """Tests for the caching issues detector."""

    def test_partial_cache(self) -> None:
        """fraction_cached < 0.5 => WARNING."""
        engine = AnalyticsEngine()
        storage = [
            RDDInfo(
                rdd_id=1,
                name="my_df",
                num_partitions=100,
                num_cached_partitions=30,
                memory_used=1 * 1024**3,
            ),
        ]
        executors = [_make_executor("1")]
        insights = engine.detect_caching_issues(storage, executors, _make_cluster())
        partial = [i for i in insights if "partially" in i.title.lower()]
        assert len(partial) == 1
        assert partial[0].severity == Severity.WARNING
        assert partial[0].category == InsightCategory.CACHING

    def test_excessive_cache(self) -> None:
        """Cache > 60% executor memory => WARNING."""
        engine = AnalyticsEngine()
        max_mem = 10 * 1024**3
        storage = [
            RDDInfo(
                rdd_id=1,
                name="big_table",
                num_partitions=100,
                num_cached_partitions=100,
                memory_used=7 * 1024**3,
            ),
        ]
        executors = [_make_executor("1", max_memory=max_mem)]
        insights = engine.detect_caching_issues(storage, executors, _make_cluster())
        cache_mem = [i for i in insights if "cache memory" in i.title.lower() or "high cache" in i.title.lower()]
        assert len(cache_mem) == 1
        assert cache_mem[0].severity == Severity.WARNING

    def test_no_storage_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_caching_issues(None, [_make_executor("1")], _make_cluster()) == []

    def test_empty_storage_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_caching_issues([], [_make_executor("1")], _make_cluster()) == []

    def test_well_cached_no_issue(self) -> None:
        engine = AnalyticsEngine()
        storage = [
            RDDInfo(
                rdd_id=1,
                name="well_cached",
                num_partitions=100,
                num_cached_partitions=100,
                memory_used=1 * 1024**3,
            ),
        ]
        executors = [_make_executor("1", max_memory=10 * 1024**3)]
        assert engine.detect_caching_issues(storage, executors, _make_cluster()) == []


# ===================================================================
# TestDetectAutoTermination
# ===================================================================


class TestDetectAutoTermination:
    """Tests for the auto-termination detector."""

    def test_disabled_idle_critical(self) -> None:
        """autotermination=0, no jobs/tasks => CRITICAL."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(autotermination_minutes=0)
        executors = [_make_executor("1", active_tasks=0)]
        insights = engine.detect_auto_termination(cluster, [], executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.CRITICAL
        assert "idle" in insights[0].title.lower()

    def test_disabled_active_warning(self) -> None:
        """autotermination=0, has running jobs => WARNING."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(autotermination_minutes=0)
        jobs = [SparkJob(job_id=1, status=JobStatus.RUNNING)]
        executors = [_make_executor("1", active_tasks=2)]
        insights = engine.detect_auto_termination(cluster, jobs, executors)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert "disabled" in insights[0].title.lower()

    def test_normal_termination_empty(self) -> None:
        """autotermination=60 => no insights."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(autotermination_minutes=60)
        assert engine.detect_auto_termination(cluster, [], [_make_executor("1")]) == []

    def test_long_timeout_info(self) -> None:
        """autotermination > 120 min => INFO."""
        engine = AnalyticsEngine()
        cluster = _make_cluster(autotermination_minutes=180)
        insights = engine.detect_auto_termination(cluster, [], [_make_executor("1")])
        assert len(insights) == 1
        assert insights[0].severity == Severity.INFO
        assert "180" in insights[0].title

    def test_no_cluster_returns_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_auto_termination(None, [], []) == []


# ===================================================================
# TestDetectDynamicAllocation
# ===================================================================


class TestDetectDynamicAllocation:
    """Tests for the dynamic allocation detector."""

    def test_no_cache_empty(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_dynamic_allocation([_make_executor("1")], None) == []

    def test_insufficient_history_empty(self) -> None:
        engine = AnalyticsEngine()
        cache = MagicMock()
        cache.get_history.return_value = deque(
            [
                [_make_executor("1")],
                [_make_executor("1")],
            ]
        )  # Only 2 snapshots, need >= 3
        assert engine.detect_dynamic_allocation([_make_executor("1")], cache) == []

    def test_stable_executor_count_no_insight(self) -> None:
        engine = AnalyticsEngine()
        cache = MagicMock()
        snapshot = [_make_executor(str(i)) for i in range(4)]
        cache.get_history.return_value = deque([snapshot] * 5)
        assert engine.detect_dynamic_allocation([_make_executor("1")], cache) == []

    def test_unstable_executor_count_warning(self) -> None:
        engine = AnalyticsEngine()
        cache = MagicMock()
        snapshots = []
        for count in [2, 8, 2, 8, 2]:
            snapshots.append([_make_executor(str(i)) for i in range(count)])
        cache.get_history.return_value = deque(snapshots)
        insights = engine.detect_dynamic_allocation([_make_executor("1")], cache)
        assert len(insights) == 1
        assert insights[0].severity == Severity.WARNING
        assert insights[0].category == InsightCategory.DYNAMIC_ALLOCATION


# ===================================================================
# All detectors return list with None/empty inputs
# ===================================================================


class TestAllDetectorsGraceful:
    """Verify every detector returns a list (never raises) with None/empty inputs."""

    def test_all_return_list_with_none_inputs(self) -> None:
        engine = AnalyticsEngine()
        assert isinstance(engine.detect_aqe_config(None), list)
        assert isinstance(engine.detect_serialization(None, []), list)
        assert isinstance(engine.detect_executor_sizing([]), list)
        assert isinstance(engine.detect_driver_bottleneck([]), list)
        assert isinstance(engine.detect_config_anti_patterns(None, []), list)
        assert isinstance(engine.detect_photon_opportunity(None, None), list)
        assert isinstance(engine.detect_io_patterns([]), list)
        assert isinstance(engine.detect_cpu_io_classification([]), list)
        assert isinstance(engine.detect_stage_retries([]), list)
        assert isinstance(engine.detect_sql_anomalies(None), list)
        assert isinstance(engine.detect_join_strategy([], None), list)
        assert isinstance(engine.detect_caching_issues(None, [], None), list)
        assert isinstance(engine.detect_auto_termination(None, [], []), list)
        assert isinstance(engine.detect_dynamic_allocation([], None), list)
