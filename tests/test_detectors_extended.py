"""Unit tests for the 14 extended optimization detectors."""

from __future__ import annotations

from dbxtop.analytics.engine import AnalyticsEngine, _parse_byte_string
from dbxtop.analytics.models import InsightCategory, Severity
from dbxtop.api.models import (
    ClusterInfo,
    ClusterState,
    ExecutorInfo,
    RDDInfo,
    SparkJob,
    SparkStage,
    SQLQuery,
    StageStatus,
    JobStatus,
)


# ---------------------------------------------------------------------------
# Test data factories
# ---------------------------------------------------------------------------


def _cluster(spark_conf: dict[str, str] | None = None, **kw: object) -> ClusterInfo:
    defaults: dict[str, object] = {
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
    defaults.update(kw)
    return ClusterInfo(**defaults)  # type: ignore[arg-type]


def _executor(executor_id: str = "1", **kw: object) -> ExecutorInfo:
    defaults: dict[str, object] = {
        "executor_id": executor_id,
        "is_active": True,
        "total_cores": 4,
        "max_memory": 8 * 1024**3,
        "memory_used": 4 * 1024**3,
        "total_duration_ms": 100_000,
        "total_gc_time_ms": 2_000,
        "active_tasks": 2,
        "max_tasks": 4,
        "total_shuffle_read": 0,
        "total_shuffle_write": 0,
    }
    defaults.update(kw)
    return ExecutorInfo(**defaults)  # type: ignore[arg-type]


def _stage(stage_id: int = 0, **kw: object) -> SparkStage:
    defaults: dict[str, object] = {
        "stage_id": stage_id,
        "status": StageStatus.ACTIVE,
        "num_tasks": 100,
        "input_bytes": 1024**3,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "executor_run_time_ms": 60_000,
        "executor_cpu_time_ns": 30_000_000_000,
        "attempt_id": 0,
        "num_complete_tasks": 0,
    }
    defaults.update(kw)
    return SparkStage(**defaults)  # type: ignore[arg-type]


def _sql(execution_id: int = 1, **kw: object) -> SQLQuery:
    defaults: dict[str, object] = {
        "execution_id": execution_id,
        "duration_ms": 30_000,
        "status": "COMPLETED",
        "failed_jobs": 0,
        "success_jobs": 1,
    }
    defaults.update(kw)
    return SQLQuery(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _parse_byte_string
# ---------------------------------------------------------------------------


class TestParseByteString:
    def test_megabytes(self) -> None:
        assert _parse_byte_string("64m") == 64 * 1024**2
        assert _parse_byte_string("64MB") == 64 * 1024**2

    def test_gigabytes(self) -> None:
        assert _parse_byte_string("1g") == 1024**3
        assert _parse_byte_string("2GB") == 2 * 1024**3

    def test_plain_int(self) -> None:
        assert _parse_byte_string("1024") == 1024

    def test_invalid(self) -> None:
        assert _parse_byte_string("abc") is None

    def test_kilobytes(self) -> None:
        assert _parse_byte_string("512k") == 512 * 1024


# ---------------------------------------------------------------------------
# Phase A detectors
# ---------------------------------------------------------------------------


class TestDetectAqeConfig:
    def test_aqe_disabled(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster({"spark.sql.adaptive.enabled": "false"})
        insights = engine.detect_aqe_config(cluster)
        assert len(insights) >= 1
        assert insights[0].severity == Severity.CRITICAL
        assert insights[0].category == InsightCategory.AQE_CONFIG

    def test_coalesce_disabled(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster({"spark.sql.adaptive.coalescePartitions.enabled": "false"})
        insights = engine.detect_aqe_config(cluster)
        assert any(i.severity == Severity.WARNING for i in insights)

    def test_all_enabled(self) -> None:
        engine = AnalyticsEngine()
        insights = engine.detect_aqe_config(_cluster())
        assert len(insights) == 0

    def test_no_cluster(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_aqe_config(None) == []


class TestDetectSerialization:
    def test_java_with_high_gc(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster()
        executors = [_executor("1", total_gc_time_ms=15_000, total_duration_ms=100_000)]
        insights = engine.detect_serialization(cluster, executors)
        assert len(insights) == 1
        assert insights[0].category == InsightCategory.SERIALIZATION

    def test_kryo_no_insights(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster({"spark.serializer": "org.apache.spark.serializer.KryoSerializer"})
        executors = [_executor("1", total_gc_time_ms=15_000, total_duration_ms=100_000)]
        assert engine.detect_serialization(cluster, executors) == []

    def test_java_low_gc(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster()
        executors = [_executor("1", total_gc_time_ms=1_000, total_duration_ms=100_000)]
        assert engine.detect_serialization(cluster, executors) == []


class TestDetectExecutorSizing:
    def test_large_heap(self) -> None:
        engine = AnalyticsEngine()
        executors = [_executor("1", max_memory=70 * 1024**3)]
        insights = engine.detect_executor_sizing(executors)
        assert any(i.category == InsightCategory.EXECUTOR_SIZING for i in insights)

    def test_many_cores(self) -> None:
        engine = AnalyticsEngine()
        executors = [_executor("1", total_cores=12)]
        insights = engine.detect_executor_sizing(executors)
        assert any(i.category == InsightCategory.EXECUTOR_SIZING for i in insights)

    def test_low_mem_per_core(self) -> None:
        engine = AnalyticsEngine()
        executors = [_executor("1", max_memory=3 * 1024**3, total_cores=4)]
        insights = engine.detect_executor_sizing(executors)
        assert any("memory per core" in i.title.lower() or "mem" in i.title.lower() for i in insights)

    def test_normal_sizing(self) -> None:
        engine = AnalyticsEngine()
        executors = [_executor("1", max_memory=16 * 1024**3, total_cores=4)]
        assert engine.detect_executor_sizing(executors) == []


class TestDetectDriverBottleneck:
    def test_driver_high_gc(self) -> None:
        engine = AnalyticsEngine()
        driver = _executor("driver", total_gc_time_ms=12_000, total_duration_ms=100_000)
        workers = [_executor("1", total_gc_time_ms=2_000, total_duration_ms=100_000)]
        insights = engine.detect_driver_bottleneck([driver] + workers)
        assert any(i.category == InsightCategory.DRIVER_BOTTLENECK for i in insights)

    def test_driver_memory_critical(self) -> None:
        engine = AnalyticsEngine()
        driver = _executor("driver", memory_used=int(9.6 * 1024**3), max_memory=10 * 1024**3)
        workers = [_executor("1")]
        insights = engine.detect_driver_bottleneck([driver] + workers)
        assert any(i.severity == Severity.CRITICAL for i in insights)

    def test_no_driver(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_driver_bottleneck([_executor("1")]) == []


class TestDetectConfigAntiPatterns:
    def test_broadcast_disabled(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster({"spark.sql.autoBroadcastJoinThreshold": "-1"})
        insights = engine.detect_config_anti_patterns(cluster, [_executor()])
        assert len(insights) >= 1

    def test_no_cluster(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_config_anti_patterns(None, []) == []


class TestDetectPhotonOpportunity:
    def test_non_photon_with_sql(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster(runtime_engine="STANDARD")
        queries = [_sql(i, duration_ms=30_000) for i in range(10)]
        insights = engine.detect_photon_opportunity(cluster, queries)
        assert len(insights) >= 1
        assert insights[0].category == InsightCategory.PHOTON_OPPORTUNITY

    def test_photon_runtime(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster(runtime_engine="PHOTON")
        queries = [_sql(i) for i in range(10)]
        assert engine.detect_photon_opportunity(cluster, queries) == []

    def test_no_sql(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_photon_opportunity(_cluster(), []) == []


# ---------------------------------------------------------------------------
# Phase B detectors
# ---------------------------------------------------------------------------


class TestDetectIoPatterns:
    def test_small_files(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(input_bytes=50 * 1024**2, num_tasks=200)  # 250KB/task
        insights = engine.detect_io_patterns([stage])
        assert any(i.category == InsightCategory.IO_PATTERN for i in insights)

    def test_io_bound(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(executor_cpu_time_ns=10_000_000_000, executor_run_time_ms=60_000)  # 16.7% CPU
        insights = engine.detect_io_patterns([stage])
        assert any(i.category == InsightCategory.IO_PATTERN for i in insights)

    def test_normal(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(input_bytes=10 * 1024**3, num_tasks=100)  # 100MB/task
        insights = engine.detect_io_patterns([stage])
        # Should not flag small files (100MB/task is fine)
        assert not any("small" in i.title.lower() for i in insights)


class TestDetectCpuIoClassification:
    def test_cpu_bound(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(executor_cpu_time_ns=50_000_000_000, executor_run_time_ms=60_000)  # 83% CPU
        insights = engine.detect_cpu_io_classification([stage])
        assert any(i.category == InsightCategory.CPU_IO_BOUND for i in insights)

    def test_io_bound(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(executor_cpu_time_ns=10_000_000_000, executor_run_time_ms=60_000)  # 16.7% CPU
        insights = engine.detect_cpu_io_classification([stage])
        assert any(i.category == InsightCategory.CPU_IO_BOUND for i in insights)

    def test_no_active_stages(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(status=StageStatus.PENDING)
        assert engine.detect_cpu_io_classification([stage]) == []


class TestDetectStageRetries:
    def test_single_retry(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(attempt_id=1)
        insights = engine.detect_stage_retries([stage])
        assert len(insights) >= 1
        assert insights[0].category == InsightCategory.STAGE_RETRY
        assert insights[0].severity == Severity.WARNING

    def test_multiple_retries(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(attempt_id=3)
        insights = engine.detect_stage_retries([stage])
        assert any(i.severity == Severity.CRITICAL for i in insights)

    def test_no_retries(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_stage_retries([_stage(attempt_id=0)]) == []


class TestDetectSqlAnomalies:
    def test_outlier_query(self) -> None:
        engine = AnalyticsEngine()
        queries = [_sql(i, duration_ms=10_000) for i in range(5)]
        queries.append(_sql(99, duration_ms=300_000, status="COMPLETED"))  # 30x median
        insights = engine.detect_sql_anomalies(queries)
        assert any(i.category == InsightCategory.SQL_ANOMALY for i in insights)

    def test_failed_jobs(self) -> None:
        engine = AnalyticsEngine()
        queries = [_sql(1, failed_jobs=3, success_jobs=1)]
        insights = engine.detect_sql_anomalies(queries)
        assert any(i.category == InsightCategory.SQL_ANOMALY for i in insights)

    def test_long_running(self) -> None:
        engine = AnalyticsEngine()
        queries = [_sql(1, duration_ms=700_000, status="RUNNING")]
        insights = engine.detect_sql_anomalies(queries)
        assert any(i.category == InsightCategory.SQL_ANOMALY for i in insights)

    def test_no_queries(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_sql_anomalies(None) == []
        assert engine.detect_sql_anomalies([]) == []


# ---------------------------------------------------------------------------
# Phase C detectors
# ---------------------------------------------------------------------------


class TestDetectJoinStrategy:
    def test_broadcast_candidate(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(
            input_bytes=50 * 1024**2,  # 50MB
            shuffle_read_bytes=300 * 1024**2,
            shuffle_write_bytes=300 * 1024**2,  # 12x input
        )
        insights = engine.detect_join_strategy([stage], _cluster())
        assert any(i.category == InsightCategory.JOIN_STRATEGY for i in insights)

    def test_normal_join(self) -> None:
        engine = AnalyticsEngine()
        stage = _stage(input_bytes=10 * 1024**3, shuffle_read_bytes=5 * 1024**3)
        assert engine.detect_join_strategy([stage], _cluster()) == []


class TestDetectCachingIssues:
    def test_partial_cache(self) -> None:
        engine = AnalyticsEngine()
        rdd = RDDInfo(rdd_id=1, num_partitions=100, num_cached_partitions=30, memory_used=1024**3)
        executors = [_executor("1", max_memory=10 * 1024**3)]
        insights = engine.detect_caching_issues([rdd], executors, _cluster())
        assert any(i.category == InsightCategory.CACHING for i in insights)

    def test_no_storage(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_caching_issues(None, [_executor()], _cluster()) == []
        assert engine.detect_caching_issues([], [_executor()], _cluster()) == []


class TestDetectAutoTermination:
    def test_disabled_idle(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster(autotermination_minutes=0)
        executors = [_executor("1", active_tasks=0)]
        insights = engine.detect_auto_termination(cluster, [], executors)
        assert any(i.severity == Severity.CRITICAL for i in insights)

    def test_disabled_active(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster(autotermination_minutes=0)
        jobs = [SparkJob(job_id=1, status=JobStatus.RUNNING)]
        insights = engine.detect_auto_termination(cluster, jobs, [_executor()])
        assert any(i.severity == Severity.WARNING for i in insights)

    def test_normal(self) -> None:
        engine = AnalyticsEngine()
        cluster = _cluster(autotermination_minutes=60)
        assert engine.detect_auto_termination(cluster, [], [_executor()]) == []


class TestDetectDynamicAllocation:
    def test_no_cache(self) -> None:
        engine = AnalyticsEngine()
        assert engine.detect_dynamic_allocation([_executor()], None) == []


# ---------------------------------------------------------------------------
# All detectors graceful with None/empty
# ---------------------------------------------------------------------------


class TestAllDetectorsGraceful:
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
