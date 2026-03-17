"""Unit tests for SparkRESTClient URL construction and mapping."""

from __future__ import annotations

import time
from typing import Any, Dict, List

import httpx
import pytest

from dbxtop.api.models import JobStatus, StageStatus
from dbxtop.api.spark_api import SparkRESTClient, _map_executor, _map_spark_job, _map_spark_stage


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


class TestURLConstruction:
    def _make_client(self, workspace_url: str = "https://adb-123.1.azuredatabricks.net") -> SparkRESTClient:
        return SparkRESTClient(
            workspace_url=workspace_url,
            cluster_id="0123-456789-abcdefgh",
            org_id="123",
            port=40001,
            token="dapi-test",
        )

    def test_base_url(self) -> None:
        client = self._make_client()
        assert client._base_url == (
            "https://adb-123.1.azuredatabricks.net/driver-proxy-api/o/123/0123-456789-abcdefgh/40001"
        )

    def test_base_url_strips_trailing_slash(self) -> None:
        client = self._make_client("https://adb-123.1.azuredatabricks.net/")
        assert not client._base_url.endswith("//")
        assert "/driver-proxy-api/" in client._base_url

    def test_app_url(self) -> None:
        client = self._make_client()
        client._app_id = "app-20260316-0000"
        url = client._app_url("jobs")
        assert url == (
            "https://adb-123.1.azuredatabricks.net"
            "/driver-proxy-api/o/123/0123-456789-abcdefgh/40001"
            "/api/v1/applications/app-20260316-0000/jobs"
        )


# ---------------------------------------------------------------------------
# JSON → model mapping
# ---------------------------------------------------------------------------


class TestMapSparkJob:
    def test_basic_mapping(self) -> None:
        raw: Dict[str, Any] = {
            "jobId": 42,
            "name": "count at MyJob.scala:10",
            "status": "RUNNING",
            "numTasks": 100,
            "numActiveTasks": 30,
            "numCompletedTasks": 60,
            "numFailedTasks": 0,
            "stageIds": [1, 2, 3],
        }
        job = _map_spark_job(raw)
        assert job.job_id == 42
        assert job.name == "count at MyJob.scala:10"
        assert job.status == JobStatus.RUNNING
        assert job.num_tasks == 100
        assert job.stage_ids == [1, 2, 3]

    def test_unknown_status_defaults(self) -> None:
        raw: Dict[str, Any] = {"jobId": 1, "status": "WEIRD_STATE"}
        job = _map_spark_job(raw)
        assert job.status == JobStatus.UNKNOWN

    def test_missing_fields_default(self) -> None:
        raw: Dict[str, Any] = {"jobId": 1}
        job = _map_spark_job(raw)
        assert job.num_tasks == 0
        assert job.name == ""


class TestMapSparkStage:
    def test_basic_mapping(self) -> None:
        raw: Dict[str, Any] = {
            "stageId": 5,
            "attemptId": 0,
            "name": "map at Transform.scala:20",
            "status": "ACTIVE",
            "numTasks": 200,
            "numActiveTasks": 50,
            "numCompleteTasks": 140,
            "numFailedTasks": 0,
            "inputBytes": 1024,
            "shuffleReadBytes": 2048,
            "shuffleWriteBytes": 512,
            "memoryBytesSpilled": 100,
            "diskBytesSpilled": 50,
        }
        stage = _map_spark_stage(raw)
        assert stage.stage_id == 5
        assert stage.status == StageStatus.ACTIVE
        assert stage.input_bytes == 1024
        assert stage.spill_bytes == 150

    def test_unknown_status(self) -> None:
        raw: Dict[str, Any] = {"stageId": 1, "status": "BOGUS"}
        stage = _map_spark_stage(raw)
        assert stage.status == StageStatus.PENDING


class TestMapExecutor:
    def test_driver_executor(self) -> None:
        raw: Dict[str, Any] = {
            "id": "driver",
            "isActive": True,
            "hostPort": "10.0.0.1:40001",
            "totalCores": 4,
            "maxMemory": 4294967296,
            "memoryUsed": 1073741824,
            "totalDuration": 10000,
            "totalGCTime": 500,
        }
        exe = _map_executor(raw)
        assert exe.is_driver is True
        assert exe.total_cores == 4
        assert exe.gc_ratio == pytest.approx(0.05)
        assert exe.memory_used_pct == pytest.approx(25.0)

    def test_peak_memory_metrics(self) -> None:
        raw: Dict[str, Any] = {
            "id": "1",
            "peakMemoryMetrics": {
                "JVMHeapMemory": 2048000000,
                "JVMOffHeapMemory": 512000000,
            },
        }
        exe = _map_executor(raw)
        assert exe.peak_jvm_heap == 2048000000
        assert exe.peak_jvm_off_heap == 512000000

    def test_missing_peak_metrics(self) -> None:
        raw: Dict[str, Any] = {"id": "2"}
        exe = _map_executor(raw)
        assert exe.peak_jvm_heap == 0
        assert exe.peak_jvm_off_heap == 0


# ---------------------------------------------------------------------------
# SparkRESTClient with mock transport
# ---------------------------------------------------------------------------


class TestSparkRESTClientWithMock:
    @pytest.mark.asyncio
    async def test_discover_app_id(self) -> None:
        """Verify app_id is cached from /applications response."""

        def handler(request: httpx.Request) -> httpx.Response:
            if "/api/v1/applications" in str(request.url) and "app-" not in str(request.url):
                return httpx.Response(200, json=[{"id": "app-test-001", "name": "Spark"}])
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        client = SparkRESTClient(
            workspace_url="https://adb-123.1.azuredatabricks.net",
            cluster_id="cluster-1",
            org_id="123",
        )
        client._client = httpx.AsyncClient(transport=transport)

        app_id = await client.discover_app_id()
        assert app_id == "app-test-001"
        assert client._available is True

        await client.close()

    @pytest.mark.asyncio
    async def test_discover_app_id_no_apps(self) -> None:
        """RuntimeError when no Spark apps found."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        transport = httpx.MockTransport(handler)
        client = SparkRESTClient(
            workspace_url="https://adb-123.1.azuredatabricks.net",
            cluster_id="cluster-1",
            org_id="123",
        )
        client._client = httpx.AsyncClient(transport=transport)

        with pytest.raises(RuntimeError, match="No Spark applications"):
            await client.discover_app_id()

        await client.close()

    @pytest.mark.asyncio
    async def test_get_jobs_with_mock(self) -> None:
        """Verify get_jobs parses a mock response."""
        jobs_response: List[Dict[str, Any]] = [
            {"jobId": 1, "status": "SUCCEEDED", "numTasks": 10},
            {"jobId": 2, "status": "RUNNING", "numTasks": 50},
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.endswith("/applications"):
                return httpx.Response(200, json=[{"id": "app-1"}])
            if "/jobs" in url:
                return httpx.Response(200, json=jobs_response)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        client = SparkRESTClient(
            workspace_url="https://adb-123.1.azuredatabricks.net",
            cluster_id="cluster-1",
            org_id="123",
        )
        client._client = httpx.AsyncClient(transport=transport)
        await client.discover_app_id()

        jobs = await client.get_jobs()
        assert len(jobs) == 2
        assert jobs[0].job_id == 1
        assert jobs[1].status == JobStatus.RUNNING

        await client.close()


# ---------------------------------------------------------------------------
# Data fetcher tests with mock transport
# ---------------------------------------------------------------------------


def _make_mock_client(handler: Any) -> SparkRESTClient:
    """Helper: create a SparkRESTClient with a mock transport and pre-set app_id."""
    transport = httpx.MockTransport(handler)
    client = SparkRESTClient(
        workspace_url="https://adb-123.1.azuredatabricks.net",
        cluster_id="cluster-1",
        org_id="123",
    )
    client._client = httpx.AsyncClient(transport=transport)
    client._app_id = "app-1"
    client._available = True
    return client


class TestGetStages:
    """Tests for SparkRESTClient.get_stages()."""

    @pytest.mark.asyncio
    async def test_get_stages_returns_models(self) -> None:
        """Verify get_stages parses mock stage data into SparkStage models."""
        stages_response: List[Dict[str, Any]] = [
            {
                "stageId": 10,
                "attemptId": 0,
                "name": "count at Script.py:42",
                "status": "COMPLETE",
                "numTasks": 200,
                "numActiveTasks": 0,
                "numCompleteTasks": 200,
                "numFailedTasks": 0,
                "inputBytes": 5000,
                "shuffleReadBytes": 1024,
                "shuffleWriteBytes": 2048,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
            },
            {
                "stageId": 11,
                "attemptId": 0,
                "name": "save at Script.py:50",
                "status": "ACTIVE",
                "numTasks": 100,
                "numActiveTasks": 40,
                "numCompleteTasks": 55,
                "numFailedTasks": 5,
                "inputBytes": 0,
                "shuffleReadBytes": 0,
                "shuffleWriteBytes": 0,
                "memoryBytesSpilled": 512,
                "diskBytesSpilled": 256,
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/stages" in url:
                return httpx.Response(200, json=stages_response)
            return httpx.Response(404)

        client = _make_mock_client(handler)

        stages = await client.get_stages()
        assert len(stages) == 2

        assert stages[0].stage_id == 10
        assert stages[0].status == StageStatus.COMPLETE
        assert stages[0].name == "count at Script.py:42"
        assert stages[0].num_tasks == 200
        assert stages[0].num_complete_tasks == 200
        assert stages[0].input_bytes == 5000
        assert stages[0].shuffle_read_bytes == 1024
        assert stages[0].shuffle_write_bytes == 2048
        assert stages[0].spill_bytes == 0

        assert stages[1].stage_id == 11
        assert stages[1].status == StageStatus.ACTIVE
        assert stages[1].num_active_tasks == 40
        assert stages[1].num_failed_tasks == 5
        assert stages[1].spill_bytes == 768  # 512 + 256

        await client.close()

    @pytest.mark.asyncio
    async def test_get_stages_with_status_filter(self) -> None:
        """Verify status query param is forwarded."""
        captured_params: Dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/stages" in url:
                for key, val in request.url.params.items():
                    captured_params[key] = val
                return httpx.Response(200, json=[{"stageId": 1, "status": "ACTIVE"}])
            return httpx.Response(404)

        client = _make_mock_client(handler)
        stages = await client.get_stages(status="active")
        assert len(stages) == 1
        assert captured_params.get("status") == "active"

        await client.close()

    @pytest.mark.asyncio
    async def test_get_stages_empty_response(self) -> None:
        """Verify empty list returned when API returns []."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        stages = await client.get_stages()
        assert stages == []

        await client.close()


class TestGetExecutors:
    """Tests for SparkRESTClient.get_executors()."""

    @pytest.mark.asyncio
    async def test_get_executors_returns_models(self) -> None:
        """Verify get_executors parses mock data into ExecutorInfo models."""
        executors_response: List[Dict[str, Any]] = [
            {
                "id": "driver",
                "isActive": True,
                "hostPort": "10.0.0.1:40001",
                "totalCores": 4,
                "maxMemory": 4294967296,
                "memoryUsed": 1073741824,
                "totalDuration": 20000,
                "totalGCTime": 1000,
                "activeTasks": 0,
                "completedTasks": 50,
                "failedTasks": 0,
                "totalInputBytes": 500000,
                "totalShuffleRead": 100000,
                "totalShuffleWrite": 200000,
            },
            {
                "id": "1",
                "isActive": True,
                "hostPort": "10.0.0.2:40001",
                "totalCores": 8,
                "maxMemory": 8589934592,
                "memoryUsed": 4294967296,
                "totalDuration": 50000,
                "totalGCTime": 5000,
                "activeTasks": 3,
                "completedTasks": 200,
                "failedTasks": 2,
                "peakMemoryMetrics": {
                    "JVMHeapMemory": 6000000000,
                    "JVMOffHeapMemory": 1000000000,
                },
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/allexecutors" in url:
                return httpx.Response(200, json=executors_response)
            return httpx.Response(404)

        client = _make_mock_client(handler)

        executors = await client.get_executors()
        assert len(executors) == 2

        driver = executors[0]
        assert driver.is_driver is True
        assert driver.executor_id == "driver"
        assert driver.total_cores == 4
        assert driver.max_memory == 4294967296
        assert driver.memory_used == 1073741824
        assert driver.memory_used_pct == pytest.approx(25.0)
        assert driver.gc_ratio == pytest.approx(0.05)
        assert driver.total_input_bytes == 500000
        assert driver.completed_tasks == 50

        worker = executors[1]
        assert worker.is_driver is False
        assert worker.executor_id == "1"
        assert worker.total_cores == 8
        assert worker.active_tasks == 3
        assert worker.failed_tasks == 2
        assert worker.peak_jvm_heap == 6000000000
        assert worker.peak_jvm_off_heap == 1000000000

        await client.close()

    @pytest.mark.asyncio
    async def test_get_executors_empty(self) -> None:
        """Verify empty list when no executors returned."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        executors = await client.get_executors()
        assert executors == []

        await client.close()


class TestGetSQL:
    """Tests for SparkRESTClient.get_sql()."""

    @pytest.mark.asyncio
    async def test_get_sql_returns_models(self) -> None:
        """Verify get_sql parses mock data into SQLQuery models."""
        sql_response: List[Dict[str, Any]] = [
            {
                "id": 100,
                "status": "COMPLETED",
                "description": "SELECT * FROM users",
                "submissionTime": "2026-03-16T12:00:00.000+0000",
                "duration": 5432,
                "runningJobIds": [],
                "successJobIds": [1, 2, 3],
                "failedJobIds": [],
            },
            {
                "id": 101,
                "status": "RUNNING",
                "description": "INSERT INTO results SELECT ...",
                "duration": 12000,
                "runningJobIds": [4, 5],
                "successJobIds": [3],
                "failedJobIds": [6],
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/sql" in url:
                return httpx.Response(200, json=sql_response)
            return httpx.Response(404)

        client = _make_mock_client(handler)

        sqls = await client.get_sql()
        assert len(sqls) == 2

        assert sqls[0].execution_id == 100
        assert sqls[0].status == "COMPLETED"
        assert sqls[0].description == "SELECT * FROM users"
        assert sqls[0].duration_ms == 5432
        assert sqls[0].running_jobs == 0
        assert sqls[0].success_jobs == 3
        assert sqls[0].failed_jobs == 0

        assert sqls[1].execution_id == 101
        assert sqls[1].status == "RUNNING"
        assert sqls[1].running_jobs == 2
        assert sqls[1].success_jobs == 1
        assert sqls[1].failed_jobs == 1

        await client.close()

    @pytest.mark.asyncio
    async def test_get_sql_passes_details_param(self) -> None:
        """Verify details=true query param is sent."""
        captured_params: Dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            for key, val in request.url.params.items():
                captured_params[key] = val
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        await client.get_sql()
        assert captured_params.get("details") == "true"

        await client.close()

    @pytest.mark.asyncio
    async def test_get_sql_empty(self) -> None:
        """Verify empty list when no SQL queries returned."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        sqls = await client.get_sql()
        assert sqls == []

        await client.close()


class TestGetStorage:
    """Tests for SparkRESTClient.get_storage()."""

    @pytest.mark.asyncio
    async def test_get_storage_returns_models(self) -> None:
        """Verify get_storage parses mock data into RDDInfo models."""
        storage_response: List[Dict[str, Any]] = [
            {
                "id": 1,
                "name": "MapPartitionsRDD[5] at cache",
                "numPartitions": 200,
                "numCachedPartitions": 200,
                "storageLevel": "Memory Deserialized 1x Replicated",
                "memoryUsed": 536870912,
                "diskUsed": 0,
            },
            {
                "id": 2,
                "name": "MapPartitionsRDD[10] at persist",
                "numPartitions": 100,
                "numCachedPartitions": 50,
                "storageLevel": "Disk Serialized 1x Replicated",
                "memoryUsed": 0,
                "diskUsed": 1073741824,
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/storage/rdd" in url:
                return httpx.Response(200, json=storage_response)
            return httpx.Response(404)

        client = _make_mock_client(handler)

        rdds = await client.get_storage()
        assert len(rdds) == 2

        assert rdds[0].rdd_id == 1
        assert rdds[0].name == "MapPartitionsRDD[5] at cache"
        assert rdds[0].num_partitions == 200
        assert rdds[0].num_cached_partitions == 200
        assert rdds[0].fraction_cached == pytest.approx(1.0)
        assert rdds[0].storage_level == "Memory Deserialized 1x Replicated"
        assert rdds[0].memory_used == 536870912
        assert rdds[0].disk_used == 0

        assert rdds[1].rdd_id == 2
        assert rdds[1].num_partitions == 100
        assert rdds[1].num_cached_partitions == 50
        assert rdds[1].fraction_cached == pytest.approx(0.5)
        assert rdds[1].disk_used == 1073741824

        await client.close()

    @pytest.mark.asyncio
    async def test_get_storage_empty(self) -> None:
        """Verify empty list when no cached RDDs."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        rdds = await client.get_storage()
        assert rdds == []

        await client.close()


# ---------------------------------------------------------------------------
# _get() error handling
# ---------------------------------------------------------------------------


class TestGetErrorHandling:
    """Tests for SparkRESTClient._get() error paths."""

    @pytest.mark.asyncio
    async def test_429_rate_limiting(self) -> None:
        """Verify 429 sets _rate_limited flag and calculates backoff."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429)

        client = _make_mock_client(handler)
        assert client._rate_limited is False
        assert client._rate_limit_backoff == 0.0

        result = await client._get("jobs")
        assert result is None
        assert client._rate_limited is True
        assert client._rate_limit_backoff > 0.0
        assert client._rate_limit_until > 0.0

        await client.close()

    @pytest.mark.asyncio
    async def test_429_exponential_backoff(self) -> None:
        """Verify successive 429s double the backoff."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(429)

        client = _make_mock_client(handler)

        # First 429 — initial backoff is (0 or 1) * 2 = 2.0
        await client._get("jobs")
        first_backoff = client._rate_limit_backoff
        assert first_backoff > 0.0

        # Clear the rate limit cooldown so the next request goes through
        client._rate_limit_until = 0.0
        client._rate_limited = False

        # Second 429 — backoff doubles
        await client._get("jobs")
        second_backoff = client._rate_limit_backoff
        assert second_backoff == first_backoff * 2

        await client.close()

    @pytest.mark.asyncio
    async def test_429_backoff_capped_at_30s(self) -> None:
        """Verify backoff does not exceed 30 seconds."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429)

        client = _make_mock_client(handler)
        # Simulate already high backoff
        client._rate_limit_backoff = 20.0

        await client._get("jobs")
        assert client._rate_limit_backoff <= 30.0

        await client.close()

    @pytest.mark.asyncio
    async def test_429_skips_request_during_cooldown(self) -> None:
        """Verify requests are skipped while rate limit cooldown is active."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        # Set the cooldown far in the future
        client._rate_limit_until = time.monotonic() + 3600
        client._rate_limited = True

        result = await client._get("jobs")
        assert result is None
        assert call_count == 0  # No HTTP request was made

        await client.close()

    @pytest.mark.asyncio
    async def test_404_triggers_app_rediscovery(self) -> None:
        """Verify 404 response triggers discover_app_id call."""
        request_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            url = str(request.url)
            if url.endswith("/applications"):
                return httpx.Response(200, json=[{"id": "app-new"}])
            # First request to the endpoint returns 404, retry returns 200
            if request_count <= 2:
                return httpx.Response(404)
            return httpx.Response(200, json=[{"jobId": 99, "status": "RUNNING"}])

        client = _make_mock_client(handler)
        await client._get("jobs")

        # After 404, discover_app_id should have been called
        assert client._app_id == "app-new"

        await client.close()

    @pytest.mark.asyncio
    async def test_404_rediscovery_fails_sets_unavailable(self) -> None:
        """Verify failed re-discovery sets _available to False."""

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            # Discovery endpoint also returns 404
            if url.endswith("/applications"):
                return httpx.Response(404)
            return httpx.Response(404)

        client = _make_mock_client(handler)
        result = await client._get("jobs")
        assert result is None
        assert client._available is False

        await client.close()

    @pytest.mark.asyncio
    async def test_timeout_returns_none(self) -> None:
        """Verify timeout returns None without crashing."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("Connection timed out")

        client = _make_mock_client(handler)
        result = await client._get("jobs")
        assert result is None
        # Timeout alone should not mark client unavailable (transient)
        assert client._available is True

        await client.close()

    @pytest.mark.asyncio
    async def test_connect_error_sets_unavailable(self) -> None:
        """Verify ConnectError sets _available to False."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

        client = _make_mock_client(handler)
        result = await client._get("jobs")
        assert result is None
        assert client._available is False

        await client.close()

    @pytest.mark.asyncio
    async def test_no_app_id_returns_none(self) -> None:
        """Verify _get returns None when no app_id is set."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        client._app_id = None  # Clear the pre-set app_id

        result = await client._get("jobs")
        assert result is None

        await client.close()

    @pytest.mark.asyncio
    async def test_successful_response_clears_rate_limit(self) -> None:
        """Verify a success response resets the rate limit state."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _make_mock_client(handler)
        # Simulate prior rate limiting state
        client._rate_limit_backoff = 8.0
        client._rate_limited = True
        client._rate_limit_until = 0.0  # Expired cooldown

        result = await client._get("jobs")
        assert result == []
        assert client._rate_limited is False
        assert client._rate_limit_backoff == 0.0

        await client.close()


# ---------------------------------------------------------------------------
# reset_app_id
# ---------------------------------------------------------------------------


class TestResetAppId:
    """Tests for SparkRESTClient.reset_app_id()."""

    def test_reset_clears_app_id_and_available(self) -> None:
        """Verify reset_app_id clears _app_id and sets _available to False."""
        client = SparkRESTClient(
            workspace_url="https://adb-123.1.azuredatabricks.net",
            cluster_id="cluster-1",
            org_id="123",
        )
        client._app_id = "app-123"
        client._available = True

        client.reset_app_id()

        assert client._app_id is None
        assert client._available is False

    def test_reset_is_idempotent(self) -> None:
        """Verify calling reset_app_id twice does not error."""
        client = SparkRESTClient(
            workspace_url="https://adb-123.1.azuredatabricks.net",
            cluster_id="cluster-1",
            org_id="123",
        )
        client.reset_app_id()
        client.reset_app_id()

        assert client._app_id is None
        assert client._available is False
