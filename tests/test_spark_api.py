"""Unit tests for SparkRESTClient URL construction and mapping."""

from __future__ import annotations

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
