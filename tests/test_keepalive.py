"""Unit tests for the keep-alive feature (TDD red phase).

Tests cover three layers:
  1. Settings/config — new keepalive fields and validation
  2. DatabricksClient — keepalive_ping, destroy_keepalive_context methods
  3. MetricsPoller — keepalive tick scheduling, failure counting, messages
"""

from __future__ import annotations

import os
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbxtop.api.cache import DataCache
from dbxtop.api.models import ClusterInfo, ClusterState
from dbxtop.config import Settings


# ---------------------------------------------------------------------------
# Section 1: Config / Settings tests
# ---------------------------------------------------------------------------


class TestKeepAliveSettings:
    """Verify that Settings exposes keepalive fields with correct defaults and validation."""

    def test_keepalive_defaults_to_false(self) -> None:
        """Settings() should have keepalive=False by default."""
        s = Settings(profile="default", cluster_id="abc-123")
        assert s.keepalive is False

    def test_keepalive_interval_default_300(self) -> None:
        """Default keepalive interval should be 300.0 seconds."""
        s = Settings(profile="default", cluster_id="abc-123", keepalive=True)
        assert s.keepalive_interval_s == 300.0

    def test_keepalive_interval_too_low_raises(self) -> None:
        """keepalive_interval_s=30.0 should raise ValueError (minimum is 60)."""
        with pytest.raises(ValueError, match="keepalive_interval_s"):
            Settings(profile="p", cluster_id="c", keepalive=True, keepalive_interval_s=30.0)

    def test_keepalive_interval_too_high_raises(self) -> None:
        """keepalive_interval_s=2000.0 should raise ValueError (maximum is 1800)."""
        with pytest.raises(ValueError, match="keepalive_interval_s"):
            Settings(profile="p", cluster_id="c", keepalive=True, keepalive_interval_s=2000.0)

    def test_keepalive_interval_at_lower_boundary(self) -> None:
        """keepalive_interval_s=60.0 should be accepted."""
        s = Settings(profile="p", cluster_id="c", keepalive=True, keepalive_interval_s=60.0)
        assert s.keepalive_interval_s == 60.0

    def test_keepalive_interval_at_upper_boundary(self) -> None:
        """keepalive_interval_s=1800.0 should be accepted."""
        s = Settings(profile="p", cluster_id="c", keepalive=True, keepalive_interval_s=1800.0)
        assert s.keepalive_interval_s == 1800.0

    def test_keepalive_disabled_skips_interval_validation(self) -> None:
        """When keepalive=False, interval validation should not trigger even with an out-of-range value."""
        # The default interval (300) should be used silently when keepalive is disabled.
        s = Settings(profile="p", cluster_id="c", keepalive=False)
        assert s.keepalive is False


# ---------------------------------------------------------------------------
# Section 2: DatabricksClient keepalive methods
# ---------------------------------------------------------------------------


def _make_mock_workspace() -> MagicMock:
    """Create a mock WorkspaceClient with command_execution service."""
    ws = MagicMock()
    ws.config.host = "https://adb-123.1.azuredatabricks.net"
    ws.config.token = "dapi-test-token"
    return ws


class TestKeepAlivePing:
    """Verify DatabricksClient.keepalive_ping() behaviour."""

    @pytest.mark.asyncio
    async def test_keepalive_ping_creates_context_first_call(self) -> None:
        """On the first call (no existing context), create_and_wait should be called,
        then execute_and_wait to run a lightweight command."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = None

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            # Mock create_and_wait to return a context with id
            create_result = MagicMock()
            create_result.id = "ctx-123"
            mock_ws.command_execution.create_and_wait.return_value = create_result

            # Mock execute_and_wait to return a finished command
            exec_result = MagicMock()
            exec_result.status.value = "Finished"
            mock_ws.command_execution.execute_and_wait.return_value = exec_result

            # Wrap sync calls for asyncio.to_thread
            result = await client.keepalive_ping()

            assert result is True
            mock_ws.command_execution.create_and_wait.assert_called_once()
            mock_ws.command_execution.execute_and_wait.assert_called_once()
            assert client._keepalive_context_id == "ctx-123"

    @pytest.mark.asyncio
    async def test_keepalive_ping_reuses_context(self) -> None:
        """When a context_id already exists, only execute_and_wait should be called
        (no new create_and_wait)."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-existing"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            exec_result = MagicMock()
            exec_result.status.value = "Finished"
            mock_ws.command_execution.execute_and_wait.return_value = exec_result

            result = await client.keepalive_ping()

            assert result is True
            mock_ws.command_execution.create_and_wait.assert_not_called()
            mock_ws.command_execution.execute_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_keepalive_ping_returns_true_on_finished(self) -> None:
        """keepalive_ping should return True when the command execution status is Finished."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-ok"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            exec_result = MagicMock()
            exec_result.status.value = "Finished"
            mock_ws.command_execution.execute_and_wait.return_value = exec_result

            result = await client.keepalive_ping()
            assert result is True

    @pytest.mark.asyncio
    async def test_keepalive_ping_returns_false_on_exception(self) -> None:
        """keepalive_ping should return False (not raise) when the SDK throws."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-bad"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws
            mock_ws.command_execution.execute_and_wait.side_effect = Exception("network error")

            result = await client.keepalive_ping()
            assert result is False

    @pytest.mark.asyncio
    async def test_keepalive_ping_clears_context_on_resource_gone(self) -> None:
        """When the SDK raises RESOURCE_DOES_NOT_EXIST, the cached context_id should be cleared
        so the next call creates a fresh context."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-stale"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            # Simulate RESOURCE_DOES_NOT_EXIST — the SDK raises an error
            # whose message or type indicates the resource is gone.
            error = Exception("RESOURCE_DOES_NOT_EXIST: Context ctx-stale not found")
            mock_ws.command_execution.execute_and_wait.side_effect = error

            result = await client.keepalive_ping()

            assert result is False
            assert client._keepalive_context_id is None


class TestDestroyKeepAliveContext:
    """Verify DatabricksClient.destroy_keepalive_context() behaviour."""

    @pytest.mark.asyncio
    async def test_destroy_context_calls_sdk(self) -> None:
        """When a context_id exists, destroy should call command_execution.destroy."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-to-destroy"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            await client.destroy_keepalive_context()

            mock_ws.command_execution.destroy.assert_called_once()
            assert client._keepalive_context_id is None

    @pytest.mark.asyncio
    async def test_destroy_context_noop_when_none(self) -> None:
        """When no context_id exists, destroy should be a no-op (no SDK calls)."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = None

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            await client.destroy_keepalive_context()

            mock_ws.command_execution.destroy.assert_not_called()

    @pytest.mark.asyncio
    async def test_destroy_context_swallows_errors(self) -> None:
        """SDK errors during destroy should be swallowed (no exception propagated)."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-err"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws
            mock_ws.command_execution.destroy.side_effect = Exception("destroy failed")

            # Should not raise
            await client.destroy_keepalive_context()
            assert client._keepalive_context_id is None


# ---------------------------------------------------------------------------
# Section 3: MetricsPoller keepalive tier
# ---------------------------------------------------------------------------


def _make_poller_deps(
    cluster_state: Optional[ClusterState] = ClusterState.RUNNING,
    keepalive: bool = True,
    keepalive_interval_s: float = 300.0,
) -> tuple[Any, ...]:
    """Create mock dependencies for MetricsPoller tests.

    Returns:
        (dbx_client, spark_client, cache, app, settings)
    """
    dbx_client = MagicMock()
    dbx_client.keepalive_ping = AsyncMock(return_value=True)
    dbx_client.destroy_keepalive_context = AsyncMock()

    spark_client = MagicMock()
    spark_client.is_available = AsyncMock(return_value=True)
    spark_client.close = AsyncMock()

    cache = DataCache()
    if cluster_state is not None:
        cache.update("cluster", ClusterInfo(cluster_id="test-cluster", state=cluster_state))

    app = MagicMock()
    app.post_message = MagicMock()
    app.set_interval = MagicMock(return_value=MagicMock())
    app.call_later = MagicMock()

    settings = Settings(
        profile="test",
        cluster_id="test-cluster",
        keepalive=keepalive,
        keepalive_interval_s=keepalive_interval_s,
    )

    return dbx_client, spark_client, cache, app, settings


class TestKeepAliveTick:
    """Verify the keepalive polling tier in MetricsPoller."""

    @pytest.mark.asyncio
    async def test_keepalive_tick_skips_when_not_running(self) -> None:
        """When the cluster state is TERMINATED, keepalive_ping should NOT be called."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(cluster_state=ClusterState.TERMINATED)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        await poller._keepalive_tick()

        dbx.keepalive_ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_keepalive_tick_skips_when_no_cluster_data(self) -> None:
        """When the cache has no cluster data yet, keepalive_ping should NOT be called."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(cluster_state=None)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        await poller._keepalive_tick()

        dbx.keepalive_ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_keepalive_tick_calls_ping_when_running(self) -> None:
        """When cluster is RUNNING, keepalive_ping should be called."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(cluster_state=ClusterState.RUNNING)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        await poller._keepalive_tick()

        dbx.keepalive_ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_keepalive_success_resets_failure_count(self) -> None:
        """After consecutive failures then a success, the failure counter should reset to 0."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps()
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        # Simulate 2 failures
        dbx.keepalive_ping = AsyncMock(return_value=False)
        await poller._keepalive_tick()
        await poller._keepalive_tick()
        assert poller._keepalive_failures == 2

        # Now succeed
        dbx.keepalive_ping = AsyncMock(return_value=True)
        await poller._keepalive_tick()
        assert poller._keepalive_failures == 0

    @pytest.mark.asyncio
    async def test_keepalive_disables_after_3_failures(self) -> None:
        """After 3 consecutive failures, keepalive should be disabled (no more pings)."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps()
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        dbx.keepalive_ping = AsyncMock(return_value=False)

        # 3 failures
        await poller._keepalive_tick()
        await poller._keepalive_tick()
        await poller._keepalive_tick()

        assert poller._keepalive_failures >= 3

        # Reset mock call count to verify 4th tick doesn't call ping
        dbx.keepalive_ping.reset_mock()
        await poller._keepalive_tick()
        dbx.keepalive_ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_keepalive_posts_success_message(self) -> None:
        """A successful ping should post a KeepAliveUpdated message with active=True."""
        from dbxtop.api.poller import KeepAliveUpdated, MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps()
        poller = MetricsPoller(dbx, spark, cache, app, settings)
        dbx.keepalive_ping = AsyncMock(return_value=True)

        await poller._keepalive_tick()

        # Find the KeepAliveUpdated message in post_message calls
        posted = [
            call.args[0] for call in app.post_message.call_args_list if isinstance(call.args[0], KeepAliveUpdated)
        ]
        assert len(posted) >= 1
        msg = posted[-1]
        assert msg.active is True
        assert msg.failed is False

    @pytest.mark.asyncio
    async def test_keepalive_posts_failed_message(self) -> None:
        """After 3 consecutive failures, a KeepAliveUpdated(failed=True) message should be posted."""
        from dbxtop.api.poller import KeepAliveUpdated, MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps()
        poller = MetricsPoller(dbx, spark, cache, app, settings)
        dbx.keepalive_ping = AsyncMock(return_value=False)

        # 3 failures to trigger disable
        await poller._keepalive_tick()
        await poller._keepalive_tick()
        await poller._keepalive_tick()

        # Find the final KeepAliveUpdated message
        posted = [
            call.args[0] for call in app.post_message.call_args_list if isinstance(call.args[0], KeepAliveUpdated)
        ]
        assert len(posted) >= 1
        # The last posted message should indicate failure
        msg = posted[-1]
        assert msg.failed is True


# ---------------------------------------------------------------------------
# Section 4: Iteration 2 — gap-coverage tests
# ---------------------------------------------------------------------------


class TestKeepAlivePingNoneStatus:
    """Cover the case where result.status is None (client.py:247 risk)."""

    @pytest.mark.asyncio
    async def test_keepalive_ping_returns_false_on_none_status(self) -> None:
        """When execute_and_wait returns a result with status=None,
        keepalive_ping should return False instead of raising AttributeError."""
        from dbxtop.api.client import DatabricksClient

        with patch.object(DatabricksClient, "__init__", lambda self, *a, **kw: None):
            client = DatabricksClient.__new__(DatabricksClient)
            client._cluster_id = "cluster-1"
            client._profile = "test"
            client._keepalive_context_id = "ctx-ok"

            mock_ws = _make_mock_workspace()
            client._workspace = mock_ws

            # result.status is None — accessing .value should not crash
            exec_result = MagicMock()
            exec_result.status = None
            mock_ws.command_execution.execute_and_wait.return_value = exec_result

            result = await client.keepalive_ping()
            assert result is False


class TestKeepAliveTickLock:
    """Verify that _keepalive_tick acquires _keepalive_lock to prevent concurrent pings."""

    @pytest.mark.asyncio
    async def test_keepalive_tick_acquires_lock(self) -> None:
        """_keepalive_tick should acquire _keepalive_lock before calling keepalive_ping,
        so that concurrent ticks are serialised."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(cluster_state=ClusterState.RUNNING)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        lock_was_held = False

        original_tick_inner = poller._keepalive_tick_inner

        async def _spy_tick_inner() -> None:
            nonlocal lock_was_held
            # The lock should be held when the inner method executes
            lock_was_held = poller._keepalive_lock.locked()
            await original_tick_inner()

        with patch.object(poller, "_keepalive_tick_inner", side_effect=_spy_tick_inner):
            await poller._keepalive_tick()

        assert lock_was_held, (
            "_keepalive_lock was not held during _keepalive_tick_inner — concurrent pings are not protected"
        )


class TestAppHandlesKeepAliveUpdated:
    """Verify that DbxTopApp wires KeepAliveUpdated messages to the footer."""

    def test_app_has_keepalive_updated_handler(self) -> None:
        """DbxTopApp should have a handler method for KeepAliveUpdated messages.

        Textual dispatches messages to on_<message_name> methods.  The
        KeepAliveUpdated message class should map to a handler named
        ``on_keep_alive_updated`` on the app.
        """
        from dbxtop.app import DbxTopApp

        # The handler should exist as a method on the class
        handler = getattr(DbxTopApp, "on_keep_alive_updated", None)
        assert handler is not None, (
            "DbxTopApp is missing on_keep_alive_updated handler — "
            "KeepAliveUpdated messages are posted but never consumed"
        )
        assert callable(handler)

    def test_keepalive_handler_updates_footer(self) -> None:
        """on_keep_alive_updated should update footer properties
        to reflect keepalive status (e.g., keepalive_active, keepalive_failed)."""
        from dbxtop.api.poller import KeepAliveUpdated
        from dbxtop.app import DbxTopApp

        app = DbxTopApp.__new__(DbxTopApp)
        # Set up the minimal footer mock
        mock_footer = MagicMock()
        app._footer = mock_footer

        handler = getattr(app, "on_keep_alive_updated", None)
        assert handler is not None, "on_keep_alive_updated handler not found"

        # Simulate a successful keepalive message
        msg = KeepAliveUpdated(active=True, last_success=None, failed=False)
        handler(msg)

        # The handler should set keepalive properties on the footer
        assert mock_footer.keepalive_active is True, "keepalive_active should be set to True"
        assert mock_footer.keepalive_failed is False, "keepalive_failed should be set to False"


class TestSettingsFromCliKeepAlive:
    """Verify Settings.from_cli() handles keepalive params and env vars."""

    def test_from_cli_keepalive_true(self) -> None:
        """from_cli(keepalive=True) should produce Settings with keepalive=True."""
        s = Settings.from_cli(profile="test", cluster_id="c-123", keepalive=True, keepalive_interval=120.0)
        assert s.keepalive is True
        assert s.keepalive_interval_s == 120.0

    def test_from_cli_keepalive_false_by_default(self) -> None:
        """from_cli() without keepalive arg should default to False."""
        s = Settings.from_cli(profile="test", cluster_id="c-123")
        assert s.keepalive is False

    def test_from_cli_keepalive_from_env_var(self) -> None:
        """from_cli() should read DBXTOP_KEEPALIVE env var when no CLI arg."""
        with patch.dict(os.environ, {"DBXTOP_KEEPALIVE": "true"}):
            s = Settings.from_cli(profile="test", cluster_id="c-123")
            assert s.keepalive is True

    def test_from_cli_keepalive_interval_from_env_var(self) -> None:
        """from_cli() should read DBXTOP_KEEPALIVE_INTERVAL env var when no CLI arg."""
        with patch.dict(os.environ, {"DBXTOP_KEEPALIVE": "1", "DBXTOP_KEEPALIVE_INTERVAL": "120.0"}):
            s = Settings.from_cli(profile="test", cluster_id="c-123")
            assert s.keepalive is True
            assert s.keepalive_interval_s == 120.0

    def test_from_cli_keepalive_cli_overrides_env(self) -> None:
        """CLI keepalive=False should override DBXTOP_KEEPALIVE=true."""
        with patch.dict(os.environ, {"DBXTOP_KEEPALIVE": "true"}):
            s = Settings.from_cli(profile="test", cluster_id="c-123", keepalive=False)
            assert s.keepalive is False


class TestPollerStartStopKeepAlive:
    """Verify poller.start() creates keepalive timer and poller.stop() cleans up."""

    def test_start_creates_keepalive_timer_when_enabled(self) -> None:
        """poller.start() should call set_interval with keepalive interval when keepalive=True."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(keepalive=True, keepalive_interval_s=120.0)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        poller.start()

        # set_interval should be called 3 times: fast, slow, keepalive
        assert app.set_interval.call_count == 3
        # The third call should be the keepalive timer
        keepalive_call = app.set_interval.call_args_list[2]
        assert keepalive_call.args[0] == 120.0  # interval
        assert keepalive_call.kwargs.get("name") == "keepalive" or (
            len(keepalive_call.args) > 2 and keepalive_call.args[2] == "keepalive"
        )

    def test_start_skips_keepalive_timer_when_disabled(self) -> None:
        """poller.start() should NOT create a keepalive timer when keepalive=False."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(keepalive=False)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        poller.start()

        # set_interval should only be called twice: fast + slow
        assert app.set_interval.call_count == 2

    @pytest.mark.asyncio
    async def test_stop_destroys_keepalive_context(self) -> None:
        """poller.stop() should call destroy_keepalive_context on the client."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(keepalive=True)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        # Simulate started timers
        mock_timer = MagicMock()
        poller._fast_timer = mock_timer
        poller._slow_timer = mock_timer
        poller._keepalive_timer = mock_timer

        await poller.stop()

        # keepalive timer should be stopped
        assert mock_timer.stop.call_count == 3
        # destroy_keepalive_context should be called
        dbx.destroy_keepalive_context.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_stops_keepalive_timer(self) -> None:
        """poller.stop() should stop the keepalive timer if it exists."""
        from dbxtop.api.poller import MetricsPoller

        dbx, spark, cache, app, settings = _make_poller_deps(keepalive=True)
        poller = MetricsPoller(dbx, spark, cache, app, settings)

        keepalive_timer = MagicMock()
        poller._keepalive_timer = keepalive_timer
        poller._fast_timer = MagicMock()
        poller._slow_timer = MagicMock()

        await poller.stop()

        keepalive_timer.stop.assert_called_once()
