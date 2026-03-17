"""Unit tests for DatabricksClient._call() error translation and get_token().

Covers:
  1. _call() — SDK exception → custom exception mapping
  2. get_token() — PAT, OAuth Bearer, and failure paths
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied, Unauthenticated

from dbxtop.api.client import (
    AuthenticationError,
    ClusterNotFoundError,
    DatabricksClient,
    DatabricksConnectionError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> DatabricksClient:
    """Create a DatabricksClient with a mocked WorkspaceClient (no __init__)."""
    client = DatabricksClient.__new__(DatabricksClient)
    client._profile = "test"
    client._cluster_id = "test-cluster"
    client._workspace = MagicMock()
    return client


# ---------------------------------------------------------------------------
# Section 1: _call() error translation
# ---------------------------------------------------------------------------


class TestCallErrorTranslation:
    """Verify that _call() translates SDK exceptions to custom dbxtop exceptions."""

    @pytest.mark.asyncio
    async def test_not_found_raises_cluster_not_found(self) -> None:
        """NotFound from the SDK should become ClusterNotFoundError."""
        client = _make_client()

        def _boom() -> None:
            raise NotFound("Cluster does not exist")

        with pytest.raises(ClusterNotFoundError, match="test-cluster"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_unauthenticated_raises_authentication_error(self) -> None:
        """Unauthenticated from the SDK should become AuthenticationError."""
        client = _make_client()

        def _boom() -> None:
            raise Unauthenticated("Invalid token")

        with pytest.raises(AuthenticationError, match="test"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_permission_denied_raises_authentication_error(self) -> None:
        """PermissionDenied from the SDK should become AuthenticationError."""
        client = _make_client()

        def _boom() -> None:
            raise PermissionDenied("Forbidden")

        with pytest.raises(AuthenticationError, match="test"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_timeout_exception_raises_connection_error(self) -> None:
        """An exception whose message contains 'timeout' should become DatabricksConnectionError."""
        client = _make_client()

        def _boom() -> None:
            raise Exception("Connection timeout after 30s")

        with pytest.raises(DatabricksConnectionError, match="timeout"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_connect_exception_raises_connection_error(self) -> None:
        """An exception whose message contains 'connect' should become DatabricksConnectionError."""
        client = _make_client()

        def _boom() -> None:
            raise Exception("Failed to connect to host")

        with pytest.raises(DatabricksConnectionError, match="connect"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_generic_exception_reraised_as_is(self) -> None:
        """A generic exception without timeout/connect keywords should be re-raised unchanged."""
        client = _make_client()

        def _boom() -> None:
            raise ValueError("something unexpected")

        with pytest.raises(ValueError, match="something unexpected"):
            await client._call(_boom)

    @pytest.mark.asyncio
    async def test_successful_call_returns_result(self) -> None:
        """A successful SDK call should return the result directly."""
        client = _make_client()

        def _ok() -> str:
            return "cluster-info"

        result = await client._call(_ok)
        assert result == "cluster-info"

    @pytest.mark.asyncio
    async def test_call_passes_args_and_kwargs(self) -> None:
        """_call() should forward positional and keyword arguments to the wrapped function."""
        client = _make_client()
        received: dict = {}

        def _capture(a: int, b: str, key: str = "") -> str:
            received["a"] = a
            received["b"] = b
            received["key"] = key
            return "done"

        result = await client._call(_capture, 1, "two", key="three")
        assert result == "done"
        assert received == {"a": 1, "b": "two", "key": "three"}

    @pytest.mark.asyncio
    async def test_not_found_preserves_original_cause(self) -> None:
        """ClusterNotFoundError should chain the original NotFound as __cause__."""
        client = _make_client()
        original = NotFound("gone")

        def _boom() -> None:
            raise original

        with pytest.raises(ClusterNotFoundError) as exc_info:
            await client._call(_boom)

        assert exc_info.value.__cause__ is original

    @pytest.mark.asyncio
    async def test_authentication_error_preserves_original_cause(self) -> None:
        """AuthenticationError should chain the original Unauthenticated as __cause__."""
        client = _make_client()
        original = Unauthenticated("bad token")

        def _boom() -> None:
            raise original

        with pytest.raises(AuthenticationError) as exc_info:
            await client._call(_boom)

        assert exc_info.value.__cause__ is original

    @pytest.mark.asyncio
    async def test_connection_error_preserves_original_cause(self) -> None:
        """DatabricksConnectionError should chain the original timeout exception as __cause__."""
        client = _make_client()
        original = Exception("connection timeout")

        def _boom() -> None:
            raise original

        with pytest.raises(DatabricksConnectionError) as exc_info:
            await client._call(_boom)

        assert exc_info.value.__cause__ is original


# ---------------------------------------------------------------------------
# Section 2: get_token()
# ---------------------------------------------------------------------------


class TestGetToken:
    """Verify DatabricksClient.get_token() token retrieval paths."""

    @pytest.mark.asyncio
    async def test_pat_token_returned_directly(self) -> None:
        """When config.token is set (PAT), it should be returned without calling authenticate()."""
        client = _make_client()
        client._workspace.config.token = "dapi-test-pat-token"

        token = await client.get_token()

        assert token == "dapi-test-pat-token"
        client._workspace.config.authenticate.assert_not_called()

    @pytest.mark.asyncio
    async def test_oauth_bearer_token_extracted(self) -> None:
        """When no PAT but authenticate() returns a Bearer header, the token should be extracted."""
        client = _make_client()
        client._workspace.config.token = None
        client._workspace.config.authenticate.return_value = {"Authorization": "Bearer oauth-token-abc123"}

        token = await client.get_token()

        assert token == "oauth-token-abc123"

    @pytest.mark.asyncio
    async def test_authenticate_failure_raises_authentication_error(self) -> None:
        """When authenticate() throws, AuthenticationError should be raised."""
        client = _make_client()
        client._workspace.config.token = None
        client._workspace.config.authenticate.side_effect = Exception("OAuth refresh failed")

        with pytest.raises(AuthenticationError, match="Token retrieval failed"):
            await client.get_token()

    @pytest.mark.asyncio
    async def test_non_bearer_header_raises_authentication_error(self) -> None:
        """When authenticate() returns a non-Bearer Authorization header, AuthenticationError should be raised."""
        client = _make_client()
        client._workspace.config.token = None
        client._workspace.config.authenticate.return_value = {"Authorization": "Basic dXNlcjpwYXNz"}

        with pytest.raises(AuthenticationError, match="No valid token"):
            await client.get_token()

    @pytest.mark.asyncio
    async def test_empty_auth_header_raises_authentication_error(self) -> None:
        """When authenticate() returns an empty Authorization header, AuthenticationError should be raised."""
        client = _make_client()
        client._workspace.config.token = None
        client._workspace.config.authenticate.return_value = {"Authorization": ""}

        with pytest.raises(AuthenticationError, match="No valid token"):
            await client.get_token()

    @pytest.mark.asyncio
    async def test_no_auth_header_raises_authentication_error(self) -> None:
        """When authenticate() returns headers without Authorization, AuthenticationError should be raised."""
        client = _make_client()
        client._workspace.config.token = None
        client._workspace.config.authenticate.return_value = {"X-Other": "value"}

        with pytest.raises(AuthenticationError, match="No valid token"):
            await client.get_token()

    @pytest.mark.asyncio
    async def test_authenticate_error_chains_original_cause(self) -> None:
        """AuthenticationError from authenticate() failure should chain the original exception."""
        client = _make_client()
        client._workspace.config.token = None
        original = RuntimeError("network down")
        client._workspace.config.authenticate.side_effect = original

        with pytest.raises(AuthenticationError) as exc_info:
            await client.get_token()

        assert exc_info.value.__cause__ is original

    @pytest.mark.asyncio
    async def test_empty_pat_token_falls_through_to_authenticate(self) -> None:
        """An empty-string PAT token (falsy) should fall through to authenticate()."""
        client = _make_client()
        client._workspace.config.token = ""
        client._workspace.config.authenticate.return_value = {"Authorization": "Bearer fallback-token"}

        token = await client.get_token()

        assert token == "fallback-token"
        client._workspace.config.authenticate.assert_called_once()
