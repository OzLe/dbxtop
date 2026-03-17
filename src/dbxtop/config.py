"""Configuration and settings management for dbxtop.

Handles loading Databricks CLI profiles, environment variables,
and runtime configuration for the dashboard.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _parse_env_float(value: str | None, default: float) -> float:
    """Parse a float from an environment variable string, returning *default* on failure."""
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Immutable runtime configuration for dbxtop.

    Resolution order per field: CLI flag > environment variable > default value.

    Attributes:
        profile: Databricks CLI profile name.
        cluster_id: Target Databricks cluster ID.
        spark_port: Spark UI driver proxy port.
        fast_poll_s: Polling interval (seconds) for fast-changing data.
        slow_poll_s: Polling interval (seconds) for slow-changing data.
        max_retries: Maximum retry attempts per API request.
        request_timeout_s: HTTP request timeout in seconds.
        theme: Color theme name (``'dark'`` or ``'light'``).
    """

    profile: str
    cluster_id: str
    spark_port: int = 40001
    fast_poll_s: float = 3.0
    slow_poll_s: float = 15.0
    max_retries: int = 3
    request_timeout_s: float = 10.0
    theme: str = "dark"
    keepalive: bool = False
    keepalive_interval_s: float = 300.0

    def __post_init__(self) -> None:
        """Validate field constraints after initialisation."""
        if not self.profile:
            raise ValueError("profile must not be empty")
        if not self.cluster_id:
            raise ValueError("cluster_id must not be empty")
        if not 1.0 <= self.fast_poll_s <= 30.0:
            raise ValueError(f"fast_poll_s must be between 1.0 and 30.0, got {self.fast_poll_s}")
        if not 5.0 <= self.slow_poll_s <= 120.0:
            raise ValueError(f"slow_poll_s must be between 5.0 and 120.0, got {self.slow_poll_s}")
        if self.slow_poll_s < self.fast_poll_s:
            raise ValueError(f"slow_poll_s ({self.slow_poll_s}) must be >= fast_poll_s ({self.fast_poll_s})")
        if self.theme not in ("dark", "light"):
            raise ValueError(f"theme must be 'dark' or 'light', got {self.theme!r}")
        if self.keepalive:
            if not 60.0 <= self.keepalive_interval_s <= 1800.0:
                raise ValueError(
                    f"keepalive_interval_s must be between 60.0 and 1800.0, got {self.keepalive_interval_s}"
                )

    @classmethod
    def from_cli(
        cls,
        profile: str | None = None,
        cluster_id: str | None = None,
        refresh: float | None = None,
        slow_refresh: float | None = None,
        theme: str | None = None,
        keepalive: bool | None = None,
        keepalive_interval: float | None = None,
    ) -> Settings:
        """Create a Settings instance by merging CLI args, env vars, and defaults.

        Args:
            profile: CLI ``--profile`` value (overrides ``DBXTOP_PROFILE``).
            cluster_id: CLI ``--cluster-id`` value (overrides ``DBXTOP_CLUSTER_ID``).
            refresh: CLI ``--refresh`` value (overrides ``DBXTOP_REFRESH``).
            slow_refresh: CLI ``--slow-refresh`` value (overrides ``DBXTOP_SLOW_REFRESH``).
            theme: CLI ``--theme`` value (overrides ``DBXTOP_THEME``).
            keepalive: CLI ``--keepalive`` value (overrides ``DBXTOP_KEEPALIVE``).
            keepalive_interval: CLI ``--keepalive-interval`` value (overrides ``DBXTOP_KEEPALIVE_INTERVAL``).

        Returns:
            A validated Settings instance.

        Raises:
            ValueError: If required fields are missing or constraints are violated.
        """
        resolved_profile = profile or os.environ.get("DBXTOP_PROFILE", "")
        resolved_cluster_id = cluster_id or os.environ.get("DBXTOP_CLUSTER_ID", "")

        env_refresh = os.environ.get("DBXTOP_REFRESH")
        resolved_fast = refresh if refresh is not None else _parse_env_float(env_refresh, 3.0)

        env_slow = os.environ.get("DBXTOP_SLOW_REFRESH")
        resolved_slow = slow_refresh if slow_refresh is not None else _parse_env_float(env_slow, 15.0)

        resolved_theme = theme or os.environ.get("DBXTOP_THEME", "dark")

        env_keepalive = os.environ.get("DBXTOP_KEEPALIVE", "").lower() in ("1", "true", "yes")
        resolved_keepalive = keepalive if keepalive is not None else env_keepalive

        env_keepalive_interval = os.environ.get("DBXTOP_KEEPALIVE_INTERVAL")
        resolved_keepalive_interval = (
            keepalive_interval if keepalive_interval is not None else _parse_env_float(env_keepalive_interval, 300.0)
        )

        return cls(
            profile=resolved_profile,
            cluster_id=resolved_cluster_id,
            fast_poll_s=resolved_fast,
            slow_poll_s=resolved_slow,
            theme=resolved_theme,
            keepalive=resolved_keepalive,
            keepalive_interval_s=resolved_keepalive_interval,
        )
