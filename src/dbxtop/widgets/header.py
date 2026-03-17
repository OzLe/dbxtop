"""Custom header widget.

Displays cluster name, ID, state, and connection status
in a compact top-of-screen bar.
"""

from __future__ import annotations

from typing import Any, Optional

from textual.reactive import reactive
from textual.widgets import Static

from dbxtop import __version__
from dbxtop.api.cache import DataCache
from dbxtop.api.models import ClusterInfo, ClusterState, format_duration
from dbxtop.widgets.status_indicator import CLUSTER_STATES

_STATE_STYLES = CLUSTER_STATES


class ClusterHeader(Static):
    """Single-row header: ``dbxtop | {name} | {state} | up {uptime} | next {n}s | {profile}``."""

    DEFAULT_CSS = """
    ClusterHeader {
        dock: top;
        height: 1;
        background: $boost;
        color: $text;
        text-style: bold;
        padding: 0 1;
    }
    """

    countdown: reactive[float] = reactive(0.0)
    spark_available: reactive[bool] = reactive(False)

    def __init__(
        self,
        profile: str = "",
        poll_interval: float = 3.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(f"dbxtop v{__version__} | connecting...", **kwargs)
        self._profile = profile
        self._poll_interval = poll_interval
        self._cluster: Optional[ClusterInfo] = None
        self._stale = False

    def on_mount(self) -> None:
        """Start the 1-second countdown/uptime ticker."""
        self.set_interval(1.0, self._tick)
        self.countdown = self._poll_interval

    def _tick(self) -> None:
        """Called every second to update countdown and re-render."""
        self.countdown = max(0.0, self.countdown - 1.0)
        self._render_bar()

    def reset_countdown(self) -> None:
        """Reset the poll countdown (called after each poll completes)."""
        self.countdown = self._poll_interval

    def update_from_cache(self, cache: DataCache) -> None:
        """Refresh header content from the data cache.

        Args:
            cache: The shared data cache.
        """
        slot = cache.get("cluster")
        self._cluster = slot.data
        self._stale = slot.stale
        self.reset_countdown()
        self._render_bar()

    def _render_bar(self) -> None:
        """Compose and update the header text."""
        parts: list[str] = [f"dbxtop v{__version__}"]

        info = self._cluster
        if info is not None:
            parts.append(info.cluster_name or info.cluster_id)
            state_str = info.state.value
            colour, symbol = _STATE_STYLES.get(state_str, ("dim", "?"))
            parts.append(f"[{colour}]{symbol} {state_str}[/{colour}]")

            if info.state == ClusterState.RUNNING:
                uptime_ms = info.uptime_seconds * 1000
                parts.append(f"up {format_duration(uptime_ms)}")
            else:
                parts.append("up --")
        else:
            parts.append("connecting...")

        parts.append(f"next {int(self.countdown)}s")

        if self._profile:
            parts.append(self._profile)

        if self._stale:
            parts.append("[yellow]⚠ stale[/yellow]")

        spark_indicator = (
            "[green]Spark: connected[/green]" if self.spark_available else "[red]Spark: disconnected[/red]"
        )
        parts.append(spark_indicator)

        self.update(" | ".join(parts))
