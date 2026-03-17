"""Keyboard shortcuts footer widget.

Displays available key bindings and current refresh status
in a compact bottom-of-screen bar.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from textual.reactive import reactive
from textual.widgets import Static


# Per-tab extra bindings (tab_id → hint string)
_VIEW_BINDINGS: dict[str, str] = {
    "jobs": "s:Sort | Enter:Detail",
    "stages": "s:Sort | Enter:Detail",
    "executors": "s:Sort",
    "sql": "s:Sort | Enter:Detail",
    "storage": "s:Sort",
}

_GLOBAL_HINTS = "1-6:Views | r:Refresh | /:Filter | ?:Help | q:Quit"


class KeyboardFooter(Static):
    """Single-row footer with context-sensitive key hints and connection status."""

    DEFAULT_CSS = """
    KeyboardFooter {
        dock: bottom;
        height: 1;
        background: $boost;
        color: $text-muted;
        padding: 0 1;
    }
    """

    active_tab: reactive[str] = reactive("cluster")
    spark_connected: reactive[bool] = reactive(False)
    sdk_only: reactive[bool] = reactive(False)
    keepalive_active: reactive[bool] = reactive(False)
    keepalive_last: reactive[Optional[datetime]] = reactive[Optional[datetime]](None)
    keepalive_failed: reactive[bool] = reactive(False)

    def __init__(self, **kwargs: object) -> None:
        super().__init__("", **kwargs)

    def on_mount(self) -> None:
        """Initial render."""
        self._render_bar()

    def watch_active_tab(self) -> None:
        """Re-render when the active tab changes."""
        self._render_bar()

    def watch_spark_connected(self) -> None:
        """Re-render when Spark connection status changes."""
        self._render_bar()

    def watch_sdk_only(self) -> None:
        """Re-render when SDK-only mode changes."""
        self._render_bar()

    def watch_keepalive_active(self) -> None:
        """Re-render when keep-alive status changes."""
        self._render_bar()

    def watch_keepalive_last(self) -> None:
        """Re-render when last keep-alive timestamp changes."""
        self._render_bar()

    def watch_keepalive_failed(self) -> None:
        """Re-render when keep-alive failure state changes."""
        self._render_bar()

    def _render_bar(self) -> None:
        """Compose and update the footer text."""
        parts: list[str] = [_GLOBAL_HINTS]

        view_extra = _VIEW_BINDINGS.get(self.active_tab)
        if view_extra:
            parts.append(view_extra)

        if self.spark_connected:
            parts.append("[green]Spark: connected[/green]")
        elif self.sdk_only:
            parts.append("[yellow]Spark: SDK only[/yellow]")
        else:
            parts.append("[red]Spark: disconnected[/red]")

        # Keep-alive status
        if self.keepalive_failed:
            parts.append("[red]Keep-alive: FAILED[/red]")
        elif self.keepalive_active:
            if self.keepalive_last is not None:
                age = (datetime.now(timezone.utc) - self.keepalive_last).total_seconds()
                if age < 60:
                    age_str = f"{int(age)}s ago"
                else:
                    age_str = f"{int(age // 60)}m ago"
                parts.append(f"[green]Keep-alive: ON | last: {age_str}[/green]")
            else:
                parts.append("[green]Keep-alive: ON[/green]")

        self.update(" | ".join(parts))
