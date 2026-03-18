"""Keyboard shortcuts footer widget.

Displays available key bindings and current refresh status
in a compact bottom-of-screen bar.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from textual.reactive import reactive
from textual.widgets import Static


# Per-tab extra bindings (tab_id → hint string)
_VIEW_BINDINGS: dict[str, str] = {
    "jobs": "s:Sort | Enter:Detail | e:Errors | f:Failures | g:GoTo",
    "stages": "s:Sort | Enter:Detail | e:Errors | f:Failures | t:Tasks",
    "executors": "s:Sort | Enter:Detail | e:Errors",
    "sql": "s:Sort | Enter:Detail | g:GoTo",
    "storage": "s:Sort",
}

_GLOBAL_HINTS = "1-7:Views | r:Refresh | /:Filter | ^R:Record | ^L:Runs | ?:Help | q:Quit"


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
    error_failed_tasks: reactive[int] = reactive(0)
    error_failed_stages: reactive[int] = reactive(0)

    def __init__(self, **kwargs: Any) -> None:
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

    def watch_error_failed_tasks(self) -> None:
        """Re-render when error counts change."""
        self._render_bar()

    def watch_error_failed_stages(self) -> None:
        """Re-render when error counts change."""
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

        # Error summary
        if self.error_failed_tasks > 0 or self.error_failed_stages > 0:
            error_parts: list[str] = []
            if self.error_failed_tasks > 0:
                error_parts.append(f"{self.error_failed_tasks} failed tasks")
            if self.error_failed_stages > 0:
                error_parts.append(f"{self.error_failed_stages} failed stages")
            parts.append(f"[red]Errors: {', '.join(error_parts)}[/red]")

        self.update(" | ".join(parts))
