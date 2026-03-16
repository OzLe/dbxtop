"""Keyboard shortcuts footer widget.

Displays available key bindings and current refresh status
in a compact bottom-of-screen bar.
"""

from __future__ import annotations

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

        self.update(" | ".join(parts))
