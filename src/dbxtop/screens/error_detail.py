"""Error detail screen.

Full-screen scrollable modal for viewing complete error messages
and stack traces from failed tasks, stages, or executors.
"""

from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import RichLog, Static


class ErrorDetailScreen(ModalScreen[None]):
    """Scrollable modal displaying a full error message or stack trace.

    Accepts any ``title`` + ``content`` pair so it can display stage
    failure reasons, executor remove reasons, or task error messages.
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
    ]

    DEFAULT_CSS = """
    ErrorDetailScreen {
        align: center middle;
    }
    ErrorDetailScreen > #error-detail-container {
        width: 90%;
        height: 85%;
        border: thick $accent;
        background: $surface;
    }
    ErrorDetailScreen > #error-detail-container > #error-detail-header {
        height: auto;
        padding: 1 2;
    }
    ErrorDetailScreen > #error-detail-container > RichLog {
        height: 1fr;
        padding: 0 2;
    }
    """

    def __init__(self, title: str, content: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._title = title
        self._content = content

    def compose(self) -> ComposeResult:
        with Static(id="error-detail-container"):
            yield Static(f"[bold]{self._title}[/bold]\n", id="error-detail-header")
            yield RichLog(id="error-detail-log", wrap=True, markup=False)

    def on_mount(self) -> None:
        """Write the error content into the log widget after mount."""
        log = self.query_one("#error-detail-log", RichLog)
        log.write(self._content)
