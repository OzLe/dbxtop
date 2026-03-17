"""Run list screen for browsing and selecting saved runs.

Displays saved monitoring runs for the current cluster with selection
support for side-by-side comparison.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Set

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static
from textual.coordinate import Coordinate

from dbxtop.analytics.run import RunSession

logger = logging.getLogger(__name__)


class RunListScreen(ModalScreen[Optional[List[str]]]):
    """Modal screen showing saved runs for the current cluster.

    Returns a list of selected run_ids when dismissed, or None if cancelled.
    Selecting exactly 2 runs and pressing 'c' returns them for comparison.
    """

    BINDINGS = [
        Binding("escape", "dismiss_cancel", "Close"),
        Binding("c", "compare", "Compare"),
        Binding("d", "delete_run", "Delete"),
        Binding("space", "toggle_select", "Select"),
    ]

    DEFAULT_CSS = """
    RunListScreen {
        align: center middle;
    }
    RunListScreen > #run-list-container {
        width: 80;
        height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    RunListScreen > #run-list-container > #run-list-title {
        text-style: bold;
        margin-bottom: 1;
    }
    RunListScreen > #run-list-container > #run-list-status {
        margin-top: 1;
        color: $text-muted;
    }
    RunListScreen > #run-list-container > DataTable {
        height: 1fr;
    }
    """

    def __init__(self, runs: List[RunSession], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._runs = runs
        self._selected_ids: Set[str] = set()

    def compose(self) -> ComposeResult:
        """Compose the run list screen layout."""
        with Static(id="run-list-container"):
            yield Static("Saved Runs", id="run-list-title")
            table: DataTable[str] = DataTable(id="run-list-table")
            table.cursor_type = "row"
            yield table
            yield Static("Space: select | c: compare (2) | d: delete | Esc: close", id="run-list-status")

    def on_mount(self) -> None:
        """Populate the table with run data."""
        table = self.query_one("#run-list-table", DataTable)
        table.add_columns("", "Name", "Date", "Duration", "Avg Score", "Insights")

        if not self._runs:
            # Show empty state
            self.query_one("#run-list-title", Static).update("No saved runs for this cluster")
            return

        for run in self._runs:
            # Format fields
            selected = "●" if run.run_id in self._selected_ids else " "
            date_str = run.started_at.strftime("%Y-%m-%d %H:%M")

            # Duration
            dur = run.duration_seconds
            if dur < 60:
                dur_str = f"{int(dur)}s"
            elif dur < 3600:
                dur_str = f"{int(dur / 60)}m {int(dur % 60)}s"
            else:
                dur_str = f"{int(dur / 3600)}h {int((dur % 3600) / 60)}m"

            avg_score = f"{run.avg_health_score:.0f}" if run.health_history else "--"
            insight_count = str(len(run.accumulated_insights))

            table.add_row(selected, run.name, date_str, dur_str, avg_score, insight_count, key=run.run_id)

    def _refresh_selection_column(self) -> None:
        """Update the selection indicator column."""
        table = self.query_one("#run-list-table", DataTable)
        for row_idx, run in enumerate(self._runs):
            selected = "●" if run.run_id in self._selected_ids else " "
            table.update_cell_at(Coordinate(row_idx, 0), selected)

    def action_toggle_select(self) -> None:
        """Toggle selection of the current row."""
        table = self.query_one("#run-list-table", DataTable)
        if table.cursor_row is None or not self._runs:
            return
        row_idx = table.cursor_row
        if row_idx >= len(self._runs):
            return
        run_id = self._runs[row_idx].run_id
        if run_id in self._selected_ids:
            self._selected_ids.discard(run_id)
        else:
            self._selected_ids.add(run_id)
        self._refresh_selection_column()
        count = len(self._selected_ids)
        self.query_one("#run-list-status", Static).update(
            f"{count} selected | Space: select | c: compare (need 2) | d: delete | Esc: close"
        )

    def action_compare(self) -> None:
        """Compare two selected runs."""
        if len(self._selected_ids) != 2:
            self.notify("Select exactly 2 runs to compare", severity="warning")
            return
        self.dismiss(list(self._selected_ids))

    def action_delete_run(self) -> None:
        """Delete the currently highlighted run."""
        table = self.query_one("#run-list-table", DataTable)
        if table.cursor_row is None or not self._runs:
            return
        row_idx = table.cursor_row
        if row_idx >= len(self._runs):
            return
        run = self._runs[row_idx]
        # Import here to avoid circular dependency
        from dbxtop.analytics.run_manager import RunManager

        if RunManager.delete_run(run.cluster_id, run.run_id):
            self._selected_ids.discard(run.run_id)
            self._runs.pop(row_idx)
            table.remove_row(run.run_id)
            self.notify(f"Deleted run '{run.name}'", severity="information")

    def action_dismiss_cancel(self) -> None:
        """Cancel and close without selection."""
        self.dismiss(None)
