"""SQL Queries view.

Displays running and completed Spark SQL queries with duration,
row counts, and physical plan summaries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import SQLQuery, format_duration, format_timestamp
from dbxtop.views.base import BaseView

_COLUMNS = ("ID", "Description", "Status", "Duration", "Running", "Success", "Failed")

_SORT_KEYS = (
    "execution_id",
    "description",
    "status",
    "duration_ms",
    "running_jobs",
    "success_jobs",
    "failed_jobs",
)

_STATUS_COLOURS: dict[str, str] = {
    "RUNNING": "green",
    "COMPLETED": "dim",
    "FAILED": "red",
}


class SQLView(BaseView):
    """SQL queries DataTable."""

    DEFAULT_CSS = """
    SQLView {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "execution_id"
        self.sort_reverse = True
        self._current_queries: List[SQLQuery] = []

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="sql-placeholder")
        yield Static("No SQL queries found", classes="empty-state", id="sql-empty")
        table: DataTable[str] = DataTable(id="sql-table")
        table.display = False
        table.add_columns(*_COLUMNS)
        yield table

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render SQL queries table from cache."""
        slot = cache.get("sql_queries")
        self.update_stale_status(slot, "sql-table")
        queries: Optional[List[SQLQuery]] = slot.data
        if queries is None:
            return

        self._current_queries = queries

        try:
            self.query_one("#sql-placeholder").display = False
        except Exception:
            pass

        rows = self._build_rows(queries)

        if self.filter_text:
            rows = self.filter_rows(rows, self.filter_text, ["description", "status"])

        rows = self.sort_rows(
            rows,
            sort_key=self.current_sort_key,
            reverse=self.sort_reverse,
            pin_key="status",
            pin_values=["RUNNING"],
        )

        table = self.query_one("#sql-table", DataTable)
        empty_msg = self.query_one("#sql-empty", Static)

        if not rows:
            table.display = False
            empty_msg.display = True
            return

        empty_msg.display = False
        table.display = True
        table.clear()
        for row in rows:
            table.add_row(
                str(row["execution_id"]),
                row["desc_display"],
                row["status_display"],
                row["duration_display"],
                row["running_display"],
                str(row["success_jobs"]),
                row["failed_display"],
            )

    def get_selected_detail(self, cache: DataCache) -> Optional[str]:
        """Return detail text for the currently selected SQL query row."""
        try:
            table = self.query_one("#sql-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            exec_id = int(row_cells[0])
        except Exception:
            return None

        query = next((q for q in self._current_queries if q.execution_id == exec_id), None)
        if query is None:
            return None

        submitted = format_timestamp(query.submission_time)
        duration = format_duration(query.duration_ms) if query.duration_ms else "running"

        # Escape brackets in data values to prevent Rich markup parsing errors
        description = query.description.replace("[", "\\[")

        return (
            f"[bold]SQL Query {query.execution_id}[/bold]\n\n"
            f"  Description:  {description}\n"
            f"  Status:       {query.status}\n"
            f"  Submitted:    {submitted}\n"
            f"  Duration:     {duration}\n\n"
            f"[bold]Jobs[/bold]\n"
            f"  Running:   {query.running_jobs}\n"
            f"  Success:   {query.success_jobs}\n"
            f"  Failed:    {query.failed_jobs}\n\n"
            f"[dim]Press Escape to close[/dim]"
        )

    def get_selected_query(self) -> Optional[SQLQuery]:
        """Return the SQLQuery model for the currently selected row."""
        try:
            table = self.query_one("#sql-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            exec_id = int(row_cells[0])
        except Exception:
            return None
        return next((q for q in self._current_queries if q.execution_id == exec_id), None)

    def cycle_sort_column(self) -> None:
        """Advance to the next sort column."""
        self._sort_index = (self._sort_index + 1) % len(_SORT_KEYS)
        self.cycle_sort(_SORT_KEYS[self._sort_index])

    @staticmethod
    def _build_rows(queries: List[SQLQuery]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for q in queries:
            status = q.status.upper() if q.status else "UNKNOWN"
            colour = _STATUS_COLOURS.get(status, "")
            status_display = f"[{colour}]{status}[/{colour}]" if colour else status

            desc = q.description
            if len(desc) > 60:
                desc = desc[:57] + "..."
            # Escape Rich markup in SQL descriptions
            desc_display = desc.replace("[", "\\[")

            if status == "RUNNING":
                duration_display = "[green]running...[/green]"
            else:
                duration_display = format_duration(q.duration_ms) if q.duration_ms else "--"

            running_display = f"[bold]{q.running_jobs}[/bold]" if q.running_jobs > 0 else "0"
            failed_display = f"[red]{q.failed_jobs}[/red]" if q.failed_jobs > 0 else "0"

            rows.append(
                {
                    "execution_id": q.execution_id,
                    "description": q.description,
                    "desc_display": desc_display,
                    "status": status,
                    "status_display": status_display,
                    "duration_ms": q.duration_ms,
                    "duration_display": duration_display,
                    "running_jobs": q.running_jobs,
                    "running_display": running_display,
                    "success_jobs": q.success_jobs,
                    "failed_jobs": q.failed_jobs,
                    "failed_display": failed_display,
                }
            )
        return rows
