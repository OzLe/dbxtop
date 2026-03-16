"""SQL Queries view.

Displays running and completed Spark SQL queries with duration,
row counts, and physical plan summaries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import SQLQuery, format_duration
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

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "execution_id"
        self.sort_reverse = True

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="sql-placeholder")

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render SQL queries table from cache."""
        slot = cache.get("sql_queries")
        queries: Optional[List[SQLQuery]] = slot.data
        if queries is None:
            return

        try:
            self.query_one("#sql-placeholder").remove()
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

        for child in list(self.children):
            if isinstance(child, DataTable):
                child.remove()

        if not rows:
            self.mount(Static("No SQL queries found", classes="empty-state"))
            return

        table: DataTable[str] = DataTable()
        table.add_columns(*_COLUMNS)
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

        self.mount(table)

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
