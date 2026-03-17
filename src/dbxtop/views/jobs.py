"""Spark Jobs view.

Displays active and recent Spark jobs with stage progress,
duration, and task completion metrics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import SparkJob, format_duration, format_timestamp
from dbxtop.views.base import BaseView
from dbxtop.widgets.progress_cell import render_progress

# Columns in display order
_COLUMNS = ("Job ID", "Description", "Status", "Progress", "Stages", "Duration", "Submitted")

# Sortable keys mapped to SparkJob attribute names
_SORT_KEYS = (
    "job_id",
    "name",
    "status",
    "num_completed_tasks",
    "num_completed_stages",
    "elapsed_ms",
    "submission_time",
)

_STATUS_COLOURS: dict[str, str] = {
    "RUNNING": "green",
    "SUCCEEDED": "dim",
    "FAILED": "red",
    "UNKNOWN": "yellow",
}


class JobsView(BaseView):
    """Spark jobs DataTable with progress bars, sorting, and filtering."""

    DEFAULT_CSS = """
    JobsView {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "job_id"
        self.sort_reverse = True  # newest first
        self._current_jobs: List[SparkJob] = []

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="jobs-placeholder")
        table: DataTable[str] = DataTable(id="jobs-table")
        table.display = False
        table.add_columns(*_COLUMNS)
        yield table

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render the jobs table from cache."""
        slot = cache.get("spark_jobs")
        jobs: Optional[List[SparkJob]] = slot.data
        if jobs is None:
            return

        self._current_jobs = jobs

        # Hide placeholder, show table
        try:
            self.query_one("#jobs-placeholder").display = False
        except Exception:
            pass
        table = self.query_one("#jobs-table", DataTable)
        table.display = True

        # Build row dicts
        rows = self._build_rows(jobs)

        # Apply filter
        if self.filter_text:
            rows = self.filter_rows(rows, self.filter_text, ["name", "status_str"])

        # Sort: pin running jobs to top
        rows = self.sort_rows(
            rows,
            sort_key=self.current_sort_key,
            reverse=self.sort_reverse,
            pin_key="status_str",
            pin_values=["RUNNING"],
        )

        # In-place update: clear and re-add rows
        table.clear()
        for row in rows:
            table.add_row(
                str(row["job_id"]),
                row["name_display"],
                row["status_display"],
                row["progress"],
                row["stages"],
                row["duration"],
                row["submitted"],
            )

    def get_selected_detail(self, cache: DataCache) -> Optional[str]:
        """Return detail text for the currently selected job row."""
        try:
            table = self.query_one("#jobs-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            job_id = int(row_cells[0])
        except Exception:
            return None

        job = next((j for j in self._current_jobs if j.job_id == job_id), None)
        if job is None:
            return None

        submitted = format_timestamp(job.submission_time)
        completed = format_timestamp(job.completion_time) if job.completion_time else "running"
        duration = "--"
        if job.submission_time:
            elapsed_ms = int((datetime.now(timezone.utc) - job.submission_time).total_seconds() * 1000)
            duration = format_duration(elapsed_ms)

        return (
            f"[bold]Job {job.job_id}[/bold]\n\n"
            f"  Description:  {job.name}\n"
            f"  Status:       {job.status.value}\n"
            f"  Submitted:    {submitted}\n"
            f"  Completed:    {completed}\n"
            f"  Duration:     {duration}\n\n"
            f"[bold]Tasks[/bold]\n"
            f"  Total:     {job.num_tasks}\n"
            f"  Active:    {job.num_active_tasks}\n"
            f"  Completed: {job.num_completed_tasks}\n"
            f"  Failed:    {job.num_failed_tasks}\n\n"
            f"[bold]Stages[/bold]\n"
            f"  Total:     {job.num_stages}\n"
            f"  Active:    {job.num_active_stages}\n"
            f"  Completed: {job.num_completed_stages}\n"
            f"  Failed:    {job.num_failed_stages}\n"
            f"  IDs:       {job.stage_ids}\n\n"
            f"[dim]Press Escape to close[/dim]"
        )

    def cycle_sort_column(self) -> None:
        """Advance to the next sort column."""
        self._sort_index = (self._sort_index + 1) % len(_SORT_KEYS)
        key = _SORT_KEYS[self._sort_index]
        self.cycle_sort(key)

    # -- row building --------------------------------------------------------

    @staticmethod
    def _build_rows(jobs: List[SparkJob]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        for job in jobs:
            status_str = job.status.value
            colour = _STATUS_COLOURS.get(status_str, "")
            status_display = f"[{colour}]{status_str}[/{colour}]" if colour else status_str

            progress = render_progress(
                job.num_completed_tasks,
                job.num_tasks,
                job.num_failed_tasks,
            )

            completed_stages = job.num_completed_stages + job.num_active_stages
            stages = f"{completed_stages}/{job.num_stages}"

            if job.submission_time:
                elapsed_ms = int((now - job.submission_time).total_seconds() * 1000)
                duration = format_duration(elapsed_ms)
            else:
                elapsed_ms = 0
                duration = "--"

            submitted = format_timestamp(job.submission_time)

            name_display = job.name
            if len(name_display) > 50:
                name_display = name_display[:47] + "..."

            rows.append(
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "name_display": name_display,
                    "status_str": status_str,
                    "status_display": status_display,
                    "progress": progress,
                    "stages": stages,
                    "duration": duration,
                    "submitted": submitted,
                    "num_completed_tasks": job.num_completed_tasks,
                    "num_completed_stages": job.num_completed_stages,
                    "elapsed_ms": elapsed_ms,
                    "submission_time": job.submission_time,
                }
            )
        return rows
