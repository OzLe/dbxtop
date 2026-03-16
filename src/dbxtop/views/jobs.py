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
    "submission_time",
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

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="jobs-placeholder")

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render the jobs table from cache."""
        slot = cache.get("spark_jobs")
        jobs: Optional[List[SparkJob]] = slot.data
        if jobs is None:
            return

        # Remove placeholder
        try:
            self.query_one("#jobs-placeholder").remove()
        except Exception:
            pass

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

        # Remove existing table if present
        for child in list(self.children):
            if isinstance(child, DataTable):
                child.remove()

        # Build fresh table
        table: DataTable[str] = DataTable()
        table.add_columns(*_COLUMNS)
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

        self.mount(table)

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
                    "submission_time": job.submission_time,
                }
            )
        return rows
