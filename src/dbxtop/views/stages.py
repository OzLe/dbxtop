"""Stages view.

Displays Spark stages with task progress, shuffle read/write,
input/output bytes, and per-stage timing breakdown.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import SparkStage, format_bytes, format_duration, format_timestamp
from dbxtop.views.base import BaseView
from dbxtop.widgets.progress_cell import render_progress

_COLUMNS = (
    "Stage ID",
    "Name",
    "Status",
    "Tasks",
    "Input",
    "Output",
    "Shuf Read",
    "Shuf Write",
    "Spill",
    "Duration",
    "Submitted",
)

_SORT_KEYS = (
    "stage_id",
    "name",
    "status_str",
    "num_complete_tasks",
    "input_bytes",
    "output_bytes",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "spill_bytes",
    "executor_run_time_ms",
    "submission_time",
)

_STATUS_COLOURS: dict[str, str] = {
    "ACTIVE": "green",
    "COMPLETE": "dim",
    "PENDING": "yellow",
    "FAILED": "red",
    "SKIPPED": "dim",
}


class StagesView(BaseView):
    """Spark stages DataTable with shuffle/spill metrics."""

    DEFAULT_CSS = """
    StagesView {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "stage_id"
        self.sort_reverse = True
        self._current_stages: List[SparkStage] = []

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="stages-placeholder")
        table: DataTable[str] = DataTable(id="stages-table")
        table.display = False
        table.add_columns(*_COLUMNS)
        yield table

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render stages table from cache."""
        slot = cache.get("stages")
        self.update_stale_status(slot, "stages-table")
        stages: Optional[List[SparkStage]] = slot.data
        if stages is None:
            return

        self._current_stages = stages

        try:
            self.query_one("#stages-placeholder").display = False
        except Exception:
            pass
        table = self.query_one("#stages-table", DataTable)
        table.display = True

        rows = self._build_rows(stages)

        if self.filter_text:
            rows = self.filter_rows(rows, self.filter_text, ["name", "status_str"])

        rows = self.sort_rows(
            rows,
            sort_key=self.current_sort_key,
            reverse=self.sort_reverse,
            pin_key="status_str",
            pin_values=["ACTIVE"],
        )

        table.clear()
        for row in rows:
            table.add_row(
                str(row["stage_id"]),
                row["name_display"],
                row["status_display"],
                row["tasks"],
                row["input_display"],
                row["output_display"],
                row["shuf_read"],
                row["shuf_write"],
                row["spill_display"],
                row["duration"],
                row["submitted"],
            )

    def get_selected_detail(self, cache: DataCache) -> Optional[str]:
        """Return detail text for the currently selected stage row."""
        try:
            table = self.query_one("#stages-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            stage_id = int(row_cells[0])
        except Exception:
            return None

        stage = next((s for s in self._current_stages if s.stage_id == stage_id), None)
        if stage is None:
            return None

        submitted = format_timestamp(stage.submission_time)
        completed = format_timestamp(stage.completion_time) if stage.completion_time else "running"

        # Escape brackets in data values to prevent Rich markup parsing errors
        name = stage.name.replace("[", "\\[")

        lines = [
            f"[bold]Stage {stage.stage_id} (attempt {stage.attempt_id})[/bold]\n",
            f"  Name:        {name}",
            f"  Status:      {stage.status.value}",
            f"  Submitted:   {submitted}",
            f"  Completed:   {completed}\n",
            "[bold]Tasks[/bold]",
            f"  Total:     {stage.num_tasks}",
            f"  Active:    {stage.num_active_tasks}",
            f"  Complete:  {stage.num_complete_tasks}",
            f"  Failed:    {stage.num_failed_tasks}",
            f"  Killed:    {stage.num_killed_tasks}",
        ]

        if stage.killed_tasks_summary:
            for reason, count in stage.killed_tasks_summary.items():
                reason_escaped = reason.replace("[", "\\[")
                lines.append(f"    {reason_escaped}: {count}")

        lines += [
            "",
            "[bold]I/O[/bold]",
            f"  Input:         {format_bytes(stage.input_bytes)} ({stage.input_records:,} records)",
            f"  Output:        {format_bytes(stage.output_bytes)} ({stage.output_records:,} records)",
            f"  Shuffle Read:  {format_bytes(stage.shuffle_read_bytes)}",
            f"  Shuffle Write: {format_bytes(stage.shuffle_write_bytes)}\n",
            "[bold]Resources[/bold]",
            f"  Executor Time:    {format_duration(stage.executor_run_time_ms)}",
            f"  CPU Time:         {format_duration(stage.executor_cpu_time_ns // 1_000_000)}",
            f"  JVM GC Time:      {format_duration(stage.jvm_gc_time_ms)}",
            f"  Peak Exec Memory: {format_bytes(stage.peak_execution_memory)}",
            f"  Memory Spill:     {format_bytes(stage.memory_spill_bytes)}",
            f"  Disk Spill:       {format_bytes(stage.disk_spill_bytes)}",
        ]

        if stage.failure_reason:
            reason_escaped = stage.failure_reason.replace("[", "\\[")
            # Truncate very long stack traces for the modal
            if len(reason_escaped) > 2000:
                reason_escaped = reason_escaped[:2000] + "\n  ... (truncated)"
            lines += ["", "[bold red]Failure Reason[/bold red]", f"  {reason_escaped}"]

        lines += ["", "[dim]Press Escape to close[/dim]"]
        return "\n".join(lines)

    def get_selected_stage(self) -> Optional[SparkStage]:
        """Return the SparkStage model for the currently selected row."""
        try:
            table = self.query_one("#stages-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            stage_id = int(row_cells[0])
        except Exception:
            return None
        return next((s for s in self._current_stages if s.stage_id == stage_id), None)

    def cycle_sort_column(self) -> None:
        """Advance to the next sort column."""
        self._sort_index = (self._sort_index + 1) % len(_SORT_KEYS)
        key = _SORT_KEYS[self._sort_index]
        self.cycle_sort(key)

    @staticmethod
    def _build_rows(stages: List[SparkStage]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for stage in stages:
            status_str = stage.status.value
            colour = _STATUS_COLOURS.get(status_str, "")
            status_display = f"[{colour}]{status_str}[/{colour}]" if colour else status_str

            if status_str == "FAILED":
                status_display = f"[red bold]{status_str}[/red bold]"

            tasks = render_progress(
                stage.num_complete_tasks,
                stage.num_tasks,
                stage.num_failed_tasks,
            )

            spill = stage.spill_bytes
            spill_display = format_bytes(spill)
            if spill > 0:
                spill_display = f"[yellow]{spill_display}[/yellow]"

            name_display = stage.name
            if len(name_display) > 40:
                name_display = name_display[:37] + "..."

            rows.append(
                {
                    "stage_id": stage.stage_id,
                    "name": stage.name,
                    "name_display": name_display,
                    "status_str": status_str,
                    "status_display": status_display,
                    "tasks": tasks,
                    "input_bytes": stage.input_bytes,
                    "input_display": format_bytes(stage.input_bytes),
                    "output_bytes": stage.output_bytes,
                    "output_display": format_bytes(stage.output_bytes),
                    "shuffle_read_bytes": stage.shuffle_read_bytes,
                    "shuf_read": format_bytes(stage.shuffle_read_bytes),
                    "shuffle_write_bytes": stage.shuffle_write_bytes,
                    "shuf_write": format_bytes(stage.shuffle_write_bytes),
                    "spill_bytes": spill,
                    "spill_display": spill_display,
                    "executor_run_time_ms": stage.executor_run_time_ms,
                    "duration": format_duration(stage.executor_run_time_ms),
                    "submission_time": stage.submission_time,
                    "submitted": format_timestamp(stage.submission_time),
                    "num_complete_tasks": stage.num_complete_tasks,
                }
            )
        return rows
