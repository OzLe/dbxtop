"""Task list screen.

Modal screen showing failed tasks for a given stage, with
drill-down to full error details via ErrorDetailScreen.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static

from dbxtop.api.models import SparkTask, format_bytes, format_duration
from dbxtop.api.spark_api import SparkRESTClient
from dbxtop.screens.error_detail import ErrorDetailScreen

logger = logging.getLogger(__name__)

_COLUMNS = ("Task ID", "Attempt", "Executor", "Host", "Duration", "Error", "Spill", "GC%")


class TaskListScreen(ModalScreen[None]):
    """Modal showing failed tasks for a specific stage.

    On mount, fetches the task list from the Spark REST API.
    Enter on a task row opens the full error in an ErrorDetailScreen.
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("enter", "show_error", "View Error"),
    ]

    DEFAULT_CSS = """
    TaskListScreen {
        align: center middle;
    }
    TaskListScreen > #task-list-container {
        width: 90%;
        height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    TaskListScreen > #task-list-container > #task-list-header {
        height: auto;
    }
    TaskListScreen > #task-list-container > DataTable {
        height: 1fr;
    }
    """

    def __init__(
        self,
        spark_client: SparkRESTClient,
        stage_id: int,
        stage_name: str,
        attempt_id: int = 0,
        num_failed_tasks: int = 0,
        num_tasks: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._spark_client = spark_client
        self._stage_id = stage_id
        self._stage_name = stage_name
        self._attempt_id = attempt_id
        self._num_failed_tasks = num_failed_tasks
        self._num_tasks = num_tasks
        self._tasks: List[SparkTask] = []

    def compose(self) -> ComposeResult:
        name_display = self._stage_name
        if len(name_display) > 60:
            name_display = name_display[:57] + "..."
        header_text = (
            f"[bold]Stage {self._stage_id}[/bold] (attempt {self._attempt_id}) — "
            f"{self._num_failed_tasks} failed / {self._num_tasks} total tasks\n"
            f"  {name_display}\n"
        )
        with Static(id="task-list-container"):
            yield Static(header_text, id="task-list-header")
            table: DataTable[str] = DataTable(id="task-list-table")
            table.add_columns(*_COLUMNS)
            yield table

    async def on_mount(self) -> None:
        """Fetch failed tasks from the Spark REST API."""
        table = self.query_one("#task-list-table", DataTable)
        try:
            self._tasks = await self._spark_client.get_stage_tasks(self._stage_id, self._attempt_id, status="failed")
        except Exception:
            logger.debug("Failed to fetch tasks for stage %d", self._stage_id, exc_info=True)
            self._tasks = []

        if not self._tasks:
            table.add_row("--", "--", "--", "--", "--", "No failed tasks found", "--", "--")
            return

        # Sort by attempt descending (most recent failures first)
        self._tasks.sort(key=lambda t: t.attempt, reverse=True)

        for task in self._tasks:
            error_display = task.error_message or ""
            if len(error_display) > 50:
                error_display = error_display[:47] + "..."
            # Escape Rich markup in error text
            error_display = error_display.replace("[", "\\[").replace("\n", " ")

            spill = task.metrics.memory_bytes_spilled + task.metrics.disk_bytes_spilled
            spill_display = format_bytes(spill) if spill > 0 else "--"
            if spill > 0:
                spill_display = f"[yellow]{spill_display}[/yellow]"

            gc_pct = task.metrics.gc_ratio * 100
            if gc_pct > 10:
                gc_display = f"[red]{gc_pct:.1f}%[/red]"
            elif gc_pct > 5:
                gc_display = f"[yellow]{gc_pct:.1f}%[/yellow]"
            else:
                gc_display = f"{gc_pct:.1f}%"

            table.add_row(
                str(task.task_id),
                str(task.attempt),
                task.executor_id,
                task.host[:20] if len(task.host) > 20 else task.host,
                format_duration(task.duration_ms),
                error_display,
                spill_display,
                gc_display,
            )

    def _get_selected_task(self) -> Optional[SparkTask]:
        """Return the SparkTask for the currently selected row."""
        try:
            table = self.query_one("#task-list-table", DataTable)
            if table.cursor_row is None or table.cursor_row < 0:
                return None
            row_cells = table.get_row_at(table.cursor_row)
            task_id_str = str(row_cells[0]).strip()
            if task_id_str == "--":
                return None
            task_id = int(task_id_str)
            return next((t for t in self._tasks if t.task_id == task_id), None)
        except Exception:
            return None

    def action_show_error(self) -> None:
        """Open full error detail for the selected task."""
        task = self._get_selected_task()
        if task is None or not task.error_message:
            return
        title = f"Task {task.task_id} (attempt {task.attempt}) — {task.executor_id} @ {task.host}"
        self.app.push_screen(ErrorDetailScreen(title=title, content=task.error_message))
