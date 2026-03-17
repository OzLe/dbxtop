"""Executors view.

Displays executor resource utilization including memory, disk,
active tasks, GC time, and shuffle data per executor.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import ExecutorInfo, format_bytes, format_duration
from dbxtop.views.base import BaseView
from dbxtop.widgets.spark_line import render_sparkline

_COLUMNS = (
    "ID",
    "Host",
    "Status",
    "Cores",
    "Active",
    "Done",
    "Failed",
    "Memory",
    "Mem▁▃▅",
    "GC%",
    "GC▁▃▅",
    "Disk",
    "Shuf Read",
    "Shuf Write",
    "Duration",
    "Uptime",
)

_SORT_KEYS = (
    "executor_id",
    "host_port",
    "is_active",
    "total_cores",
    "active_tasks",
    "completed_tasks",
    "failed_tasks",
    "memory_used",
    "gc_ratio",
    "disk_used",
    "total_shuffle_read",
    "total_shuffle_write",
    "total_duration_ms",
    "add_time",
)


class ExecutorsView(BaseView):
    """Executor metrics DataTable with memory bars and GC highlighting."""

    DEFAULT_CSS = """
    ExecutorsView {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "executor_id"
        self.sort_reverse = False

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="exec-placeholder")
        table: DataTable[str] = DataTable(id="exec-table")
        table.display = False
        table.add_columns(*_COLUMNS)
        yield table

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render executors table from cache."""
        slot = cache.get("executors")
        self.update_stale_status(slot, "executors-table")
        executors: Optional[List[ExecutorInfo]] = slot.data
        if executors is None:
            return

        try:
            self.query_one("#exec-placeholder").display = False
        except Exception:
            pass
        table = self.query_one("#exec-table", DataTable)
        table.display = True

        # Build per-executor sparkline history from cache ring buffer
        mem_history: Dict[str, list[float]] = {}
        gc_history: Dict[str, list[float]] = {}
        for snapshot in cache.get_history("executors"):
            if isinstance(snapshot, list):
                for e in snapshot:
                    mem_history.setdefault(e.executor_id, []).append(e.memory_used_pct)
                    gc_history.setdefault(e.executor_id, []).append(e.gc_ratio * 100)

        rows = self._build_rows(executors)

        if self.filter_text:
            rows = self.filter_rows(rows, self.filter_text, ["executor_id", "host_port", "status_str"])

        # Pin driver to top, dead executors to bottom
        driver = [r for r in rows if r["is_driver"]]
        active = [r for r in rows if not r["is_driver"] and r["is_active"]]
        dead = [r for r in rows if not r["is_driver"] and not r["is_active"]]

        def _sort_key(r: dict[str, Any]) -> tuple[int, Any]:
            val = r.get(self.current_sort_key)
            if val is None:
                return (0, "")  # tuple ensures consistent comparison
            return (1, val)

        active.sort(key=_sort_key, reverse=self.sort_reverse)
        dead.sort(key=_sort_key, reverse=self.sort_reverse)
        sorted_rows = driver + active + dead

        table.clear()
        for row in sorted_rows:
            eid = row["executor_id"]
            mem_spark = render_sparkline(mem_history.get(eid, []), width=8, min_val=0, max_val=100)
            gc_spark = render_sparkline(gc_history.get(eid, []), width=8, min_val=0)
            table.add_row(
                row["id_display"],
                row["host_display"],
                row["status_display"],
                str(row["total_cores"]),
                row["active_display"],
                str(row["completed_tasks"]),
                row["failed_display"],
                row["memory_display"],
                mem_spark,
                row["gc_display"],
                gc_spark,
                row["disk_display"],
                row["shuf_read"],
                row["shuf_write"],
                row["duration"],
                row["uptime"],
            )

        # Summary row
        if executors:
            total_cores = sum(e.total_cores for e in executors)
            total_active = sum(e.active_tasks for e in executors)
            total_mem_used = sum(e.memory_used for e in executors)
            total_mem_max = sum(e.max_memory for e in executors)
            avg_gc = sum(e.gc_ratio for e in executors) / len(executors) * 100

            mem_summary = f"{format_bytes(total_mem_used)}/{format_bytes(total_mem_max)}"
            table.add_row(
                "[bold]TOTAL[/bold]",
                "",
                f"{len([e for e in executors if e.is_active])} active",
                str(total_cores),
                str(total_active),
                str(sum(e.completed_tasks for e in executors)),
                str(sum(e.failed_tasks for e in executors)),
                mem_summary,
                "",
                f"{avg_gc:.1f}%",
                "",
                "",
                "",
                "",
                "",
                "",
            )

    def cycle_sort_column(self) -> None:
        """Advance to the next sort column."""
        self._sort_index = (self._sort_index + 1) % len(_SORT_KEYS)
        self.cycle_sort(_SORT_KEYS[self._sort_index])

    @staticmethod
    def _build_rows(executors: List[ExecutorInfo]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        for exe in executors:
            # ID display
            id_display = "[bold]driver[/bold]" if exe.is_driver else exe.executor_id

            # Status
            if exe.is_active:
                status_display = "[green]Active[/green]"
                status_str = "Active"
            else:
                status_display = "[red]Dead[/red]"
                status_str = "Dead"

            # Active tasks
            active_display = f"[bold]{exe.active_tasks}[/bold]" if exe.active_tasks > 0 else "0"

            # Failed tasks
            failed_display = f"[red]{exe.failed_tasks}[/red]" if exe.failed_tasks > 0 else "0"

            # Memory bar
            if exe.max_memory > 0:
                used_gb = exe.memory_used / (1024**3)
                max_gb = exe.max_memory / (1024**3)
                pct = exe.memory_used_pct
                bar_width = 10
                filled = int(pct / 100 * bar_width)
                bar = "=" * filled + " " * (bar_width - filled)
                memory_display = f"\\[{bar}] {used_gb:.1f}/{max_gb:.1f}G"
            else:
                memory_display = "--"

            # GC%
            gc_pct = exe.gc_ratio * 100
            if gc_pct > 10:
                gc_display = f"[red bold]{gc_pct:.1f}%[/red bold]"
            elif gc_pct > 5:
                gc_display = f"[yellow bold]{gc_pct:.1f}%[/yellow bold]"
            else:
                gc_display = f"{gc_pct:.1f}%"

            # Uptime
            if exe.add_time:
                uptime_ms = int((now - exe.add_time).total_seconds() * 1000)
                uptime = format_duration(uptime_ms)
            else:
                uptime = "--"

            # Host truncation
            host_display = exe.host_port
            if len(host_display) > 20:
                host_display = host_display[:17] + "..."

            rows.append(
                {
                    "executor_id": exe.executor_id,
                    "is_driver": exe.is_driver,
                    "is_active": exe.is_active,
                    "id_display": id_display,
                    "host_port": exe.host_port,
                    "host_display": host_display,
                    "status_str": status_str,
                    "status_display": status_display,
                    "total_cores": exe.total_cores,
                    "active_tasks": exe.active_tasks,
                    "active_display": active_display,
                    "completed_tasks": exe.completed_tasks,
                    "failed_tasks": exe.failed_tasks,
                    "failed_display": failed_display,
                    "memory_used": exe.memory_used,
                    "memory_display": memory_display,
                    "gc_ratio": exe.gc_ratio,
                    "gc_display": gc_display,
                    "disk_used": exe.disk_used,
                    "disk_display": format_bytes(exe.disk_used),
                    "total_shuffle_read": exe.total_shuffle_read,
                    "shuf_read": format_bytes(exe.total_shuffle_read),
                    "total_shuffle_write": exe.total_shuffle_write,
                    "shuf_write": format_bytes(exe.total_shuffle_write),
                    "total_duration_ms": exe.total_duration_ms,
                    "duration": format_duration(exe.total_duration_ms),
                    "add_time": exe.add_time,
                    "uptime": uptime,
                }
            )
        return rows
