"""Storage/RDD view.

Displays cached RDDs and DataFrames with memory/disk usage,
partition counts, and storage level details.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import RDDInfo, format_bytes
from dbxtop.views.base import BaseView

_COLUMNS = ("RDD ID", "Name", "Partitions", "Storage Level", "Memory", "Disk", "% Cached")

_SORT_KEYS = (
    "rdd_id",
    "name",
    "num_cached_partitions",
    "storage_level",
    "memory_used",
    "disk_used",
    "fraction_cached",
)


class StorageView(BaseView):
    """Cached RDDs/DataFrames DataTable with summary bar."""

    DEFAULT_CSS = """
    StorageView {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._sort_index = 0
        self.current_sort_key = "memory_used"
        self.sort_reverse = True

    def compose(self) -> ComposeResult:
        yield Static("Waiting for Spark application...", classes="spark-unavailable", id="storage-placeholder")

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render storage table from cache."""
        slot = cache.get("storage")
        rdds: Optional[List[RDDInfo]] = slot.data
        if rdds is None:
            return

        try:
            self.query_one("#storage-placeholder").remove()
        except Exception:
            pass

        # Clear existing children
        for child in list(self.children):
            child.remove()

        if not rdds:
            self.mount(Static("No cached RDDs or DataFrames", classes="empty-state"))
            return

        rows = self._build_rows(rdds)

        if self.filter_text:
            rows = self.filter_rows(rows, self.filter_text, ["name", "storage_level"])

        rows = self.sort_rows(rows, sort_key=self.current_sort_key, reverse=self.sort_reverse)

        # Summary bar
        total_mem = sum(r.memory_used for r in rdds)
        total_disk = sum(r.disk_used for r in rdds)
        summary = (
            f"  Cached: {len(rdds)} RDDs  |  Memory: {format_bytes(total_mem)}  |  Disk: {format_bytes(total_disk)}"
        )
        self.mount(Static(summary))

        table: DataTable[str] = DataTable()
        table.add_columns(*_COLUMNS)
        for row in rows:
            table.add_row(
                str(row["rdd_id"]),
                row["name_display"],
                row["partitions"],
                row["storage_level_display"],
                row["memory_display"],
                row["disk_display"],
                row["cached_pct"],
            )

        self.mount(table)

    def cycle_sort_column(self) -> None:
        """Advance to the next sort column."""
        self._sort_index = (self._sort_index + 1) % len(_SORT_KEYS)
        self.cycle_sort(_SORT_KEYS[self._sort_index])

    @staticmethod
    def _build_rows(rdds: List[RDDInfo]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for rdd in rdds:
            name = rdd.name
            if len(name) > 40:
                name = name[:37] + "..."
            name_display = name.replace("[", "\\[")

            storage_level = rdd.storage_level
            if isinstance(storage_level, str):
                storage_level_display = storage_level.replace("[", "\\[")
            else:
                storage_level_display = str(storage_level)

            rows.append(
                {
                    "rdd_id": rdd.rdd_id,
                    "name": rdd.name,
                    "name_display": name_display,
                    "num_cached_partitions": rdd.num_cached_partitions,
                    "partitions": f"{rdd.num_cached_partitions}/{rdd.num_partitions}",
                    "storage_level": rdd.storage_level,
                    "storage_level_display": storage_level_display,
                    "memory_used": rdd.memory_used,
                    "memory_display": format_bytes(rdd.memory_used),
                    "disk_used": rdd.disk_used,
                    "disk_display": format_bytes(rdd.disk_used),
                    "fraction_cached": rdd.fraction_cached,
                    "cached_pct": f"{rdd.fraction_cached * 100:.1f}%",
                }
            )
        return rows
