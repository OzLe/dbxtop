"""Cluster Overview view.

Displays cluster state, node counts, uptime, Spark version,
driver/worker specs, and recent cluster events.
"""

from __future__ import annotations

from typing import List, Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Static

from dbxtop.api.cache import DataCache
from dbxtop.api.models import (
    ClusterEvent,
    ClusterInfo,
    LibraryInfo,
    format_duration,
    format_timestamp,
)
from dbxtop.views.base import BaseView

# -- state colour helpers ---------------------------------------------------

_STATE_COLOURS: dict[str, str] = {
    "RUNNING": "green",
    "PENDING": "yellow",
    "RESTARTING": "yellow",
    "RESIZING": "yellow",
    "TERMINATING": "red",
    "TERMINATED": "red",
    "ERROR": "red",
    "UNKNOWN": "dim",
}

_EVENT_COLOURS: dict[str, str] = {
    "TERMINATING": "red",
    "TERMINATED": "red",
    "ERROR": "red",
    "UPSIZE_COMPLETED": "yellow",
    "DOWNSCALED": "yellow",
    "AUTOSCALING_STATS_REPORT": "yellow",
    "RUNNING": "green",
    "CREATING": "green",
    "STARTING": "green",
    "RESTARTING": "yellow",
    "EDITED": "dim",
    "INIT_SCRIPTS_FINISHED": "dim",
}

_LIB_STATUS_COLOURS: dict[str, str] = {
    "INSTALLED": "green",
    "PENDING": "yellow",
    "RESOLVING": "yellow",
    "INSTALLING": "yellow",
    "FAILED": "red",
    "UNINSTALL_ON_RESTART": "dim",
}


class ClusterView(BaseView):
    """Multi-section Cluster overview tab.

    Sections:
    - Identity card (name, ID, Spark version, etc.)
    - Resource gauges (cores, memory, workers)
    - Spark configuration key/values
    - State card
    - Events sub-table (last 20)
    - Libraries sub-table (if present)
    """

    DEFAULT_CSS = """
    ClusterView {
        height: 1fr;
        overflow-y: auto;
    }

    .cluster-identity {
        height: auto;
        padding: 0 1;
        margin-bottom: 1;
    }

    .cluster-section-title {
        text-style: bold underline;
        margin-top: 1;
        padding: 0 1;
    }

    .cluster-resources {
        height: auto;
        padding: 0 1;
    }

    .cluster-sparkconf {
        height: auto;
        padding: 0 1;
        max-height: 10;
    }

    .cluster-state-card {
        height: auto;
        padding: 0 1;
    }

    .events-table {
        height: 12;
        margin-top: 1;
    }

    .libraries-table {
        height: 8;
        margin-top: 1;
    }
    """

    def compose(self) -> ComposeResult:
        """Build the initial placeholder layout."""
        yield Static("Connecting to cluster...", classes="empty-state", id="cluster-placeholder")

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render all sections from cache data."""
        cluster_slot = cache.get("cluster")
        events_slot = cache.get("events")
        libs_slot = cache.get("libraries")

        info: Optional[ClusterInfo] = cluster_slot.data
        if info is None:
            return

        # Remove placeholder if still present
        try:
            self.query_one("#cluster-placeholder").remove()
        except Exception:
            pass

        # Re-mount everything (simple full-refresh strategy)
        self._clear_children()

        # Identity card
        self.mount(self._build_identity(info))

        # Resource gauges
        self.mount(Static("Resources", classes="cluster-section-title"))
        self.mount(self._build_resources(info))

        # Spark configuration
        if info.spark_conf:
            self.mount(Static("Spark Configuration", classes="cluster-section-title"))
            self.mount(self._build_spark_conf(info))

        # State card
        self.mount(self._build_state_card(info))

        # Events table
        events: Optional[List[ClusterEvent]] = events_slot.data
        if events:
            self.mount(Static("Events", classes="cluster-section-title"))
            self.mount(self._build_events_table(events))

        # Libraries table
        libs: Optional[List[LibraryInfo]] = libs_slot.data
        if libs:
            self.mount(Static("Libraries", classes="cluster-section-title"))
            self.mount(self._build_libraries_table(libs))

    def _clear_children(self) -> None:
        """Remove all child widgets for a full refresh."""
        for child in list(self.children):
            child.remove()

    # -- identity card -------------------------------------------------------

    @staticmethod
    def _build_identity(info: ClusterInfo) -> Static:
        lines = [
            f"[bold]{info.cluster_name}[/bold]  ({info.cluster_id})",
            f"  Spark: {info.spark_version or '--'}  |  "
            f"Runtime: {info.runtime_engine or 'Standard'}  |  "
            f"Security: {info.data_security_mode or '--'}",
            f"  Creator: {info.creator or '--'}  |  Auto-terminate: {info.autotermination_minutes}m"
            if info.autotermination_minutes
            else f"  Creator: {info.creator or '--'}  |  Auto-terminate: disabled",
        ]
        return Static("\n".join(lines), classes="cluster-identity")

    # -- resource gauges -----------------------------------------------------

    @staticmethod
    def _build_resources(info: ClusterInfo) -> Static:
        mem_gb = info.total_memory_mb / 1024 if info.total_memory_mb else 0
        workers_str = str(info.num_workers)
        if info.autoscale_min is not None and info.autoscale_max is not None:
            workers_str = f"{info.num_workers} \\[{info.autoscale_min}-{info.autoscale_max}]"

        uptime_str = format_duration(info.uptime_seconds * 1000) if info.uptime_seconds else "--"

        lines = [
            f"  Cores: {info.total_cores}  |  Memory: {mem_gb:.1f} GB  |  Workers: {workers_str}",
            f"  Driver: {info.driver_node_type or '--'}  |  Worker: {info.worker_node_type or '--'}",
            f"  Uptime: {uptime_str}",
        ]
        return Static("\n".join(lines), classes="cluster-resources")

    # -- spark conf ----------------------------------------------------------

    @staticmethod
    def _build_spark_conf(info: ClusterInfo) -> Static:
        lines: list[str] = []
        for key, value in sorted(info.spark_conf.items()):
            display_val = value if len(value) <= 60 else value[:57] + "..."
            # Escape Rich markup in user-provided values
            safe_key = key.replace("[", "\\[")
            safe_val = display_val.replace("[", "\\[")
            lines.append(f"  {safe_key} = {safe_val}")
        return Static("\n".join(lines) or "  (none)", classes="cluster-sparkconf")

    # -- state card ----------------------------------------------------------

    @staticmethod
    def _build_state_card(info: ClusterInfo) -> Static:
        state_str = info.state.value
        colour = _STATE_COLOURS.get(state_str, "dim")
        msg = info.state_message or ""
        text = f"\n  [{colour} bold]● {state_str}[/{colour} bold]"
        if msg:
            text += f"\n  {msg}"
        text += "\n"
        return Static(text, classes="cluster-state-card")

    # -- events sub-table ----------------------------------------------------

    @staticmethod
    def _build_events_table(events: List[ClusterEvent]) -> DataTable:
        table: DataTable[str] = DataTable(classes="events-table")
        table.add_columns("Timestamp", "Event Type", "Details")
        for event in events[:20]:
            ts = format_timestamp(event.timestamp)
            etype = event.event_type
            colour = _EVENT_COLOURS.get(etype, "")
            styled_type = f"[{colour}]{etype}[/{colour}]" if colour else etype
            detail = event.message or ""
            if len(detail) > 80:
                detail = detail[:77] + "..."
            table.add_row(ts, styled_type, detail)
        return table

    # -- libraries sub-table -------------------------------------------------

    @staticmethod
    def _build_libraries_table(libs: List[LibraryInfo]) -> DataTable:
        table: DataTable[str] = DataTable(classes="libraries-table")
        table.add_columns("Library", "Type", "Status")
        for lib in libs:
            colour = _LIB_STATUS_COLOURS.get(lib.status, "")
            styled_status = f"[{colour}]{lib.status}[/{colour}]" if colour else lib.status
            table.add_row(lib.name, lib.library_type, styled_status)
        return table
