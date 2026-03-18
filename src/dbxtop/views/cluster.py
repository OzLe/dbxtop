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

# -- state colour helpers (derived from canonical status_indicator maps) -----

from dbxtop.widgets.status_indicator import CLUSTER_STATES

_STATE_COLOURS: dict[str, str] = {k: v[0] for k, v in CLUSTER_STATES.items()}

_EVENT_COLOURS: dict[str, str] = {
    # Error events (red)
    "SPARK_EXCEPTION": "red bold",
    "DRIVER_UNAVAILABLE": "red bold",
    "DRIVER_NOT_RESPONDING": "red bold",
    "NODES_LOST": "red bold",
    "FAILED_TO_EXPAND_DISK": "red bold",
    "METASTORE_DOWN": "red bold",
    "DBFS_DOWN": "red bold",
    "TERMINATING": "red",
    "TERMINATED": "red",
    "ERROR": "red",
    # Warning events (yellow)
    "UPSIZE_COMPLETED": "yellow",
    "DOWNSCALED": "yellow",
    "AUTOSCALING_STATS_REPORT": "yellow",
    "RESTARTING": "yellow",
    "INIT_SCRIPTS_FINISHED": "yellow",
    # Normal events (green/dim)
    "RUNNING": "green",
    "CREATING": "green",
    "STARTING": "green",
    "EDITED": "dim",
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
        """Build the stable layout — widgets are updated in-place on refresh."""
        yield Static("Connecting to cluster...", classes="empty-state", id="cluster-placeholder")
        yield Static("", classes="cluster-identity", id="cluster-identity")
        yield Static("Resources", classes="cluster-section-title", id="cluster-res-title")
        yield Static("", classes="cluster-resources", id="cluster-resources")
        yield Static("Spark Configuration", classes="cluster-section-title", id="cluster-conf-title")
        yield Static("", classes="cluster-sparkconf", id="cluster-sparkconf")
        yield Static("", classes="cluster-state-card", id="cluster-state-card")
        yield Static("Events", classes="cluster-section-title", id="cluster-events-title")
        events_table: DataTable[str] = DataTable(classes="events-table", id="cluster-events-table")
        events_table.add_columns("Timestamp", "Event Type", "Details")
        yield events_table
        yield Static("Libraries", classes="cluster-section-title", id="cluster-libs-title")
        libs_table: DataTable[str] = DataTable(classes="libraries-table", id="cluster-libs-table")
        libs_table.add_columns("Library", "Type", "Status")
        yield libs_table

    def on_mount(self) -> None:
        """Hide all sections until data arrives."""
        for widget_id in (
            "cluster-identity",
            "cluster-res-title",
            "cluster-resources",
            "cluster-conf-title",
            "cluster-sparkconf",
            "cluster-state-card",
            "cluster-events-title",
            "cluster-events-table",
            "cluster-libs-title",
            "cluster-libs-table",
        ):
            try:
                self.query_one(f"#{widget_id}").display = False
            except Exception:
                pass

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render all sections from cache data using in-place updates."""
        cluster_slot = cache.get("cluster")
        events_slot = cache.get("events")
        libs_slot = cache.get("libraries")

        info: Optional[ClusterInfo] = cluster_slot.data
        if info is None:
            return

        # Hide placeholder
        try:
            self.query_one("#cluster-placeholder").display = False
        except Exception:
            pass

        # Identity card
        identity = self.query_one("#cluster-identity", Static)
        identity.update(self._build_identity_text(info))
        identity.display = True

        # Resources
        self.query_one("#cluster-res-title").display = True
        resources = self.query_one("#cluster-resources", Static)
        resources.update(self._build_resources_text(info))
        resources.display = True

        # Spark configuration
        if info.spark_conf:
            self.query_one("#cluster-conf-title").display = True
            sparkconf = self.query_one("#cluster-sparkconf", Static)
            sparkconf.update(self._build_spark_conf_text(info))
            sparkconf.display = True
        else:
            self.query_one("#cluster-conf-title").display = False
            self.query_one("#cluster-sparkconf").display = False

        # State card
        state_card = self.query_one("#cluster-state-card", Static)
        state_card.update(self._build_state_card_text(info))
        state_card.display = True

        # Events table
        events: Optional[List[ClusterEvent]] = events_slot.data
        events_title = self.query_one("#cluster-events-title")
        events_table = self.query_one("#cluster-events-table", DataTable)
        if events:
            events_title.display = True
            events_table.display = True
            events_table.clear()
            for event in events[:20]:
                ts = format_timestamp(event.timestamp)
                etype = event.event_type
                colour = _EVENT_COLOURS.get(etype, "")
                styled_type = f"[{colour}]{etype}[/{colour}]" if colour else etype
                detail = event.message or ""
                if len(detail) > 80:
                    detail = detail[:77] + "..."
                events_table.add_row(ts, styled_type, detail)
        else:
            events_title.display = False
            events_table.display = False

        # Libraries table
        libs: Optional[List[LibraryInfo]] = libs_slot.data
        libs_title = self.query_one("#cluster-libs-title")
        libs_table = self.query_one("#cluster-libs-table", DataTable)
        if libs:
            libs_title.display = True
            libs_table.display = True
            libs_table.clear()
            for lib in libs:
                colour = _LIB_STATUS_COLOURS.get(lib.status, "")
                styled_status = f"[{colour}]{lib.status}[/{colour}]" if colour else lib.status
                libs_table.add_row(lib.name, lib.library_type, styled_status)
        else:
            libs_title.display = False
            libs_table.display = False

    # -- text builders (return strings for in-place widget updates) -----------

    @staticmethod
    def _build_identity_text(info: ClusterInfo) -> str:
        lines = [
            f"[bold]{info.cluster_name}[/bold]  ({info.cluster_id})",
            f"  Spark: {info.spark_version or '--'}  |  "
            f"Runtime: {info.runtime_engine or 'Standard'}  |  "
            f"Security: {info.data_security_mode or '--'}",
            f"  Creator: {info.creator or '--'}  |  Auto-terminate: {info.autotermination_minutes}m"
            if info.autotermination_minutes
            else f"  Creator: {info.creator or '--'}  |  Auto-terminate: disabled",
        ]
        return "\n".join(lines)

    @staticmethod
    def _build_resources_text(info: ClusterInfo) -> str:
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
        return "\n".join(lines)

    @staticmethod
    def _build_spark_conf_text(info: ClusterInfo) -> str:
        lines: list[str] = []
        for key, value in sorted(info.spark_conf.items()):
            display_val = value if len(value) <= 60 else value[:57] + "..."
            safe_key = key.replace("[", "\\[")
            safe_val = display_val.replace("[", "\\[")
            lines.append(f"  {safe_key} = {safe_val}")
        return "\n".join(lines) or "  (none)"

    @staticmethod
    def _build_state_card_text(info: ClusterInfo) -> str:
        state_str = info.state.value
        colour = _STATE_COLOURS.get(state_str, "dim")
        msg = info.state_message or ""
        text = f"\n  [{colour} bold]● {state_str}[/{colour} bold]"
        if msg:
            text += f"\n  {msg}"
        text += "\n"
        return text
