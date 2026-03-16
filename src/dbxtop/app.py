"""Main Textual application."""

from __future__ import annotations

import logging
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Static, TabbedContent, TabPane

from dbxtop.api.cache import DataCache
from dbxtop.api.client import DatabricksClient
from dbxtop.api.models import ClusterState
from dbxtop.api.poller import DataUpdated, MetricsPoller
from dbxtop.api.spark_api import SparkRESTClient
from dbxtop.config import Settings
from dbxtop.views.base import BaseView
from dbxtop.views.cluster import ClusterView
from dbxtop.widgets.footer import KeyboardFooter
from dbxtop.widgets.header import ClusterHeader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Help overlay
# ---------------------------------------------------------------------------


class HelpScreen(ModalScreen[None]):
    """Full-screen help overlay listing all keyboard shortcuts."""

    BINDINGS = [
        Binding("question_mark", "dismiss", "Close", show=False),
        Binding("escape", "dismiss", "Close"),
    ]

    DEFAULT_CSS = """
    HelpScreen {
        align: center middle;
    }
    HelpScreen > Static {
        width: 60;
        height: auto;
        max-height: 80%;
        padding: 1 2;
        border: thick $accent;
        background: $surface;
    }
    """

    def compose(self) -> ComposeResult:
        help_text = (
            "[bold]dbxtop Keyboard Shortcuts[/bold]\n\n"
            "[bold]Navigation[/bold]\n"
            "  1-6        Switch between views\n"
            "  Tab        Next tab\n"
            "  Shift+Tab  Previous tab\n\n"
            "[bold]Data Controls[/bold]\n"
            "  r          Force refresh all data\n"
            "  /          Filter rows (type to search)\n"
            "  Escape     Clear filter\n\n"
            "[bold]View Controls[/bold]\n"
            "  s          Cycle sort column\n"
            "  Enter      Open detail popup\n\n"
            "[bold]Application[/bold]\n"
            "  ?          Toggle this help\n"
            "  q          Quit\n"
        )
        yield Static(help_text)


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


class DbxTopApp(App[None]):
    """dbxtop — Real-time Databricks/Spark cluster dashboard."""

    CSS_PATH = "styles/default.tcss"
    TITLE = "dbxtop"

    BINDINGS = [
        Binding("1", "switch_tab('cluster')", "Cluster", show=False),
        Binding("2", "switch_tab('jobs')", "Jobs", show=False),
        Binding("3", "switch_tab('stages')", "Stages", show=False),
        Binding("4", "switch_tab('executors')", "Executors", show=False),
        Binding("5", "switch_tab('sql')", "SQL", show=False),
        Binding("6", "switch_tab('storage')", "Storage", show=False),
        Binding("r", "force_refresh", "Refresh"),
        Binding("question_mark", "toggle_help", "Help"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(
        self,
        profile: str = "DEFAULT",
        cluster_id: str = "",
        refresh_interval: float = 3.0,
        slow_refresh_interval: float = 15.0,
        theme_name: str = "dark",
    ) -> None:
        super().__init__()
        self._profile = profile
        self._cluster_id = cluster_id
        self._refresh_interval = refresh_interval
        self._slow_refresh_interval = slow_refresh_interval
        self._theme_name = theme_name

        self._settings: Optional[Settings] = None
        self._dbx_client: Optional[DatabricksClient] = None
        self._spark_client: Optional[SparkRESTClient] = None
        self._cache: Optional[DataCache] = None
        self._poller: Optional[MetricsPoller] = None
        self._header: Optional[ClusterHeader] = None
        self._footer: Optional[KeyboardFooter] = None

    # -- compose -------------------------------------------------------------

    def compose(self) -> ComposeResult:
        self._header = ClusterHeader(
            profile=self._profile,
            poll_interval=self._refresh_interval,
        )
        yield self._header

        with TabbedContent():
            with TabPane("Cluster", id="cluster"):
                yield ClusterView()
            with TabPane("Jobs", id="jobs"):
                yield Static("Waiting for Spark application...", classes="spark-unavailable")
            with TabPane("Stages", id="stages"):
                yield Static("Waiting for Spark application...", classes="spark-unavailable")
            with TabPane("Executors", id="executors"):
                yield Static("Waiting for Spark application...", classes="spark-unavailable")
            with TabPane("SQL", id="sql"):
                yield Static("Waiting for Spark application...", classes="spark-unavailable")
            with TabPane("Storage", id="storage"):
                yield Static("Waiting for Spark application...", classes="spark-unavailable")

        self._footer = KeyboardFooter()
        yield self._footer

    # -- lifecycle -----------------------------------------------------------

    async def on_mount(self) -> None:
        """Initialise clients, cache, and poller on app mount."""
        try:
            self._settings = Settings.from_cli(
                profile=self._profile,
                cluster_id=self._cluster_id,
                refresh=self._refresh_interval,
                slow_refresh=self._slow_refresh_interval,
                theme=self._theme_name,
            )
        except ValueError as exc:
            self._show_error(f"Configuration error: {exc}")
            return

        self._cache = DataCache()

        try:
            self._dbx_client = DatabricksClient(
                profile=self._settings.profile,
                cluster_id=self._settings.cluster_id,
            )
        except Exception as exc:
            self._show_error(f"Failed to create Databricks client: {exc}")
            return

        # Attempt Spark REST client setup (non-fatal if it fails)
        await self._try_init_spark_client()

        # Start polling
        self._poller = MetricsPoller(
            dbx_client=self._dbx_client,
            spark_client=self._spark_client,
            cache=self._cache,
            app=self,
            settings=self._settings,
        )
        self._poller.start()

    async def _try_init_spark_client(self) -> None:
        """Attempt to initialise the Spark REST client.

        Non-fatal — if Spark is not yet available, the poller will
        work in SDK-only mode and retry later.
        """
        if self._dbx_client is None or self._settings is None:
            return

        try:
            workspace_url = await self._dbx_client.get_workspace_url()
            org_id = await self._dbx_client.get_org_id()
            token = self._dbx_client.get_token()

            self._spark_client = SparkRESTClient(
                workspace_url=workspace_url,
                cluster_id=self._settings.cluster_id,
                org_id=org_id,
                port=self._settings.spark_port,
                token=token,
                timeout=self._settings.request_timeout_s,
            )
            await self._spark_client.discover_app_id()

            if self._header:
                self._header.spark_available = True
            if self._footer:
                self._footer.spark_connected = True

            logger.info("Spark REST client initialised")
        except Exception as exc:
            logger.info("Spark REST not available yet: %s", exc)
            self._spark_client = None

    async def on_unmount(self) -> None:
        """Clean up poller and clients on app exit."""
        if self._poller:
            await self._poller.stop()

    # -- data update handler -------------------------------------------------

    def on_data_updated(self, event: DataUpdated) -> None:
        """Forward cache updates to the header and active view."""
        if self._cache is None:
            return

        # Update header
        if self._header and "cluster" in event.updated_slots:
            self._header.update_from_cache(self._cache)

            # Update Spark availability status
            cluster_slot = self._cache.get("cluster")
            if cluster_slot.data is not None:
                is_running = cluster_slot.data.state == ClusterState.RUNNING
                spark_ok = self._spark_client is not None and is_running
                if self._header:
                    self._header.spark_available = spark_ok
                if self._footer:
                    self._footer.spark_connected = spark_ok

        # Forward to active view
        try:
            tabbed = self.query_one(TabbedContent)
            active_pane = tabbed.get_pane(tabbed.active)
            for child in active_pane.walk_children():
                if isinstance(child, BaseView):
                    child.refresh_data(self._cache)
                    break
        except Exception:
            pass  # View not yet mounted or tab mismatch

    # -- actions -------------------------------------------------------------

    def action_switch_tab(self, tab_id: str) -> None:
        """Switch to a named tab."""
        self.query_one(TabbedContent).active = tab_id
        if self._footer:
            self._footer.active_tab = tab_id

    async def action_force_refresh(self) -> None:
        """Trigger an immediate full poll cycle."""
        if self._poller:
            await self._poller.force_refresh()
        if self._header:
            self._header.reset_countdown()

    def action_toggle_help(self) -> None:
        """Show or dismiss the help overlay."""
        self.push_screen(HelpScreen())

    # -- error display -------------------------------------------------------

    def _show_error(self, message: str) -> None:
        """Display an error message in the header area."""
        logger.error(message)
        if self._header:
            self._header.update(f"[red]ERROR:[/red] {message}")
