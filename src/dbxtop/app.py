"""Main Textual application."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Input, Static, TabbedContent, TabPane

from dbxtop.api.cache import DataCache

if TYPE_CHECKING:
    from dbxtop.analytics.run_manager import RunManager
from dbxtop.api.client import DatabricksClient
from dbxtop.api.models import ClusterState
from dbxtop.api.poller import DataUpdated, KeepAliveUpdated, MetricsPoller
from dbxtop.api.spark_api import SparkRESTClient
from dbxtop.config import Settings
from dbxtop.views.analytics import AnalyticsView
from dbxtop.views.base import BaseView
from dbxtop.views.cluster import ClusterView
from dbxtop.views.executors import ExecutorsView
from dbxtop.views.jobs import JobsView
from dbxtop.views.sql import SQLView
from dbxtop.views.stages import StagesView
from dbxtop.views.storage import StorageView
from dbxtop.widgets.footer import KeyboardFooter
from dbxtop.widgets.header import ClusterHeader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error / auth screens
# ---------------------------------------------------------------------------


class AuthErrorScreen(ModalScreen[None]):
    """Full-screen modal shown when Databricks authentication fails."""

    BINDINGS = [Binding("escape", "dismiss", "Close"), Binding("q", "quit_app", "Quit")]

    DEFAULT_CSS = """
    AuthErrorScreen {
        align: center middle;
    }
    AuthErrorScreen > Static {
        width: 70;
        height: auto;
        padding: 2 3;
        border: thick $error;
        background: $surface;
    }
    """

    def __init__(self, message: str, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._message = message

    def compose(self) -> ComposeResult:
        yield Static(
            f"[bold red]Authentication Failed[/bold red]\n\n"
            f"{self._message}\n\n"
            f"[dim]Please refresh your Databricks token or check your profile configuration.[/dim]\n"
            f"[dim]  1. Run: databricks auth login --profile <profile>[/dim]\n"
            f"[dim]  2. Or update ~/.databrickscfg with a valid token[/dim]\n\n"
            f"Press [bold]Escape[/bold] to dismiss or [bold]q[/bold] to quit."
        )

    def action_quit_app(self) -> None:
        self.app.exit()


class DetailScreen(ModalScreen[None]):
    """Full-screen modal for job/stage/SQL detail views."""

    BINDINGS = [Binding("escape", "dismiss", "Close")]

    DEFAULT_CSS = """
    DetailScreen {
        align: center middle;
    }
    DetailScreen > Static {
        width: 80;
        height: auto;
        max-height: 80%;
        padding: 1 2;
        border: thick $accent;
        background: $surface;
        overflow-y: auto;
    }
    """

    def __init__(self, content: str, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._content = content

    def compose(self) -> ComposeResult:
        yield Static(self._content)


class RunNameModal(ModalScreen[Optional[str]]):
    """Modal dialog for entering a run session name."""

    BINDINGS = [Binding("escape", "dismiss", "Cancel")]

    DEFAULT_CSS = """
    RunNameModal {
        align: center middle;
    }
    RunNameModal > #run-name-container {
        width: 50;
        height: auto;
        padding: 1 2;
        border: thick $accent;
        background: $surface;
    }
    """

    def compose(self) -> ComposeResult:
        """Build the modal layout with an input field for the run name."""
        from datetime import datetime, timezone

        default_name = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        with Static(id="run-name-container"):
            yield Static("[bold]Start New Run[/bold]\n")
            yield Input(value=default_name, placeholder="Run name...", id="run-name-input")
            yield Static("\n[dim]Enter to start, Esc to cancel[/dim]")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Submit the run name when Enter is pressed."""
        if event.input.id == "run-name-input" and event.value.strip():
            self.dismiss(event.value.strip())


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
            "  1-7        Switch between views\n"
            "  Tab        Next tab\n"
            "  Shift+Tab  Previous tab\n\n"
            "[bold]Data Controls[/bold]\n"
            "  r          Force refresh all data\n"
            "  /          Filter rows (type to search)\n"
            "  Escape     Clear filter\n\n"
            "[bold]View Controls[/bold]\n"
            "  s          Cycle sort column\n"
            "  Enter      Open detail popup\n\n"
            "[bold]Run Recording[/bold]\n"
            "  Ctrl+R     Start/stop run recording\n"
            "  Ctrl+L     Show saved runs\n\n"
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
        Binding("7", "switch_tab('analytics')", "Analytics", show=False),
        Binding("r", "force_refresh", "Refresh"),
        Binding("s", "cycle_sort", "Sort", show=False),
        Binding("slash", "activate_filter", "Filter", show=False),
        Binding("escape", "clear_filter", "Clear filter", show=False),
        Binding("enter", "show_detail", "Detail", show=False),
        Binding("ctrl+r", "toggle_run", "Run", show=False),
        Binding("ctrl+l", "show_run_list", "Runs", show=False),
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
        keepalive: bool = False,
        keepalive_interval: float = 300.0,
    ) -> None:
        super().__init__()
        # Apply light/dark theme via Textual's built-in theme system
        if theme_name == "light":
            self.theme = "textual-light"
        self._profile = profile
        self._cluster_id = cluster_id
        self._refresh_interval = refresh_interval
        self._slow_refresh_interval = slow_refresh_interval
        self._theme_name = theme_name
        self._keepalive = keepalive
        self._keepalive_interval = keepalive_interval

        self._settings: Optional[Settings] = None
        self._dbx_client: Optional[DatabricksClient] = None
        self._spark_client: Optional[SparkRESTClient] = None
        self._cache: Optional[DataCache] = None
        self._poller: Optional[MetricsPoller] = None
        self._header: Optional[ClusterHeader] = None
        self._footer: Optional[KeyboardFooter] = None
        self._run_manager: Optional[RunManager] = None

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
                yield JobsView()
            with TabPane("Stages", id="stages"):
                yield StagesView()
            with TabPane("Executors", id="executors"):
                yield ExecutorsView()
            with TabPane("SQL", id="sql"):
                yield SQLView()
            with TabPane("Storage", id="storage"):
                yield StorageView()
            with TabPane("Analytics", id="analytics"):
                yield AnalyticsView()

        filter_input = Input(placeholder="Filter...", id="filter-input")
        filter_input.display = False
        yield filter_input

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
                keepalive=self._keepalive,
                keepalive_interval=self._keepalive_interval,
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

            self._spark_client = SparkRESTClient(
                workspace_url=workspace_url,
                cluster_id=self._settings.cluster_id,
                org_id=org_id,
                port=self._settings.spark_port,
                token_provider=self._dbx_client.get_token,
                timeout=self._settings.request_timeout_s,
            )
            await self._spark_client.discover_app_id()

            self._update_spark_status(True)
            logger.info("Spark REST client initialised")

            if self._poller is not None and self._spark_client is not None:
                await self._poller.set_spark_client(self._spark_client)
        except Exception as exc:
            logger.info("Spark REST not available yet: %s", exc)
            if self._spark_client is not None:
                try:
                    await self._spark_client.close()
                except Exception:
                    pass
            self._spark_client = None
            self._update_spark_status(False)

    async def on_unmount(self) -> None:
        """Clean up poller and clients on app exit."""
        if self._poller:
            await self._poller.stop()

    # -- data update handler -------------------------------------------------

    def on_data_updated(self, event: DataUpdated) -> None:
        """Forward cache updates to the header and active view.

        Also handles error state detection:
        - Auth errors → show auth error screen, stop polling
        - Cluster terminated → update Spark status indicators
        - Rate limiting → show indicator in footer
        """
        if self._cache is None:
            return

        # Check for auth errors in any slot
        for slot_name in event.updated_slots:
            slot = self._cache.get(slot_name)
            if slot.error and ("authentication" in slot.error.lower() or "401" in slot.error or "403" in slot.error):
                self._handle_auth_error(slot.error)
                return

        # Update header on cluster data
        if self._header and "cluster" in event.updated_slots:
            self._header.update_from_cache(self._cache)
            self._handle_cluster_state_change()

        # Update rate limit / connection status in footer
        self._update_footer_status()

        # Forward to active view (only if relevant slots changed)
        self._forward_to_active_view(event.updated_slots)

    def on_keep_alive_updated(self, event: KeepAliveUpdated) -> None:
        """Update footer with keepalive status."""
        if self._footer is not None:
            self._footer.keepalive_active = event.active
            self._footer.keepalive_last = event.last_success
            self._footer.keepalive_failed = event.failed

    def _handle_cluster_state_change(self) -> None:
        """Update Spark availability based on cluster state."""
        if self._cache is None:
            return
        cluster_slot = self._cache.get("cluster")
        if cluster_slot.data is None:
            return

        state = cluster_slot.data.state
        is_running = state == ClusterState.RUNNING
        spark_ok = self._spark_client is not None and is_running

        self._update_spark_status(spark_ok)

        # If cluster just came back to RUNNING and we have no Spark client, try reconnecting
        if is_running and self._spark_client is None and self._dbx_client is not None:
            self.call_later(self._try_init_spark_client)

    def _handle_auth_error(self, error_msg: str) -> None:
        """Show auth error modal and stop polling."""
        logger.error("Authentication error detected: %s", error_msg)
        if self._poller:
            self.call_later(self._poller.stop)
        self.push_screen(AuthErrorScreen(error_msg))

    def _update_spark_status(self, connected: bool) -> None:
        """Update Spark connection indicators in header and footer."""
        if self._header:
            self._header.spark_available = connected
        if self._footer:
            self._footer.spark_connected = connected

    def _update_footer_status(self) -> None:
        """Update footer with rate limit and SDK-only mode indicators."""
        if self._footer is None:
            return

        # Check rate limiting
        if self._spark_client and self._spark_client.is_rate_limited:
            self._footer.spark_connected = False
            self._footer.sdk_only = False
        # Check if we're in SDK-only mode (cluster running but no Spark client)
        elif self._spark_client is None:
            self._footer.spark_connected = False
            cluster_slot = self._cache.get("cluster") if self._cache else None
            cluster_running = (
                cluster_slot is not None
                and cluster_slot.data is not None
                and cluster_slot.data.state == ClusterState.RUNNING
            )
            self._footer.sdk_only = cluster_running
        else:
            self._footer.sdk_only = False

    # Map view types to the cache slots they care about
    _VIEW_SLOTS: dict[type, set[str]] = {
        ClusterView: {"cluster", "events", "libraries"},
        JobsView: {"spark_jobs"},
        StagesView: {"stages"},
        ExecutorsView: {"executors"},
        SQLView: {"sql_queries"},
        StorageView: {"storage"},
        AnalyticsView: {"executors", "stages", "spark_jobs", "sql_queries", "cluster"},
    }

    def _forward_to_active_view(self, updated_slots: Optional[set[str]] = None) -> None:
        """Forward cache data to the currently active view.

        Also feeds data to the RunManager when a run is recording and the
        analytics view has produced a diagnostic report.

        Args:
            updated_slots: If provided, only refresh when the view's
                relevant slots intersect with the updated ones.
        """
        if self._cache is None:
            return
        try:
            tabbed = self.query_one(TabbedContent)
            active_pane = tabbed.get_pane(tabbed.active)
            for child in active_pane.walk_children():
                if isinstance(child, BaseView):
                    if updated_slots is not None:
                        relevant = self._VIEW_SLOTS.get(type(child), set())
                        if relevant and not relevant.intersection(updated_slots):
                            return
                    child.refresh_data(self._cache)
                    break
        except Exception:
            logger.debug("Could not forward data to active view", exc_info=True)

        # If a run is recording, feed the latest report to the run manager
        if self._run_manager is not None and self._run_manager.is_recording:
            try:
                analytics_pane = self.query_one(TabbedContent).get_pane("analytics")
                for child in analytics_pane.walk_children():
                    if isinstance(child, AnalyticsView) and child._last_report is not None:
                        executors = self._cache.get("executors").data or []
                        self._run_manager.on_report(child._last_report, executors)
                        break
            except Exception:
                pass

    # -- actions -------------------------------------------------------------

    def action_switch_tab(self, tab_id: str) -> None:
        """Switch to a named tab and refresh its view."""
        self.query_one(TabbedContent).active = tab_id
        if self._footer:
            self._footer.active_tab = tab_id
        # Refresh the newly-active view with current cache data
        if self._cache:
            self._forward_to_active_view()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Handle tab clicks — sync footer and refresh the newly active view."""
        tab_id = event.pane.id or ""
        if self._footer:
            self._footer.active_tab = tab_id
        if self._cache:
            self._forward_to_active_view()

    async def action_force_refresh(self) -> None:
        """Trigger an immediate full poll cycle."""
        if self._poller:
            await self._poller.force_refresh()
        if self._header:
            self._header.reset_countdown()

    def action_cycle_sort(self) -> None:
        """Cycle the sort column on the active view (if supported)."""
        try:
            tabbed = self.query_one(TabbedContent)
            active_pane = tabbed.get_pane(tabbed.active)
            for child in active_pane.walk_children():
                if hasattr(child, "cycle_sort_column"):
                    child.cycle_sort_column()
                    if self._cache is not None and isinstance(child, BaseView):
                        child.refresh_data(self._cache)
                    break
        except Exception:
            logger.debug("Could not cycle sort on active view", exc_info=True)

    def action_show_detail(self) -> None:
        """Show detail popup for the selected row in the active view."""
        try:
            tabbed = self.query_one(TabbedContent)
            active_pane = tabbed.get_pane(tabbed.active)
            for child in active_pane.walk_children():
                if hasattr(child, "get_selected_detail") and self._cache is not None:
                    detail = child.get_selected_detail(self._cache)
                    if detail:
                        self.push_screen(DetailScreen(detail))
                    break
        except Exception:
            logger.debug("Could not show detail for selected row", exc_info=True)

    def action_activate_filter(self) -> None:
        """Show the filter input and focus it."""
        try:
            filter_input = self.query_one("#filter-input", Input)
            filter_input.display = True
            filter_input.focus()
        except Exception:
            pass

    def action_clear_filter(self) -> None:
        """Clear filter text and hide the input."""
        try:
            filter_input = self.query_one("#filter-input", Input)
            filter_input.value = ""
            filter_input.display = False
        except Exception:
            pass
        # Clear filter on active view
        self._apply_filter_to_active_view("")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Apply filter when Enter is pressed in the filter input."""
        if event.input.id == "filter-input":
            self._apply_filter_to_active_view(event.value)
            event.input.display = False

    def on_input_changed(self, event: Input.Changed) -> None:
        """Apply filter as user types."""
        if event.input.id == "filter-input":
            self._apply_filter_to_active_view(event.value)

    def _apply_filter_to_active_view(self, text: str) -> None:
        """Set filter text on the active view and refresh."""
        try:
            tabbed = self.query_one(TabbedContent)
            active_pane = tabbed.get_pane(tabbed.active)
            for child in active_pane.walk_children():
                if isinstance(child, BaseView):
                    child.apply_filter(text)
                    if self._cache is not None:
                        child.refresh_data(self._cache)
                    break
        except Exception:
            logger.debug("Could not apply filter to active view", exc_info=True)

    def action_toggle_help(self) -> None:
        """Show or dismiss the help overlay."""
        self.push_screen(HelpScreen())

    def action_toggle_run(self) -> None:
        """Toggle run recording on/off.

        If a run is currently recording, stop it and save to disk.
        Otherwise, show the RunNameModal to start a new run.
        """
        if self._run_manager is not None and self._run_manager.is_recording:
            run = self._run_manager.stop_run()
            if run:
                self.notify(f"Run '{run.name}' saved ({run.duration_seconds:.0f}s)", severity="information")
            self._set_analytics_run_state(None, None)
        else:
            self.push_screen(RunNameModal(), callback=self._on_run_name_submitted)

    def _on_run_name_submitted(self, name: Optional[str]) -> None:
        """Callback when user submits a run name from the modal.

        Args:
            name: The run name entered by the user, or None if cancelled.
        """
        if name is None:
            return

        if self._run_manager is None:
            from dbxtop.analytics.run_manager import RunManager

            self._run_manager = RunManager(self._cluster_id)

        # Capture Spark config snapshot from the cluster cache slot,
        # filtering out keys that may contain sensitive values.
        config_snapshot: dict[str, str] = {}
        if self._cache:
            cluster_slot = self._cache.get("cluster")
            if cluster_slot.data is not None:
                from dbxtop.analytics.run_manager import filter_sensitive_config

                config_snapshot = filter_sensitive_config(dict(cluster_slot.data.spark_conf))

        run = self._run_manager.start_run(name, config_snapshot)
        self._set_analytics_run_state(name, run.started_at)
        self.notify(f"Recording: {name}", severity="information")

    def _set_analytics_run_state(self, name: Optional[str], started: Optional[datetime]) -> None:
        """Update the analytics view run indicator.

        Args:
            name: The active run name, or None to clear.
            started: The run start time, or None to clear.
        """
        try:
            tabbed = self.query_one(TabbedContent)
            analytics_pane = tabbed.get_pane("analytics")
            for child in analytics_pane.walk_children():
                if hasattr(child, "set_run_state"):
                    child.set_run_state(name, started)
                    break
        except Exception:
            pass

    def action_show_run_list(self) -> None:
        """Show the list of saved runs."""
        from dbxtop.analytics.run_manager import RunManager
        from dbxtop.views.run_list import RunListScreen

        runs = RunManager.list_runs(self._cluster_id)
        self.push_screen(RunListScreen(runs))

    # -- error display -------------------------------------------------------

    def _show_error(self, message: str) -> None:
        """Display an error message in the header area."""
        logger.error(message)
        if self._header:
            self._header.update(f"[red]ERROR:[/red] {message}")
