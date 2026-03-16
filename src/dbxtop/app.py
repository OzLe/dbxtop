"""Main Textual application."""
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, TabbedContent, TabPane


class DbxTopApp(App):
    """dbxtop — Real-time Databricks/Spark cluster dashboard."""

    CSS_PATH = "styles/default.tcss"
    TITLE = "dbxtop"

    BINDINGS = [
        ("1", "switch_tab('cluster')", "Cluster"),
        ("2", "switch_tab('jobs')", "Jobs"),
        ("3", "switch_tab('stages')", "Stages"),
        ("4", "switch_tab('executors')", "Executors"),
        ("5", "switch_tab('sql')", "SQL"),
        ("6", "switch_tab('storage')", "Storage"),
        ("r", "force_refresh", "Refresh"),
        ("q", "quit", "Quit"),
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
        self.profile = profile
        self.cluster_id = cluster_id
        self.refresh_interval = refresh_interval
        self.slow_refresh_interval = slow_refresh_interval
        self.theme_name = theme_name

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent():
            with TabPane("Cluster", id="cluster"):
                pass  # TODO: ClusterView
            with TabPane("Jobs", id="jobs"):
                pass  # TODO: JobsView
            with TabPane("Stages", id="stages"):
                pass  # TODO: StagesView
            with TabPane("Executors", id="executors"):
                pass  # TODO: ExecutorsView
            with TabPane("SQL", id="sql"):
                pass  # TODO: SQLView
            with TabPane("Storage", id="storage"):
                pass  # TODO: StorageView
        yield Footer()

    def action_switch_tab(self, tab_id: str) -> None:
        self.query_one(TabbedContent).active = tab_id

    def action_force_refresh(self) -> None:
        pass  # TODO: trigger immediate poll
