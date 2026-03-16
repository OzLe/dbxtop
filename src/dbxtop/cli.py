"""CLI entry point for dbxtop."""
import click

from dbxtop import __version__


@click.command()
@click.option("--profile", default="DEFAULT", envvar="DATABRICKS_CONFIG_PROFILE", help="Databricks CLI profile name")
@click.option("--cluster-id", required=True, envvar="DATABRICKS_CLUSTER_ID", help="Cluster ID to monitor")
@click.option("--refresh", default=3.0, type=float, help="Fast refresh interval in seconds")
@click.option("--slow-refresh", default=15.0, type=float, help="Slow refresh interval for cluster/events")
@click.option("--theme", default="dark", type=click.Choice(["dark", "light"]), help="Color theme")
@click.version_option(version=__version__, prog_name="dbxtop")
def main(profile: str, cluster_id: str, refresh: float, slow_refresh: float, theme: str) -> None:
    """Real-time terminal dashboard for Databricks/Spark clusters."""
    from dbxtop.app import DbxTopApp

    app = DbxTopApp(
        profile=profile,
        cluster_id=cluster_id,
        refresh_interval=refresh,
        slow_refresh_interval=slow_refresh,
        theme_name=theme,
    )
    app.run()
