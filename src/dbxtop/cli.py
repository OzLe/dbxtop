"""CLI entry point for dbxtop."""

from __future__ import annotations

import click

from dbxtop import __version__


@click.command()
@click.option(
    "-p",
    "--profile",
    default="DEFAULT",
    envvar="DBXTOP_PROFILE",
    help="Databricks CLI profile name.",
)
@click.option(
    "-c",
    "--cluster-id",
    required=True,
    envvar="DBXTOP_CLUSTER_ID",
    help="Cluster ID to monitor.",
)
@click.option(
    "--refresh",
    default=3.0,
    type=click.FloatRange(min=1.0, max=30.0),
    envvar="DBXTOP_REFRESH",
    help="Fast refresh interval in seconds (1.0–30.0).",
)
@click.option(
    "--slow-refresh",
    default=15.0,
    type=click.FloatRange(min=5.0, max=120.0),
    envvar="DBXTOP_SLOW_REFRESH",
    help="Slow refresh interval in seconds (5.0–120.0).",
)
@click.option(
    "--theme",
    default="dark",
    type=click.Choice(["dark", "light"]),
    envvar="DBXTOP_THEME",
    help="Color theme.",
)
@click.version_option(version=__version__, prog_name="dbxtop")
def main(
    profile: str,
    cluster_id: str,
    refresh: float,
    slow_refresh: float,
    theme: str,
) -> None:
    """Real-time terminal dashboard for Databricks/Spark clusters."""
    if slow_refresh < refresh:
        raise click.BadParameter(
            f"--slow-refresh ({slow_refresh}) must be >= --refresh ({refresh})",
            param_hint="'--slow-refresh'",
        )

    from dbxtop.app import DbxTopApp

    app = DbxTopApp(
        profile=profile,
        cluster_id=cluster_id,
        refresh_interval=refresh,
        slow_refresh_interval=slow_refresh,
        theme_name=theme,
    )
    app.run()
