"""Analytics performance dashboard view.

Displays computed health score, detected issues, resource efficiency
metrics, and actionable recommendations synthesized from executor,
stage, job, SQL, and cluster data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static

from dbxtop.analytics.accumulator import AccumulatedInsight, InsightAccumulator
from dbxtop.analytics.engine import AnalyticsEngine
from dbxtop.analytics.models import (
    DiagnosticReport,
    HealthScore,
    Recommendation,
    Severity,
)
from dbxtop.api.cache import DataCache
from dbxtop.api.models import ExecutorInfo, format_bytes
from dbxtop.views.base import BaseView
from dbxtop.widgets.spark_line import render_sparkline


class AnalyticsView(BaseView):
    """Performance analytics dashboard with health score and insights.

    Displays a 4-panel layout:
    - Top-left: Health score with component breakdown
    - Top-right: Top issues detected (sorted by severity)
    - Bottom-left: Resource efficiency metrics
    - Bottom-right: Prioritized recommendations
    """

    DEFAULT_CSS = """
    AnalyticsView {
        height: 1fr;
        overflow-y: auto;
    }

    .analytics-placeholder {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: $text-muted;
        text-style: italic;
    }

    .analytics-top-row {
        height: 12;
        layout: horizontal;
    }

    .analytics-bottom-row {
        height: 1fr;
        layout: horizontal;
        min-height: 8;
    }

    .health-panel {
        width: 36;
        height: 100%;
        padding: 0 1;
        border-right: solid $accent;
    }

    .issues-panel {
        width: 1fr;
        height: 100%;
        padding: 0 1;
        overflow-y: auto;
    }

    .efficiency-panel {
        width: 36;
        height: 100%;
        padding: 0 1;
        border-right: solid $accent;
    }

    .recommendations-panel {
        width: 1fr;
        height: 100%;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    _MIN_REFRESH_INTERVAL_S: float = 5.0
    """Minimum seconds between analytics recomputes to avoid unnecessary work."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._engine = AnalyticsEngine()
        self._accumulator = InsightAccumulator()
        self._last_refresh: Optional[datetime] = None
        self._last_report: Optional[DiagnosticReport] = None
        self._run_name: Optional[str] = None
        self._run_started: Optional[datetime] = None

    def compose(self) -> ComposeResult:
        """Build the stable layout -- widgets are updated in-place on refresh."""
        yield Static(
            "Waiting for Spark application...",
            classes="analytics-placeholder",
            id="analytics-placeholder",
        )

        # Top row: Health Score + Top Issues
        yield Horizontal(
            Static("", classes="health-panel", id="health-panel"),
            Static("", classes="issues-panel", id="issues-panel"),
            classes="analytics-top-row",
            id="analytics-top-row",
        )

        # Bottom row: Resource Efficiency + Recommendations
        yield Horizontal(
            Static("", classes="efficiency-panel", id="efficiency-panel"),
            Static("", classes="recommendations-panel", id="recommendations-panel"),
            classes="analytics-bottom-row",
            id="analytics-bottom-row",
        )

    def on_mount(self) -> None:
        """Hide content panels until data arrives."""
        for widget_id in ("analytics-top-row", "analytics-bottom-row"):
            try:
                self.query_one(f"#{widget_id}").display = False
            except Exception:
                pass

    def refresh_data(self, cache: DataCache) -> None:
        """Re-render all panels from cache data.

        Reads from cache slots: executors, stages, spark_jobs, sql_queries,
        cluster. Runs the AnalyticsEngine.analyze() method and renders the
        result into the four panels.

        Args:
            cache: The shared data cache.
        """
        executors = cache.get("executors").data
        stages = cache.get("stages").data
        jobs = cache.get("spark_jobs").data
        sql_queries = cache.get("sql_queries").data
        cluster = cache.get("cluster").data

        # If executors is None, Spark data is not yet available
        if executors is None:
            return

        # Throttle: avoid recomputing analytics too frequently
        now = datetime.now(timezone.utc)
        if self._last_refresh is not None:
            elapsed = (now - self._last_refresh).total_seconds()
            if elapsed < self._MIN_REFRESH_INTERVAL_S:
                return
        self._last_refresh = now

        storage = cache.get("storage").data

        # Run the analytics engine (all args match engine signature)
        report = self._engine.analyze(
            executors,
            stages,
            jobs,
            sql_queries,
            cluster,
            storage=storage,
            cache=cache,
        )

        if report is None:
            return

        self._last_report = report
        self._accumulator.update(report.insights, report.timestamp)
        self._update_panels(report, executors, cache)

    def _update_panels(
        self,
        report: DiagnosticReport,
        executors: object,
        cache: DataCache,
    ) -> None:
        """Update all four panel widgets with rendered content.

        Args:
            report: The diagnostic report from the analytics engine.
            executors: List of executor info objects.
            cache: The data cache (for efficiency panel sparklines).
        """
        try:
            # Hide placeholder, show panels
            self.query_one("#analytics-placeholder").display = False
            self.query_one("#analytics-top-row").display = True
            self.query_one("#analytics-bottom-row").display = True

            # Update each panel
            self.query_one("#health-panel", Static).update(self._render_health_panel(report.health))
            self.query_one("#issues-panel", Static).update(self._render_issues_panel(self._accumulator.get_all()))
            self.query_one("#efficiency-panel", Static).update(
                self._render_efficiency_panel(executors, cache)  # type: ignore[arg-type]
            )
            self.query_one("#recommendations-panel", Static).update(
                self._render_recommendations_panel(report.recommendations)
            )
        except Exception:  # noqa: BLE001 — View may not be mounted yet
            pass

    def set_run_state(self, name: Optional[str], started: Optional[datetime]) -> None:
        """Set the active run indicator state.

        Args:
            name: The name of the active recording run, or None to clear.
            started: The UTC start time of the run, or None to clear.
        """
        self._run_name = name
        self._run_started = started

    # -- panel rendering methods ------------------------------------------------

    def _render_health_panel(self, health: HealthScore) -> str:
        """Render the health score card as Rich markup.

        Args:
            health: The computed health score.

        Returns:
            Rich markup string for the health panel.
        """
        color = health.color
        lines = [
            "[bold]HEALTH SCORE[/bold]",
            "",
            f"  [{color} bold]{health.score}[/{color} bold]",
            f"  [{color}]{health.label}[/{color}]",
        ]

        # Show run recording indicator when active
        if self._run_name is not None and self._run_started is not None:
            elapsed = self._format_time_ago(self._run_started)
            lines.insert(0, f"[red bold]●[/red bold] REC [bold]{self._run_name}[/bold] ({elapsed})")
            lines.insert(1, "")

        if health.component_scores:
            lines.append("")
            lines.append("  [bold]Components:[/bold]")

            # Display each component with color-coded score
            component_display = {
                "gc": "GC",
                "spill": "Spill",
                "skew": "Skew",
                "utilization": "Utilization",
                "shuffle": "Shuffle",
                "task_failures": "Failures",
            }
            for key, display_name in component_display.items():
                score = health.component_scores.get(key)
                if score is not None:
                    score_color = _score_color(score)
                    padding = "." * max(1, 14 - len(display_name))
                    lines.append(f"  {display_name} {padding} [{score_color}]{score}[/{score_color}]")

        return "\n".join(lines)

    def _render_issues_panel(self, accumulated: List[AccumulatedInsight]) -> str:
        """Render detected issues with occurrence tracking and resolved history.

        Active insights show occurrence count and age. Resolved insights
        are shown dimmed with the time since resolution.

        Args:
            accumulated: List of accumulated insights (active first, then resolved).

        Returns:
            Rich markup string for the issues panel.
        """
        lines = ["[bold]TOP ISSUES[/bold]", ""]

        active = [a for a in accumulated if not a.is_resolved]
        resolved = [a for a in accumulated if a.is_resolved]

        if not active and not resolved:
            lines.append("  [green]No issues detected[/green]")
            return "\n".join(lines)

        # Show active insights (at most 10)
        for acc in active[:10]:
            icon = _severity_icon(acc.insight.severity)
            age = self._format_time_ago(acc.first_seen)
            count_str = f"  x{acc.occurrence_count}" if acc.occurrence_count > 1 else ""
            lines.append(f"  {icon} {acc.insight.title}{count_str}  {age}")

        remaining_active = len(active) - 10
        if remaining_active > 0:
            lines.append(f"  ... and {remaining_active} more")

        # Show resolved insights (at most 5)
        if resolved:
            lines.append("")
            for acc in resolved[:5]:
                resolved_ago = self._format_time_ago(acc.resolved_at) if acc.resolved_at else "?"
                lines.append(f"  [green]✓[/green] [dim]{acc.insight.title}[/dim]  resolved {resolved_ago}")

        return "\n".join(lines)

    def _format_time_ago(self, dt: datetime) -> str:
        """Format a datetime as a short relative time string.

        Args:
            dt: The UTC datetime to format relative to now.

        Returns:
            Short string like '12s', '3m', or '1h'.
        """
        delta = (datetime.now(timezone.utc) - dt).total_seconds()
        if delta < 60:
            return f"{int(delta)}s"
        if delta < 3600:
            return f"{int(delta / 60)}m"
        return f"{int(delta / 3600)}h"

    def _render_efficiency_panel(
        self,
        executors: List[ExecutorInfo],
        cache: DataCache,
    ) -> str:
        """Render resource efficiency metrics with bars and sparklines.

        Args:
            executors: List of ExecutorInfo objects.
            cache: The data cache for ring buffer history.

        Returns:
            Rich markup string for the efficiency panel.
        """
        lines = ["[bold]RESOURCE EFFICIENCY[/bold]", ""]

        if not executors:
            lines.append("  No executor data")
            return "\n".join(lines)

        # Compute cluster-wide memory
        total_memory = sum(e.max_memory for e in executors)
        used_memory = sum(e.memory_used for e in executors)
        mem_pct = (used_memory / total_memory * 100) if total_memory > 0 else 0.0
        lines.append(f"  Memory  {_progress_bar(mem_pct)} {mem_pct:.1f}%")

        # Memory sparkline from ring buffer history
        mem_history: list[float] = []
        for snapshot in cache.get_history("executors"):
            if snapshot:
                snap_used = sum(e.memory_used for e in snapshot)
                snap_total = sum(e.max_memory for e in snapshot)
                if snap_total > 0:
                    mem_history.append(snap_used / snap_total * 100)
        if mem_history:
            lines.append(f"  Mem {render_sparkline(mem_history, width=20, min_val=0, max_val=100)}")

        # Disk usage
        total_disk = sum(e.disk_used for e in executors)
        if total_disk > 0:
            lines.append(f"  Disk    {format_bytes(total_disk)}")

        # Task slots
        total_active = sum(e.active_tasks for e in executors)
        total_slots = sum(e.max_tasks or e.total_cores for e in executors)
        task_pct = (total_active / total_slots * 100) if total_slots > 0 else 0.0
        lines.append(f"  Tasks   {total_active}/{total_slots} slots ({task_pct:.1f}%)")

        # GC average
        total_gc = sum(e.total_gc_time_ms for e in executors)
        total_dur = sum(e.total_duration_ms for e in executors)
        gc_pct = (total_gc / total_dur * 100) if total_dur > 0 else 0.0
        lines.append(f"  GC avg  {gc_pct:.1f}%")

        # GC sparkline from ring buffer history
        gc_history: list[float] = []
        for snapshot in cache.get_history("executors"):
            if snapshot:
                snap_gc = sum(e.total_gc_time_ms for e in snapshot)
                snap_dur = sum(e.total_duration_ms for e in snapshot)
                if snap_dur > 0:
                    gc_history.append(snap_gc / snap_dur * 100)
        if gc_history:
            lines.append(f"  GC  {render_sparkline(gc_history, width=20, min_val=0)}")

        return "\n".join(lines)

    def _render_recommendations_panel(self, recommendations: List[Recommendation]) -> str:
        """Render prioritized recommendations as a numbered list.

        Args:
            recommendations: List of recommendations, pre-sorted by priority.

        Returns:
            Rich markup string for the recommendations panel.
        """
        lines = ["[bold]RECOMMENDATIONS[/bold]", ""]

        if not recommendations:
            lines.append("  [green]No recommendations -- cluster is healthy![/green]")
            return "\n".join(lines)

        # Sort by priority (ascending = highest priority first)
        sorted_recs = sorted(recommendations, key=lambda r: r.priority)

        # Show at most 5
        for i, rec in enumerate(sorted_recs[:5], start=1):
            lines.append(f"  [bold]{i}.[/bold] {rec.title}")
            lines.append(f"     {rec.description}")
            lines.append("")

        return "\n".join(lines)


# -- helpers ----------------------------------------------------------------


def _score_color(score: int) -> str:
    """Return a Rich color string for a component score.

    Args:
        score: Score value 0-100.

    Returns:
        Color name string.
    """
    if score >= 70:
        return "green"
    if score >= 40:
        return "yellow"
    return "red"


def _severity_icon(severity: Severity) -> str:
    """Return a Rich-styled severity icon.

    Args:
        severity: The insight severity level.

    Returns:
        Rich markup string for the icon.
    """
    if severity == Severity.CRITICAL:
        return "[red bold]![/red bold]"
    if severity == Severity.WARNING:
        return "[yellow bold]![/yellow bold]"
    return "[dim]i[/dim]"


def _progress_bar(pct: float, width: int = 14) -> str:
    """Render an inline ASCII progress bar.

    Args:
        pct: Percentage value 0-100.
        width: Total bar width in characters.

    Returns:
        String like ``[========      ]``.
    """
    filled = int(round(pct / 100 * width))
    filled = max(0, min(width, filled))
    return "[" + "=" * filled + " " * (width - filled) + "]"
