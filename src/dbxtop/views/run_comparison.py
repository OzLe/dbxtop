"""Side-by-side run comparison screen.

Compares two saved monitoring runs showing health score deltas,
component score diffs, insight diffs (resolved/new/persistent),
and Spark configuration changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Static

from dbxtop.analytics.run import AccumulatedInsightExport, RunSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Comparison model
# ---------------------------------------------------------------------------


@dataclass
class RunComparison:
    """Result of comparing two monitoring runs."""

    run_a: RunSession
    run_b: RunSession

    health_score_a: int = 0
    health_score_b: int = 0
    health_delta: int = 0

    avg_score_a: float = 0.0
    avg_score_b: float = 0.0
    worst_score_a: int = 0
    worst_score_b: int = 0

    component_deltas: Dict[str, Tuple[int, int, int]] = field(default_factory=dict)
    """key -> (score_a, score_b, delta)."""

    resolved_insights: List[AccumulatedInsightExport] = field(default_factory=list)
    """Insights present in run A but not run B (resolved)."""

    new_insights: List[AccumulatedInsightExport] = field(default_factory=list)
    """Insights present in run B but not run A (new)."""

    persistent_insights: List[Tuple[AccumulatedInsightExport, AccumulatedInsightExport]] = field(default_factory=list)
    """Insights present in both runs, as (from_a, from_b) pairs."""

    config_changes: List[Tuple[str, str, str]] = field(default_factory=list)
    """(key, value_a, value_b) for changed config keys."""


def compare_runs(run_a: RunSession, run_b: RunSession) -> RunComparison:
    """Compare two runs and produce a diff.

    Args:
        run_a: The "before" run.
        run_b: The "after" run.

    Returns:
        A RunComparison with all computed deltas.
    """
    # Health scores from final snapshot
    health_a = run_a.snapshots[-1].health.score if run_a.snapshots else 0
    health_b = run_b.snapshots[-1].health.score if run_b.snapshots else 0

    # Component scores from final snapshot
    comp_a = run_a.snapshots[-1].health.component_scores if run_a.snapshots else {}
    comp_b = run_b.snapshots[-1].health.component_scores if run_b.snapshots else {}
    all_keys = sorted(set(comp_a.keys()) | set(comp_b.keys()))
    component_deltas: Dict[str, Tuple[int, int, int]] = {}
    for key in all_keys:
        sa = comp_a.get(key, 100)
        sb = comp_b.get(key, 100)
        component_deltas[key] = (sa, sb, sb - sa)

    # Insight diff by (category, affected_entity, title)
    insights_a = {(i.category, i.affected_entity, i.title): i for i in run_a.accumulated_insights}
    insights_b = {(i.category, i.affected_entity, i.title): i for i in run_b.accumulated_insights}

    resolved = [insights_a[k] for k in insights_a if k not in insights_b]
    new = [insights_b[k] for k in insights_b if k not in insights_a]
    persistent = [(insights_a[k], insights_b[k]) for k in insights_a if k in insights_b]

    # Config diff
    all_config_keys = sorted(set(run_a.config_snapshot.keys()) | set(run_b.config_snapshot.keys()))
    config_changes: List[Tuple[str, str, str]] = []
    for key in all_config_keys:
        va = run_a.config_snapshot.get(key, "(unset)")
        vb = run_b.config_snapshot.get(key, "(unset)")
        if va != vb:
            config_changes.append((key, va, vb))

    return RunComparison(
        run_a=run_a,
        run_b=run_b,
        health_score_a=health_a,
        health_score_b=health_b,
        health_delta=health_b - health_a,
        avg_score_a=run_a.avg_health_score,
        avg_score_b=run_b.avg_health_score,
        worst_score_a=run_a.worst_health_score,
        worst_score_b=run_b.worst_health_score,
        component_deltas=component_deltas,
        resolved_insights=resolved,
        new_insights=new,
        persistent_insights=persistent,
        config_changes=config_changes,
    )


# ---------------------------------------------------------------------------
# Comparison screen
# ---------------------------------------------------------------------------

_COMPONENT_DISPLAY: Dict[str, str] = {
    "gc": "GC",
    "spill": "Spill",
    "skew": "Skew",
    "utilization": "Utilization",
    "shuffle": "Shuffle",
    "task_failures": "Failures",
    "config": "Config",
    "driver": "Driver",
    "io_efficiency": "I/O Eff.",
    "stability": "Stability",
}


class RunComparisonScreen(ModalScreen[None]):
    """Full-screen modal showing side-by-side comparison of two runs."""

    BINDINGS = [Binding("escape", "dismiss", "Close")]

    DEFAULT_CSS = """
    RunComparisonScreen {
        align: center middle;
    }
    RunComparisonScreen > #comparison-container {
        width: 85;
        height: 85%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
        overflow-y: auto;
    }
    """

    def __init__(self, comparison: RunComparison, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._comp = comparison

    def compose(self) -> ComposeResult:
        yield Static(self._render_comparison(), id="comparison-container")

    def _render_comparison(self) -> str:
        """Render the full comparison as Rich markup."""
        c = self._comp
        lines: List[str] = []

        # Title
        lines.append(f'[bold]Run Comparison:[/bold] "{c.run_a.name}" vs "{c.run_b.name}"')
        lines.append("")

        # Health scores
        lines.append("[bold]HEALTH SCORES[/bold]")
        lines.append("")
        lines.append(f"  {c.run_a.name:<20s}  {c.health_score_a}")
        delta_str = _format_delta(c.health_delta)
        lines.append(f"  {c.run_b.name:<20s}  {c.health_score_b}  ({delta_str})")
        lines.append("")

        avg_delta = c.avg_score_b - c.avg_score_a
        worst_delta = c.worst_score_b - c.worst_score_a
        lines.append(f"  Avg:   {c.avg_score_a:.0f} -> {c.avg_score_b:.0f}  ({_format_delta(avg_delta)})")
        lines.append(f"  Worst: {c.worst_score_a} -> {c.worst_score_b}  ({_format_delta(worst_delta)})")
        lines.append("")

        # Component scores
        if c.component_deltas:
            lines.append("[bold]COMPONENT SCORES[/bold]")
            lines.append("")
            lines.append(f"  {'Component':<14s}  {'Before':>6s}  {'After':>6s}  {'Delta':>8s}")
            lines.append(f"  {'─' * 14}  {'─' * 6}  {'─' * 6}  {'─' * 8}")
            for key, (sa, sb, delta) in c.component_deltas.items():
                display = _COMPONENT_DISPLAY.get(key, key.title())
                lines.append(f"  {display:<14s}  {sa:>6d}  {sb:>6d}  {_format_delta(delta):>8s}")
            lines.append("")

        # Insights diff
        lines.append("[bold]INSIGHTS DIFF[/bold]")
        lines.append("")

        if c.resolved_insights:
            lines.append(f"  [green bold]Resolved ({len(c.resolved_insights)}):[/green bold]")
            for ins in c.resolved_insights[:10]:
                sev = f"[dim]{ins.peak_severity}[/dim]"
                lines.append(f"    [green]✓[/green] {ins.title}  {sev}")
            if len(c.resolved_insights) > 10:
                lines.append(f"    ... and {len(c.resolved_insights) - 10} more")
            lines.append("")

        if c.new_insights:
            lines.append(f"  [red bold]New ({len(c.new_insights)}):[/red bold]")
            for ins in c.new_insights[:10]:
                sev_color = "red" if ins.severity == "CRITICAL" else "yellow"
                lines.append(f"    [{sev_color}]✗[/{sev_color}] {ins.title}  [{sev_color}]{ins.severity}[/{sev_color}]")
            if len(c.new_insights) > 10:
                lines.append(f"    ... and {len(c.new_insights) - 10} more")
            lines.append("")

        if c.persistent_insights:
            lines.append(f"  [yellow bold]Persistent ({len(c.persistent_insights)}):[/yellow bold]")
            for ins_a, ins_b in c.persistent_insights[:10]:
                metric_delta = ins_b.metric_value - ins_a.metric_value
                delta_str = _format_delta(metric_delta, precision=1)
                lines.append(f"    = {ins_b.title}  ({delta_str})")
            if len(c.persistent_insights) > 10:
                lines.append(f"    ... and {len(c.persistent_insights) - 10} more")
            lines.append("")

        if not c.resolved_insights and not c.new_insights and not c.persistent_insights:
            lines.append("  [dim]No insight differences[/dim]")
            lines.append("")

        # Config diff
        if c.config_changes:
            lines.append("[bold]CONFIG DIFF[/bold]")
            lines.append("")
            for key, va, vb in c.config_changes[:20]:
                # Truncate long values
                va_short = va[:25] + "..." if len(va) > 28 else va
                vb_short = vb[:25] + "..." if len(vb) > 28 else vb
                padding = "." * max(1, 40 - len(key))
                lines.append(f"  {key} {padding} {va_short} -> {vb_short}")
            if len(c.config_changes) > 20:
                lines.append(f"  ... and {len(c.config_changes) - 20} more")
            lines.append("")

        lines.append("[dim]Press Esc to close[/dim]")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_delta(delta: float, precision: int = 0) -> str:
    """Format a numeric delta with arrow and color.

    Args:
        delta: The difference (positive = improvement).
        precision: Decimal places.

    Returns:
        Rich markup string like ``[green]+13 ↑[/green]`` or ``[red]-5 ↓[/red]``.
    """
    if precision > 0:
        fmt = f".{precision}f"
    else:
        fmt = ".0f"

    if delta > 0:
        return f"[green]+{delta:{fmt}} ↑[/green]"
    elif delta < 0:
        return f"[red]{delta:{fmt}} ↓[/red]"
    else:
        return "[dim]0 =[/dim]"
