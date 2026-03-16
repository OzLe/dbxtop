"""Status badge widget.

Renders a colored status indicator for cluster state (RUNNING,
PENDING, TERMINATED, etc.) and job/stage status.
"""

from __future__ import annotations

from typing import Dict, Tuple

# Predefined state maps: state_name → (colour, symbol)

CLUSTER_STATES: Dict[str, Tuple[str, str]] = {
    "RUNNING": ("green", "●"),
    "PENDING": ("yellow", "●"),
    "RESTARTING": ("yellow", "●"),
    "RESIZING": ("yellow", "●"),
    "TERMINATING": ("red", "○"),
    "TERMINATED": ("red", "○"),
    "ERROR": ("red", "✖"),
    "UNKNOWN": ("dim", "?"),
}

JOB_STATES: Dict[str, Tuple[str, str]] = {
    "RUNNING": ("green", "●"),
    "SUCCEEDED": ("dim", "●"),
    "FAILED": ("red", "●"),
    "UNKNOWN": ("yellow", "○"),
}

STAGE_STATES: Dict[str, Tuple[str, str]] = {
    "ACTIVE": ("green", "●"),
    "COMPLETE": ("dim", "●"),
    "PENDING": ("yellow", "○"),
    "FAILED": ("red", "●"),
    "SKIPPED": ("dim", "○"),
}


def render_status(state: str, state_map: Dict[str, Tuple[str, str]]) -> str:
    """Render a Rich-markup status badge.

    Args:
        state: The state string (e.g. ``'RUNNING'``).
        state_map: Mapping of state names to ``(colour, symbol)`` tuples.

    Returns:
        A Rich-markup string like ``'[green]● RUNNING[/green]'``.
    """
    colour, symbol = state_map.get(state, ("dim", "?"))
    return f"[{colour}]{symbol} {state}[/{colour}]"
