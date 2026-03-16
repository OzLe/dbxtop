"""Sparkline widget.

Renders a compact Unicode sparkline chart for time-series metrics
such as task throughput, memory usage, or shuffle bytes over time.
"""

from __future__ import annotations

from typing import Deque, Optional, Sequence, Union

_BLOCKS = "▁▂▃▄▅▆▇█"


def render_sparkline(
    values: Union[Sequence[float], Deque[float]],
    width: int = 20,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> str:
    """Render a compact Unicode sparkline string.

    Args:
        values: Time-series data points.
        width: Number of characters in the output.
        min_val: Floor for scaling (auto-detected if ``None``).
        max_val: Ceiling for scaling (auto-detected if ``None``).

    Returns:
        A string of Unicode block characters (length ≤ *width*).
    """
    if not values:
        return " " * width

    # Take the most recent *width* values
    recent = list(values)[-width:]

    lo = min_val if min_val is not None else min(recent)
    hi = max_val if max_val is not None else max(recent)
    span = hi - lo

    chars: list[str] = []
    for v in recent:
        if span <= 0:
            idx = 0
        else:
            ratio = (v - lo) / span
            idx = int(ratio * (len(_BLOCKS) - 1))
            idx = max(0, min(idx, len(_BLOCKS) - 1))
        chars.append(_BLOCKS[idx])

    # Pad to width if fewer values than width
    result = "".join(chars)
    if len(result) < width:
        result = " " * (width - len(result)) + result
    return result


def render_sparkline_rich(
    values: Union[Sequence[float], Deque[float]],
    width: int = 20,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    rising_colour: str = "red",
    falling_colour: str = "green",
) -> str:
    """Render a sparkline with Rich colour based on trend.

    Args:
        values: Time-series data points.
        width: Number of characters in the output.
        min_val: Floor for scaling.
        max_val: Ceiling for scaling.
        rising_colour: Colour when the latest value is above the mean.
        falling_colour: Colour when the latest value is at or below the mean.

    Returns:
        A Rich-markup sparkline string.
    """
    line = render_sparkline(values, width, min_val, max_val)
    if not values:
        return line

    vals = list(values)
    mean = sum(vals) / len(vals) if vals else 0
    latest = vals[-1]
    colour = rising_colour if latest > mean else falling_colour
    return f"[{colour}]{line}[/{colour}]"
