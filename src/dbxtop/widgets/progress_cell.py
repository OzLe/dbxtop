"""Progress bar cell widget.

Renders an inline progress bar for use within DataTable cells,
showing task or stage completion percentage.
"""

from __future__ import annotations


def render_progress(completed: int, total: int, failed: int = 0, width: int = 20) -> str:
    """Render an inline text progress bar.

    Args:
        completed: Number of completed items.
        total: Total number of items.
        failed: Number of failed items (shown in red portion).
        width: Character width of the bar (excluding brackets and counts).

    Returns:
        A string like ``'[=========>   ] 847/1000'``.
    """
    if total <= 0:
        bar = " " * width
        return f"\\[{bar}] 0/0"

    ok_ratio = min(completed / total, 1.0)
    fail_ratio = min(failed / total, 1.0) if failed else 0.0

    ok_chars = int(ok_ratio * width)
    fail_chars = int(fail_ratio * width)
    empty_chars = width - ok_chars - fail_chars

    # Build bar segments
    ok_segment = "=" * ok_chars
    fail_segment = "!" * fail_chars
    empty_segment = " " * empty_chars

    # Add arrow head at the progress front when in progress
    if ok_chars > 0 and ok_chars < width and fail_chars == 0:
        ok_segment = ok_segment[:-1] + ">"

    bar = ok_segment + fail_segment + empty_segment

    count_str = f"{completed}/{total}"
    return f"\\[{bar}] {count_str}"


def render_progress_rich(completed: int, total: int, failed: int = 0, width: int = 20) -> str:
    """Render a Rich-markup progress bar with colour.

    Args:
        completed: Number of completed items.
        total: Total number of items.
        failed: Number of failed items (red portion).
        width: Character width of the bar.

    Returns:
        A Rich-markup string with green/red colouring.
    """
    if total <= 0:
        bar = " " * width
        return f"\\[{bar}] 0/0"

    ok_ratio = min(completed / total, 1.0)
    fail_ratio = min(failed / total, 1.0) if failed else 0.0

    ok_chars = int(ok_ratio * width)
    fail_chars = int(fail_ratio * width)
    empty_chars = width - ok_chars - fail_chars

    ok_segment = "=" * ok_chars
    fail_segment = "!" * fail_chars
    empty_segment = " " * empty_chars

    if ok_chars > 0 and ok_chars < width and fail_chars == 0:
        ok_segment = ok_segment[:-1] + ">"

    parts: list[str] = []
    if ok_segment:
        colour = "green" if not failed else "yellow"
        parts.append(f"[{colour}]{ok_segment}[/{colour}]")
    if fail_segment:
        parts.append(f"[red]{fail_segment}[/red]")
    if empty_segment:
        parts.append(empty_segment)

    bar = "".join(parts)
    count_str = f"{completed}/{total}"
    return f"\\[{bar}] {count_str}"
