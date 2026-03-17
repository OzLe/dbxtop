"""Base view class.

Abstract base for all tabbed views, providing a common interface
for data refresh, layout composition, and event handling.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Sequence

from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Static

from dbxtop.api.cache import CacheSlot, DataCache


class BaseView(Widget):
    """Abstract base that all seven dashboard views extend.

    Provides sorting, text filtering, stale-data rendering,
    and a generic DataTable builder.

    Subclasses must implement :meth:`refresh_data`.
    """

    current_sort_key: reactive[str] = reactive("")
    sort_reverse: reactive[bool] = reactive(False)
    filter_text: reactive[str] = reactive("")

    @abstractmethod
    def refresh_data(self, cache: DataCache) -> None:
        """Re-render the view from the latest cache data.

        Called by the app whenever a ``DataUpdated`` message arrives
        whose ``updated_slots`` intersect with this view's data needs.

        Args:
            cache: The shared data cache.
        """

    # -- sorting helpers -----------------------------------------------------

    def cycle_sort(self, column: str) -> None:
        """Cycle the sort state for a column.

        If *column* is already the active sort key, toggle direction.
        Otherwise, switch to *column* in ascending order.

        Args:
            column: Column key to sort by.
        """
        if self.current_sort_key == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.current_sort_key = column
            self.sort_reverse = False

    def sort_rows(
        self,
        rows: List[Dict[str, Any]],
        sort_key: str,
        reverse: bool = False,
        pin_key: Optional[str] = None,
        pin_values: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Sort rows by *sort_key*, optionally pinning certain values to the top.

        Args:
            rows: List of row dicts.
            sort_key: Dict key to sort on.
            reverse: Whether to reverse the sort.
            pin_key: If set, rows whose *pin_key* value is in *pin_values*
                are pinned to the top (preserving relative order among them).
            pin_values: Values to pin to the top.

        Returns:
            A new sorted list.
        """
        if pin_key and pin_values:
            pin_set = set(pin_values)
            pinned = [r for r in rows if r.get(pin_key) in pin_set]
            rest = [r for r in rows if r.get(pin_key) not in pin_set]
        else:
            pinned = []
            rest = list(rows)

        def _sort_val(row: Dict[str, Any]) -> Any:
            v = row.get(sort_key, "")
            if v is None:
                return ""
            return v

        pinned.sort(key=_sort_val, reverse=reverse)
        rest.sort(key=_sort_val, reverse=reverse)
        return pinned + rest

    # -- filter helpers ------------------------------------------------------

    def apply_filter(self, text: str) -> None:
        """Set the filter text, triggering a re-render via reactivity.

        Args:
            text: Filter string (case-insensitive substring match).
        """
        self.filter_text = text.strip()

    def filter_rows(
        self,
        rows: List[Dict[str, Any]],
        text: str,
        search_keys: Sequence[str],
    ) -> List[Dict[str, Any]]:
        """Return rows matching *text* against any of the *search_keys*.

        Args:
            rows: List of row dicts.
            text: Case-insensitive substring to match.
            search_keys: Dict keys to search within.

        Returns:
            Filtered list of rows.
        """
        if not text:
            return rows
        lower = text.lower()
        return [r for r in rows if any(lower in str(r.get(k, "")).lower() for k in search_keys)]

    # -- stale indicator -----------------------------------------------------

    @staticmethod
    def render_stale_indicator(slot: CacheSlot[Any]) -> str:
        """Return a stale-data annotation string.

        Args:
            slot: The cache slot to check.

        Returns:
            ``'(stale: Xs ago)'`` if stale with a known age, ``'(stale)'``
            if stale with unknown age, or ``''`` if fresh.
        """
        if not slot.stale and slot.last_updated is not None:
            return ""
        if slot.stale and slot.error:
            return f"(stale: {slot.error})"
        return "(stale)" if slot.stale else "(no data)"

    # -- empty state ---------------------------------------------------------

    @staticmethod
    def empty_message(label: str = "data") -> Static:
        """Create a centered placeholder for empty/waiting states.

        Args:
            label: Noun to display (e.g. ``'jobs'``, ``'executors'``).

        Returns:
            A ``Static`` widget with the placeholder text.
        """
        return Static(f"Waiting for {label}...", classes="empty-state")
