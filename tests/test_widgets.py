"""Unit tests for widget pure functions and format utilities."""

from __future__ import annotations

from collections import deque

import pytest

from dbxtop.api.models import format_bytes, format_duration
from dbxtop.widgets.progress_cell import render_progress
from dbxtop.widgets.spark_line import render_sparkline
from dbxtop.widgets.status_indicator import (
    CLUSTER_STATES,
    JOB_STATES,
    STAGE_STATES,
    render_status,
)


# ---------------------------------------------------------------------------
# render_progress (plain text)
# ---------------------------------------------------------------------------


class TestRenderProgress:
    def test_half_done(self) -> None:
        result = render_progress(50, 100)
        assert "50/100" in result
        # 50% of 20-width = 10 filled chars; last one is ">" arrow
        assert ">" in result

    def test_full_completed(self) -> None:
        result = render_progress(100, 100)
        assert "100/100" in result
        # All 20 chars should be "=", no arrow since ok_chars == width
        assert ">" not in result
        assert "=" * 20 in result

    def test_with_failed_tasks(self) -> None:
        result = render_progress(60, 100, failed=20)
        assert "60/100" in result
        assert "!" in result

    def test_empty_zero_total(self) -> None:
        result = render_progress(0, 0)
        assert "0/0" in result
        # Bar should be all spaces
        assert "=" not in result
        assert "!" not in result

    def test_completed_exceeds_total(self) -> None:
        """Edge case: completed > total should clamp to 100%."""
        result = render_progress(120, 100)
        assert "120/100" in result
        # ok_ratio is clamped to 1.0, so full bar of "="
        assert "=" * 20 in result

    def test_custom_width(self) -> None:
        result = render_progress(50, 100, width=10)
        assert "50/100" in result
        # 50% of 10 = 5 filled chars
        assert len(result.split("]")[0].split("[")[1]) == 10

    def test_one_completed_of_many(self) -> None:
        result = render_progress(1, 1000, width=20)
        assert "1/1000" in result

    def test_negative_total_treated_as_zero(self) -> None:
        result = render_progress(5, -1)
        assert "0/0" in result


# ---------------------------------------------------------------------------
# render_sparkline
# ---------------------------------------------------------------------------


class TestRenderSparkline:
    def test_normal_sequence(self) -> None:
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = render_sparkline(values, width=10)
        assert len(result) == 10
        # Last value (5.0) should be the tallest block
        assert result.rstrip()[-1] == "\u2588"  # Full block character

    def test_empty_input(self) -> None:
        result = render_sparkline([], width=10)
        assert result == " " * 10

    def test_all_zeros(self) -> None:
        values = [0.0, 0.0, 0.0, 0.0]
        result = render_sparkline(values, width=10)
        assert len(result) == 10
        # With span=0, all chars map to idx=0 (lowest block)
        stripped = result.strip()
        for ch in stripped:
            assert ch == "\u2581"  # Lowest block

    def test_single_value(self) -> None:
        result = render_sparkline([42.0], width=5)
        assert len(result) == 5
        # Single value: span=0, so idx=0 -> lowest block
        assert result[-1] == "\u2581"

    def test_negative_values(self) -> None:
        values = [-5.0, -3.0, -1.0, 0.0, 2.0]
        result = render_sparkline(values, width=10)
        assert len(result) == 10
        # Should still produce valid sparkline, highest at the end
        stripped = result.strip()
        assert stripped[-1] == "\u2588"  # Full block for max value
        assert stripped[0] == "\u2581"  # Low block for min value

    def test_deque_input(self) -> None:
        values: deque[float] = deque([1.0, 2.0, 3.0], maxlen=60)
        result = render_sparkline(values, width=5)
        assert len(result) == 5

    def test_more_values_than_width(self) -> None:
        """Only the most recent `width` values should be used."""
        values = list(range(50))
        result = render_sparkline([float(v) for v in values], width=10)
        assert len(result) == 10

    def test_custom_min_max(self) -> None:
        values = [5.0, 5.0, 5.0]
        result = render_sparkline(values, width=5, min_val=0.0, max_val=10.0)
        assert len(result) == 5
        # 5.0 is exactly mid-range -> should map to a middle block
        stripped = result.strip()
        assert stripped[0] not in ("\u2581", "\u2588")  # Not min or max block

    def test_padding_when_fewer_values_than_width(self) -> None:
        values = [1.0, 2.0]
        result = render_sparkline(values, width=10)
        assert len(result) == 10
        # Leading spaces for padding
        assert result[:8] == " " * 8


# ---------------------------------------------------------------------------
# render_status
# ---------------------------------------------------------------------------


class TestRenderStatus:
    @pytest.mark.parametrize(
        "state,expected_colour,expected_symbol",
        [
            ("RUNNING", "green", "\u25cf"),
            ("PENDING", "yellow", "\u25cf"),
            ("RESTARTING", "yellow", "\u25cf"),
            ("RESIZING", "yellow", "\u25cf"),
            ("TERMINATING", "red", "\u25cb"),
            ("TERMINATED", "red", "\u25cb"),
            ("ERROR", "red", "\u2716"),
            ("UNKNOWN", "dim", "?"),
        ],
    )
    def test_cluster_states(self, state: str, expected_colour: str, expected_symbol: str) -> None:
        result = render_status(state, CLUSTER_STATES)
        assert f"[{expected_colour}]" in result
        assert expected_symbol in result
        assert state in result

    @pytest.mark.parametrize(
        "state,expected_colour",
        [
            ("RUNNING", "green"),
            ("SUCCEEDED", "dim"),
            ("FAILED", "red"),
            ("UNKNOWN", "yellow"),
        ],
    )
    def test_job_states(self, state: str, expected_colour: str) -> None:
        result = render_status(state, JOB_STATES)
        assert f"[{expected_colour}]" in result
        assert state in result

    @pytest.mark.parametrize(
        "state,expected_colour",
        [
            ("ACTIVE", "green"),
            ("COMPLETE", "dim"),
            ("PENDING", "yellow"),
            ("FAILED", "red"),
            ("SKIPPED", "dim"),
        ],
    )
    def test_stage_states(self, state: str, expected_colour: str) -> None:
        result = render_status(state, STAGE_STATES)
        assert f"[{expected_colour}]" in result
        assert state in result

    def test_unknown_state_falls_back(self) -> None:
        result = render_status("NONEXISTENT", CLUSTER_STATES)
        assert "[dim]" in result
        assert "?" in result
        assert "NONEXISTENT" in result

    def test_output_format(self) -> None:
        result = render_status("RUNNING", CLUSTER_STATES)
        assert result == "[green]\u25cf RUNNING[/green]"


# ---------------------------------------------------------------------------
# format_bytes (additional edge cases beyond test_models.py)
# ---------------------------------------------------------------------------


class TestFormatBytesWidget:
    def test_zero(self) -> None:
        assert format_bytes(0) == "0 B"

    def test_one_byte(self) -> None:
        assert format_bytes(1) == "1 B"

    def test_one_kb(self) -> None:
        assert format_bytes(1024) == "1.0 KB"

    def test_one_gb(self) -> None:
        assert format_bytes(1073741824) == "1.0 GB"

    def test_negative(self) -> None:
        assert format_bytes(-1024) == "-1.0 KB"

    def test_negative_large(self) -> None:
        assert format_bytes(-1073741824) == "-1.0 GB"

    def test_very_large_petabytes(self) -> None:
        result = format_bytes(1024**5)
        assert "PB" in result
        assert "1.0" in result

    def test_very_large_beyond_petabytes(self) -> None:
        # 1024^6 = 1 exabyte, but PB is the max unit; should show as 1024.0 PB
        result = format_bytes(1024**6)
        assert "PB" in result

    def test_exact_512_bytes(self) -> None:
        assert format_bytes(512) == "512 B"

    def test_fractional_display(self) -> None:
        # 1.5 GB
        result = format_bytes(int(1.5 * 1024**3))
        assert "1.5 GB" in result


# ---------------------------------------------------------------------------
# format_duration (additional edge cases beyond test_models.py)
# ---------------------------------------------------------------------------


class TestFormatDurationWidget:
    def test_zero(self) -> None:
        assert format_duration(0) == "0s"

    def test_sub_second(self) -> None:
        assert format_duration(500) == "0s"

    def test_one_minute_thirty_seconds(self) -> None:
        assert format_duration(90_000) == "1m 30s"

    def test_one_day_exact(self) -> None:
        assert format_duration(86_400_000) == "1d"

    def test_negative_value(self) -> None:
        assert format_duration(-90_000) == "-1m 30s"

    def test_negative_sub_second(self) -> None:
        assert format_duration(-500) == "-0s"

    def test_large_duration_days_hours_minutes(self) -> None:
        # 2 days, 3 hours, 45 minutes, 10 seconds
        ms = (2 * 86400 + 3 * 3600 + 45 * 60 + 10) * 1000
        assert format_duration(ms) == "2d 3h 45m 10s"

    def test_exactly_one_hour(self) -> None:
        assert format_duration(3_600_000) == "1h"

    def test_just_seconds(self) -> None:
        assert format_duration(5_000) == "5s"

    def test_minutes_no_seconds(self) -> None:
        assert format_duration(120_000) == "2m"

    def test_999_ms(self) -> None:
        """999ms rounds down to 0 seconds."""
        assert format_duration(999) == "0s"
