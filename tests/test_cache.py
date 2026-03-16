"""Unit tests for the DataCache layer."""

from __future__ import annotations

from datetime import datetime, timezone

from dbxtop.api.cache import MAX_RING_BUFFER_SIZE, DataCache


class TestDataCacheUpdate:
    def test_update_stores_data(self) -> None:
        cache = DataCache()
        cache.update("cluster", {"fake": "data"})
        slot = cache.get("cluster")
        assert slot.data == {"fake": "data"}
        assert slot.error is None
        assert slot.stale is False
        assert slot.last_updated is not None

    def test_update_clears_error(self) -> None:
        cache = DataCache()
        cache.mark_error("cluster", "something broke")
        cache.update("cluster", "fresh-data")
        slot = cache.get("cluster")
        assert slot.error is None
        assert slot.stale is False
        assert slot.data == "fresh-data"

    def test_update_appends_to_history(self) -> None:
        cache = DataCache()
        for i in range(5):
            cache.update("executors", [f"exec-{i}"])
        history = cache.get_history("executors")
        assert len(history) == 5
        assert history[0] == ["exec-0"]
        assert history[-1] == ["exec-4"]

    def test_unknown_slot_raises_key_error(self) -> None:
        cache = DataCache()
        try:
            cache.update("nonexistent", "data")
            assert False, "Expected KeyError"
        except KeyError:
            pass


class TestDataCacheMarkError:
    def test_mark_error_preserves_data(self) -> None:
        cache = DataCache()
        cache.update("stages", ["stage-1"])
        cache.mark_error("stages", "timeout")
        slot = cache.get("stages")
        assert slot.data == ["stage-1"]
        assert slot.stale is True
        assert slot.error == "timeout"

    def test_mark_error_on_empty_slot(self) -> None:
        cache = DataCache()
        cache.mark_error("cluster", "no data yet")
        slot = cache.get("cluster")
        assert slot.data is None
        assert slot.stale is True


class TestDataCacheIsStale:
    def test_stale_when_never_updated(self) -> None:
        cache = DataCache()
        assert cache.is_stale("cluster", 15.0) is True

    def test_not_stale_after_fresh_update(self) -> None:
        cache = DataCache()
        cache.update("cluster", "data")
        assert cache.is_stale("cluster", 15.0) is False

    def test_stale_when_error_flagged(self) -> None:
        cache = DataCache()
        cache.update("cluster", "data")
        cache.mark_error("cluster", "err")
        assert cache.is_stale("cluster", 15.0) is True

    def test_stale_when_too_old(self) -> None:
        cache = DataCache()
        cache.update("cluster", "data")
        # Simulate time passing: set last_updated to 60s ago
        old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        cache.get("cluster").last_updated = old_time
        assert cache.is_stale("cluster", 15.0) is True


class TestRingBuffer:
    def test_ring_buffer_max_size(self) -> None:
        cache = DataCache()
        for i in range(MAX_RING_BUFFER_SIZE + 10):
            cache.update("executors", f"sample-{i}")
        history = cache.get_history("executors")
        assert len(history) == MAX_RING_BUFFER_SIZE
        # Oldest samples evicted — first item should be sample-10
        assert history[0] == "sample-10"
        assert history[-1] == f"sample-{MAX_RING_BUFFER_SIZE + 9}"


class TestSlotNames:
    def test_all_slots_present(self) -> None:
        cache = DataCache()
        expected = {
            "cluster",
            "events",
            "spark_jobs",
            "stages",
            "executors",
            "sql_queries",
            "storage",
            "job_runs",
            "libraries",
        }
        assert set(cache.slot_names) == expected
