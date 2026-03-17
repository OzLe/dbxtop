"""Comprehensive unit tests for RunManager.

Covers the full run lifecycle: start, on_report, stop, persistence,
listing, loading, deleting, and sensitive config filtering.
"""

from __future__ import annotations

import json
import os
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

from dbxtop.analytics.accumulator import InsightAccumulator
from dbxtop.analytics.models import (
    DiagnosticReport,
    HealthScore,
    Insight,
    InsightCategory,
    Severity,
)
from dbxtop.analytics.run import RunSession
from dbxtop.analytics.run_manager import (
    SNAPSHOT_INTERVAL_S,
    RunManager,
    _export_insight,
    filter_sensitive_config,
)
from dbxtop.api.models import ExecutorInfo


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

CLUSTER_ID = "0123-456789-abcdefgh"
FIXED_NOW = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)


def make_executor(
    executor_id: str = "1",
    *,
    is_active: bool = True,
) -> ExecutorInfo:
    """Create a minimal ExecutorInfo for run manager tests."""
    return ExecutorInfo(
        executor_id=executor_id,
        is_active=is_active,
        host_port=f"10.0.0.{executor_id}:42000",
        total_cores=4,
        max_tasks=4,
    )


def make_report(
    timestamp: datetime = FIXED_NOW,
    score: int = 80,
    insights: List[Insight] | None = None,
) -> DiagnosticReport:
    """Create a DiagnosticReport with sensible defaults."""
    return DiagnosticReport(
        health=HealthScore(score=score, label="Good", color="green"),
        insights=insights or [],
        timestamp=timestamp,
    )


def make_insight(
    category: InsightCategory = InsightCategory.GC,
    severity: Severity = Severity.WARNING,
    entity: str = "exec-1",
    metric_value: float = 0.15,
) -> Insight:
    """Create a minimal Insight for testing."""
    return Insight(
        id=f"{category.value}_001",
        category=category,
        severity=severity,
        title=f"Test {category.value} insight",
        description="Test description.",
        metric_value=metric_value,
        threshold_value=0.1,
        recommendation="Fix it.",
        affected_entity=entity,
    )


def _make_saved_run(
    run_dir: Path,
    run_id: str,
    name: str,
    started_at: datetime,
    stopped_at: datetime | None = None,
) -> Path:
    """Write a RunSession JSON file to disk and return its path."""
    run = RunSession(
        run_id=run_id,
        name=name,
        cluster_id=CLUSTER_ID,
        started_at=started_at,
        stopped_at=stopped_at,
    )
    path = run_dir / f"{run_id}.json"
    path.write_text(run.model_dump_json(indent=2))
    return path


# ---------------------------------------------------------------------------
# filter_sensitive_config
# ---------------------------------------------------------------------------


class TestFilterSensitiveConfig:
    """Tests for the filter_sensitive_config helper function."""

    def test_removes_key_with_password(self) -> None:
        """Config entries containing 'password' in the key are removed."""
        config = {"spark.sql.shuffle.partitions": "200", "fs.azure.password": "s3cret"}
        result = filter_sensitive_config(config)
        assert "fs.azure.password" not in result
        assert result["spark.sql.shuffle.partitions"] == "200"

    def test_removes_key_with_token(self) -> None:
        """Config entries containing 'token' in the key are removed."""
        config = {"spark.databricks.token": "dapi123", "spark.executor.memory": "4g"}
        result = filter_sensitive_config(config)
        assert "spark.databricks.token" not in result
        assert "spark.executor.memory" in result

    def test_removes_key_with_secret(self) -> None:
        """Config entries containing 'secret' in the key are removed."""
        config = {"spark.hadoop.fs.s3a.secret.key": "abc123"}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_removes_key_with_credential(self) -> None:
        """Config entries containing 'credential' in the key are removed."""
        config = {"azure.credential.provider": "managed-identity"}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_removes_key_with_account_key(self) -> None:
        """Config entries containing 'account.key' in the key are removed."""
        config = {"fs.azure.account.key.myaccount": "base64=="}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_removes_key_with_sas(self) -> None:
        """Config entries containing 'sas' in the key are removed."""
        config = {"fs.azure.sas.token.provider": "org.apache.hadoop"}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_removes_key_with_connection_string(self) -> None:
        """Config entries containing 'connection.string' in the key are removed."""
        config = {"spark.jdbc.connection.string": "jdbc://host:port/db"}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_case_insensitive(self) -> None:
        """Pattern matching is case-insensitive."""
        config = {"spark.SECRET.KEY": "abc", "fs.azure.PASSWORD": "xyz"}
        result = filter_sensitive_config(config)
        assert len(result) == 0

    def test_preserves_safe_keys(self) -> None:
        """Non-sensitive keys are preserved."""
        config = {
            "spark.sql.shuffle.partitions": "200",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
        }
        result = filter_sensitive_config(config)
        assert result == config

    def test_empty_config(self) -> None:
        """Empty input returns empty output."""
        assert filter_sensitive_config({}) == {}

    def test_returns_copy(self) -> None:
        """Result is a new dict, not a mutation of the input."""
        config: Dict[str, str] = {"spark.executor.memory": "4g"}
        result = filter_sensitive_config(config)
        assert result is not config


# ---------------------------------------------------------------------------
# RunManager.start_run
# ---------------------------------------------------------------------------


class TestStartRun:
    """Tests for RunManager.start_run."""

    def test_creates_run_session(self) -> None:
        """start_run returns a RunSession with correct fields."""
        mgr = RunManager(CLUSTER_ID)
        run = mgr.start_run("my-run", {"spark.executor.memory": "4g"})
        assert run.name == "my-run"
        assert run.cluster_id == CLUSTER_ID
        assert run.config_snapshot == {"spark.executor.memory": "4g"}
        assert run.stopped_at is None

    def test_sets_active_run(self) -> None:
        """After start_run, is_recording is True and active_run is set."""
        mgr = RunManager(CLUSTER_ID)
        run = mgr.start_run("my-run", {})
        assert mgr.is_recording
        assert mgr.active_run is run

    def test_creates_accumulator(self) -> None:
        """start_run creates a fresh InsightAccumulator."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("my-run", {})
        assert mgr.accumulator is not None
        assert isinstance(mgr.accumulator, InsightAccumulator)

    def test_resets_last_snapshot(self) -> None:
        """start_run resets the internal _last_snapshot to None."""
        mgr = RunManager(CLUSTER_ID)
        # Simulate a prior run that set _last_snapshot
        mgr._last_snapshot = FIXED_NOW
        mgr.start_run("my-run", {})
        assert mgr._last_snapshot is None

    def test_generates_unique_run_id(self) -> None:
        """Each start_run generates a different run_id."""
        mgr = RunManager(CLUSTER_ID)
        run1 = mgr.start_run("run-1", {})
        mgr._active_run = None  # Reset without full stop
        run2 = mgr.start_run("run-2", {})
        assert run1.run_id != run2.run_id

    @patch("dbxtop.analytics.run_manager.datetime")
    def test_started_at_uses_utc_now(self, mock_dt: object) -> None:
        """started_at is set to datetime.now(timezone.utc)."""
        # We need to allow datetime() constructor calls to work normally for RunSession,
        # but mock datetime.now() for the timestamp.
        import dbxtop.analytics.run_manager as rm_module

        class MockDatetime(datetime):
            @classmethod  # type: ignore[override]
            def now(cls, tz: object = None) -> datetime:
                return FIXED_NOW

        with patch.object(rm_module, "datetime", MockDatetime):
            mgr = RunManager(CLUSTER_ID)
            run = mgr.start_run("my-run", {})
            assert run.started_at == FIXED_NOW


# ---------------------------------------------------------------------------
# RunManager.on_report
# ---------------------------------------------------------------------------


class TestOnReport:
    """Tests for RunManager.on_report."""

    def test_noop_when_no_active_run(self) -> None:
        """on_report does nothing when there is no active run."""
        mgr = RunManager(CLUSTER_ID)
        report = make_report()
        # Should not raise
        mgr.on_report(report, [make_executor()])

    def test_accumulates_health_history(self) -> None:
        """Each on_report call appends to health_history."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        t1 = FIXED_NOW
        t2 = FIXED_NOW + timedelta(seconds=3)
        mgr.on_report(make_report(timestamp=t1, score=80), [])
        mgr.on_report(make_report(timestamp=t2, score=60), [])
        assert mgr.active_run is not None
        assert len(mgr.active_run.health_history) == 2
        assert mgr.active_run.health_history[0] == (t1, 80)
        assert mgr.active_run.health_history[1] == (t2, 60)

    def test_accumulates_executor_counts(self) -> None:
        """Each on_report call appends the active executor count."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        executors = [make_executor("1"), make_executor("2"), make_executor("3", is_active=False)]
        mgr.on_report(make_report(), executors)
        assert mgr.active_run is not None
        assert len(mgr.active_run.executor_count_history) == 1
        # Only 2 active executors
        assert mgr.active_run.executor_count_history[0][1] == 2

    def test_takes_snapshot_on_first_report(self) -> None:
        """The first on_report call captures a full diagnostic snapshot."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        report = make_report()
        mgr.on_report(report, [])
        assert mgr.active_run is not None
        assert len(mgr.active_run.snapshots) == 1
        assert mgr.active_run.snapshots[0] is report

    def test_respects_snapshot_interval(self) -> None:
        """Snapshots are taken only after SNAPSHOT_INTERVAL_S has elapsed."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        t0 = FIXED_NOW
        # First report: snapshot taken
        mgr.on_report(make_report(timestamp=t0), [])
        assert mgr.active_run is not None
        assert len(mgr.active_run.snapshots) == 1

        # Second report at t0 + 10s: too soon, no new snapshot
        t1 = t0 + timedelta(seconds=10)
        mgr.on_report(make_report(timestamp=t1), [])
        assert len(mgr.active_run.snapshots) == 1

        # Third report at t0 + SNAPSHOT_INTERVAL_S: snapshot taken
        t2 = t0 + timedelta(seconds=SNAPSHOT_INTERVAL_S)
        mgr.on_report(make_report(timestamp=t2), [])
        assert len(mgr.active_run.snapshots) == 2

    def test_updates_accumulator(self) -> None:
        """on_report passes insights to the InsightAccumulator."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        insight = make_insight()
        report = make_report(insights=[insight])
        mgr.on_report(report, [])
        assert mgr.accumulator is not None
        assert mgr.accumulator.active_count == 1

    def test_zero_active_executors(self) -> None:
        """When all executors are inactive, count is 0."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        executors = [make_executor("1", is_active=False)]
        mgr.on_report(make_report(), executors)
        assert mgr.active_run is not None
        assert mgr.active_run.executor_count_history[0][1] == 0

    def test_empty_executors_list(self) -> None:
        """When executors list is empty, count is 0."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        mgr.on_report(make_report(), [])
        assert mgr.active_run is not None
        assert mgr.active_run.executor_count_history[0][1] == 0


# ---------------------------------------------------------------------------
# RunManager.stop_run
# ---------------------------------------------------------------------------


class TestStopRun:
    """Tests for RunManager.stop_run."""

    def test_returns_none_when_no_active_run(self) -> None:
        """stop_run returns None when no run is active."""
        mgr = RunManager(CLUSTER_ID)
        assert mgr.stop_run() is None

    def test_sets_stopped_at(self, tmp_path: Path) -> None:
        """stop_run sets stopped_at on the run session."""
        stop_time = FIXED_NOW + timedelta(minutes=30)

        import dbxtop.analytics.run_manager as rm_module

        class MockDatetime(datetime):
            @classmethod  # type: ignore[override]
            def now(cls, tz: object = None) -> datetime:
                return stop_time

        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        with (
            patch.object(rm_module, "datetime", MockDatetime),
            patch.object(mgr, "_save_run"),
        ):
            run = mgr.stop_run()

        assert run is not None
        assert run.stopped_at == stop_time

    def test_exports_accumulated_insights(self, tmp_path: Path) -> None:
        """stop_run exports accumulated insights from the accumulator."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        # Feed an insight through on_report so the accumulator has data
        insight = make_insight(category=InsightCategory.GC)
        report = make_report(insights=[insight])
        mgr.on_report(report, [])

        with patch.object(mgr, "_save_run"):
            run = mgr.stop_run()

        assert run is not None
        assert len(run.accumulated_insights) == 1
        assert run.accumulated_insights[0].category == "GC"

    def test_resets_state_after_stop(self, tmp_path: Path) -> None:
        """After stop_run, is_recording is False and accumulator is None."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        with patch.object(mgr, "_save_run"):
            mgr.stop_run()

        assert not mgr.is_recording
        assert mgr.active_run is None
        assert mgr.accumulator is None

    def test_calls_save_run(self) -> None:
        """stop_run calls _save_run with the finalized session."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        with patch.object(mgr, "_save_run") as mock_save:
            run = mgr.stop_run()
            mock_save.assert_called_once_with(run)

    def test_handles_save_failure_gracefully(self) -> None:
        """When _save_run raises, stop_run still returns the run and resets state."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})

        with patch.object(mgr, "_save_run", side_effect=OSError("disk full")):
            run = mgr.stop_run()

        assert run is not None
        assert run.name == "test"
        # State is still reset despite save failure
        assert not mgr.is_recording
        assert mgr.active_run is None
        assert mgr.accumulator is None

    def test_returns_finalized_run(self) -> None:
        """stop_run returns the run with stopped_at and insights populated."""
        mgr = RunManager(CLUSTER_ID)
        mgr.start_run("test", {})
        mgr.on_report(make_report(insights=[make_insight()]), [make_executor()])

        with patch.object(mgr, "_save_run"):
            run = mgr.stop_run()

        assert run is not None
        assert run.stopped_at is not None
        assert len(run.health_history) == 1
        assert len(run.executor_count_history) == 1
        assert len(run.accumulated_insights) == 1


# ---------------------------------------------------------------------------
# RunManager._save_run
# ---------------------------------------------------------------------------


class TestSaveRun:
    """Tests for RunManager._save_run (persistence to disk)."""

    def test_writes_json_file(self, tmp_path: Path) -> None:
        """_save_run creates a JSON file in the correct directory."""
        mgr = RunManager(CLUSTER_ID)
        run = RunSession(
            run_id="run-abc",
            name="test-save",
            cluster_id=CLUSTER_ID,
            started_at=FIXED_NOW,
            stopped_at=FIXED_NOW + timedelta(minutes=10),
        )

        with patch("pathlib.Path.home", return_value=tmp_path):
            mgr._save_run(run)

        expected_path = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID / "run-abc.json"
        assert expected_path.exists()
        loaded = RunSession.model_validate_json(expected_path.read_text())
        assert loaded.run_id == "run-abc"
        assert loaded.name == "test-save"

    def test_creates_directory_structure(self, tmp_path: Path) -> None:
        """_save_run creates the full directory path if it doesn't exist."""
        mgr = RunManager(CLUSTER_ID)
        run = RunSession(
            run_id="run-new",
            name="test",
            cluster_id=CLUSTER_ID,
            started_at=FIXED_NOW,
        )

        with patch("pathlib.Path.home", return_value=tmp_path):
            mgr._save_run(run)

        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        assert run_dir.is_dir()

    def test_file_permissions_0o600(self, tmp_path: Path) -> None:
        """Saved files have 0o600 (owner read/write only) permissions."""
        mgr = RunManager(CLUSTER_ID)
        run = RunSession(
            run_id="run-perm",
            name="test-perms",
            cluster_id=CLUSTER_ID,
            started_at=FIXED_NOW,
        )

        with patch("pathlib.Path.home", return_value=tmp_path):
            mgr._save_run(run)

        path = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID / "run-perm.json"
        file_stat = os.stat(path)
        file_mode = stat.S_IMODE(file_stat.st_mode)
        assert file_mode == 0o600

    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        """_save_run overwrites an existing file with the same run_id."""
        mgr = RunManager(CLUSTER_ID)
        run = RunSession(
            run_id="run-overwrite",
            name="original",
            cluster_id=CLUSTER_ID,
            started_at=FIXED_NOW,
        )

        with patch("pathlib.Path.home", return_value=tmp_path):
            mgr._save_run(run)
            run.name = "updated"  # type: ignore[assignment]
            mgr._save_run(run)

        path = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID / "run-overwrite.json"
        loaded = RunSession.model_validate_json(path.read_text())
        assert loaded.name == "updated"

    def test_json_is_indented(self, tmp_path: Path) -> None:
        """Saved JSON uses indent=2 for human readability."""
        mgr = RunManager(CLUSTER_ID)
        run = RunSession(
            run_id="run-indent",
            name="test",
            cluster_id=CLUSTER_ID,
            started_at=FIXED_NOW,
        )

        with patch("pathlib.Path.home", return_value=tmp_path):
            mgr._save_run(run)

        path = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID / "run-indent.json"
        content = path.read_text()
        # Indented JSON will have lines starting with spaces
        parsed = json.loads(content)
        re_serialized = json.dumps(parsed, indent=2)
        # Both should parse to the same structure
        assert json.loads(content) == json.loads(re_serialized)
        # Content should contain newlines (not single-line JSON)
        assert "\n" in content


# ---------------------------------------------------------------------------
# RunManager.list_runs
# ---------------------------------------------------------------------------


class TestListRuns:
    """Tests for RunManager.list_runs (static method)."""

    def test_returns_empty_for_nonexistent_directory(self, tmp_path: Path) -> None:
        """list_runs returns [] when the runs directory doesn't exist."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            result = RunManager.list_runs(CLUSTER_ID)
        assert result == []

    def test_loads_all_runs(self, tmp_path: Path) -> None:
        """list_runs loads all valid JSON files in the directory."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        _make_saved_run(run_dir, "run-1", "first", FIXED_NOW)
        _make_saved_run(run_dir, "run-2", "second", FIXED_NOW + timedelta(hours=1))

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert len(runs) == 2

    def test_sorted_by_started_at_descending(self, tmp_path: Path) -> None:
        """list_runs returns runs sorted newest-first."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        t1 = FIXED_NOW
        t2 = FIXED_NOW + timedelta(hours=1)
        t3 = FIXED_NOW + timedelta(hours=2)
        _make_saved_run(run_dir, "run-old", "oldest", t1)
        _make_saved_run(run_dir, "run-mid", "middle", t2)
        _make_saved_run(run_dir, "run-new", "newest", t3)

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert runs[0].name == "newest"
        assert runs[1].name == "middle"
        assert runs[2].name == "oldest"

    def test_skips_invalid_json(self, tmp_path: Path) -> None:
        """list_runs skips files that contain invalid JSON."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        _make_saved_run(run_dir, "run-valid", "valid", FIXED_NOW)
        (run_dir / "run-bad.json").write_text("not json at all")

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert len(runs) == 1
        assert runs[0].name == "valid"

    def test_ignores_non_json_files(self, tmp_path: Path) -> None:
        """list_runs only reads *.json files, ignoring others."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        _make_saved_run(run_dir, "run-valid", "valid", FIXED_NOW)
        (run_dir / "notes.txt").write_text("some notes")

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert len(runs) == 1

    def test_empty_directory(self, tmp_path: Path) -> None:
        """list_runs returns [] when the directory exists but is empty."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert runs == []


# ---------------------------------------------------------------------------
# RunManager.load_run
# ---------------------------------------------------------------------------


class TestLoadRun:
    """Tests for RunManager.load_run (static method)."""

    def test_loads_existing_run(self, tmp_path: Path) -> None:
        """load_run returns the RunSession for a valid run_id."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        _make_saved_run(run_dir, "run-abc", "my-run", FIXED_NOW)

        with patch("pathlib.Path.home", return_value=tmp_path):
            run = RunManager.load_run(CLUSTER_ID, "run-abc")

        assert run is not None
        assert run.run_id == "run-abc"
        assert run.name == "my-run"

    def test_returns_none_for_missing_run(self, tmp_path: Path) -> None:
        """load_run returns None when the file doesn't exist."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            run = RunManager.load_run(CLUSTER_ID, "nonexistent")

        assert run is None

    def test_returns_none_for_invalid_json(self, tmp_path: Path) -> None:
        """load_run returns None when the file contains invalid JSON."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        (run_dir / "run-bad.json").write_text("{not valid json}")

        with patch("pathlib.Path.home", return_value=tmp_path):
            run = RunManager.load_run(CLUSTER_ID, "run-bad")

        assert run is None

    def test_returns_none_for_invalid_schema(self, tmp_path: Path) -> None:
        """load_run returns None when JSON is valid but doesn't match RunSession schema."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        (run_dir / "run-schema.json").write_text('{"foo": "bar"}')

        with patch("pathlib.Path.home", return_value=tmp_path):
            run = RunManager.load_run(CLUSTER_ID, "run-schema")

        assert run is None


# ---------------------------------------------------------------------------
# RunManager.delete_run
# ---------------------------------------------------------------------------


class TestDeleteRun:
    """Tests for RunManager.delete_run (static method)."""

    def test_deletes_existing_run(self, tmp_path: Path) -> None:
        """delete_run removes the file and returns True."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        path = _make_saved_run(run_dir, "run-del", "to-delete", FIXED_NOW)
        assert path.exists()

        with patch("pathlib.Path.home", return_value=tmp_path):
            result = RunManager.delete_run(CLUSTER_ID, "run-del")

        assert result is True
        assert not path.exists()

    def test_returns_false_for_missing_run(self, tmp_path: Path) -> None:
        """delete_run returns False when the file doesn't exist."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            result = RunManager.delete_run(CLUSTER_ID, "nonexistent")

        assert result is False

    def test_only_deletes_target_file(self, tmp_path: Path) -> None:
        """delete_run does not affect other run files in the same directory."""
        run_dir = tmp_path / ".dbxtop" / "runs" / CLUSTER_ID
        run_dir.mkdir(parents=True)
        _make_saved_run(run_dir, "run-keep", "keeper", FIXED_NOW)
        _make_saved_run(run_dir, "run-del", "to-delete", FIXED_NOW)

        with patch("pathlib.Path.home", return_value=tmp_path):
            RunManager.delete_run(CLUSTER_ID, "run-del")

        assert (run_dir / "run-keep.json").exists()
        assert not (run_dir / "run-del.json").exists()


# ---------------------------------------------------------------------------
# _export_insight helper
# ---------------------------------------------------------------------------


class TestExportInsight:
    """Tests for the _export_insight module-level helper."""

    def test_converts_accumulated_to_export(self) -> None:
        """_export_insight maps all fields from AccumulatedInsight to AccumulatedInsightExport."""
        from dbxtop.analytics.accumulator import AccumulatedInsight

        insight = make_insight(
            category=InsightCategory.SPILL,
            severity=Severity.WARNING,
            entity="stage-5",
            metric_value=0.25,
        )
        acc = AccumulatedInsight(
            insight=insight,
            first_seen=FIXED_NOW,
            last_seen=FIXED_NOW + timedelta(minutes=5),
            resolved_at=FIXED_NOW + timedelta(minutes=10),
            peak_severity=Severity.CRITICAL,
            occurrence_count=7,
            peak_metric_value=0.45,
        )

        export = _export_insight(acc)
        assert export.category == "SPILL"
        assert export.severity == "WARNING"
        assert export.peak_severity == "CRITICAL"
        assert export.title == insight.title
        assert export.description == insight.description
        assert export.recommendation == insight.recommendation
        assert export.affected_entity == "stage-5"
        assert export.first_seen == FIXED_NOW
        assert export.last_seen == FIXED_NOW + timedelta(minutes=5)
        assert export.resolved_at == FIXED_NOW + timedelta(minutes=10)
        assert export.occurrence_count == 7
        assert export.peak_metric_value == 0.45
        assert export.metric_value == 0.25
        assert export.threshold_value == 0.1


# ---------------------------------------------------------------------------
# Integration: full lifecycle
# ---------------------------------------------------------------------------


class TestFullLifecycle:
    """Integration-style tests covering start -> report -> stop -> persist -> load."""

    def test_full_run_lifecycle(self, tmp_path: Path) -> None:
        """A complete run lifecycle: start, accumulate data, stop, save, reload."""
        mgr = RunManager(CLUSTER_ID)
        config = {"spark.executor.memory": "4g", "spark.sql.shuffle.partitions": "200"}
        run = mgr.start_run("lifecycle-test", config)
        run_id = run.run_id

        # Simulate several report cycles
        executors = [make_executor("1"), make_executor("2")]
        t0 = FIXED_NOW
        for i in range(5):
            t = t0 + timedelta(seconds=3 * i)
            report = make_report(timestamp=t, score=80 - i * 5)
            mgr.on_report(report, executors)

        assert mgr.active_run is not None
        assert len(mgr.active_run.health_history) == 5
        assert len(mgr.active_run.executor_count_history) == 5

        # Stop and save
        with patch("pathlib.Path.home", return_value=tmp_path):
            stopped_run = mgr.stop_run()

        assert stopped_run is not None
        assert not mgr.is_recording

        # Reload from disk
        with patch("pathlib.Path.home", return_value=tmp_path):
            loaded = RunManager.load_run(CLUSTER_ID, run_id)

        assert loaded is not None
        assert loaded.run_id == run_id
        assert loaded.name == "lifecycle-test"
        assert loaded.config_snapshot == config
        assert len(loaded.health_history) == 5
        assert len(loaded.executor_count_history) == 5

    def test_multiple_runs_list_order(self, tmp_path: Path) -> None:
        """Multiple runs are listed in descending chronological order."""
        import dbxtop.analytics.run_manager as rm_module

        times = [
            FIXED_NOW,
            FIXED_NOW + timedelta(hours=1),
            FIXED_NOW + timedelta(hours=2),
        ]
        names = ["first", "second", "third"]

        for i, (t, name) in enumerate(zip(times, names)):

            class MockDT(datetime):
                _t = t

                @classmethod  # type: ignore[override]
                def now(cls, tz: object = None) -> datetime:
                    return cls._t

            mgr = RunManager(CLUSTER_ID)

            with patch.object(rm_module, "datetime", MockDT):
                mgr.start_run(name, {})

            with (
                patch("pathlib.Path.home", return_value=tmp_path),
                patch.object(rm_module, "datetime", MockDT),
            ):
                mgr.stop_run()

        with patch("pathlib.Path.home", return_value=tmp_path):
            runs = RunManager.list_runs(CLUSTER_ID)

        assert len(runs) == 3
        assert runs[0].name == "third"
        assert runs[1].name == "second"
        assert runs[2].name == "first"
