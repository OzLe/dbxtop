"""Run lifecycle manager for analytics sessions.

Manages run start/stop, periodic snapshots, insight accumulation,
and JSON persistence to ~/.dbxtop/runs/.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from dbxtop.analytics.accumulator import AccumulatedInsight, InsightAccumulator
from dbxtop.analytics.models import DiagnosticReport
from dbxtop.analytics.run import AccumulatedInsightExport, RunSession
from dbxtop.api.models import ExecutorInfo

logger = logging.getLogger(__name__)

SNAPSHOT_INTERVAL_S: float = 30.0

# Patterns that indicate a Spark config key may contain sensitive data.
_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(key|secret|password|token|credential|account\.key|sas|connection\.string)",
    re.IGNORECASE,
)


def filter_sensitive_config(config: Dict[str, str]) -> Dict[str, str]:
    """Remove config entries whose keys match sensitive patterns.

    Args:
        config: Raw Spark configuration dictionary.

    Returns:
        A copy with sensitive entries removed.
    """
    return {k: v for k, v in config.items() if not _SENSITIVE_KEY_PATTERNS.search(k)}


class RunManager:
    """Manages run lifecycle: start, snapshot, stop, persistence."""

    def __init__(self, cluster_id: str) -> None:
        self._cluster_id = cluster_id
        self._active_run: Optional[RunSession] = None
        self._accumulator: Optional[InsightAccumulator] = None
        self._last_snapshot: Optional[datetime] = None

    @property
    def is_recording(self) -> bool:
        """Whether a run is currently being recorded."""
        return self._active_run is not None

    @property
    def active_run(self) -> Optional[RunSession]:
        """The currently active run session, or None."""
        return self._active_run

    @property
    def accumulator(self) -> Optional[InsightAccumulator]:
        """The insight accumulator for the active run, or None."""
        return self._accumulator

    def start_run(self, name: str, config_snapshot: Dict[str, str]) -> RunSession:
        """Start a new run session.

        Args:
            name: Human-readable name for the run.
            config_snapshot: Cluster configuration at run start time.

        Returns:
            The newly created RunSession.
        """
        run = RunSession(
            run_id=str(uuid4()),
            name=name,
            cluster_id=self._cluster_id,
            started_at=datetime.now(timezone.utc),
            config_snapshot=config_snapshot,
        )
        self._active_run = run
        self._accumulator = InsightAccumulator()
        self._last_snapshot = None
        logger.info("Run started: %s (%s)", name, run.run_id)
        return run

    def on_report(self, report: DiagnosticReport, executors: List[ExecutorInfo]) -> None:
        """Called on each analytics cycle to accumulate insights and capture periodic snapshots.

        Args:
            report: The diagnostic report from the current analytics cycle.
            executors: Current list of executor info objects.
        """
        if self._active_run is None or self._accumulator is None:
            return

        now = report.timestamp
        self._accumulator.update(report.insights, now)

        # Health history (every cycle)
        self._active_run.health_history.append((now, report.health.score))

        # Executor count history
        active_count = len([e for e in executors if e.is_active])
        self._active_run.executor_count_history.append((now, active_count))

        # Periodic full snapshot
        if self._last_snapshot is None or (now - self._last_snapshot).total_seconds() >= SNAPSHOT_INTERVAL_S:
            self._active_run.snapshots.append(report)
            self._last_snapshot = now

    def stop_run(self) -> Optional[RunSession]:
        """Stop the active run, finalize, and save to disk.

        Returns:
            The finalized RunSession, or None if no run was active.
        """
        if self._active_run is None or self._accumulator is None:
            return None

        run = self._active_run
        run.stopped_at = datetime.now(timezone.utc)

        # Export accumulated insights
        run.accumulated_insights = [_export_insight(acc) for acc in self._accumulator.get_all()]

        # Save to disk
        self._save_run(run)

        logger.info("Run stopped: %s (%s)", run.name, run.run_id)

        # Reset
        self._active_run = None
        self._accumulator = None
        return run

    def _save_run(self, run: RunSession) -> None:
        """Persist run to ~/.dbxtop/runs/{cluster_id}/{run_id}.json.

        Files are created with 0o600 (owner read/write only) permissions
        because run data may contain cluster configuration values.
        """
        run_dir = Path.home() / ".dbxtop" / "runs" / self._cluster_id
        run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = run_dir / f"{run.run_id}.json"
        # Use os.open with restrictive permissions to avoid the brief window
        # where the file exists with default permissions.
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, run.model_dump_json(indent=2).encode())
        finally:
            os.close(fd)
        logger.info("Run saved to %s", path)

    @staticmethod
    def list_runs(cluster_id: str) -> List[RunSession]:
        """Load all saved runs for a cluster, sorted by started_at descending.

        Args:
            cluster_id: The Databricks cluster ID.

        Returns:
            List of RunSession objects, newest first.
        """
        run_dir = Path.home() / ".dbxtop" / "runs" / cluster_id
        if not run_dir.exists():
            return []
        runs: List[RunSession] = []
        for path in run_dir.glob("*.json"):
            try:
                runs.append(RunSession.model_validate_json(path.read_text()))
            except Exception:
                logger.warning("Failed to load run from %s", path, exc_info=True)
                continue
        runs.sort(key=lambda r: r.started_at, reverse=True)
        return runs

    @staticmethod
    def load_run(cluster_id: str, run_id: str) -> Optional[RunSession]:
        """Load a specific run by ID.

        Args:
            cluster_id: The Databricks cluster ID.
            run_id: The run UUID.

        Returns:
            The RunSession, or None if not found or invalid.
        """
        path = Path.home() / ".dbxtop" / "runs" / cluster_id / f"{run_id}.json"
        if not path.exists():
            return None
        try:
            return RunSession.model_validate_json(path.read_text())
        except Exception:
            logger.warning("Failed to load run %s", run_id, exc_info=True)
            return None

    @staticmethod
    def delete_run(cluster_id: str, run_id: str) -> bool:
        """Delete a saved run.

        Args:
            cluster_id: The Databricks cluster ID.
            run_id: The run UUID.

        Returns:
            True if the file was deleted, False if it didn't exist.
        """
        path = Path.home() / ".dbxtop" / "runs" / cluster_id / f"{run_id}.json"
        if path.exists():
            path.unlink()
            return True
        return False


def _export_insight(acc: AccumulatedInsight) -> AccumulatedInsightExport:
    """Convert an AccumulatedInsight to a serializable export model."""
    return AccumulatedInsightExport(
        category=acc.insight.category.value,
        severity=acc.insight.severity.value,
        peak_severity=acc.peak_severity.value,
        title=acc.insight.title,
        description=acc.insight.description,
        recommendation=acc.insight.recommendation,
        affected_entity=acc.insight.affected_entity,
        first_seen=acc.first_seen,
        last_seen=acc.last_seen,
        resolved_at=acc.resolved_at,
        occurrence_count=acc.occurrence_count,
        peak_metric_value=acc.peak_metric_value,
        metric_value=acc.insight.metric_value,
        threshold_value=acc.insight.threshold_value,
    )
