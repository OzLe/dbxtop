# CLAUDE.md

This file provides guidance to Claude Code when working with code in the `dbxtop` repository.

## Project Overview

**dbxtop** is a real-time terminal dashboard for monitoring Databricks/Spark clusters — think `htop` for Spark. It targets data engineers and platform operators who need to quickly understand cluster health, Spark job progress, executor utilization, and query performance without leaving the terminal.

**Audience:** Data engineers, ML engineers, and platform teams who work with Databricks clusters daily.

**Design Philosophy:**
- **Read-only observer** — dbxtop never mutates cluster state. All operations are pure monitoring reads. **Exception:** The optional `--keepalive` flag creates a lightweight command execution context and runs `print("keepalive")` to prevent auto-termination.
- **Graceful degradation** — When the cluster is terminated or the Spark REST API is unreachable, degrade to SDK-only mode rather than crashing.
- **Single cluster per session** — Focus on depth, not breadth. One cluster, all angles.
- **Minimal resource footprint** — Efficient 2-tier polling, lazy rendering, no unnecessary data fetching.

## Architecture

### Module Structure

```
src/dbxtop/
├── __init__.py              # Package init, imports __version__ from _version.py
├── __main__.py              # `python -m dbxtop` entry point
├── cli.py                   # Click CLI: --profile, --cluster-id, --refresh, --theme
├── app.py                   # DbxTopApp(textual.App) — root app, tab management, poller lifecycle
├── config.py                # Settings dataclass: profile, cluster_id, intervals, theme
│
├── api/
│   ├── client.py            # DatabricksClient — wraps SDK WorkspaceClient (async via to_thread)
│   ├── spark_api.py         # SparkRESTClient — httpx async client for Spark UI REST API v1
│   ├── poller.py            # MetricsPoller — 2-tier async polling (fast 3s / slow 15s)
│   ├── models.py            # Pydantic v2 models: ClusterInfo, SparkJob, Stage, Executor, SQL, RDD
│   └── cache.py             # DataCache — slot-based cache with ring buffers for sparklines
│
├── views/
│   ├── base.py              # BaseView — abstract base with sort, filter, stale indicator
│   ├── cluster.py           # ClusterView — state card, resources, events, libraries
│   ├── jobs.py              # JobsView — Spark jobs with progress bars
│   ├── stages.py            # StagesView — stages with shuffle/spill metrics
│   ├── executors.py         # ExecutorsView — memory bars, GC%, driver pinned to top
│   ├── sql.py               # SQLView — SQL queries with duration, job counts
│   └── storage.py           # StorageView — cached RDDs/DataFrames
│
├── widgets/
│   ├── header.py            # ClusterHeader — name, state badge, uptime, refresh countdown
│   ├── footer.py            # KeyboardFooter — context-sensitive shortcuts + connection status
│   ├── spark_line.py        # MiniSparkline — Unicode block chart for time-series in cells
│   ├── progress_cell.py     # ProgressCell — inline progress bar for DataTable cells
│   └── status_indicator.py  # StatusIndicator — colored dot + text for state display
│
└── styles/
    └── default.tcss         # Textual CSS — dark/light themes, DataTable styling, layout
```

### Data Flow

```
CLI args ──► Settings ──► DbxTopApp
                              │
                         on_mount()
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            DatabricksClient    SparkRESTClient
            (SDK, sync→async)   (httpx, async)
                    │                   │
                    └─────────┬─────────┘
                              ▼
                       MetricsPoller
                     (fast 3s / slow 15s)
                              │
                         DataCache
                     (slot-based + ring buffers)
                              │
                      DataUpdated message
                              │
                         Active View
                     (reads from cache,
                      renders into widgets)
```

### Key Patterns

- **2-tier polling:** Fast poll (3s) hits Spark REST for executors/jobs/stages. Slow poll (15s) hits Databricks SDK for cluster/events/libraries/SQL/storage.
- **Async bridge:** Databricks SDK is synchronous — wrap all calls with `asyncio.to_thread()`.
- **Cache-based rendering:** Views never call APIs directly. They read from `DataCache` in response to `DataUpdated` messages from the poller.
- **Ring buffers:** Each cache slot maintains a `deque(maxlen=60)` of historical values for sparkline rendering.
- **Graceful degradation:** When Spark REST is unavailable, Cluster view works from SDK data, other views show "Waiting for Spark application..." messages.

## API Reference

### Databricks SDK Endpoints Used

| Method | Purpose | Poll Tier |
|--------|---------|-----------|
| `w.clusters.get(cluster_id)` | Cluster state, config, nodes, memory | Slow (15s) |
| `w.clusters.events(cluster_id)` | Lifecycle events (resize, terminate, etc.) | Slow (15s) |
| `w.jobs.list_runs(active_only=True)` | Active Databricks job runs | Slow (15s) |
| `w.libraries.cluster_status(cluster_id)` | Installed library status | Slow (15s) |

**Auth:** Uses `WorkspaceClient(profile=<name>)` which reads from `~/.databrickscfg`.

### Spark REST API v1 Endpoints Used

All accessed via the Databricks driver proxy.

**Base URL pattern:**
```
{workspace_url}/driver-proxy-api/o/{org_id}/{cluster_id}/{port}/api/v1/applications/{app_id}/
```

| Endpoint | Purpose | Poll Tier |
|----------|---------|-----------|
| `GET /applications` | Discover Spark application ID | On connect |
| `GET /jobs?status={status}` | Spark jobs list | Fast (3s) |
| `GET /stages?status={status}` | Spark stages list | Fast (3s) |
| `GET /allexecutors` | All executor metrics | Fast (3s) |
| `GET /sql?details=true` | SQL query list | Slow (15s) |
| `GET /storage/rdd` | Cached RDDs/DataFrames | Slow (15s) |

**Auth:** Bearer token from WorkspaceClient, passed as `Authorization: Bearer {token}` header.

**Port:** Default `40001` (Spark UI proxy port on the driver).

### URL Construction Example

```python
# For workspace https://adb-1234567890.1.azuredatabricks.net
# with org_id "1234567890", cluster_id "0123-456789-abcdefgh"
url = (
    "https://adb-1234567890.1.azuredatabricks.net"
    "/driver-proxy-api/o/1234567890/0123-456789-abcdefgh/40001"
    "/api/v1/applications/app-20260316120000-0000/jobs"
)
```

## Development Workflow

### Setup

```bash
# Clone and install in dev mode
git clone git@github.com:OzLe/dbxtop.git
cd dbxtop
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Running the App

```bash
# Standard usage (requires Databricks CLI configured)
dbxtop --cluster-id 0123-456789-abcdefgh --profile my-profile

# Short flags
dbxtop -c 0123-456789-abcdefgh -p my-profile

# Custom refresh interval
dbxtop -c 0123-456789-abcdefgh -p my-profile --refresh 1.0

# Light theme
dbxtop -c 0123-456789-abcdefgh -p my-profile --theme light

# Via module
python -m dbxtop -c 0123-456789-abcdefgh -p my-profile
```

### Development Mode

```bash
# Live reload during development
textual-dev run --dev "dbxtop -c CLUSTER_ID -p PROFILE"

# Textual console (run in separate terminal for debug output)
textual console
```

### Environment Variables

| Variable | Maps to CLI Flag | Description |
|----------|-----------------|-------------|
| `DBXTOP_PROFILE` | `--profile` | Databricks CLI profile name |
| `DBXTOP_CLUSTER_ID` | `--cluster-id` | Target cluster ID |
| `DBXTOP_REFRESH` | `--refresh` | Fast poll interval (seconds) |
| `DBXTOP_SLOW_REFRESH` | `--slow-refresh` | Slow poll interval (seconds) |
| `DBXTOP_THEME` | `--theme` | Color theme (dark/light) |

## Testing

### Run Unit Tests

```bash
# All unit tests
pytest tests/ -v --ignore=tests/test_integration.py

# With coverage
pytest tests/ --cov=dbxtop --cov-report=html --ignore=tests/test_integration.py

# Specific test file
pytest tests/test_models.py -v
```

### Run Snapshot Tests

```bash
# Verify snapshots match
pytest tests/test_snapshots.py

# Update snapshots after intentional UI changes
pytest tests/test_snapshots.py --snapshot-update
```

### Run Integration Tests

Requires a running Databricks cluster:

```bash
export DBXTOP_TEST_CLUSTER_ID="0123-456789-abcdefgh"
export DATABRICKS_CONFIG_PROFILE="my-profile"
pytest tests/test_integration.py -v
```

### Lint and Type Check

```bash
# Lint
ruff check src/ tests/

# Format check
ruff format --check src/ tests/

# Auto-fix lint issues
ruff check --fix src/ tests/

# Type check (strict mode)
mypy src/dbxtop/
```

## Milestone Status

| # | Milestone | Status | Description |
|---|-----------|--------|-------------|
| M1 | Core API Infrastructure | Done | Pydantic models, API clients, cache, poller |
| M2 | TUI Shell | Done | App wiring, header/footer, base view, CSS |
| M3 | Cluster View | Done | First live data — cluster state, events, libraries |
| M4 | Jobs + Stages Views | Done | Core Spark monitoring with progress bars |
| M5 | Executors + SQL + Storage | Done | Complete all 6 views |
| M6 | Polish & Error Handling | Done | Error states, reconnection, help overlay |
| M7 | Packaging & Release | Done | CI/CD, PyPI, README, snapshot tests |

Update this table as milestones are completed.

## Design Documents

The three planning artifacts that inform this implementation are stored in the companion repository:

| Document | Location | Contents |
|----------|----------|----------|
| API Research | `yad2-id-link/.a5c/runs/01KKVN0KAYGS7DEGRRVJSG4XTX/artifacts/api-research.md` | Complete Databricks SDK + Spark REST API endpoint inventory with fields, types, and rate limit notes |
| Architecture | `yad2-id-link/.a5c/runs/01KKVN0KAYGS7DEGRRVJSG4XTX/artifacts/architecture.md` | Module structure, data flow, Pydantic model definitions, polling loop design, error handling strategy |
| Specification | `yad2-id-link/.a5c/runs/01KKVN0KAYGS7DEGRRVJSG4XTX/artifacts/specification.md` | Field-level spec for every view (columns, widths, colors, formatting), all keyboard bindings, CLI validation, color scheme, widget specs |
| Implementation Plan | `yad2-id-link/.a5c/runs/01KKVN0KAYGS7DEGRRVJSG4XTX/artifacts/implementation-plan.md` | 7-milestone plan with 38 tasks, sizing, dependencies, testing strategy |

When implementing any view or component, consult the specification document for exact column definitions, colors, and formatting rules.

## Conventions

### Code Style

- **Linter:** ruff (configured in `pyproject.toml`)
- **Formatter:** ruff format. **Always run `ruff format src/ tests/` before committing** — CI enforces `ruff format --check` and will reject unformatted code.
- **Line length:** 120 characters
- **Target Python:** 3.9+ (use `from __future__ import annotations` for modern type syntax)
- **Type hints:** Strict mypy (`strict = true` in pyproject.toml). All public functions must have complete type annotations.
- **Imports:** Use absolute imports (`from dbxtop.api.models import ClusterInfo`), not relative.
- **Docstrings:** Google-style docstrings on all public classes and functions.

### Naming

- **Files:** `snake_case.py`
- **Classes:** `PascalCase` (e.g., `SparkRESTClient`, `ClusterHeader`, `DataCache`)
- **Functions/methods:** `snake_case` (e.g., `get_cluster`, `format_bytes`, `refresh_data`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `DEFAULT_FAST_POLL_S`, `MAX_RING_BUFFER_SIZE`)
- **Private methods:** Single underscore prefix (e.g., `_map_cluster`, `_build_proxy_url`)

### Git & Branching (Gitflow)

The project follows a **gitflow** branching model:

- **`main`** — Production-ready code. Protected: requires PR (from `develop` only), CI must pass.
- **`develop`** — Integration branch. Protected: requires PR (from feature/fix branches), CI must pass.
- **Feature/fix branches** — Created from `develop`, merged back to `develop` via PR.

**Branch naming:**
- Features: `feature/{ticket-or-description}` (e.g., `feature/sem2-168-markup-fix`)
- Fixes: `fix/{ticket-or-description}` (e.g., `fix/sem2-144-executors-memory`)

**Workflow:**
1. Create feature branch from `develop`
2. Open PR targeting `develop` — CI runs lint + tests
3. Merge to `develop` after review
4. When ready to release: open PR from `develop` → `main`
5. On merge to `main`: version is auto-bumped, tagged, and a GitHub Release is created

**Version bumping (automated):**
- Default: **patch** bump (e.g., 0.2.0 → 0.2.1)
- Add `minor` label to PR → minor bump (e.g., 0.2.0 → 0.3.0)
- Add `major` label to PR → major bump (e.g., 0.2.0 → 1.0.0)
- Version is derived from git tags via `hatch-vcs` — no version file to update manually
- The workflow creates an annotated tag and a GitHub Release (no commit to `main` needed)

**Commit style:** Imperative mood, short first line (<72 chars), optional body with details.
- **Never commit directly to `main` or `develop`.**
- **PR per feature/fix** (or per large task within a milestone).

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| textual | >=2.1.0,<3.0 | TUI framework (async, CSS-based layout, widgets) |
| databricks-sdk | >=0.38.0,<1.0 | Databricks API access (clusters, jobs, libraries) |
| httpx | >=0.27.0,<1.0 | Async HTTP client for Spark REST API |
| click | >=8.1.0,<9.0 | CLI argument parsing |
| pydantic | >=2.5.0,<3.0 | Data validation and serialization |

Dev dependencies: pytest, pytest-asyncio, textual-dev, ruff, mypy.

## Databricks Connection

### How Auth Works

dbxtop uses the Databricks CLI profile system (`~/.databrickscfg`):

```ini
# ~/.databrickscfg
[DEFAULT]
host = https://adb-1234567890.1.azuredatabricks.net
token = dapi...

[my-profile]
host = https://adb-1234567890.1.azuredatabricks.net
token = dapi...
```

The `--profile` flag (or `DBXTOP_PROFILE` env var) selects which profile to use. The SDK's `WorkspaceClient(profile=name)` handles reading the config file and authenticating.

### Getting a Cluster ID

- **Databricks UI:** Navigate to Compute, click on a cluster, copy the ID from the URL or the cluster details page.
- **Databricks CLI:** `databricks clusters list --profile my-profile`
- **Format:** `XXXX-XXXXXX-XXXXXXXX` (e.g., `0123-456789-abcdefgh`)

### Shared vs Single-User Cluster Differences

| Feature | Single-User Cluster | Shared (Unity Catalog) |
|---------|--------------------|-----------------------|
| Spark REST API proxy | Full access | May be restricted |
| `sparkContext` access | Yes | Blocked |
| Driver proxy port | 40001 (default) | 40001 (default) |
| All 6 views | Full | Cluster view always works; Spark views may be limited |

If the Spark REST proxy is blocked on shared clusters, dbxtop falls back to SDK-only mode (M6.6), showing full Cluster view but "unavailable" messages on Spark views.

### Org ID Discovery

The org_id is needed for constructing driver proxy URLs. It can be extracted from:
1. The workspace URL itself (the numeric part of `adb-{org_id}.{N}.azuredatabricks.net`)
2. The cluster details response
3. The WorkspaceClient's internal config

## Key Design Decisions

### Why Textual?

Textual provides an async-native TUI framework with CSS-based styling, built-in widgets (DataTable, TabbedContent, Header/Footer), and snapshot testing support. It is the most actively maintained Python TUI framework (by the Rich creator) and supports Python 3.9+. Alternatives considered: `urwid` (lower-level, no CSS), `blessed`/`curses` (too primitive), `ratatui` (Rust, would require rewrite).

### Why httpx (not requests)?

The Spark REST API client needs to be async-native to avoid blocking the Textual event loop during polling. `httpx` provides `AsyncClient` with connection pooling, timeouts, and a familiar requests-like API. The Databricks SDK uses `requests` internally, but we wrap those calls with `asyncio.to_thread()` to avoid blocking.

### Why 2-Tier Polling?

Not all data changes at the same rate:
- **Executors, active jobs, active stages** change every few seconds during workloads -> fast poll (3s)
- **Cluster state, events, libraries, SQL history, cached storage** change infrequently -> slow poll (15s)

This reduces API load by ~5x compared to polling everything at 3s, while keeping the most important real-time data fresh.

### Why Pydantic v2?

Raw API responses from the SDK and Spark REST have different shapes, inconsistent null handling, and sometimes change between versions. Pydantic v2 models provide:
- **Type safety:** Strict validation catches data format issues early
- **Computed fields:** `gc_ratio`, `memory_used_pct`, `is_driver` are computed once on construction
- **Decoupling:** Views depend on stable model interfaces, not raw API shapes
- **Serialization:** Easy to serialize for testing fixtures and snapshot comparisons

### Why a DataCache Layer?

Without a cache, views would either need direct API access (coupling) or the poller would need to know about view rendering (coupling). The cache provides:
- **Decoupling:** Poller writes, views read. Neither knows about the other.
- **Staleness tracking:** Each slot has `last_updated` and `stale` flags.
- **History for sparklines:** Ring buffers accumulate time-series data without the poller caring about visualization.
- **Error resilience:** Failed API calls mark slots stale but preserve previous data, so views always have something to show.

## Implementation Notes

### Critical: asyncio.to_thread for SDK Calls

The Databricks SDK is synchronous. All SDK calls MUST be wrapped:

```python
# CORRECT
async def get_cluster(self) -> ClusterInfo:
    raw = await asyncio.to_thread(self._workspace.clusters.get, self._cluster_id)
    return self._map_cluster(raw)

# WRONG — blocks the event loop
async def get_cluster(self) -> ClusterInfo:
    raw = self._workspace.clusters.get(self._cluster_id)  # BLOCKING!
    return self._map_cluster(raw)
```

### Textual Message Pattern for Data Updates

```python
# In poller.py
class DataUpdated(Message):
    """Posted when cache is updated with fresh data."""
    def __init__(self, updated_slots: set[str]) -> None:
        super().__init__()
        self.updated_slots = updated_slots

# In app.py
def on_data_updated(self, event: DataUpdated) -> None:
    active_view = self.query_one(TabbedContent).active_pane
    if hasattr(active_view, "refresh_data"):
        active_view.refresh_data(self.cache, event.updated_slots)
```

### Format Utilities

All human-readable formatting lives in `api/models.py`:

```python
def format_bytes(n: int) -> str:
    """1024 -> '1.0 KB', 1073741824 -> '1.0 GB'"""

def format_duration(ms: int) -> str:
    """90061000 -> '1d 1h 1m'"""
```

These are used by every view and must handle edge cases: 0, negative values, very large numbers.

<!-- BEGIN GLOBAL RULES -->
## Global Rules

### Workflow Preferences
- Always create a feature branch before starting any implementation work. Never commit directly to main or develop without explicit permission.
- Before implementing anything, state your planned approach including: which files you will modify, which execution engine/environment you will target, and which branch you will work on. Wait for approval before proceeding.
- After completing an orchestration or babysitter run, stop cleanly. Do not re-trigger stop hooks or send redundant completion messages.

### Tool & Infrastructure Context
- When debugging MCP server issues: check stdout leaks first (processes writing to stdout corrupt JSON-RPC), verify auth key naming conventions per provider, check binary/bin field availability, and understand which config file controls which client.
- When working with Databricks/Spark: prefer server-side execution, avoid collecting large datasets to driver, use checkpoint-based lineage breaking for long DAGs, and always confirm the target schema before writing.
- For IAM/infrastructure work: always verify region settings match the target environment, validate ARN resource scoping before adding permissions, start with minimal permissions and iterate via terraform plan output.
<!-- END GLOBAL RULES -->
