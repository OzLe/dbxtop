# Architecture

This document describes the architectural design, data flow, and key patterns
of `dbxtop`. For day-to-day commands and conventions, see
[CLAUDE.md](CLAUDE.md). For end-user docs, see the [README](README.md).

## Overview

**dbxtop** is a real-time terminal dashboard for monitoring Databricks/Spark
clusters — `htop` for Spark. It targets data engineers and platform operators
who need to quickly understand cluster health, Spark job progress, executor
utilization, and query performance without leaving the terminal.

## Design Philosophy

- **Read-only observer** — dbxtop never mutates cluster state. All operations
  are pure monitoring reads. **Exception:** the optional `--keepalive` flag
  creates a lightweight command execution context and runs
  `spark.sql("SELECT 1").collect()` to prevent auto-termination.
- **Graceful degradation** — when the cluster is terminated or the Spark REST
  API is unreachable, degrade to SDK-only mode rather than crashing.
- **Single cluster per session** — depth, not breadth. One cluster, all angles.
- **Minimal resource footprint** — efficient 2-tier polling, lazy rendering,
  no unnecessary data fetching.

## Module Structure

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

## Data Flow

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

## Key Patterns

- **2-tier polling** — fast poll (3s) hits Spark REST for executors/jobs/stages.
  Slow poll (15s) hits Databricks SDK for cluster/events/libraries/SQL/storage.
- **Async bridge** — Databricks SDK is synchronous; wrap all calls with
  `asyncio.to_thread()`.
- **Cache-based rendering** — views never call APIs directly. They read from
  `DataCache` in response to `DataUpdated` messages from the poller.
- **Ring buffers** — each cache slot keeps a `deque(maxlen=60)` of historical
  values for sparkline rendering.
- **Graceful degradation** — when Spark REST is unavailable, Cluster view
  works from SDK data, other views show "Waiting for Spark application..."
  messages.

## API Reference

### Databricks SDK Endpoints Used

| Method | Purpose | Poll tier |
|--------|---------|-----------|
| `w.clusters.get(cluster_id)` | Cluster state, config, nodes, memory | Slow (15s) |
| `w.clusters.events(cluster_id)` | Lifecycle events (resize, terminate, etc.) | Slow (15s) |
| `w.jobs.list_runs(active_only=True)` | Active Databricks job runs | Slow (15s) |
| `w.libraries.cluster_status(cluster_id)` | Installed library status | Slow (15s) |

Auth: `WorkspaceClient(profile=<name>)` reads from `~/.databrickscfg`.

### Spark REST API v1 Endpoints Used

All accessed via the Databricks driver proxy.

**Base URL pattern:**
```
{workspace_url}/driver-proxy-api/o/{org_id}/{cluster_id}/{port}/api/v1/applications/{app_id}/
```

| Endpoint | Purpose | Poll tier |
|----------|---------|-----------|
| `GET /applications` | Discover Spark application ID | On connect |
| `GET /jobs?status={status}` | Spark jobs list | Fast (3s) |
| `GET /stages?status={status}` | Spark stages list | Fast (3s) |
| `GET /allexecutors` | All executor metrics | Fast (3s) |
| `GET /sql?details=true` | SQL query list | Slow (15s) |
| `GET /storage/rdd` | Cached RDDs/DataFrames | Slow (15s) |

Auth: bearer token from WorkspaceClient, sent as `Authorization: Bearer {token}`.
Default port: `40001` (Spark UI proxy on the driver).

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

## Databricks Connection

### How Auth Works

dbxtop uses the Databricks CLI profile system (`~/.databrickscfg`):

```ini
[DEFAULT]
host = https://adb-1234567890.1.azuredatabricks.net
token = dapi...

[my-profile]
host = https://adb-1234567890.1.azuredatabricks.net
token = dapi...
```

`--profile` (or `DBXTOP_PROFILE`) selects the profile.
`WorkspaceClient(profile=name)` reads the config file and authenticates.

### Getting a Cluster ID

- **Databricks UI:** Compute → click cluster → copy ID from URL or details
- **Databricks CLI:** `databricks clusters list --profile my-profile`
- **Format:** `XXXX-XXXXXX-XXXXXXXX` (e.g. `0123-456789-abcdefgh`)

### Shared vs Single-User Cluster Differences

| Feature | Single-user | Shared (Unity Catalog) |
|---------|-------------|------------------------|
| Spark REST API proxy | Full access | May be restricted |
| `sparkContext` access | Yes | Blocked |
| Driver proxy port | 40001 | 40001 |
| All views | Full | Cluster always works; Spark views may be limited |

When the Spark REST proxy is blocked on shared clusters, dbxtop falls back to
SDK-only mode, showing the full Cluster view but "unavailable" messages on
Spark views.

### Org ID Discovery

The `org_id` is needed for driver-proxy URLs. It can be extracted from:
1. The workspace URL (numeric part of `adb-{org_id}.{N}.azuredatabricks.net`)
2. The cluster details response
3. The WorkspaceClient's internal config

## Key Design Decisions

### Why Textual?

Async-native TUI framework with CSS-based styling, built-in widgets
(`DataTable`, `TabbedContent`, `Header`/`Footer`), and snapshot testing
support. The most actively maintained Python TUI framework (by the Rich
creator) and supports Python 3.9+. Alternatives considered: `urwid`
(lower-level, no CSS), `blessed`/`curses` (too primitive), `ratatui`
(Rust — would require a rewrite).

### Why httpx (not requests)?

The Spark REST client must be async-native to avoid blocking the Textual
event loop during polling. `httpx.AsyncClient` provides connection pooling,
timeouts, and a familiar requests-like API. The Databricks SDK uses
`requests` internally, but those calls are wrapped with `asyncio.to_thread()`.

### Why 2-tier Polling?

Not all data changes at the same rate:
- Executors, active jobs, active stages change every few seconds → fast (3s).
- Cluster state, events, libraries, SQL history, cached storage change
  infrequently → slow (15s).

Reduces API load by ~5× compared to polling everything at 3s, while keeping
the most important real-time data fresh.

### Why Pydantic v2?

Raw API responses from the SDK and Spark REST have different shapes,
inconsistent null handling, and sometimes change between versions. Pydantic
v2 provides:
- **Type safety** — strict validation catches data format issues early.
- **Computed fields** — `gc_ratio`, `memory_used_pct`, `is_driver` computed
  once on construction.
- **Decoupling** — views depend on stable model interfaces, not raw API
  shapes.
- **Serialization** — easy for testing fixtures and snapshot comparisons.

### Why a DataCache Layer?

Without a cache, views would either need direct API access (coupling) or the
poller would need to know about view rendering (also coupling). The cache
provides:
- **Decoupling** — poller writes, views read; neither knows about the other.
- **Staleness tracking** — each slot has `last_updated` and `stale` flags.
- **History for sparklines** — ring buffers accumulate time-series data
  without the poller caring about visualization.
- **Error resilience** — failed API calls mark slots stale but preserve
  previous data, so views always have something to show.

## Implementation Notes

### Critical: `asyncio.to_thread` for SDK calls

The Databricks SDK is synchronous. **All SDK calls MUST be wrapped:**

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

### Rich Markup Escaping in DataTable Cells

Both `DataTable` cell content and `Static.update()` input are parsed as Rich
markup. Any literal `[` in user-facing strings (progress bars, table values,
error reasons) **must** be escaped as `\\[` in the source. The canonical
example lives in `widgets/progress_cell.py:render_progress`:

```python
return f"\\[{bar}] {count_str}"
```

Forgetting this raises `MarkupError` at render time — see PR #54 for a
historical instance where `views/analytics._progress_bar` returned
`[====   ]` unescaped and crashed the analytics view whenever cluster
memory utilisation was non-zero.

### Format Utilities

All human-readable formatting lives in `api/models.py`:

```python
def format_bytes(n: int) -> str:
    """1024 -> '1.0 KB', 1073741824 -> '1.0 GB'"""

def format_duration(ms: int) -> str:
    """90061000 -> '1d 1h 1m'"""
```

Used by every view; must handle 0, negative values, and very large numbers.

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| textual | `>=2.1.0,<3.0` | TUI framework (async, CSS-based layout, widgets) |
| databricks-sdk | `>=0.38.0,<1.0` | Databricks API access (clusters, jobs, libraries) |
| httpx | `>=0.27.0,<1.0` | Async HTTP client for Spark REST API |
| click | `>=8.1.0,<9.0` | CLI argument parsing |
| pydantic | `>=2.5.0,<3.0` | Data validation and serialization |

Dev: `pytest`, `pytest-asyncio`, `textual-dev`, `ruff`, `mypy`.

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

## Design Documents

The four planning artifacts that informed this implementation live in
[`docs/planning/`](docs/planning/):

| Document | Location | Contents |
|----------|----------|----------|
| API Research | `docs/planning/api-research.md` | Complete Databricks SDK + Spark REST API endpoint inventory with fields, types, and rate-limit notes |
| Architecture | `docs/planning/architecture.md` | Module structure, data flow, Pydantic model definitions, polling loop design, error-handling strategy |
| Specification | `docs/planning/specification.md` | Field-level spec for every view (columns, widths, colors, formatting), keyboard bindings, CLI validation, color scheme, widget specs |
| Implementation Plan | `docs/planning/implementation-plan.md` | 7-milestone plan with 38 tasks, sizing, dependencies, testing strategy |

When implementing any view or component, consult the specification document
for exact column definitions, colors, and formatting rules.
