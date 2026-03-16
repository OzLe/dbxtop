# dbxtop Architecture Document

> **Version:** 1.0.0
> **Date:** 2026-03-16
> **Status:** Design — Pre-Implementation
> **Scope:** Complete architecture for `dbxtop`, a real-time terminal UI for monitoring Databricks/Spark clusters.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Module Structure](#2-module-structure)
3. [Data Flow Architecture](#3-data-flow-architecture)
4. [TUI Layout — ASCII Wireframes](#4-tui-layout--ascii-wireframes)
5. [Keyboard Bindings](#5-keyboard-bindings)
6. [Error Handling Strategy](#6-error-handling-strategy)
7. [Dependency Specification](#7-dependency-specification)
8. [Key Design Decisions](#8-key-design-decisions)
9. [Configuration & CLI](#9-configuration--cli)
10. [Testing Strategy](#10-testing-strategy)
11. [Future Considerations](#11-future-considerations)

---

## 1. Overview

`dbxtop` is a `top`/`htop`-style terminal application for monitoring a single Databricks cluster in real time. It presents six views — Cluster, Jobs, Stages, Executors, SQL, and Storage — populated by two data sources:

- **Databricks SDK** — cluster metadata, lifecycle events, Databricks job runs, library status
- **Spark REST API v1** — Spark jobs, stages, executors, SQL queries, cached storage (accessed via the Databricks driver-proxy-api)

The application targets Python >=3.9, ships as a PyPI package installable via `pip install dbxtop` or `pipx install dbxtop`, and uses the [Textual](https://textual.textualize.io/) framework for its async TUI.

### Core Principles

1. **Read-only observer** — `dbxtop` never mutates cluster state. It only reads from monitoring APIs.
2. **Graceful degradation** — When the cluster is terminated or the Spark REST API is unreachable, degrade to SDK-only mode rather than crashing.
3. **Single cluster per session** — Focus on depth, not breadth. One cluster, all angles.
4. **Minimal resource footprint** — Efficient polling, no unnecessary data fetching, lazy rendering.

---

## 2. Module Structure

```
src/dbxtop/
├── __init__.py              # Package version (__version__)
├── __main__.py              # `python -m dbxtop` entry point
├── cli.py                   # Click CLI: arg parsing, profile/cluster resolution
├── app.py                   # DbxtopApp(textual.App) — root app, screen management
├── config.py                # Settings dataclass, profile loading, interval defaults
│
├── api/
│   ├── __init__.py
│   ├── client.py            # DatabricksClient — wraps SDK + raw HTTP for driver proxy
│   ├── spark_api.py         # SparkRESTClient — all /api/v1/* proxy endpoints
│   ├── poller.py            # MetricsPoller — async 2-tier polling orchestrator
│   ├── models.py            # Pydantic models for normalized API responses
│   └── cache.py             # DataCache — in-memory TTL cache between API and views
│
├── views/
│   ├── __init__.py
│   ├── base.py              # BaseView — abstract base with common refresh logic
│   ├── cluster.py           # ClusterView — overview, config, events, libraries
│   ├── jobs.py              # JobsView — Spark jobs table with progress bars
│   ├── stages.py            # StagesView — stage detail, task distribution
│   ├── executors.py         # ExecutorsView — executor table, memory/GC charts
│   ├── sql.py               # SQLView — SQL query list with duration, status
│   └── storage.py           # StorageView — cached RDDs, memory/disk usage
│
├── widgets/
│   ├── __init__.py
│   ├── header.py            # ClusterHeader — name, state, uptime, refresh timer
│   ├── footer.py            # KeyboardFooter — context-sensitive shortcut bar
│   ├── spark_line.py        # MiniSparkline — inline time-series chart widget
│   ├── progress_cell.py     # ProgressCell — inline progress bar for DataTable cells
│   └── status_indicator.py  # StatusIndicator — colored dot for state (RUNNING, etc.)
│
└── styles/
    └── default.tcss         # Textual CSS — layout, colors, responsive breakpoints
```

### Module Responsibilities

#### `__init__.py`
```python
__version__ = "0.1.0"
```

#### `__main__.py`
Entry point for `python -m dbxtop`. Delegates to `cli.main()`.

#### `cli.py`
Click-based CLI. Parses `--profile`, `--cluster-id`, `--refresh`, `--spark-port`. Instantiates config, creates the app, calls `app.run()`.

#### `app.py`
The Textual `App` subclass. Owns the `MetricsPoller`, mounts the header/footer/tab container, handles global keybindings, and manages view switching. The app lifecycle:

1. `on_mount()` — start poller, show initial cluster view
2. Poller fires `DataUpdated` messages — views react to new data
3. `on_unmount()` — stop poller, close HTTP sessions

#### `config.py`
Immutable settings resolved from CLI args + environment + defaults:

```python
@dataclass(frozen=True)
class Settings:
    profile: str | None          # ~/.databrickscfg profile name
    cluster_id: str              # Target cluster
    spark_port: int = 40001      # Driver proxy port for Spark UI
    fast_poll_s: float = 3.0     # Spark REST poll interval
    slow_poll_s: float = 15.0    # SDK poll interval
    max_retries: int = 3         # Per-request retry limit
    request_timeout_s: float = 10.0  # HTTP timeout per request
```

#### `api/client.py` — `DatabricksClient`
Wraps the Databricks SDK `WorkspaceClient` with async-compatible methods (uses `asyncio.to_thread` since the SDK is synchronous). Provides:

- `get_cluster() -> ClusterInfo` — normalized cluster state
- `get_events(since: datetime | None) -> list[ClusterEvent]` — lifecycle events
- `get_job_runs(active_only: bool) -> list[JobRun]` — Databricks job runs
- `get_library_status() -> list[LibraryInfo]` — installed library status
- `get_workspace_url() -> str` — workspace host for proxy URL construction
- `get_org_id() -> str` — organization ID for proxy URL construction

#### `api/spark_api.py` — `SparkRESTClient`
Async HTTP client (using `httpx.AsyncClient`) for the Spark REST API v1 via the driver proxy. All methods return normalized Pydantic models. Handles app ID discovery and caching.

- `discover_app_id() -> str` — GET /applications, cache result
- `get_jobs(status: str | None) -> list[SparkJob]`
- `get_stages(status: str | None) -> list[SparkStage]`
- `get_stage_detail(stage_id, attempt_id) -> StageDetail`
- `get_executors(include_dead: bool) -> list[ExecutorInfo]`
- `get_sql_queries(offset, length) -> list[SQLQuery]`
- `get_storage_rdds() -> list[RDDInfo]`
- `get_environment() -> SparkEnvironment`
- `is_available() -> bool` — health check, returns False if proxy unreachable

#### `api/poller.py` — `MetricsPoller`
The async orchestrator that drives all data collection. Uses Textual's `set_interval()` to schedule two polling loops:

```
Fast loop (every fast_poll_s seconds):
  1. SparkRESTClient.get_executors()
  2. SparkRESTClient.get_jobs(status="running")
  3. SparkRESTClient.get_stages(status="active")
  Parallel via asyncio.gather()

Slow loop (every slow_poll_s seconds):
  1. DatabricksClient.get_cluster()
  2. DatabricksClient.get_events(since=last_event_ts)
  3. DatabricksClient.get_job_runs(active_only=True)
  4. DatabricksClient.get_library_status()
  5. SparkRESTClient.get_sql_queries()
  6. SparkRESTClient.get_storage_rdds()
  Parallel via asyncio.gather()
```

After each poll, the poller writes results to `DataCache` and posts a Textual `DataUpdated` message so views can re-render.

#### `api/models.py`
Pydantic v2 models that normalize the raw API responses into a stable internal schema. This decouples views from API response format changes.

Key models:

```python
class ClusterInfo(BaseModel):
    cluster_id: str
    cluster_name: str
    state: ClusterState                 # enum: RUNNING, TERMINATED, PENDING, ...
    state_message: str
    uptime_seconds: float | None
    driver_node_type: str
    worker_node_type: str
    num_workers: int | None
    autoscale_min: int | None
    autoscale_max: int | None
    total_cores: float | None
    total_memory_mb: int | None
    spark_version: str
    data_security_mode: str
    runtime_engine: str                 # STANDARD or PHOTON
    creator: str
    autotermination_minutes: int | None
    spark_conf: dict[str, str]
    tags: dict[str, str]

class ClusterState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RESTARTING = "RESTARTING"
    RESIZING = "RESIZING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

class ClusterEvent(BaseModel):
    timestamp: datetime
    event_type: str
    message: str
    details: dict | None

class SparkJob(BaseModel):
    job_id: int
    name: str
    status: str                         # RUNNING, SUCCEEDED, FAILED, UNKNOWN
    submission_time: datetime
    completion_time: datetime | None
    num_tasks: int
    num_active_tasks: int
    num_completed_tasks: int
    num_failed_tasks: int
    num_stages: int
    num_active_stages: int
    num_completed_stages: int
    num_failed_stages: int
    stage_ids: list[int]

class SparkStage(BaseModel):
    stage_id: int
    attempt_id: int
    name: str
    status: str                         # ACTIVE, COMPLETE, PENDING, FAILED
    num_tasks: int
    num_active_tasks: int
    num_complete_tasks: int
    num_failed_tasks: int
    input_bytes: int
    input_records: int
    output_bytes: int
    output_records: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    executor_run_time_ms: int
    executor_cpu_time_ns: int
    submission_time: datetime | None
    completion_time: datetime | None

class ExecutorInfo(BaseModel):
    executor_id: str                    # "driver" or numeric
    is_driver: bool
    is_active: bool
    host_port: str
    total_cores: int
    max_tasks: int
    active_tasks: int
    completed_tasks: int
    failed_tasks: int
    total_duration_ms: int
    total_gc_time_ms: int
    gc_ratio: float                     # computed: gc_time / duration
    max_memory: int                     # bytes
    memory_used: int                    # bytes
    memory_used_pct: float              # computed
    disk_used: int
    total_input_bytes: int
    total_shuffle_read: int
    total_shuffle_write: int
    rdd_blocks: int
    peak_jvm_heap: int | None
    peak_jvm_off_heap: int | None
    add_time: datetime | None
    remove_time: datetime | None
    remove_reason: str | None

class SQLQuery(BaseModel):
    execution_id: int
    status: str                         # RUNNING, COMPLETED, FAILED
    description: str
    submission_time: datetime
    duration_ms: int | None
    running_jobs: list[int]
    success_jobs: list[int]
    failed_jobs: list[int]

class RDDInfo(BaseModel):
    rdd_id: int
    name: str
    num_partitions: int
    num_cached_partitions: int
    storage_level: str
    memory_used: int
    disk_used: int
    fraction_cached: float              # computed

class JobRun(BaseModel):
    """Databricks job run (not Spark job)."""
    run_id: int
    job_id: int
    run_name: str
    state: str
    result_state: str | None
    start_time: datetime
    end_time: datetime | None
    setup_duration_ms: int | None
    execution_duration_ms: int | None
    creator: str
    run_type: str
    task_count: int

class LibraryInfo(BaseModel):
    name: str
    library_type: str                   # pypi, maven, jar, etc.
    status: str                         # INSTALLED, PENDING, FAILED, etc.
    messages: list[str]
```

#### `api/cache.py` — `DataCache`
Thread-safe in-memory cache with timestamps. Views read from the cache; the poller writes to it. Each data category has its own slot with a `last_updated` timestamp.

```python
class DataCache:
    cluster: CacheSlot[ClusterInfo]
    events: CacheSlot[list[ClusterEvent]]
    spark_jobs: CacheSlot[list[SparkJob]]
    stages: CacheSlot[list[SparkStage]]
    executors: CacheSlot[list[ExecutorInfo]]
    sql_queries: CacheSlot[list[SQLQuery]]
    storage: CacheSlot[list[RDDInfo]]
    job_runs: CacheSlot[list[JobRun]]
    libraries: CacheSlot[list[LibraryInfo]]

@dataclass
class CacheSlot(Generic[T]):
    data: T | None = None
    last_updated: datetime | None = None
    error: str | None = None           # last error message if fetch failed
    stale: bool = False                # True if data exists but refresh failed
```

#### `views/base.py` — `BaseView`
Abstract base class (extends `textual.widget.Widget`) providing:

- `on_data_updated(event: DataUpdated)` — called when poller delivers fresh data
- `render_stale_indicator()` — shows "(stale: 15s ago)" if data is outdated
- `current_sort_key` and `sort_reverse` — column sorting state
- `filter_text` — active text filter

#### `views/cluster.py` — `ClusterView`
- Top section: cluster identity card (name, ID, state, DBR version, node types, auto-termination)
- Middle-left: resource summary (total cores, total memory, workers, driver)
- Middle-right: Spark configuration highlights (key spark.conf entries)
- Bottom-left: recent cluster events (scrollable table)
- Bottom-right: library status table

#### `views/jobs.py` — `JobsView`
- DataTable with columns: ID, Name, Status, Tasks (progress bar), Stages (active/total), Duration, Submitted
- Filterable by status (running/succeeded/failed)
- Sortable by any column
- Active jobs highlighted, running tasks shown as inline progress bar

#### `views/stages.py` — `StagesView`
- DataTable with columns: ID, Name, Status, Tasks (progress), Input, Output, Shuffle R/W, Duration
- Detail panel (bottom split) for selected stage: task distribution summary, skew detection
- Color-coded status: ACTIVE=green, COMPLETE=dim, FAILED=red, PENDING=yellow

#### `views/executors.py` — `ExecutorsView`
- DataTable with columns: ID, Host, Cores, Active Tasks, Memory (bar), GC%, Shuffle R/W, Status
- Memory column rendered as a mini progress bar (used/max)
- GC% highlighted red when >10%
- Driver row visually distinguished (bold or different background)
- Summary row at bottom: total cores, total memory, aggregate GC%, total active tasks

#### `views/sql.py` — `SQLView`
- DataTable with columns: ID, Description (truncated), Status, Duration, Jobs (running/success/fail)
- Running queries at top, completed below
- Select a row to see full description in a detail panel

#### `views/storage.py` — `StorageView`
- DataTable with columns: ID, Name, Partitions (cached/total), Storage Level, Memory, Disk, % Cached
- Summary bar: total cached memory, total cached disk, number of RDDs

#### `widgets/header.py` — `ClusterHeader`
Persistent top bar showing:
- App name (`dbxtop`)
- Cluster name
- State indicator (colored dot + text)
- Uptime
- Refresh countdown ("next refresh in 2s")
- Profile name
- Spark REST API status (connected / disconnected)

#### `widgets/footer.py` — `KeyboardFooter`
Context-sensitive keyboard shortcut bar. Changes based on active view.

#### `widgets/spark_line.py` — `MiniSparkline`
A compact sparkline widget for embedding time-series in the Cluster view (e.g., executor count over time, active tasks over time). Stores a fixed-length ring buffer of values and renders using Unicode block characters.

#### `widgets/progress_cell.py` — `ProgressCell`
A widget that renders as an inline progress bar inside a DataTable cell, showing completed/total with a colored bar and text label (e.g., `[=========>   ] 847/1000`).

#### `widgets/status_indicator.py` — `StatusIndicator`
A small widget rendering a colored Unicode circle plus text label for cluster/job/stage states.

#### `styles/default.tcss`
Textual CSS defining:
- Color scheme (dark terminal-friendly palette)
- Layout grid for each view
- DataTable column widths and header styling
- Footer and header bar styling
- Responsive rules for narrow terminals (<80 cols)

---

## 3. Data Flow Architecture

### 3.1 High-Level Data Flow

```
┌──────────────────────┐     ┌──────────────────────┐
│   Databricks SDK     │     │   Spark REST API v1   │
│  (WorkspaceClient)   │     │  (driver-proxy-api)   │
│                      │     │                       │
│  clusters.get()      │     │  /executors           │
│  clusters.events()   │     │  /jobs                │
│  jobs.list_runs()    │     │  /stages              │
│  libraries.status()  │     │  /sql                 │
│                      │     │  /storage/rdd         │
└──────────┬───────────┘     └───────────┬───────────┘
           │                             │
           │  slow poll (15s)            │  fast poll (3s)
           │                             │
           ▼                             ▼
     ┌─────────────────────────────────────────┐
     │            MetricsPoller                 │
     │  (async orchestrator, runs in App)       │
     │                                          │
     │  fast_timer ──► gather(executors,        │
     │                        jobs, stages)     │
     │  slow_timer ──► gather(cluster, events,  │
     │                        job_runs, libs,   │
     │                        sql, storage)     │
     └────────────────────┬────────────────────┘
                          │
                          │  write results
                          ▼
                 ┌─────────────────┐
                 │   DataCache     │
                 │                 │
                 │  cluster: ...   │
                 │  executors: ... │
                 │  jobs: ...      │
                 │  stages: ...    │
                 │  sql: ...       │
                 │  storage: ...   │
                 │  events: ...    │
                 │  job_runs: ...  │
                 │  libraries: ... │
                 └────────┬────────┘
                          │
                          │  post DataUpdated message
                          ▼
     ┌─────────────────────────────────────────┐
     │              DbxtopApp                   │
     │         (Textual App class)              │
     │                                          │
     │  on_data_updated() ──► active_view       │
     │                        .refresh_data()   │
     └─────────────────────────────────────────┘
                          │
                          ▼
     ┌──────────────────────────────────────────┐
     │            Active View                    │
     │  (ClusterView / JobsView / StagesView /  │
     │   ExecutorsView / SQLView / StorageView)  │
     │                                           │
     │  Reads from DataCache                     │
     │  Renders into DataTable / widgets         │
     └──────────────────────────────────────────┘
```

### 3.2 Polling Loop Detail

The `MetricsPoller` uses two independent Textual timers. Each timer fires an async callback that:

1. **Dispatches API calls in parallel** via `asyncio.gather(*tasks, return_exceptions=True)`
2. **Writes successes to `DataCache`** with current timestamp
3. **Handles failures per-slot** — if one API call fails, others still update. Failed slots are marked `stale=True` with the error message preserved, but previous data remains visible.
4. **Posts a `DataUpdated` Textual message** so the active view can re-render

```python
# Simplified polling logic
class MetricsPoller:
    async def _fast_poll(self) -> None:
        """Called every fast_poll_s seconds."""
        if not self.spark_api.is_available():
            return  # Skip when Spark REST unreachable

        results = await asyncio.gather(
            self.spark_api.get_executors(include_dead=False),
            self.spark_api.get_jobs(status="running"),
            self.spark_api.get_stages(status="active"),
            return_exceptions=True,
        )

        for slot_name, result in zip(["executors", "spark_jobs", "stages"], results):
            if isinstance(result, Exception):
                self.cache.mark_stale(slot_name, str(result))
            else:
                self.cache.update(slot_name, result)

        self.app.post_message(DataUpdated(source="fast"))

    async def _slow_poll(self) -> None:
        """Called every slow_poll_s seconds."""
        results = await asyncio.gather(
            self.dbx_client.get_cluster(),
            self.dbx_client.get_events(since=self.cache.events.last_updated),
            self.dbx_client.get_job_runs(active_only=True),
            self.dbx_client.get_library_status(),
            self._conditional_spark_poll(),  # SQL + storage, only if Spark available
            return_exceptions=True,
        )
        # ... write to cache, post DataUpdated
```

### 3.3 Two-Tier Polling Rationale

| Tier | Interval | Data Sources | Why |
|------|----------|-------------|-----|
| **Fast** | 2-5s (default 3s) | Executors, active jobs, active stages | These change rapidly during job execution. Users expect near-real-time task progress. Spark REST API is lightweight for these endpoints. |
| **Slow** | 10-30s (default 15s) | Cluster state, events, Databricks job runs, libraries, SQL queries, storage | These change infrequently. SDK calls have higher latency (~200-500ms). Rate-limit budget is better spent on fast-changing data. |

### 3.4 Spark REST API Availability Detection

The Spark REST API is only available when the cluster is RUNNING and the Spark context is active. The poller handles transitions:

```
Cluster State        Spark REST API       Behavior
─────────────────    ─────────────────    ──────────────────────────────
TERMINATED           Unreachable          SDK-only mode. All Spark views
                                          show "Cluster is terminated"
PENDING              Unreachable          SDK-only mode. Show "Starting..."
RUNNING (starting)   May be unreachable   Probe /applications every slow_poll.
                                          Once reachable, enable fast poll.
RUNNING (stable)     Available            Full 2-tier polling
RUNNING → ERROR      May fail             Mark Spark slots stale, show last
                                          known data with warning
TERMINATING          Degrading            Continue polling, expect failures.
                                          Degrade gracefully to SDK-only.
```

App ID discovery (`GET /applications`) is performed once at startup and re-performed whenever the Spark REST API returns a 404 (which indicates the Spark context restarted and got a new application ID).

### 3.5 Data Cache Design

The cache sits between the polling layer and the view layer. Its contract:

- **Poller writes, views read** — no locks needed because Textual is single-threaded (async event loop). The poller's async callbacks run on the same event loop as view rendering.
- **Staleness tracking** — each slot tracks `last_updated` and `stale` flag. Views can display "data from 30s ago" when stale.
- **Error memory** — the last error per slot is preserved so views can show "Failed: connection timeout" without losing existing data.
- **Time-series ring buffers** — for sparkline charts, the cache maintains fixed-size deques of historical values (e.g., last 60 executor-count readings for a 3-minute sparkline at 3s interval).

```python
class DataCache:
    # Time-series for sparklines (ring buffers)
    executor_count_history: deque[tuple[datetime, int]]  # maxlen=60
    active_tasks_history: deque[tuple[datetime, int]]     # maxlen=60
    gc_ratio_history: deque[tuple[datetime, float]]       # maxlen=60
```

---

## 4. TUI Layout — ASCII Wireframes

### 4.0 Global Layout Structure

Every view shares the same header, tab bar, and footer. The content area between tabs and footer is view-specific.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ HEADER BAR (ClusterHeader widget)                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ TAB BAR (TabbedContent)                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                                                                             │
│ VIEW CONTENT AREA (swapped per active tab)                                  │
│                                                                             │
│                                                                             │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ FOOTER BAR (KeyboardFooter widget)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

Terminal minimum: 80 columns x 24 rows. Recommended: 120+ columns x 40+ rows.

### 4.1 Cluster View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Cluster]   Jobs    Stages    Executors    SQL    Storage                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─ Cluster Identity ──────────────────────────────────────────────────┐    │
│  │ Name:    my-analytics-cluster          ID: 0311-160038-vjuv0ah9    │    │
│  │ DBR:     15.4 LTS (Spark 3.5.0)       Engine: PHOTON              │    │
│  │ Mode:    Shared Access (UC)            Creator: user@company.com   │    │
│  │ Policy:  data-engineering-medium       Auto-term: 60 min           │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─ Resources ──────────────────────┐  ┌─ Activity ────────────────────┐   │
│  │ Driver:  i3.xlarge (4c / 30 GB)  │  │ Active Tasks ▁▂▄▇▅▃▂▁▃▅▇█▆▃ │   │
│  │ Workers: 4 (autoscale 2-8)       │  │ Executors    ▃▃▃▃▃▃▄▄▅▅▅▅▅▅ │   │
│  │ Cores:   20 total                │  │ GC Ratio     ▁▁▁▁▂▁▁▁▁▂▃▁▁▁ │   │
│  │ Memory:  150 GB total            │  │                               │   │
│  │ State:   RUNNING (4h 23m)        │  │ Running Spark Jobs: 2         │   │
│  └──────────────────────────────────┘  │ Active Stages:      5         │   │
│                                         │ Pending Stages:     12        │   │
│                                         └─────────────────────────────┘   │
│                                                                             │
│  ┌─ Recent Events ─────────────────────────────────────────────────────┐   │
│  │ TIME            TYPE                    MESSAGE                      │   │
│  │ 14:30:02        AUTOSCALING_STATS       Scaled to 4 workers          │   │
│  │ 14:15:11        UPSIZE_COMPLETED        Added 2 workers              │   │
│  │ 10:07:45        RUNNING                 Cluster started               │   │
│  │ 10:07:00        STARTING                Starting cluster              │   │
│  │ 10:06:30        CREATING                Requesting instances          │   │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─ Libraries ──────────────────────────────────────────────────────────┐  │
│  │ LIBRARY               TYPE    STATUS                                 │  │
│  │ splink==4.0.5          pypi    INSTALLED                              │  │
│  │ delta-spark==3.2.0     pypi    INSTALLED                              │  │
│  │ mlflow==2.12.0         pypi    INSTALLED                              │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  /:filter  r:refresh  p:pause  q:quit  ?:help            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Jobs View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster   [Jobs]    Stages    Executors    SQL    Storage                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Filter: [____________]                        Showing: 3 running, 47 total │
│                                                                             │
│  ┌─ Spark Jobs ─────────────────────────────────────────────────────────┐  │
│  │ ID   NAME                      STATUS     TASKS         STAGES  DUR  │  │
│  │───────────────────────────────────────────────────────────────────────│  │
│  │  42  collect at dedup:245      RUNNING    [=====>  ] 847/1200  3/5  2m│  │
│  │  41  save at pipeline:189      RUNNING    [===>    ] 512/2000  2/3  5m│  │
│  │  40  count at validate:67      RUNNING    [>       ]  23/200   1/1 12s│  │
│  │  39  parquet at extract:34     SUCCEEDED  [========] 200/200   2/2 45s│  │
│  │  38  showString at cmd:12      SUCCEEDED  [========]   1/1     1/1  2s│  │
│  │  37  save at output:156        SUCCEEDED  [========] 500/500   4/4  3m│  │
│  │  36  collect at stats:89       FAILED     [====X   ] 340/500   2/3  1m│  │
│  │  35  broadcast at join:45      SUCCEEDED  [========] 100/100   1/1 30s│  │
│  │  ..  (scroll for more)                                                │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌─ Job Detail (ID: 42) ────────────────────────────────────────────────┐  │
│  │ Name:       collect at dedup:245                                      │  │
│  │ Submitted:  14:28:15                                                  │  │
│  │ Duration:   2m 14s (running)                                          │  │
│  │ Stages:     [38, 39, 40, 41, 42]                                      │  │
│  │ Active:     3    Completed: 2    Failed: 0    Skipped: 0              │  │
│  │ Tasks:      847 completed / 1200 total / 0 failed                     │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  /:filter  r:refresh  f:status-filter  enter:detail  q:quit│
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Stages View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster    Jobs   [Stages]    Executors    SQL    Storage                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Filter: [____________]                 Active: 5  Complete: 42  Failed: 1  │
│                                                                             │
│  ┌─ Stages ──────────────────────────────────────────────────────────────┐ │
│  │ ID  ATT  NAME                STATUS   TASKS         INPUT   SHFL_R   │ │
│  │─────────────────────────────────────────────────────────────────────── │ │
│  │ 42   0   Exchange hashpa..  ACTIVE   [====> ] 640/1000  2.3 GB  1.1 GB │ │
│  │ 41   0   WholeStageCodeg..  ACTIVE   [==>   ] 312/1000  1.8 GB  0 B    │ │
│  │ 40   0   Scan parquet y2..  ACTIVE   [=>    ] 156/200   4.5 GB  0 B    │ │
│  │ 39   0   Exchange SingleP   ACTIVE   [======>] 89/100   512 MB  890 MB │ │
│  │ 38   0   WholeStageCodeg..  ACTIVE   [=      ]  12/200  0 B     0 B    │ │
│  │ 37   0   Exchange hashpa..  COMPLETE [========] 200/200  1.2 GB  1.2 GB │ │
│  │ 36   0   Scan parquet y2..  COMPLETE [========] 200/200  3.1 GB  0 B    │ │
│  │ 35   1   BroadcastExchan..  COMPLETE [========] 100/100  256 MB  0 B    │ │
│  │ 34   0   Scan parquet y2..  FAILED   [===X   ] 180/200  2.8 GB  0 B    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─ Stage Detail (ID: 42, Attempt: 0) ──────────────────────────────────┐ │
│  │ Name:     Exchange hashpartitioning(customer_id#12, 200)              │ │
│  │ Status:   ACTIVE  (1m 45s elapsed)                                    │ │
│  │ Tasks:    640 complete / 1000 total / 0 failed / 0 killed             │ │
│  │ I/O:      Input 2.3 GB (18.4M records) → Output 1.1 GB (9.2M recs)  │ │
│  │ Shuffle:  Read 1.1 GB (8.1M records) / Write 890 MB (7.3M records)   │ │
│  │ Spill:    Memory 0 B / Disk 0 B                                       │ │
│  │ Exec:     CPU 4.2 min / Run 6.8 min / GC 12.3 s                      │ │
│  │                                                                        │ │
│  │ Task Distribution (p50 / p95 / max):                                  │ │
│  │   Duration:    1.2s  /  4.8s  / 12.1s                                 │ │
│  │   Shuffle Rd:  1.1MB / 4.2MB / 18.7MB  ← possible skew               │ │
│  │   GC Time:     0.1s  /  0.8s  /  3.2s                                 │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  /:filter  r:refresh  f:status-filter  enter:detail  q:quit│
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Executors View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster    Jobs    Stages   [Executors]    SQL    Storage                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Total: 5 (1 driver + 4 workers)    Cores: 20    Memory: 150 GB            │
│                                                                             │
│  ┌─ Executors ───────────────────────────────────────────────────────────┐ │
│  │ ID      HOST            CORES  TASKS  MEMORY           GC%    SHFL   │ │
│  │──────────────────────────────────────────────────────────────────────  │ │
│  │ driver  10.0.1.100:4040   4    1/4   [████░░░░] 12/30GB  2.1%   --   │ │
│  │ 0       10.0.1.101:4041   4    4/4   [██████░░] 22/30GB  3.4%  4.2GB │ │
│  │ 1       10.0.1.102:4041   4    3/4   [█████░░░] 18/30GB  2.8%  3.8GB │ │
│  │ 2       10.0.1.103:4041   4    4/4   [███████░] 26/30GB  8.7%  5.1GB │ │
│  │ 3       10.0.1.104:4041   4    2/4   [████░░░░] 14/30GB  1.9%  2.9GB │ │
│  │──────────────────────────────────────────────────────────────────────  │ │
│  │ TOTAL                     20  14/20  [█████░░░] 92/150GB  3.8% 16.0GB │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─ Executor Detail (ID: 2) ────────────────────────────────────────────┐ │
│  │ Host:           10.0.1.103:4041                                       │ │
│  │ Status:         Active (since 10:07:52)                               │ │
│  │ Cores:          4         Max Tasks: 4        Active Tasks: 4         │ │
│  │ Memory:         26.1 / 30.0 GB (87%)    ← HIGH                       │ │
│  │ Disk:           1.2 GB                                                 │ │
│  │ GC:             8.7% of runtime          ← WARNING (>5%)             │ │
│  │ Tasks:          4 active / 1,847 completed / 2 failed                 │ │
│  │ Shuffle Read:   5.1 GB     Shuffle Write: 4.8 GB                      │ │
│  │ Input:          12.3 GB    RDD Blocks: 24                             │ │
│  │                                                                        │ │
│  │ Peak Memory:                                                           │ │
│  │   JVM Heap:     28.2 GB    JVM Off-Heap: 1.8 GB                       │ │
│  │   Exec Memory:  18.4 GB    Storage Mem:  9.7 GB                       │ │
│  │   GC Count:     Major 3 / Minor 847      GC Time: 45.2s               │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  /:filter  r:refresh  d:show-dead  enter:detail  q:quit   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.5 SQL View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster    Jobs    Stages    Executors   [SQL]    Storage                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Running: 2    Completed: 31    Failed: 1                                   │
│                                                                             │
│  ┌─ SQL Queries ─────────────────────────────────────────────────────────┐ │
│  │ ID   STATUS     DURATION  JOBS       DESCRIPTION                      │ │
│  │──────────────────────────────────────────────────────────────────────  │ │
│  │  33  RUNNING       2m 14s  2 running  SELECT customer_id, first_na..  │ │
│  │  32  RUNNING         45s   1 running  INSERT INTO y2_data_platform..  │ │
│  │  31  COMPLETED     1m 02s  3 success  SELECT count(*) FROM y2_dat..  │ │
│  │  30  COMPLETED       23s   1 success  DESCRIBE TABLE y2_data_plat..  │ │
│  │  29  COMPLETED     4m 18s  5 success  CREATE TABLE y2_data_platfo..  │ │
│  │  28  FAILED          12s   1 failed   SELECT * FROM nonexistent_t..  │ │
│  │  27  COMPLETED       34s   2 success  SELECT DISTINCT city FROM y..  │ │
│  │  26  COMPLETED     2m 45s  4 success  INSERT OVERWRITE TABLE poc...  │ │
│  │  ..  (scroll for more)                                                │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─ Query Detail (ID: 33) ──────────────────────────────────────────────┐ │
│  │ Status:      RUNNING (2m 14s)                                         │ │
│  │ Submitted:   14:28:15                                                  │ │
│  │ Running Jobs: [42, 41]    Succeeded: []    Failed: []                 │ │
│  │                                                                        │ │
│  │ Full Query:                                                            │ │
│  │ SELECT customer_id, first_name, last_name, phone1, send_email         │ │
│  │ FROM y2_data_platform_prod.standard.customers_scd                      │ │
│  │ WHERE (end_date IS NULL OR end_date > current_timestamp())             │ │
│  │   AND is_deleted = false                                               │ │
│  │ ORDER BY customer_id                                                   │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  /:filter  r:refresh  enter:detail  c:copy-query  q:quit  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.6 Storage View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ● RUNNING  │  ↑ 4h 23m  │  ⟳ 3s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster    Jobs    Stages    Executors    SQL   [Storage]                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Cached RDDs: 3     Total Memory: 4.2 GB     Total Disk: 0 B               │
│                                                                             │
│  ┌─ Cached RDDs ─────────────────────────────────────────────────────────┐ │
│  │ ID  NAME                     LEVEL       PARTS   MEMORY    DISK  %$  │ │
│  │──────────────────────────────────────────────────────────────────────  │ │
│  │  8  In-memory table custo..  MEM_DESER   200/200  2.8 GB   0 B  100% │ │
│  │ 12  In-memory table match..  MEM_DESER   150/200  1.2 GB   0 B   75% │ │
│  │ 15  In-memory table looku..  MEM+DISK    100/100  200 MB   0 B  100% │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─ RDD Detail (ID: 12) ────────────────────────────────────────────────┐ │
│  │ Name:            In-memory table match_candidates                     │ │
│  │ Storage Level:   MEMORY_ONLY_DESER                                    │ │
│  │ Partitions:      150 cached / 200 total (75%)                         │ │
│  │ Memory Used:     1.2 GB                                                │ │
│  │ Disk Used:       0 B                                                   │ │
│  │                                                                        │ │
│  │ Distribution Across Executors:                                         │ │
│  │   Executor 0:  320 MB  (40 partitions)                                │ │
│  │   Executor 1:  290 MB  (38 partitions)                                │ │
│  │   Executor 2:  310 MB  (39 partitions)                                │ │
│  │   Executor 3:  280 MB  (33 partitions)                                │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  (No cached RDDs? This area shows an informational message:                 │
│   "No RDDs currently cached. Use .persist() or .cache() to cache data.")   │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  s:sort  r:refresh  enter:detail  q:quit  ?:help                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.7 Degraded Mode (Cluster Terminated)

When the cluster is not RUNNING, Spark REST views degrade:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbxtop  │  my-analytics-cluster  │  ○ TERMINATED  │  --  │  ⟳ 15s  │  yad2-prod  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Cluster   [Jobs]    Stages    Executors    SQL    Storage                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                                                                             │
│                                                                             │
│              ┌──────────────────────────────────────────┐                   │
│              │                                          │                   │
│              │   Cluster is TERMINATED                  │                   │
│              │                                          │                   │
│              │   Spark REST API unavailable.             │                   │
│              │   Spark Jobs view requires a running      │                   │
│              │   cluster with an active Spark context.   │                   │
│              │                                          │                   │
│              │   Terminated at: 2026-03-16 12:45:00     │                   │
│              │   Reason: Inactivity (auto-termination)  │                   │
│              │                                          │                   │
│              │   Switch to [Cluster] view for details.  │                   │
│              │                                          │                   │
│              └──────────────────────────────────────────┘                   │
│                                                                             │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1-6:views  r:refresh  q:quit  ?:help                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Keyboard Bindings

### 5.1 Global Bindings (active in all views)

| Key | Action | Description |
|-----|--------|-------------|
| `1` | `switch_view("cluster")` | Switch to Cluster view |
| `2` | `switch_view("jobs")` | Switch to Jobs view |
| `3` | `switch_view("stages")` | Switch to Stages view |
| `4` | `switch_view("executors")` | Switch to Executors view |
| `5` | `switch_view("sql")` | Switch to SQL view |
| `6` | `switch_view("storage")` | Switch to Storage view |
| `Tab` | `next_view()` | Cycle to next view |
| `Shift+Tab` | `prev_view()` | Cycle to previous view |
| `r` | `force_refresh()` | Force immediate poll of all data sources |
| `p` | `toggle_pause()` | Pause/resume auto-polling (data freezes) |
| `q` | `quit()` | Exit dbxtop |
| `Ctrl+C` | `quit()` | Exit dbxtop (alternative) |
| `?` | `show_help()` | Toggle help overlay with all keybindings |
| `Escape` | `dismiss_overlay()` | Close help overlay, detail panel, or filter |

### 5.2 Table View Bindings (active in Jobs, Stages, Executors, SQL, Storage)

| Key | Action | Description |
|-----|--------|-------------|
| `Up` / `k` | `cursor_up()` | Move selection up |
| `Down` / `j` | `cursor_down()` | Move selection down |
| `Home` / `g` | `cursor_top()` | Jump to first row |
| `End` / `G` | `cursor_bottom()` | Jump to last row |
| `PageUp` | `page_up()` | Scroll up one page |
| `PageDown` | `page_down()` | Scroll down one page |
| `Enter` | `toggle_detail()` | Toggle detail panel for selected row |
| `s` | `cycle_sort()` | Cycle sort column (pressing repeatedly advances) |
| `S` | `toggle_sort_dir()` | Toggle sort direction (asc/desc) |
| `/` | `open_filter()` | Open text filter input |
| `Escape` | `close_filter()` | Close filter input and clear filter |

### 5.3 View-Specific Bindings

| View | Key | Action | Description |
|------|-----|--------|-------------|
| Jobs | `f` | `cycle_status_filter()` | Cycle: all -> running -> succeeded -> failed |
| Stages | `f` | `cycle_status_filter()` | Cycle: all -> active -> complete -> pending -> failed |
| Executors | `d` | `toggle_dead()` | Toggle showing dead/removed executors |
| Executors | `m` | `toggle_memory_view()` | Toggle between absolute and percentage memory |
| SQL | `c` | `copy_query()` | Copy selected query text to clipboard |
| Cluster | `e` | `toggle_events()` | Toggle events panel expanded/collapsed |
| Cluster | `l` | `toggle_libraries()` | Toggle libraries panel expanded/collapsed |

### 5.4 Filter Input Mode

When `/` activates the filter, the footer changes to show filter-mode bindings:

| Key | Action | Description |
|-----|--------|-------------|
| Any printable | `append_to_filter()` | Build filter string |
| `Backspace` | `delete_last_char()` | Remove last character from filter |
| `Enter` | `apply_and_close()` | Apply filter and return to table navigation |
| `Escape` | `clear_and_close()` | Clear filter and return to table navigation |

### 5.5 Help Overlay Content

The `?` overlay displays all bindings organized by context, rendered as a centered modal:

```
┌─── dbxtop Help ───────────────────────────────┐
│                                                │
│  NAVIGATION                                    │
│  1-6          Switch to view by number         │
│  Tab/S-Tab    Cycle views forward/back         │
│                                                │
│  TABLE                                         │
│  j/k, Up/Down  Move cursor                     │
│  Enter          Toggle detail panel             │
│  s / S          Sort column / reverse           │
│  /              Text filter                     │
│                                                │
│  CONTROL                                       │
│  r  Force refresh     p  Pause polling          │
│  q  Quit              ?  This help              │
│                                                │
│  VIEW-SPECIFIC                                 │
│  f  Status filter (Jobs/Stages)                │
│  d  Show dead executors (Executors)            │
│  c  Copy query (SQL)                           │
│                                                │
│               [Press ? or Esc to close]         │
└────────────────────────────────────────────────┘
```

---

## 6. Error Handling Strategy

### 6.1 Error Categories and Responses

#### Cluster Terminated / Not Running

| Trigger | Detection | Response |
|---------|-----------|----------|
| Cluster state is TERMINATED, TERMINATING, PENDING, ERROR | `clusters.get()` returns non-RUNNING state | 1. Disable fast poll (Spark REST). 2. Continue slow poll (SDK only) at reduced interval. 3. Spark-dependent views (Jobs, Stages, Executors, SQL, Storage) show degraded-mode placeholder with termination reason and timestamp. 4. Cluster view shows full status including termination_reason. |
| Cluster transitions TERMINATED -> RUNNING | State change detected in slow poll | 1. Re-discover Spark app ID. 2. Re-enable fast poll. 3. Restore all views. Flash "Cluster is now RUNNING" in header. |

#### Authentication Errors

| Trigger | Detection | Response |
|---------|-----------|----------|
| Token expired (401 response) | SDK raises `NotAuthenticated` or HTTP 401 | 1. Stop all polling. 2. Display full-screen error: "Authentication failed. Your token may have expired. Run `databricks auth login --profile <name>` to refresh, then restart dbxtop." 3. App stays open so user can read the message. Press `q` to quit. |
| Profile not found | SDK raises `ValueError` on `WorkspaceClient(profile=...)` | 1. Print error to stderr and exit immediately (before TUI starts): "Profile '<name>' not found in ~/.databrickscfg. Available profiles: [list]. Use --profile <name>." |
| Cluster not found (404) | SDK raises `NotFound` on `clusters.get()` | 1. Print error to stderr and exit: "Cluster '<id>' not found. It may have been permanently deleted." |

#### API Timeouts and Network Errors

| Trigger | Detection | Response |
|---------|-----------|----------|
| Request timeout (>10s) | `httpx.TimeoutException` or SDK timeout | 1. Mark affected cache slot as `stale`. 2. Preserve last-known data in views. 3. Show `(stale: 30s ago)` indicator next to view title. 4. Retry on next poll cycle (no immediate retry to avoid pileup). |
| Connection refused (Spark REST) | `httpx.ConnectError` | 1. Mark `spark_api.is_available = False`. 2. Disable fast poll. 3. Retry availability check every slow_poll cycle. 4. Show "Spark REST API unavailable" in header status. |
| DNS resolution failure | `httpx.ConnectError` | 1. Same as connection refused. If both SDK and Spark fail, show full-screen network error with workspace URL for debugging. |

#### Rate Limiting

| Trigger | Detection | Response |
|---------|-----------|----------|
| HTTP 429 Too Many Requests | Response status 429 with `Retry-After` header | 1. Respect `Retry-After` header. 2. Double the poll interval temporarily (exponential backoff). 3. Show warning in header: "Rate limited — polling slowed to Xs". 4. Gradually restore normal interval after 3 consecutive successful polls. |
| SDK built-in retry exhausted | SDK raises `TooManyRequests` | 1. Mark slot stale. 2. Backoff on next poll. 3. Log warning. |

#### Spark Application ID Stale

| Trigger | Detection | Response |
|---------|-----------|----------|
| Spark context restarted (new app ID) | Spark REST returns 404 for known app ID | 1. Clear cached app ID. 2. Re-call `discover_app_id()`. 3. If successful, resume normal polling with new ID. 4. If discovery fails, degrade to SDK-only mode. |

### 6.2 Error Display Hierarchy

Errors are displayed at different levels depending on severity:

1. **Fatal (exit before TUI)**: Profile not found, cluster not found, invalid arguments. Print to stderr, exit code 1.
2. **Full-screen modal**: Authentication failure. TUI stays up but blocks interaction until user acknowledges.
3. **Header status indicator**: Spark REST unavailable, rate limited. Non-blocking, informational.
4. **Per-view staleness indicator**: Individual API failure. Shows "(stale: Xs ago)" next to data.
5. **Per-view placeholder**: Cluster terminated. Shows explanation with context.

### 6.3 Retry Strategy

```
Retry Budget Per Poll Cycle:
  - Each API call gets max_retries (default 3) attempts
  - Backoff: 0.5s, 1.0s, 2.0s (exponential with jitter)
  - Total max wait per call: ~3.5s + request_timeout
  - If all retries fail, mark slot stale and move on
  - Never block the poll cycle waiting for one endpoint

Backoff for Rate Limiting:
  - First 429: next poll at 2x interval
  - Second consecutive 429: next poll at 4x interval
  - Max backoff: 60 seconds
  - Recovery: after 3 consecutive successes, halve the interval
  - Minimum interval: never below the configured fast_poll_s / slow_poll_s
```

---

## 7. Dependency Specification

### 7.1 Runtime Dependencies

```toml
[project]
name = "dbxtop"
requires-python = ">=3.9"
dependencies = [
    "textual>=2.1.0,<3.0",
    "databricks-sdk>=0.38.0,<1.0",
    "httpx>=0.27.0,<1.0",
    "click>=8.1.0,<9.0",
    "pydantic>=2.5.0,<3.0",
]
```

| Package | Version Constraint | Purpose |
|---------|-------------------|---------|
| `textual` | `>=2.1.0,<3.0` | TUI framework. 2.1+ required for DataTable improvements, Sparkline widget, TabbedContent stability. Pin <3.0 to avoid breaking changes. |
| `databricks-sdk` | `>=0.38.0,<1.0` | Official Databricks SDK. 0.38+ for latest clusters/jobs API. Pin <1.0 as SDK is pre-1.0 and may have breaking changes. |
| `httpx` | `>=0.27.0,<1.0` | Async HTTP client for Spark REST API proxy. Chosen over `aiohttp` for cleaner API and `requests`-like interface. |
| `click` | `>=8.1.0,<9.0` | CLI argument parsing. Lightweight, well-tested, supports auto-completion. |
| `pydantic` | `>=2.5.0,<3.0` | Data validation and serialization for API response models. v2 for performance. |

### 7.2 Development Dependencies

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24.0",
    "pytest-textual-snapshot>=1.0",
    "mypy>=1.8",
    "ruff>=0.6.0",
    "pre-commit>=3.5",
    "respx>=0.21.0",          # httpx mocking for Spark REST tests
]
```

| Package | Purpose |
|---------|---------|
| `pytest` | Test runner |
| `pytest-asyncio` | Async test support for poller and API client tests |
| `pytest-textual-snapshot` | Textual's official snapshot testing for TUI regression |
| `mypy` | Static type checking |
| `ruff` | Linting and formatting (replaces black + flake8 + isort) |
| `pre-commit` | Git hook management |
| `respx` | Mock HTTP responses for httpx-based Spark REST client |

### 7.3 Package Configuration

```toml
[project.scripts]
dbxtop = "dbxtop.cli:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dbxtop"]

[tool.ruff]
target-version = "py39"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "ANN", "B", "A", "COM", "C4", "T20", "SIM", "TCH"]

[tool.mypy]
python_version = "3.9"
strict = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

---

## 8. Key Design Decisions

### 8.1 Textual over Rich (or curses/urwid/blessed)

**Decision:** Use Textual as the TUI framework.

**Why:**
- **Textual is built on Rich** — gets Rich's rendering quality (colors, Unicode box drawing, markdown) with full interactive widget support (DataTable, TabbedContent, Input, ProgressBar).
- **Async-native** — Textual's event loop is `asyncio`-based, which aligns perfectly with async HTTP polling. No threading needed for the UI layer.
- **CSS-like styling** — `.tcss` files enable theme-able, maintainable layouts without hardcoding positions. This matters for supporting different terminal sizes.
- **DataTable widget** — built-in sortable, scrollable table that handles thousands of rows efficiently. Critical for the Jobs/Stages/Executors views.
- **Snapshot testing** — `pytest-textual-snapshot` enables regression testing of the entire TUI output, catching visual regressions automatically.
- **Active ecosystem** — Textual is under active development by Textualize (the Rich creators), with frequent releases and good documentation.

**Alternatives considered:**
- `Rich` alone — no interactivity (no keyboard navigation, no live table scrolling)
- `curses` — low-level, no Unicode/color abstraction, painful cross-platform
- `urwid` — mature but dated API, no async-native event loop, weaker styling
- `blessed` — thin curses wrapper, similar problems

### 8.2 Two-Tier Polling over Single Interval

**Decision:** Separate fast (3s) and slow (15s) polling loops.

**Why:**
- **Spark REST API is cheap and fast** — executor/job/stage endpoints return in <100ms via proxy. Polling every 3s gives near-real-time task progress without meaningful cost.
- **Databricks SDK calls are slower** — cluster.get() takes 200-500ms, events/jobs take more. Polling these at 3s would waste 30%+ of each interval on SDK overhead.
- **Different change rates** — task counts change every second; cluster state changes every few minutes. Matching poll interval to change rate avoids wasted work.
- **Rate limit budget** — the Databricks REST API has per-workspace rate limits (~30-60 req/s). Slowing SDK calls preserves budget for the fast loop.

### 8.3 httpx over aiohttp/requests

**Decision:** Use `httpx` for the Spark REST API client.

**Why:**
- **Async-native with sync fallback** — `httpx.AsyncClient` integrates directly with Textual's `asyncio` loop. No `asyncio.to_thread()` needed (unlike `requests`).
- **requests-compatible API** — familiar interface reduces learning curve. Nearly drop-in replacement.
- **Connection pooling** — `AsyncClient` maintains a connection pool, important for frequent polling to the same proxy endpoint.
- **Better timeout control** — fine-grained timeout configuration (connect, read, write, pool) vs aiohttp's less intuitive timeout API.
- **Type hints** — better typed than aiohttp, works well with mypy strict mode.

**Why not `aiohttp`:** Heavier dependency, less intuitive API, `ClientSession` lifecycle management is error-prone.

**Why not `requests`:** Synchronous — would require `asyncio.to_thread()` wrappers for every call, adding complexity and thread-pool management overhead.

### 8.4 Pydantic Models over Raw Dicts

**Decision:** Normalize all API responses into Pydantic v2 models.

**Why:**
- **Type safety** — views operate on typed models, not nested dicts. Catches mistyped field names at development time, not runtime.
- **Decoupling** — if Databricks or Spark changes a response field name, only `models.py` needs updating. Views are insulated.
- **Computed fields** — derived metrics like `gc_ratio`, `memory_used_pct`, `fraction_cached` are computed once during model construction, not re-derived in every view render.
- **Validation** — Pydantic validates types on construction, catching malformed API responses early with clear error messages.
- **Serialization** — if we later add features like "export current state to JSON," Pydantic's `.model_dump_json()` handles it.

### 8.5 Single Cluster Per Session

**Decision:** Monitor exactly one cluster per `dbxtop` invocation.

**Why:**
- **Simplicity** — multi-cluster monitoring multiplies complexity (multiple pollers, connection pools, app IDs, error states) without proportional user value.
- **Performance** — one cluster's polling fits comfortably in a single async event loop. Multi-cluster would require careful budgeting of rate limits and connection pools.
- **UX clarity** — every view shows data for one cluster. No ambiguity about which cluster a metric belongs to.
- **Composability** — users who want multi-cluster monitoring can run multiple `dbxtop` instances in tmux/screen panes. This is the Unix way.

### 8.6 DataCache Between Poller and Views

**Decision:** Introduce an explicit cache layer rather than having views poll directly or the poller push data directly to widgets.

**Why:**
- **Decouples poll timing from render timing** — views re-render on user interaction (scrolling, sorting) between polls. They need access to the latest data without triggering new API calls.
- **Staleness tracking** — the cache tracks when each data category was last refreshed, enabling "stale data" indicators in the UI.
- **Error memory** — failed polls preserve the previous successful data. Without a cache, a transient error would blank the view.
- **Time-series accumulation** — sparkline charts need historical values. The cache accumulates these across poll cycles.
- **Thread safety is free** — since Textual is single-threaded (async event loop), there are no race conditions between the poller writing and views reading.

### 8.7 Click over argparse/typer

**Decision:** Use Click for CLI argument parsing.

**Why:**
- **Minimal dependency** — Click is already an indirect dependency of many Python packages. Small footprint.
- **Auto-completion** — Click supports shell auto-completion out of the box, useful for `--profile` and `--cluster-id` arguments.
- **Composability** — if we later add subcommands (e.g., `dbxtop version`, `dbxtop profiles`), Click handles this naturally.
- **Well-tested** — Click is one of the most battle-tested CLI frameworks in the Python ecosystem.

**Why not Typer:** Typer adds a layer over Click with type annotations. Useful for complex CLIs, overkill for dbxtop's ~5 arguments. Also adds a dependency.

**Why not argparse:** Standard library but lacks auto-completion, group support, and has a clunkier API.

### 8.8 Driver Proxy API over Command Execution API

**Decision:** Access Spark metrics via the driver-proxy-api (HTTP proxy to Spark UI) rather than executing commands on the cluster.

**Why:**
- **Read-only** — the REST API is purely observational. Command Execution API creates execution contexts and runs code, which is invasive.
- **Works on shared clusters** — shared access mode blocks `sparkContext` access via Command Execution, but the Spark UI REST API works regardless of access mode.
- **No side effects** — no execution contexts to manage, no risk of interfering with running workloads.
- **Lower latency** — HTTP GET to the proxy is faster than creating a context, executing a command, and parsing the result.
- **No state management** — Command Execution requires context creation, polling for results, and context cleanup. The REST API is stateless.

---

## 9. Configuration & CLI

### 9.1 CLI Interface

```
Usage: dbxtop [OPTIONS]

  Real-time terminal dashboard for Databricks/Spark clusters.

Options:
  -p, --profile TEXT       Databricks CLI profile name (from ~/.databrickscfg)
                           [default: DEFAULT]
  -c, --cluster-id TEXT    Cluster ID to monitor [required]
  --spark-port INTEGER     Spark UI proxy port on driver [default: 40001]
  --fast-refresh FLOAT     Fast poll interval in seconds (Spark REST)
                           [default: 3.0]
  --slow-refresh FLOAT     Slow poll interval in seconds (SDK)
                           [default: 15.0]
  --timeout FLOAT          HTTP request timeout in seconds [default: 10.0]
  --no-spark               Disable Spark REST API (SDK-only mode)
  --version                Show version and exit.
  --help                   Show this message and exit.
```

### 9.2 Examples

```bash
# Basic usage with profile and cluster ID
dbxtop --profile yad2-prod --cluster-id 0311-160038-vjuv0ah9

# Short flags
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9

# Faster refresh for active debugging
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9 --fast-refresh 1.0

# SDK-only mode (no Spark REST, works even on terminated clusters)
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9 --no-spark

# Using DEFAULT profile
dbxtop -c 0311-160038-vjuv0ah9
```

### 9.3 Configuration Resolution

Settings are resolved in this priority order (highest first):

1. **CLI arguments** — `--profile`, `--cluster-id`, etc.
2. **Environment variables** — `DBXTOP_PROFILE`, `DBXTOP_CLUSTER_ID`, `DBXTOP_SPARK_PORT`, `DBXTOP_FAST_REFRESH`, `DBXTOP_SLOW_REFRESH`
3. **Config file** — `~/.config/dbxtop/config.toml` (optional, for persistent defaults)
4. **Built-in defaults** — see `config.py` Settings dataclass

### 9.4 Config File Format (Optional)

```toml
# ~/.config/dbxtop/config.toml
profile = "yad2-prod"
spark_port = 40001
fast_refresh = 3.0
slow_refresh = 15.0
timeout = 10.0
```

### 9.5 Startup Sequence

```
1. Parse CLI args (click)
2. Resolve Settings (CLI → env → config file → defaults)
3. Validate: cluster_id is required
4. Create WorkspaceClient(profile=settings.profile)
5. Test connection: clusters.get(cluster_id)
   - If 401: print auth error, exit 1
   - If 404: print cluster-not-found, exit 1
   - If connection error: print network error, exit 1
6. Detect cluster state
   - If RUNNING: probe Spark REST API availability
   - If not RUNNING: start in SDK-only mode
7. Discover org_id from workspace URL (for proxy URL construction)
8. Create DbxtopApp with Settings + clients
9. app.run() — Textual takes over the terminal
```

### 9.6 Org ID Discovery

The Databricks driver proxy URL requires the organization ID. Discovery approach:

```python
# Method 1: Parse from workspace URL (most reliable)
# URL format: https://adb-<workspace-id>.<random>.azuredatabricks.net
# The workspace-id IS the org-id for Azure Databricks.

# Method 2: From SDK config
# w.config.host contains the workspace URL.

# Method 3: API call
# GET /api/2.0/clusters/get returns the cluster's organization context.

# For AWS Databricks, org_id may be "0" or parsed from the workspace URL.
```

---

## 10. Testing Strategy

### 10.1 Test Structure

```
tests/
├── conftest.py              # Shared fixtures: mock clients, sample data
├── test_cli.py              # CLI argument parsing and validation
├── test_config.py           # Settings resolution from multiple sources
├── api/
│   ├── test_client.py       # DatabricksClient with mocked SDK
│   ├── test_spark_api.py    # SparkRESTClient with mocked httpx (respx)
│   ├── test_poller.py       # MetricsPoller logic, error handling
│   ├── test_cache.py        # DataCache staleness, time-series
│   └── test_models.py       # Pydantic model validation edge cases
├── views/
│   ├── test_cluster.py      # Snapshot tests for ClusterView
│   ├── test_jobs.py         # Snapshot tests for JobsView
│   ├── test_stages.py       # Snapshot tests for StagesView
│   ├── test_executors.py    # Snapshot tests for ExecutorsView
│   ├── test_sql.py          # Snapshot tests for SQLView
│   └── test_storage.py      # Snapshot tests for StorageView
└── integration/
    └── test_app_lifecycle.py # Full app start/stop, view switching
```

### 10.2 Testing Approach by Layer

| Layer | Tool | Strategy |
|-------|------|----------|
| CLI | Click's `CliRunner` | Test argument parsing, validation, error messages |
| API Clients | `respx` (httpx mock) + `unittest.mock` (SDK) | Mock all external calls, test response parsing, error handling |
| Poller | `pytest-asyncio` | Test poll orchestration, error propagation, backoff logic |
| Cache | Plain pytest | Test staleness tracking, ring buffer behavior |
| Models | Plain pytest | Test edge cases: missing fields, null handling, computed fields |
| Views | `pytest-textual-snapshot` | Snapshot test rendered output against known-good SVG |
| Integration | Textual's `pilot` async test driver | Test view switching, keyboard navigation, data flow |

### 10.3 Fixtures

```python
# conftest.py — key fixtures

@pytest.fixture
def sample_cluster_info() -> ClusterInfo:
    """A RUNNING cluster with typical configuration."""

@pytest.fixture
def sample_executors() -> list[ExecutorInfo]:
    """5 executors (1 driver + 4 workers) with varied metrics."""

@pytest.fixture
def terminated_cluster_info() -> ClusterInfo:
    """A TERMINATED cluster for degraded-mode testing."""

@pytest.fixture
def mock_spark_api(sample_executors, ...) -> SparkRESTClient:
    """SparkRESTClient with pre-configured respx mocks."""

@pytest.fixture
def mock_dbx_client(sample_cluster_info, ...) -> DatabricksClient:
    """DatabricksClient with mocked SDK calls."""
```

---

## 11. Future Considerations

These are explicitly out of scope for v0.1 but inform the architecture to avoid painting ourselves into a corner:

### 11.1 Multi-Cluster Mode (v0.2+)
The single-cluster design makes this a UX problem (cluster picker or split-pane), not an architecture problem. `MetricsPoller` and `DataCache` are already per-cluster; a multi-cluster mode would instantiate multiple pairs.

### 11.2 SQL Warehouse Monitoring (v0.2+)
SQL Warehouses have their own lifecycle and metrics APIs. A new `WarehouseView` could be added to the tab bar. The `DatabricksClient` already has access to `w.warehouses`.

### 11.3 Alerting / Thresholds (v0.3+)
The `DataCache` could emit threshold-crossing events (e.g., GC% > 10% for 5 consecutive polls). Views would highlight rows, and optionally desktop notifications could be sent.

### 11.4 Historical Data Export (v0.2+)
The time-series ring buffers in `DataCache` could be exported to CSV or JSON on demand, enabling post-mortem analysis of sessions.

### 11.5 Theming (v0.2+)
Textual's `.tcss` system supports multiple themes. A `--theme` CLI flag could switch between dark, light, and high-contrast color schemes.

### 11.6 Plugin System (v0.4+)
Custom views could be registered as plugins (entry points), enabling users to add specialized views for their workloads (e.g., Structured Streaming, MLflow, DLT).

---

## Appendix A: Spark REST API Proxy URL Construction

```python
def build_spark_proxy_base(workspace_url: str, org_id: str, cluster_id: str, port: int) -> str:
    """
    Build the base URL for the Spark REST API via the Databricks driver proxy.

    Example:
        workspace_url = "https://adb-1234567890.12.azuredatabricks.net"
        org_id = "1234567890"
        cluster_id = "0311-160038-vjuv0ah9"
        port = 40001

        Returns: "https://adb-1234567890.12.azuredatabricks.net/driver-proxy-api/o/1234567890/0311-160038-vjuv0ah9/40001"
    """
    base = workspace_url.rstrip("/")
    return f"{base}/driver-proxy-api/o/{org_id}/{cluster_id}/{port}"
```

## Appendix B: Message Types (Textual Events)

```python
class DataUpdated(Message):
    """Posted by MetricsPoller after writing fresh data to cache."""
    source: str  # "fast" or "slow" — which poll loop triggered this

class SparkApiStatusChanged(Message):
    """Posted when Spark REST API becomes available or unavailable."""
    available: bool
    error: str | None

class ClusterStateChanged(Message):
    """Posted when cluster state transitions (e.g., RUNNING -> TERMINATING)."""
    old_state: ClusterState
    new_state: ClusterState

class PollPaused(Message):
    """Posted when user toggles polling pause."""
    paused: bool
```

## Appendix C: Color Scheme Reference

| Element | Color | Textual CSS Variable |
|---------|-------|---------------------|
| Header background | Dark blue | `$primary-background` |
| RUNNING state | Green | `$success` |
| TERMINATED state | Red | `$error` |
| PENDING state | Yellow | `$warning` |
| Active row highlight | Cyan background | `$accent` |
| Stale data indicator | Dim yellow | `$warning-muted` |
| GC warning (>5%) | Orange | `$warning` |
| GC critical (>10%) | Red | `$error` |
| Progress bar fill | Green | `$success` |
| Progress bar empty | Dark gray | `$surface` |
| Memory bar fill | Blue | `$primary` |
| Memory bar critical (>85%) | Red | `$error` |
| Table header | Bold white on dark | `$primary-background` |
| Table alternating rows | Slight shade | `$surface` / `$panel` |
