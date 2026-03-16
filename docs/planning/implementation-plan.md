# dbxtop Implementation Plan

> **Version:** 1.0.0
> **Date:** 2026-03-16
> **Status:** Approved — Ready for Implementation
> **Repo:** `/Users/ozlevi/Development/dbxtop/`
> **Companion Documents:** `architecture.md`, `specification.md`, `api-research.md`

---

## Overview

This plan breaks the `dbxtop` implementation into 7 milestones with 38 discrete tasks. Each milestone produces a testable, demonstrable increment of functionality. Dependencies flow strictly downward — no milestone depends on a later one.

### Sizing Legend

| Size | Meaning | Approximate Effort |
|------|---------|-------------------|
| **[S]** | Small — single file, straightforward logic | 30-60 min |
| **[M]** | Medium — multiple concerns, some design decisions | 1-3 hours |
| **[L]** | Large — cross-cutting, complex async logic, significant testing | 3-6 hours |

### Milestone Dependency Graph

```
M1: Core API Infrastructure
 └──► M2: TUI Shell
       └──► M3: Cluster View (first live data)
             ├──► M4: Jobs + Stages Views
             │     └──► M5: Executors + SQL + Storage Views
             │           └──► M6: Polish & Error Handling
             └──────────────────► M7: Packaging & Release
```

---

## M1: Core API Infrastructure

**Goal:** All data models, API clients, cache, and poller are implemented and unit-tested. No TUI yet — this is the foundation layer.

**Exit Criteria:** `pytest tests/` passes with >90% coverage on `api/` and `config.py`. Can run a script that polls a live cluster and prints structured data to stdout.

### M1.1 [M] `api/models.py` — Pydantic v2 Data Models

**File:** `src/dbxtop/api/models.py`

Implement all Pydantic v2 models that normalize raw API responses into a stable internal schema:

- **Enums:** `ClusterState` (PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR), `JobStatus` (RUNNING, SUCCEEDED, FAILED, UNKNOWN), `StageStatus` (ACTIVE, COMPLETE, PENDING, FAILED), `StorageLevel`
- **Models:**
  - `ClusterInfo` — cluster_id, cluster_name, state, state_message, uptime_seconds (computed from start_time), driver_node_type, worker_node_type, num_workers, autoscale_min/max, total_cores, total_memory_mb, spark_version, data_security_mode, runtime_engine, creator, autotermination_minutes, spark_conf, tags
  - `ClusterEvent` — timestamp, event_type, message, details
  - `SparkJob` — job_id, name, status, submission_time, completion_time, num_tasks, num_active_tasks, num_completed_tasks, num_failed_tasks, num_stages, num_active_stages, num_completed_stages, num_failed_stages, stage_ids
  - `SparkStage` — stage_id, attempt_id, name, status, num_tasks, num_active_tasks, num_complete_tasks, num_failed_tasks, input_bytes, input_records, output_bytes, output_records, shuffle_read_bytes, shuffle_write_bytes, executor_run_time_ms, executor_cpu_time_ns, spill_bytes (memory + disk), submission_time, completion_time
  - `ExecutorInfo` — executor_id, is_driver (computed: id == "driver"), is_active, host_port, total_cores, max_tasks, active_tasks, completed_tasks, failed_tasks, total_duration_ms, total_gc_time_ms, gc_ratio (computed: gc_time/duration), max_memory, memory_used, memory_used_pct (computed), disk_used, total_input_bytes, total_shuffle_read, total_shuffle_write, rdd_blocks, peak_jvm_heap, peak_jvm_off_heap, add_time, remove_time, remove_reason
  - `SQLQuery` — execution_id, status, description, submission_time, duration_ms, running_jobs, success_jobs, failed_jobs
  - `RDDInfo` — rdd_id, name, num_partitions, num_cached_partitions, storage_level, memory_used, disk_used, fraction_cached (computed)
  - `JobRun` — run_id, job_id, run_name, state, result_state, start_time, end_time, setup_duration_ms, execution_duration_ms, creator, run_type, task_count
  - `LibraryInfo` — name, library_type, status, messages
- **Computed fields:** Use `@computed_field` for gc_ratio, memory_used_pct, is_driver, fraction_cached, uptime_seconds
- **Utility functions:** `format_bytes(n: int) -> str` (human-readable: B, KB, MB, GB, TB), `format_duration(ms: int) -> str` (Xd Xh Xm Xs), `format_timestamp(dt: datetime) -> str`
- **Validation:** All byte fields default to 0 when None, timestamps use `datetime` with UTC normalization

### M1.2 [M] `config.py` — Settings and Configuration

**File:** `src/dbxtop/config.py`

Implement the `Settings` dataclass that merges configuration from CLI args, environment variables, and defaults:

```python
@dataclass(frozen=True)
class Settings:
    profile: str              # Databricks CLI profile name
    cluster_id: str           # Target cluster ID
    spark_port: int = 40001   # Driver proxy port for Spark UI
    fast_poll_s: float = 3.0  # Spark REST API poll interval
    slow_poll_s: float = 15.0 # SDK poll interval
    max_retries: int = 3      # Per-request retry limit
    request_timeout_s: float = 10.0  # HTTP timeout per request
    theme: str = "dark"       # Color theme (dark/light)
```

- **Resolution order:** CLI flag > env var (`DBXTOP_*`) > default
- **Validation:** fast_poll_s in [1.0, 30.0], slow_poll_s in [5.0, 120.0], slow_poll_s >= fast_poll_s, theme in {dark, light}
- **Factory method:** `Settings.from_cli(profile, cluster_id, refresh, slow_refresh, theme) -> Settings`

### M1.3 [L] `api/client.py` — Databricks SDK Client Wrapper

**File:** `src/dbxtop/api/client.py`

Wrap the `databricks-sdk` `WorkspaceClient` with async-compatible methods:

- **Init:** `DatabricksClient(profile: str, cluster_id: str)` — creates `WorkspaceClient(profile=profile)`
- **Methods (all async, using `asyncio.to_thread`):**
  - `get_cluster() -> ClusterInfo` — calls `w.clusters.get(cluster_id)`, maps to Pydantic model
  - `get_events(since: datetime | None = None) -> list[ClusterEvent]` — calls `w.clusters.events(cluster_id, start_time=since)`, maps to model, returns most recent 50
  - `get_job_runs(active_only: bool = True) -> list[JobRun]` — calls `w.jobs.list_runs(active_only=active_only)`, returns up to 20
  - `get_library_status() -> list[LibraryInfo]` — calls `w.libraries.cluster_status(cluster_id)`, maps to model
  - `get_workspace_url() -> str` — extracts host from WorkspaceClient config
  - `get_org_id() -> str` — extracts org_id from cluster response or workspace config
- **Error handling:**
  - `DatabricksError` with 401/403 -> raise `AuthenticationError` (custom exception)
  - 404 on cluster -> raise `ClusterNotFoundError`
  - Timeout / connection error -> raise `ConnectionError` with retry advice
- **Mapping functions:** Private `_map_cluster(raw) -> ClusterInfo`, `_map_event(raw) -> ClusterEvent`, etc. — one per model, handling None fields and type coercion

### M1.4 [L] `api/spark_api.py` — Spark REST API Client

**File:** `src/dbxtop/api/spark_api.py`

Async HTTP client using `httpx.AsyncClient` for the Spark REST API v1 via the Databricks driver proxy:

- **Init:** `SparkRESTClient(workspace_url: str, cluster_id: str, org_id: str, port: int = 40001, token: str | None = None, timeout: float = 10.0)`
- **URL construction:** `{workspace_url}/driver-proxy-api/o/{org_id}/{cluster_id}/{port}/api/v1/applications/{app_id}/{endpoint}`
- **Methods (all async):**
  - `discover_app_id() -> str` — `GET /api/v1/applications`, cache the first app ID. Handle 404 (no Spark app yet) gracefully.
  - `get_jobs(status: str | None = None) -> list[SparkJob]` — `GET /api/v1/applications/{app_id}/jobs?status={status}`
  - `get_stages(status: str | None = None) -> list[SparkStage]` — `GET /api/v1/applications/{app_id}/stages?status={status}`
  - `get_stage_detail(stage_id: int, attempt_id: int = 0) -> SparkStage` — detailed stage with task metrics
  - `get_executors() -> list[ExecutorInfo]` — `GET /api/v1/applications/{app_id}/allexecutors`
  - `get_sql() -> list[SQLQuery]` — `GET /api/v1/applications/{app_id}/sql?details=true`
  - `get_storage() -> list[RDDInfo]` — `GET /api/v1/applications/{app_id}/storage/rdd`
  - `is_available() -> bool` — health check, returns False if proxy unreachable or no Spark app
  - `close() -> None` — close the httpx client
- **Error handling:**
  - 404 on any endpoint -> Spark app not running, set `_available = False`
  - httpx.TimeoutException -> log warning, return stale data marker
  - httpx.ConnectError -> Spark REST unreachable, degrade gracefully
  - Re-discover app_id on 404 after previously being available (Spark context restarted)
- **Auth:** Pass Databricks token as `Authorization: Bearer {token}` header. Extract token from WorkspaceClient.
- **Connection pooling:** Single `httpx.AsyncClient` instance with connection keep-alive

### M1.5 [M] `api/cache.py` — Data Cache with Ring Buffers

**File:** `src/dbxtop/api/cache.py`

In-memory cache that sits between the poller and views:

```python
@dataclass
class CacheSlot(Generic[T]):
    data: T | None = None
    last_updated: datetime | None = None
    error: str | None = None
    stale: bool = False
    history: deque[T] = field(default_factory=lambda: deque(maxlen=60))

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
```

- **`update(slot_name, data)`** — set data, update last_updated, clear error/stale, append to history ring buffer
- **`mark_error(slot_name, error_msg)`** — set stale=True, preserve existing data, record error
- **`get(slot_name) -> CacheSlot`** — return the slot
- **`is_stale(slot_name, expected_interval: float) -> bool`** — True if last_updated > 2x expected_interval ago
- **`get_history(slot_name) -> deque`** — return the ring buffer for sparkline rendering
- **Ring buffers:** 60 entries (at 3s poll = 3 min of history). Used for sparkline widgets showing executor memory, GC%, active tasks over time.

### M1.6 [L] `api/poller.py` — Async 2-Tier Polling Orchestrator

**File:** `src/dbxtop/api/poller.py`

The async orchestrator that drives all data collection:

- **Init:** `MetricsPoller(dbx_client: DatabricksClient, spark_client: SparkRESTClient, cache: DataCache, app: App, settings: Settings)`
- **Lifecycle:**
  - `start()` — create two Textual timers (fast and slow) via `self.app.set_interval()`
  - `stop()` — cancel both timers, close clients
- **Fast poll callback (every `fast_poll_s`):**
  1. Check `spark_client.is_available()` — skip if False
  2. `asyncio.gather(get_executors, get_jobs, get_stages, return_exceptions=True)`
  3. For each result: success -> `cache.update()`, exception -> `cache.mark_error()`
  4. Post `DataUpdated` message to the app
- **Slow poll callback (every `slow_poll_s`):**
  1. `asyncio.gather(get_cluster, get_events, get_job_runs, get_library_status, get_sql, get_storage, return_exceptions=True)`
  2. Update cache slots, post `DataUpdated`
  3. On cluster state change to TERMINATED/ERROR: mark Spark slots as unavailable
  4. On cluster state change to RUNNING after TERMINATED: trigger `spark_client.discover_app_id()` to reconnect
- **Backoff:** On consecutive errors for the same slot, double the poll interval for that slot (up to 4x), reset on success
- **`DataUpdated` message:** Custom Textual message containing `updated_slots: set[str]` so views only re-render when their data changes

### M1.7 [S] Unit Tests — Models, URL Construction, Cache

**Files:** `tests/test_models.py`, `tests/test_cache.py`, `tests/test_spark_api.py`

- **Model tests:**
  - Serialization round-trip for each model (dict -> model -> dict)
  - Computed fields (gc_ratio, memory_used_pct, is_driver, fraction_cached)
  - Default values for None fields
  - `format_bytes()` and `format_duration()` edge cases (0, negatives, very large numbers)
- **Cache tests:**
  - Update slot, verify data and timestamp
  - Mark error, verify stale=True and data preserved
  - Ring buffer fills to maxlen and evicts oldest
  - `is_stale()` logic with mocked time
- **Spark API URL tests:**
  - Verify driver proxy URL construction with different workspace URLs (with/without trailing slash)
  - Verify app_id caching behavior
  - Use `httpx.MockTransport` for response mocking

---

## M2: TUI Shell

**Goal:** The app launches, displays header/footer/tabs, responds to keyboard bindings. Tabs show placeholder content. No real data yet.

**Exit Criteria:** `dbxtop --cluster-id test123 --profile DEFAULT` launches a TUI with 6 tabs, number keys switch tabs, `q` quits. Textual snapshot test captures the initial state.

**Depends on:** M1

### M2.1 [M] `app.py` — Full DbxTopApp Wiring

**File:** `src/dbxtop/app.py`

Extend the existing skeleton:

- **`on_mount()`:**
  1. Create `Settings` from stored init params
  2. Create `DatabricksClient(settings.profile, settings.cluster_id)`
  3. Create `DataCache()`
  4. Probe cluster state — if RUNNING, create `SparkRESTClient` with workspace_url/org_id/cluster_id
  5. Create `MetricsPoller` and call `poller.start()`
  6. Handle startup errors: auth failure -> show error screen, cluster not found -> show error screen
- **`on_unmount()`:** Stop poller, close clients
- **`compose()`:** Replace `Header`/`Footer` with custom `ClusterHeader`/`KeyboardFooter`. Mount 6 view instances into `TabPane` containers.
- **`on_data_updated(event: DataUpdated)`:** Forward to the currently active view's `refresh_data()` method
- **`action_force_refresh()`:** Trigger immediate fast and slow polls
- **`action_switch_tab(tab_id)`:** Already implemented, verify it works with new views
- **Bindings:** Add `?` for help overlay, `/` for filter input, `Escape` to clear filter

### M2.2 [M] `widgets/header.py` — Cluster Header Bar

**File:** `src/dbxtop/widgets/header.py`

Custom `Static` widget (1 row high):

- **Layout:** `dbxtop | {cluster_name} | {state_badge} | up {uptime} | next {countdown}s | {profile}`
- **State badge:** Colored circle (`●`/`○`) + state text, color mapped from `ClusterState` enum (RUNNING=green, PENDING/RESTARTING/RESIZING=yellow, TERMINATING/TERMINATED/ERROR=red)
- **Uptime:** Computed from `start_time`, formatted per spec (Xs, Xm Ys, Xh Ym, Xd Yh Zm). Show `--` when cluster not running.
- **Countdown:** Timer that counts down from `fast_poll_s` to 0, resets on each poll completion
- **Stale indicator:** When any cache slot is stale, append yellow warning text
- **Updates:** Re-render on `DataUpdated` messages and every 1s for countdown/uptime

### M2.3 [S] `widgets/footer.py` — Keyboard Shortcuts Footer

**File:** `src/dbxtop/widgets/footer.py`

Custom footer displaying context-sensitive keybindings:

- **Global bindings:** `1-6:Views | r:Refresh | /:Filter | ?:Help | q:Quit`
- **View-specific:** When on Jobs view, add `s:Sort | Enter:Detail`. Similar per-view additions.
- **Connection status:** Right-aligned indicator showing `Spark: connected` (green) or `Spark: disconnected` (red)

### M2.4 [M] `views/base.py` — BaseView Abstract Class

**File:** `src/dbxtop/views/base.py`

Abstract base that all 6 views extend:

```python
class BaseView(Widget):
    current_sort_key: str = ""
    sort_reverse: bool = False
    filter_text: str = ""

    @abstractmethod
    def refresh_data(self, cache: DataCache) -> None: ...

    def cycle_sort(self, column: str) -> None:
        # Same column: toggle direction. New column: ascending.

    def apply_filter(self, text: str) -> None:
        # Set filter_text, trigger re-render

    def render_stale_indicator(self, slot: CacheSlot) -> str:
        # Returns "(stale: Xs ago)" or empty string
```

- **DataTable integration:** Provide helper `build_table(columns, rows, sort_key, filter_text)` that handles sorting and filtering generically
- **Empty state:** When no data, show centered "Waiting for data..." or "No {items} found"

### M2.5 [S] `styles/default.tcss` — Dark Theme CSS

**File:** `src/dbxtop/styles/default.tcss`

Replace minimal CSS with full theme:

- **Color variables:** Define CSS variables for status colors (green, yellow, red, dim), backgrounds, borders
- **DataTable:** Row highlighting on hover, alternating row backgrounds, header styling (bold, underlined)
- **Status badges:** `.status-running { color: green; }`, `.status-failed { color: red; }`, etc.
- **Layout:** TabbedContent fills viewport minus header/footer. Views fill their tab pane.
- **Gauges/bars:** Styling for progress bars, memory bars, GC% highlights
- **Responsive:** Minimum terminal width 80 cols. Below that, truncate columns progressively.

### M2.6 [S] CLI-to-App Wiring Verification

**Files:** `src/dbxtop/cli.py` (update), `tests/test_cli.py` (extend)

- Update `cli.py` to pass all options through `Settings.from_cli()` and into `DbxTopApp`
- Add validation: `--refresh` range check, `--slow-refresh` >= `--refresh`
- Add `-p` and `-c` as short flags for `--profile` and `--cluster-id`
- **Test:** Verify `dbxtop --cluster-id X --profile Y` creates the app with correct settings (mock the `app.run()` call)

---

## M3: Cluster View — First Live Data

**Goal:** The Cluster tab shows real, live data from a Databricks cluster. This is the proof-of-concept milestone.

**Exit Criteria:** Connect to a real cluster, see cluster name, state, resources, events, and libraries updating in real time.

**Depends on:** M2

### M3.1 [M] `views/cluster.py` — Cluster Overview

**File:** `src/dbxtop/views/cluster.py`

Multi-section layout:

- **Top: Identity Card** — Cluster name (large text), cluster ID, Spark version, DBR runtime engine (Standard/Photon), data security mode, creator, auto-termination setting
- **Middle-Left: Resource Gauges**
  - Total CPU cores: `{used}/{total}` with a simple bar
  - Total memory: `{used}/{total}` formatted in GB with a bar
  - Workers: `{current}` (or `{current} [{min}-{max}]` if autoscaling)
  - Driver node type
  - Worker node type
- **Middle-Right: Spark Configuration** — Key spark.conf entries in a 2-column key/value list (truncate long values)
- **State card:** Large colored badge showing cluster state with state_message below it

### M3.2 [S] Events Sub-Table

**Within:** `views/cluster.py`

- DataTable at the bottom of the Cluster view: Timestamp | Event Type | Details
- Show last 20 events, most recent first
- Color-code event types: TERMINATING/ERROR=red, UPSIZE/DOWNSIZE=yellow, RUNNING=green
- Auto-scroll to newest event on update

### M3.3 [S] Libraries Sub-Table

**Within:** `views/cluster.py`

- Small DataTable: Library Name | Type | Status
- Color-code status: INSTALLED=green, PENDING=yellow, FAILED=red, UNINSTALL_ON_RESTART=dim
- Only show if libraries are present (hide section if empty)

### M3.4 [S] Integration Test — Cluster View

**File:** `tests/test_integration.py`

- Mark with `@pytest.mark.integration`
- Connect to a real cluster (cluster_id from env var `DBXTOP_TEST_CLUSTER_ID`)
- Verify `DatabricksClient.get_cluster()` returns valid `ClusterInfo`
- Verify `DatabricksClient.get_events()` returns a list
- Verify the Cluster view renders without exceptions when fed real data

---

## M4: Jobs + Stages Views — Core Spark Monitoring

**Goal:** The two most frequently used views for active Spark monitoring are complete.

**Exit Criteria:** Jobs and Stages tabs show real Spark job/stage data with progress bars, sorting, and filtering.

**Depends on:** M3

### M4.1 [M] `views/jobs.py` — Spark Jobs DataTable

**File:** `src/dbxtop/views/jobs.py`

DataTable with columns:

| Column | Width | Source | Format |
|--------|-------|--------|--------|
| Job ID | 8 | `SparkJob.job_id` | Right-aligned integer |
| Description | flex | `SparkJob.name` | Truncated to fit, ellipsis |
| Status | 12 | `SparkJob.status` | Color-coded badge (RUNNING=green, SUCCEEDED=dim, FAILED=red) |
| Progress | 20 | computed | ProgressCell widget: `[=====>    ] 847/1000` |
| Stages | 10 | computed | `{active+complete}/{total}` |
| Duration | 12 | computed | Human-readable from submission_time |
| Submitted | 16 | `SparkJob.submission_time` | `HH:MM:SS` or `Mar 16 HH:MM` if >24h ago |

- Default sort: job_id descending (newest first)
- Running jobs pinned to top, then sorted by job_id desc
- Supports `s` key to cycle sort column, `/` to filter by description text

### M4.2 [M] `widgets/progress_cell.py` — Progress Bar Widget

**File:** `src/dbxtop/widgets/progress_cell.py`

Inline progress bar for DataTable cells:

- Input: `completed: int, total: int, failed: int = 0`
- Render: `[=========>   ] 847/1000` using Unicode block characters
- Bar color: green when no failures, yellow when in progress, red portion for failed tasks
- Width: Adapts to available cell width (minimum 10 chars)
- When total=0, show `[          ] 0/0` (empty bar)

### M4.3 [L] `views/stages.py` — Stages DataTable

**File:** `src/dbxtop/views/stages.py`

DataTable with 11 columns:

| Column | Width | Source | Format |
|--------|-------|--------|--------|
| Stage ID | 8 | `SparkStage.stage_id` | Right-aligned |
| Name | flex | `SparkStage.name` | Truncated |
| Status | 10 | `SparkStage.status` | Color-coded |
| Tasks | 18 | computed | ProgressCell: `{complete}/{total}` |
| Input | 10 | `input_bytes` | `format_bytes()` |
| Output | 10 | `output_bytes` | `format_bytes()` |
| Shuffle Read | 10 | `shuffle_read_bytes` | `format_bytes()` |
| Shuffle Write | 10 | `shuffle_write_bytes` | `format_bytes()` |
| Spill | 10 | `spill_bytes` | `format_bytes()`, yellow highlight when >10% of shuffle |
| Duration | 12 | `executor_run_time_ms` | `format_duration()` |
| Submitted | 16 | `submission_time` | Timestamp |

- Active stages pinned to top, then by stage_id desc
- Spill column highlighted yellow when spill > 0 (data skew indicator)
- Failed stages highlighted red

### M4.4 [S] Job/Stage Detail Popup

**Files:** `views/jobs.py`, `views/stages.py`

- Press `Enter` on a selected row to open a modal screen with full details:
  - **Job detail:** Full description, all stage IDs, submission/completion times, task breakdown
  - **Stage detail:** Full name, task summary (active/complete/failed), all byte metrics untruncated, executor/CPU time
- Press `Escape` to close the modal

### M4.5 [S] Status Filter

**Files:** `views/jobs.py`, `views/stages.py`

- Press `/` to activate filter input
- Type a status name (running, completed, failed, active) to filter rows
- Text filter also matches against description/name fields
- `Escape` clears the filter

---

## M5: Executors + SQL + Storage Views — Complete All Views

**Goal:** All 6 views are functional.

**Exit Criteria:** Every tab shows real data, all column specifications from `specification.md` are implemented.

**Depends on:** M4

### M5.1 [L] `views/executors.py` — Executor Metrics

**File:** `src/dbxtop/views/executors.py`

DataTable with 14 columns:

| Column | Width | Source | Format |
|--------|-------|--------|--------|
| ID | 8 | `executor_id` | "driver" or numeric |
| Host | 20 | `host_port` | IP:port, truncated |
| Status | 8 | `is_active` | Active=green, Dead=red |
| Cores | 6 | `total_cores` | Integer |
| Active | 8 | `active_tasks` | Integer, bold when >0 |
| Done | 8 | `completed_tasks` | Integer |
| Failed | 8 | `failed_tasks` | Red when >0 |
| Memory | 20 | computed | Bar: `[=====>  ] 2.1/4.0 GB` |
| GC% | 8 | `gc_ratio` | Percentage, red when >10% |
| Disk | 10 | `disk_used` | `format_bytes()` |
| Shuf Read | 10 | `total_shuffle_read` | `format_bytes()` |
| Shuf Write | 10 | `total_shuffle_write` | `format_bytes()` |
| Duration | 10 | `total_duration_ms` | `format_duration()` |
| Uptime | 12 | `add_time` | Duration since added |

- **Driver row:** Always pinned to the first row, visually distinguished with bold text or different background color
- **Dead executors:** Show at bottom, dimmed, only if recently removed (within last 5 minutes)
- **Summary row:** Bottom row with totals: total cores, total active tasks, total memory used/max, average GC%
- **GC% highlighting:** Bold red text when gc_ratio > 10%, yellow when > 5%

### M5.2 [M] `widgets/spark_line.py` — Mini Sparkline Widget

**File:** `src/dbxtop/widgets/spark_line.py`

Compact sparkline for embedding in table cells or summary panels:

- **Input:** `values: deque[float]`, `width: int`, `min_val: float | None`, `max_val: float | None`
- **Render:** Unicode block characters `▁▂▃▄▅▆▇█` mapped to value range
- **Width:** Fixed character count (default 20), each character = one time sample
- **Color:** Green when values trending down, red when trending up (for GC%). Configurable per use.
- **Usage in Executors view:** Show memory_used_pct trend and gc_ratio trend for each executor, pulling from `DataCache.get_history("executors")`

### M5.3 [M] `views/sql.py` — SQL Queries View

**File:** `src/dbxtop/views/sql.py`

DataTable with 7 columns:

| Column | Width | Source | Format |
|--------|-------|--------|--------|
| ID | 8 | `execution_id` | Right-aligned |
| Description | flex | `description` | Truncated to 60 chars, ellipsis |
| Status | 12 | `status` | Color-coded (RUNNING=green, COMPLETED=dim, FAILED=red) |
| Duration | 12 | `duration_ms` | `format_duration()`, or "running..." for active |
| Running Jobs | 8 | `running_jobs` | Count, bold when >0 |
| Success Jobs | 8 | `success_jobs` | Count |
| Failed Jobs | 8 | `failed_jobs` | Count, red when >0 |

- Running queries pinned to top
- `Enter` on a row opens a detail popup with full SQL description text (can be very long — scrollable)
- Default sort: execution_id descending

### M5.4 [S] `views/storage.py` — Storage/RDD View

**File:** `src/dbxtop/views/storage.py`

DataTable with 7 columns:

| Column | Width | Source | Format |
|--------|-------|--------|--------|
| RDD ID | 8 | `rdd_id` | Right-aligned |
| Name | flex | `name` | Truncated |
| Partitions | 12 | computed | `{cached}/{total}` |
| Storage Level | 16 | `storage_level` | e.g., "Memory Deserialized 1x Replicated" |
| Memory | 12 | `memory_used` | `format_bytes()` |
| Disk | 12 | `disk_used` | `format_bytes()` |
| % Cached | 10 | `fraction_cached` | Percentage with 1 decimal |

- **Summary bar** at top: Total cached memory, total cached disk, total RDD count
- Empty state: "No cached RDDs or DataFrames" centered message
- Sort by memory_used descending by default

### M5.5 [S] `widgets/status_indicator.py` — Reusable Status Badge

**File:** `src/dbxtop/widgets/status_indicator.py`

Small composable widget for status display:

- **Input:** `state: str`, `state_map: dict[str, tuple[str, str]]` (state -> (color, symbol))
- **Render:** `{symbol} {state}` in the specified color
- **Predefined maps:**
  - `CLUSTER_STATES`: RUNNING=(green, "●"), TERMINATED=(red, "○"), etc.
  - `JOB_STATES`: RUNNING=(green, "●"), SUCCEEDED=(dim, "●"), FAILED=(red, "●")
  - `STAGE_STATES`: ACTIVE=(green, "●"), COMPLETE=(dim, "●"), FAILED=(red, "●"), PENDING=(yellow, "○")
- **Used by:** Header, Jobs view, Stages view, Executors view, SQL view

---

## M6: Polish & Error Handling

**Goal:** Handle all error states gracefully. Add quality-of-life features.

**Exit Criteria:** App behaves correctly when cluster is terminated, auth expires, API times out, or Spark REST is unavailable. Help overlay works.

**Depends on:** M5

### M6.1 [M] Error State Handling

**Files:** Multiple views + `app.py` + `api/poller.py`

Implement comprehensive error handling per `specification.md` Section 13:

- **Cluster terminated:** Detect `ClusterState.TERMINATED` or `TERMINATING`. Disable all Spark views (show "Cluster is {state}. Spark views unavailable."). Cluster view still shows last-known info + termination reason. Header shows state in red.
- **Auth expired:** Catch 401/403 from SDK or HTTP client. Show full-screen modal: "Authentication failed. Please refresh your Databricks token." with instructions. Stop all polling.
- **API timeout:** Show stale data indicator in header (`stale Xs`). Footer shows `Spark: timeout`. Data remains visible but dimmed. Poller continues retrying.
- **Spark REST unavailable:** Set `spark_available = False`. Cluster view works normally. Other views show "Spark REST API unavailable. Cluster may be starting up." Poller retries discovery periodically.
- **Network error:** Similar to timeout — show stale indicator, continue retrying

### M6.2 [S] Help Overlay

**File:** `src/dbxtop/app.py` (add help screen)

- Press `?` to open a modal overlay with all keyboard shortcuts in a formatted table
- Organized by category: Navigation, View Controls, Data Controls, Application
- Press `?` or `Escape` to close
- Implemented as a Textual `ModalScreen`

### M6.3 [S] Light Theme

**File:** `src/dbxtop/styles/default.tcss`

Add light theme variant:

- Define CSS variables for light mode: dark text on light background, adjusted status colors for readability
- Toggle via `--theme light` CLI flag
- Applied via Textual's theme system or CSS class switching

### M6.4 [M] Reconnection Logic

**Files:** `api/poller.py`, `api/spark_api.py`

Handle cluster lifecycle transitions:

- **Cluster restart detection:** When `spark_context_id` changes between polls, the Spark context has restarted. Clear cached `app_id`, re-run `discover_app_id()`.
- **Cluster start after termination:** When state transitions from TERMINATED to RUNNING, begin probing Spark REST API. Once available, enable Spark views.
- **Spark app switch:** If `discover_app_id()` returns a different app_id than cached, update and refresh all Spark data.

### M6.5 [S] Rate Limit Handling

**Files:** `api/spark_api.py`, `api/client.py`

- Detect HTTP 429 (rate limited) responses
- Implement exponential backoff: 1s, 2s, 4s, 8s, max 30s
- Show "Rate limited — backing off" in footer
- Reset backoff on successful request

### M6.6 [S] Graceful Degradation — SDK-Only Mode

**Files:** `app.py`, all views

- When Spark REST API is unavailable (cluster starting, proxy blocked, no Spark app):
  - Cluster view: fully functional from SDK data
  - Jobs/Stages/Executors/SQL/Storage views: show centered message "Waiting for Spark application..." with a spinner
  - Footer shows "SDK only" indicator
- Automatically transition to full mode when Spark REST becomes available

---

## M7: Packaging & Release

**Goal:** CI/CD, tests, documentation, and PyPI release.

**Exit Criteria:** GitHub Actions runs green. `pip install dbxtop` works from PyPI. README has a demo GIF.

**Depends on:** M6

### M7.1 [S] GitHub Actions CI

**File:** `.github/workflows/ci.yml`

Workflow triggered on push and PR:

- **Matrix:** Python 3.9, 3.10, 3.11, 3.12, 3.13
- **Steps:**
  1. `pip install -e ".[dev]"`
  2. `ruff check src/ tests/` (lint)
  3. `ruff format --check src/ tests/` (format check)
  4. `mypy src/dbxtop/` (type check)
  5. `pytest tests/ -v --ignore=tests/test_integration.py` (unit tests, skip integration)
  6. `pytest tests/ --snapshot-update` (Textual snapshot tests, if changed)

### M7.2 [S] PyPI Publishing

**File:** `.github/workflows/publish.yml`

Triggered on GitHub release:

- Build wheel and sdist with `hatch build`
- Publish to PyPI via trusted publisher (GitHub OIDC, no API token needed)
- Also publish to TestPyPI on push to `main` for pre-release testing

### M7.3 [M] README Polish

**File:** `README.md`

- **Hero section:** Project name, one-line description, badges (PyPI version, Python versions, CI status, license)
- **Demo GIF:** Record a 15-second terminal session with `vhs` or `asciinema`, showing tab switching, live data updates, sorting
- **Installation:** `pip install dbxtop` and `pipx install dbxtop`
- **Quick Start:** Minimal usage example
- **Features:** Bulleted list of all 6 views with brief descriptions
- **Keyboard Shortcuts:** Table of all bindings
- **Configuration:** CLI options, env vars, connection setup
- **Requirements:** Python 3.9+, Databricks CLI configured
- **Contributing:** Dev setup, testing, PR guidelines
- **License:** MIT

### M7.4 [S] Textual Snapshot Tests

**File:** `tests/test_snapshots.py`

Use `textual-dev` snapshot testing:

- One snapshot per view showing the expected rendering with mock data
- Snapshots committed to `tests/snapshots/`
- Updated via `pytest --snapshot-update`
- Covers: empty state, data loaded state, error state for each view

### M7.5 [S] CHANGELOG and Release

**Files:** `CHANGELOG.md`, `pyproject.toml`, `src/dbxtop/__init__.py`

- Write CHANGELOG.md following Keep a Changelog format
- Bump version to `0.1.0`
- Tag release: `git tag v0.1.0`
- Create GitHub release with changelog notes

---

## Testing Strategy

### Unit Tests (M1, every milestone)

- **Scope:** Pydantic model validation, URL construction, cache behavior, format utilities, CLI parsing
- **Mock approach:** `httpx.MockTransport` for Spark REST responses, `unittest.mock.patch` for SDK calls
- **Coverage target:** >90% on `api/` module, >80% overall
- **Run:** `pytest tests/ -v --ignore=tests/test_integration.py`

### TUI Snapshot Tests (M7)

- **Scope:** Visual regression testing for each view
- **Tool:** Textual's built-in `textual-dev` snapshot testing
- **Mock data:** Feed views with deterministic fixture data for reproducible renders
- **Run:** `pytest tests/test_snapshots.py`

### Integration Tests (M3+)

- **Scope:** Real API calls against a Databricks cluster
- **Guard:** `@pytest.mark.integration`, requires `DBXTOP_TEST_CLUSTER_ID` env var
- **Skip in CI:** Not run in GitHub Actions (no cluster access)
- **Run:** `pytest tests/test_integration.py -v` (manually, with a running cluster)

### Manual Testing

- **Live development:** `textual-dev run --dev "dbxtop -c CLUSTER_ID -p PROFILE"`
- **Console debugging:** `textual console` in a separate terminal for Textual dev tools
- **Coverage report:** `pytest --cov=dbxtop --cov-report=html`

---

## Task Summary

| Milestone | Tasks | Total Size |
|-----------|-------|------------|
| M1: Core API Infrastructure | 7 | 2S + 2M + 2L + 1S = ~20-30 hrs |
| M2: TUI Shell | 6 | 3S + 2M + 1S = ~8-14 hrs |
| M3: Cluster View | 4 | 3S + 1M = ~4-8 hrs |
| M4: Jobs + Stages | 5 | 2S + 2M + 1L = ~10-16 hrs |
| M5: Executors + SQL + Storage | 5 | 2S + 2M + 1L = ~10-16 hrs |
| M6: Polish & Error Handling | 6 | 3S + 2M + 1S = ~8-14 hrs |
| M7: Packaging & Release | 5 | 4S + 1M = ~4-8 hrs |
| **Total** | **38** | **~64-106 hrs** |

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Spark REST proxy blocked on shared clusters | Executors/Jobs/Stages/SQL/Storage views non-functional | SDK-only graceful degradation (M6.6). Test on single-user cluster. |
| Databricks SDK breaking changes | Client wrapper breaks | Pin `databricks-sdk>=0.38.0,<1.0`. Wrap all SDK calls in mapping functions. |
| Textual API changes | View rendering breaks | Pin `textual>=2.1.0,<3.0`. Snapshot tests catch regressions. |
| Driver proxy URL format changes | Spark REST client fails | Centralize URL construction in one method. Add URL discovery fallback. |
| Rate limiting on poll frequency | Data goes stale frequently | Backoff logic (M6.5). User can increase poll interval via `--refresh`. |
