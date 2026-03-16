# dbxtop Implementation Specification

> **Version:** 1.0.0
> **Date:** 2026-03-16
> **Status:** Implementation-Ready
> **Companion Documents:** `architecture.md` (system design), `api-research.md` (endpoint inventory)
> **Scope:** Complete, field-level specification for every view, widget, keyboard binding, CLI argument, and error state in `dbxtop`.

---

## Table of Contents

1. [Overview](#1-overview)
2. [CLI Specification](#2-cli-specification)
3. [Header Bar Specification](#3-header-bar-specification)
4. [Footer Bar Specification](#4-footer-bar-specification)
5. [View 1: Cluster Overview](#5-view-1-cluster-overview)
6. [View 2: Spark Jobs](#6-view-2-spark-jobs)
7. [View 3: Stages](#7-view-3-stages)
8. [View 4: Executors](#8-view-4-executors)
9. [View 5: SQL Queries](#9-view-5-sql-queries)
10. [View 6: Storage / RDD](#10-view-6-storage--rdd)
11. [Keyboard Bindings](#11-keyboard-bindings)
12. [Color Scheme](#12-color-scheme)
13. [Error States](#13-error-states)
14. [Data Formatting Standards](#14-data-formatting-standards)
15. [Widget Specifications](#15-widget-specifications)
16. [Polling and Data Flow](#16-polling-and-data-flow)

---

## 1. Overview

`dbxtop` is a real-time terminal UI for monitoring a single Databricks/Spark cluster. It presents six views -- Cluster Overview, Spark Jobs, Stages, Executors, SQL Queries, and Storage/RDD -- driven by two data sources: the Databricks Python SDK (for cluster metadata and lifecycle) and the Spark REST API v1 (for Spark-internal metrics, accessed via the driver proxy).

This specification defines every column, widget, color, keybinding, and error message that an implementer needs to build each view. It is the authoritative source for "what the user sees"; the architecture document governs "how it is built."

### Conventions Used in This Document

- **Column spec format**: `Column Name` | Type | Width | Source Field | Formatting Rule
- **Color references**: Use names from Section 12 (Color Scheme). Implementation maps these to Textual CSS variables.
- **Data source references**: `SDK:clusters.get` means the Databricks SDK's `clusters.get()` method. `SPARK:/jobs` means the Spark REST API endpoint `/api/v1/applications/{app_id}/jobs`.
- **Poll tier**: "fast" = 3s default, "slow" = 15s default (see Section 16).

---

## 2. CLI Specification

### 2.1 Command Signature

```
dbxtop [OPTIONS]

  Real-time terminal dashboard for Databricks/Spark clusters.
```

### 2.2 Options

| Flag | Short | Type | Default | Required | Description |
|------|-------|------|---------|----------|-------------|
| `--profile` | `-p` | TEXT | `DEFAULT` | No | Databricks CLI profile name (from `~/.databrickscfg`). |
| `--cluster-id` | `-c` | TEXT | -- | **Yes** | Cluster ID to monitor. |
| `--refresh` | | FLOAT | `3.0` | No | Fast poll interval in seconds (Spark REST API). Minimum: 1.0. Maximum: 30.0. |
| `--slow-refresh` | | FLOAT | `15.0` | No | Slow poll interval in seconds (SDK cluster/events). Minimum: 5.0. Maximum: 120.0. |
| `--theme` | | TEXT | `dark` | No | Color theme. Accepted values: `dark`, `light`. |
| `--version` | | FLAG | -- | No | Print `dbxtop <version>` and exit. |
| `--help` | | FLAG | -- | No | Print usage and exit. |

**Total CLI arguments: 7** (`--profile`, `--cluster-id`, `--refresh`, `--slow-refresh`, `--theme`, `--version`, `--help`)

### 2.3 Validation Rules

1. `--cluster-id` is required. If missing, print: `Error: Missing option '--cluster-id'. Specify the cluster to monitor.` Exit code 2.
2. `--refresh` must be in [1.0, 30.0]. If out of range, print: `Error: --refresh must be between 1.0 and 30.0 seconds.` Exit code 2.
3. `--slow-refresh` must be in [5.0, 120.0] and must be >= `--refresh`. If violated, print: `Error: --slow-refresh must be >= --refresh and between 5.0 and 120.0 seconds.` Exit code 2.
4. `--theme` must be one of `dark`, `light`. If invalid, print: `Error: Invalid theme '<value>'. Choose from: dark, light.` Exit code 2.

### 2.4 Environment Variable Fallbacks

| CLI Option | Environment Variable |
|------------|---------------------|
| `--profile` | `DBXTOP_PROFILE` |
| `--cluster-id` | `DBXTOP_CLUSTER_ID` |
| `--refresh` | `DBXTOP_REFRESH` |
| `--slow-refresh` | `DBXTOP_SLOW_REFRESH` |
| `--theme` | `DBXTOP_THEME` |

Priority: CLI flag > environment variable > config file (`~/.config/dbxtop/config.toml`) > built-in default.

### 2.5 Startup Sequence

```
1. Parse CLI args (Click)
2. Resolve Settings (CLI > env > config file > defaults)
3. Validate all settings
4. Create WorkspaceClient(profile=settings.profile)
5. Test connection: clusters.get(cluster_id)
   - 401/403 -> print auth error, exit 1
   - 404     -> print "Cluster not found", exit 1
   - Timeout -> print network error, exit 1
6. Detect cluster state
   - RUNNING: probe Spark REST API availability
   - Not RUNNING: start in SDK-only mode
7. Build proxy URL using workspace host + org_id + cluster_id + port
8. Create DbxtopApp(settings, dbx_client, spark_client)
9. app.run()  -- Textual takes over the terminal
```

### 2.6 Usage Examples

```bash
# Standard usage
dbxtop --profile yad2-prod --cluster-id 0311-160038-vjuv0ah9

# Short flags
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9

# Faster refresh for active debugging
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9 --refresh 1.0

# Light theme
dbxtop -p yad2-prod -c 0311-160038-vjuv0ah9 --theme light

# Using DEFAULT profile (no --profile needed)
dbxtop -c 0311-160038-vjuv0ah9
```

---

## 3. Header Bar Specification

### 3.1 Layout

```
dbxtop | {cluster_name} | {state_badge} | up {uptime} | next {countdown}s | {profile}
```

**Concrete example:**
```
dbxtop | my-analytics-cluster | ● RUNNING | up 4d 2h 23m | next 3s | yad2-prod
```

### 3.2 Fields

| Segment | Source | Format | Update Frequency |
|---------|--------|--------|-----------------|
| `dbxtop` | Static | App name, bold white | Never |
| `{cluster_name}` | `SDK:clusters.get` -> `cluster_name` | Plain text, truncated to 30 chars with ellipsis if longer | Slow poll |
| `{state_badge}` | `SDK:clusters.get` -> `state` | Colored circle + state text (see 3.3) | Slow poll |
| `up {uptime}` | Computed from `SDK:clusters.get` -> `start_time` | See 3.4 | Computed locally every 1s |
| `next {countdown}s` | Internal timer | Seconds until next fast poll, counts down from `--refresh` | Updated every 1s |
| `{profile}` | CLI `--profile` value | Plain text, dim | Never |

### 3.3 State Badge Colors

| Cluster State | Circle | Text Color | Background |
|---------------|--------|------------|------------|
| `RUNNING` | `●` green | Green (#00ff00) | None |
| `PENDING` | `●` yellow | Yellow (#ffff00) | None |
| `RESTARTING` | `●` yellow | Yellow (#ffff00) | None |
| `RESIZING` | `●` yellow | Yellow (#ffff00) | None |
| `TERMINATING` | `●` red | Red (#ff0000) | None |
| `TERMINATED` | `○` red | Red (#ff0000) | None |
| `ERROR` | `●` red | Red (#ff0000) | None |

### 3.4 Uptime Format

Computed as `now() - cluster.start_time`. Display rules:

| Duration | Format | Example |
|----------|--------|---------|
| < 1 minute | `Xs` | `up 45s` |
| 1 min to 1 hour | `Xm Ys` | `up 23m 15s` |
| 1 hour to 1 day | `Xh Ym` | `up 4h 23m` |
| >= 1 day | `Xd Yh Zm` | `up 2d 14h 7m` |
| Cluster not running | `--` | `up --` |

### 3.5 Additional Header Indicators

When data is stale, append a yellow indicator after the countdown:

```
dbxtop | my-cluster | ● RUNNING | up 4h 23m | next 3s | ⚠ stale 30s | yad2-prod
```

The `stale Xs` indicator appears when any cache slot has not been updated for more than 2x its expected poll interval. It shows the age of the oldest stale slot.

### 3.6 Textual Widget Class

**Widget:** `ClusterHeader` (extends `textual.widget.Static`)
**Height:** 1 row (fixed)
**CSS ID:** `#cluster-header`

---

## 4. Footer Bar Specification

### 4.1 Layout

```
{keybindings} | {connection_status} | Last: {timestamp}
```

**Concrete example:**
```
1-6:views  s:sort  /:filter  r:refresh  q:quit  ?:help  | Connected | Last: 14:30:02
```

### 4.2 Left Section: Keybindings

Context-sensitive. Changes based on active view and mode.

**Default (table views: Jobs, Stages, Executors, SQL, Storage):**
```
1-6:views  s:sort  /:filter  r:refresh  q:quit  ?:help
```

**Cluster view:**
```
1-6:views  r:refresh  q:quit  ?:help
```

**Filter mode active:**
```
Type to filter  Enter:apply  Esc:clear
```

**Detail popup open:**
```
Esc:close  1-6:views  q:quit
```

### 4.3 Middle Section: Connection Status

| State | Text | Color |
|-------|------|-------|
| Both SDK and Spark REST connected | `Connected` | Green (#00ff00) |
| SDK connected, Spark REST unavailable | `Spark API unavailable` | Yellow (#ffff00) |
| Reconnecting after failure | `Reconnecting...` | Yellow (#ffff00) |
| Both disconnected | `Disconnected` | Red (#ff0000) |
| Rate limited | `Rate limited -- backing off` | Yellow (#ffff00) |

### 4.4 Right Section: Last Update Timestamp

Format: `Last: HH:MM:SS` showing the timestamp of the most recent successful data update (any slot).

If no successful update has ever occurred: `Last: --:--:--`

### 4.5 Textual Widget Class

**Widget:** `KeyboardFooter` (extends `textual.widget.Static`)
**Height:** 1 row (fixed)
**CSS ID:** `#keyboard-footer`

---

## 5. View 1: Cluster Overview

### 5.1 Overview

The Cluster Overview is the landing view (shown on startup). It provides a dashboard-style summary of the cluster's configuration, resource utilization, recent lifecycle events, and installed libraries. This is the only view that remains fully functional when the cluster is terminated.

**Data sources:** All slow-poll (15s interval)
- `SDK:clusters.get` -- cluster configuration and state
- `SDK:clusters.events` -- lifecycle events
- `SDK:libraries.cluster_status` -- installed libraries

### 5.2 Layout Sections

```
+------------------------------------------+
| [A] Cluster Identity Card                |
+---------------------+--------------------+
| [B] Resource Gauges | [C] Activity Panel |
+---------------------+--------------------+
| [D] Recent Events Table                  |
+------------------------------------------+
| [E] Libraries Table                      |
+------------------------------------------+
```

### 5.3 Section A: Cluster Identity Card

A bordered panel at the top displaying core cluster metadata.

| Field | Source | Format |
|-------|--------|--------|
| **Name** | `cluster_name` | Plain text |
| **ID** | `cluster_id` | Monospace |
| **State** | `state` | Color-coded badge (see Section 3.3) |
| **Uptime** | Computed from `start_time` | See Section 3.4 |
| **DBR Version** | `spark_version` | e.g., `15.4 LTS (Spark 3.5.0)` |
| **Runtime Engine** | `runtime_engine` | `STANDARD` or `PHOTON` |
| **Cluster Source** | `cluster_source` | e.g., `UI`, `API`, `JOB` |
| **Data Security Mode** | `data_security_mode` | e.g., `USER_ISOLATION`, `SINGLE_USER` |
| **Creator** | `creator_user_name` | Email address |
| **Auto-termination** | `autotermination_minutes` | e.g., `60 min` or `Disabled` if 0/null |

**Layout:** 2-column grid within a bordered box. Labels left-aligned (dim), values right-aligned (normal weight).

### 5.4 Section B: Resource Gauges

A bordered panel showing cluster resources with visual gauge bars.

| Gauge | Source Fields | Display |
|-------|-------------|---------|
| **Total Cores** | `executors[*].totalCores` (Spark REST) or `num_workers * cores_per_node + driver_cores` (computed from node type) | `[=========>   ] 16/20 cores` |
| **Total Memory** | `executors[*].maxMemory` (Spark REST) or computed from node types | `[=======>     ] 92/150 GB` |
| **Workers** | `num_workers` (current), `autoscale.min_workers`, `autoscale.max_workers` | `4 workers (autoscale 2-8)` |
| **Driver** | `driver_node_type_id` | e.g., `i3.xlarge (4c / 30 GB)` |

**Gauge bar rendering:** Uses Unicode block characters. Green fill when < 70%, yellow when 70-85%, red when > 85%.

When autoscaling is enabled, the Workers line shows: `{current} workers (autoscale {min}-{max})`
When autoscaling is disabled, show: `{num_workers} workers (fixed)`

**Driver info sub-section:**

| Field | Source | Format |
|-------|--------|--------|
| Node Type | `driver_node_type_id` | e.g., `i3.xlarge` |
| IP | `driver.private_ip` (from cluster info) | e.g., `10.0.1.100` |
| JDBC Port | Spark conf `spark.databricks.jdbc.port` or default `10000` | e.g., `10000` |

### 5.5 Section C: Activity Panel

Real-time activity sparklines and counters. This section bridges cluster metadata (slow poll) with Spark metrics (fast poll).

| Widget | Source | Display |
|--------|--------|---------|
| **Active Tasks** sparkline | `DataCache.active_tasks_history` (ring buffer, 60 values) | `Active Tasks ▁▂▄▇▅▃▂▁▃▅▇█▆▃` |
| **Executor Count** sparkline | `DataCache.executor_count_history` (ring buffer, 60 values) | `Executors    ▃▃▃▃▃▃▄▄▅▅▅▅▅▅` |
| **GC Ratio** sparkline | `DataCache.gc_ratio_history` (ring buffer, 60 values) | `GC Ratio     ▁▁▁▁▂▁▁▁▁▂▃▁▁▁` |
| Running Spark Jobs | `len([j for j in spark_jobs if j.status == "RUNNING"])` | `Running Spark Jobs: 2` |
| Active Stages | `len([s for s in stages if s.status == "ACTIVE"])` | `Active Stages:      5` |
| Pending Stages | `len([s for s in stages if s.status == "PENDING"])` | `Pending Stages:     12` |

Sparklines use `MiniSparkline` widget (see Section 15.2). Each renders a 3-minute window at 3s intervals (60 data points).

### 5.6 Section D: Recent Events Table

Scrollable table showing the last 20 cluster lifecycle events, most recent first.

| Column | Type | Width | Source Field | Format |
|--------|------|-------|-------------|--------|
| Timestamp | datetime | 16 chars | `event.timestamp` | `HH:MM:SS` (today) or `MM-DD HH:MM` (older) |
| Event Type | string | 24 chars | `event.type` | Uppercase, e.g., `AUTOSCALING_STATS` |
| Details | string | Remaining | `event.details.reason` or `event.details.cause` | Truncated to fit, with tooltip on hover |

**Data source:** `SDK:clusters.events(cluster_id, order=DESC, limit=20)`
**Poll tier:** Slow (15s)
**Sorting:** Fixed, newest first (by timestamp desc). Not user-sortable.

**Event type colors:**
- Green: `RUNNING`, `UPSIZE_COMPLETED`, `DOWNSIZE_COMPLETED`
- Yellow: `STARTING`, `CREATING`, `AUTOSCALING_STATS`, `RESIZING`
- Red: `TERMINATING`, `TERMINATED`, `DID_NOT_EXPAND`, `NODES_LOST`
- Dim: `INIT_SCRIPTS_STARTED`, `INIT_SCRIPTS_FINISHED`

### 5.7 Section E: Libraries Table

Table showing libraries installed on the cluster.

| Column | Type | Width | Source Field | Format |
|--------|------|-------|-------------|--------|
| Library | string | 30 chars | `library.library` (parsed: package name) | e.g., `splink==4.0.5` |
| Type | string | 8 chars | `library.library` (key: pypi/maven/jar/etc.) | e.g., `pypi` |
| Status | string | 12 chars | `library.status` | Color-coded (see below) |

**Status colors:**
- Green: `INSTALLED`
- Yellow: `PENDING`, `RESOLVING_JARS`, `INSTALLING`
- Red: `FAILED`, `UNINSTALL_ON_RESTART`
- Dim: `SKIPPED`

**Data source:** `SDK:libraries.cluster_status(cluster_id)`
**Poll tier:** Slow (15s)
**Sorting:** Fixed: FAILED first, then PENDING/INSTALLING, then INSTALLED (alphabetical within group).

### 5.8 Cluster View When Terminated

When the cluster state is TERMINATED or ERROR:
- Section A: Shows full identity card with state `TERMINATED` (red) and termination reason from `state_message`.
- Section B: Shows last known node types and autoscale config. Gauges show `--` for unavailable metrics.
- Section C: Shows `Cluster not running` in place of sparklines. Counters show `--`.
- Section D: Shows events leading up to termination (still available via SDK).
- Section E: Shows `Libraries unavailable while cluster is terminated`.

---

## 6. View 2: Spark Jobs

### 6.1 Overview

Displays all Spark jobs (not Databricks Jobs/Workflows) with real-time progress tracking. Running jobs are visually prominent; completed and failed jobs scroll below.

**Data source:** `SPARK:/api/v1/applications/{app_id}/jobs`
**Poll tier:** Fast (3s)

### 6.2 Summary Bar

A single-line summary above the table:

```
Filter: [____________]                   Showing: 3 running, 47 total
```

| Element | Description |
|---------|-------------|
| Filter input | Text filter box, activated by `/` key. Filters on job description text. |
| Status counts | `{running} running, {total} total` (reflects current filter) |

### 6.3 DataTable Columns

| # | Column | Type | Width | Source Field | Format | Sortable |
|---|--------|------|-------|-------------|--------|----------|
| 1 | **Job ID** | int | 6 | `jobId` | Right-aligned integer | Yes (default: desc) |
| 2 | **Description** | string | 30 | `name` | Left-aligned, truncated to 30 chars with `..` | Yes |
| 3 | **Status** | string | 10 | `status` | Color-coded (see 6.4) | Yes |
| 4 | **Progress** | bar | 16 | Computed: `numCompletedTasks / numTasks` | `[======>   ] 75%` (ProgressCell widget) | No |
| 5 | **Tasks** | string | 16 | `numCompletedTasks`, `numTasks`, `numFailedTasks` | `847/1200 (2f)` -- completed/total (Xf = failed count, only if > 0) | Yes (by completed) |
| 6 | **Duration** | duration | 8 | Computed: `completionTime - submissionTime` (or `now - submissionTime` if running) | Human-readable (see Section 14.2) | Yes |
| 7 | **Submitted** | datetime | 10 | `submissionTime` | `HH:MM:SS` | Yes |

**Total columns: 7**

### 6.4 Status Colors

| Status | Text Color | Row Style |
|--------|------------|-----------|
| `RUNNING` | Green (#00ff00) | Normal weight, green status text |
| `SUCCEEDED` | Dim green | Dim entire row |
| `FAILED` | Red (#ff0000) | Red status text, normal weight for other columns |
| `UNKNOWN` | Yellow (#ffff00) | Yellow status text |

### 6.5 Progress Bar Specification

The Progress column renders a `ProgressCell` widget inline in the DataTable cell.

- Width: 16 characters total
- Format: `[{bar}] {pct}%`
- Bar characters: `=` for filled, `>` for head, ` ` for empty
- Color: Green fill for RUNNING/SUCCEEDED, red `X` at head for FAILED
- Example running: `[======>   ] 75%`
- Example complete: `[==========] 100%`
- Example failed: `[====X     ] 42%`

### 6.6 Sorting

Default sort: Job ID descending (newest first).

Sortable columns and their sort keys:
1. Job ID: numeric `jobId`
2. Description: alphabetic `name`
3. Status: enum order RUNNING > FAILED > UNKNOWN > SUCCEEDED
4. Tasks: numeric `numCompletedTasks`
5. Duration: numeric duration in ms
6. Submitted: numeric `submissionTime` epoch

Pressing `s` cycles through: Job ID -> Status -> Duration -> Submitted -> Tasks -> (back to Job ID).
Pressing `S` reverses current sort direction.

### 6.7 Filtering

**Status filter** (press `f`): Cycles through `All` -> `Running` -> `Succeeded` -> `Failed` -> (back to `All`).

**Text filter** (press `/`): Opens a text input at the top. Filters rows where the Description column contains the filter string (case-insensitive substring match). Press `Enter` to apply, `Escape` to clear.

### 6.8 Detail Panel

When a row is selected and `Enter` is pressed, a detail panel expands below the table (split view, table takes top 60%, detail takes bottom 40%).

**Detail panel fields:**

| Field | Source | Format |
|-------|--------|--------|
| Name | `name` | Full text (not truncated) |
| Job ID | `jobId` | Integer |
| Status | `status` | Color-coded |
| Submitted | `submissionTime` | `YYYY-MM-DD HH:MM:SS` |
| Duration | Computed | `Xm Ys (running)` or `Xm Ys` |
| Stage IDs | `stageIds` | Comma-separated list, e.g., `[38, 39, 40, 41, 42]` |
| Stages | Computed | `{active} active / {completed} completed / {failed} failed / {skipped} skipped` |
| Tasks | Computed | `{completed} completed / {total} total / {failed} failed` |

Press `Enter` again or `Escape` to close the detail panel.

### 6.9 Empty State

When no Spark jobs exist: Center a message in the view area:
```
No Spark jobs found.
Run a query or notebook command to generate Spark jobs.
```

---

## 7. View 3: Stages

### 7.1 Overview

Displays Spark stages with I/O metrics. Highlights stages with high spill as potential performance problems.

**Data source:** `SPARK:/api/v1/applications/{app_id}/stages`
**Poll tier:** Fast (3s)

### 7.2 Summary Bar

```
Filter: [____________]               Active: 5  Complete: 42  Failed: 1
```

### 7.3 DataTable Columns

| # | Column | Type | Width | Source Field | Format | Sortable |
|---|--------|------|-------|-------------|--------|----------|
| 1 | **Stage ID** | int | 6 | `stageId` | Right-aligned integer | Yes (default: desc) |
| 2 | **Description** | string | 24 | `name` | Left-aligned, truncated to 24 chars | Yes |
| 3 | **Status** | string | 10 | `status` | Color-coded (see 7.4) | Yes |
| 4 | **Tasks Progress** | bar | 16 | `numCompleteTasks / numTasks` | `[======>   ] 640/1000` (ProgressCell) | Yes (by completed count) |
| 5 | **Input Size** | bytes | 8 | `inputBytes` | Human-readable (see Section 14.1) | Yes |
| 6 | **Output Size** | bytes | 8 | `outputBytes` | Human-readable | Yes |
| 7 | **Shuffle Read** | bytes | 8 | `shuffleReadBytes` | Human-readable | Yes |
| 8 | **Shuffle Write** | bytes | 8 | `shuffleWriteBytes` | Human-readable | Yes |
| 9 | **Spill (Mem)** | bytes | 8 | `memoryBytesSpilled` | Human-readable. Yellow if > 0 | Yes |
| 10 | **Spill (Disk)** | bytes | 8 | `diskBytesSpilled` | Human-readable. Yellow if > 0 | Yes |
| 11 | **Duration** | duration | 8 | Computed: `completionTime - submissionTime` or `now - submissionTime` | Human-readable | Yes |

**Total columns: 11**

### 7.4 Status Colors

| Status | Text Color | Row Style |
|--------|------------|-----------|
| `ACTIVE` | Green (#00ff00) | Normal weight |
| `COMPLETE` | Dim green | Dim entire row |
| `PENDING` | Yellow (#ffff00) | Yellow status text |
| `FAILED` | Red (#ff0000) | Red status text |
| `SKIPPED` | Dim | Dim entire row |

### 7.5 Spill Highlighting

A row is highlighted in yellow background (dim yellow, not full yellow) when:

```
spill_condition = (memoryBytesSpilled + diskBytesSpilled) > 0.10 * shuffleWriteBytes
```

In other words, if total spill exceeds 10% of shuffle write bytes, the row gets a yellow warning background. This signals a stage that may benefit from repartitioning or memory tuning.

Additionally, individual Spill cells are colored:
- `0 B`: Dim (no spill)
- `> 0` but < 10% of shuffle: Yellow text
- `>= 10%` of shuffle: Red text

### 7.6 Sorting

Default sort: Stage ID descending (newest first).

Sort cycle (`s` key): Stage ID -> Status -> Duration -> Shuffle Write -> Spill (Disk) -> Input Size -> (back to Stage ID).

### 7.7 Filtering

**Status filter** (`f` key): Cycles through `All` -> `Active` -> `Complete` -> `Pending` -> `Failed` -> (back to `All`).

**Text filter** (`/` key): Filters on Description (stage name).

### 7.8 Detail Panel

Expands below the table when `Enter` is pressed on a selected row.

**Detail panel fields:**

| Field | Source | Format |
|-------|--------|--------|
| Name | `name` | Full text |
| Stage ID / Attempt | `stageId`, `attemptId` | `Stage 42, Attempt 0` |
| Status | `status` | Color-coded with duration |
| Tasks | Computed | `{complete} complete / {total} total / {failed} failed / {killed} killed` |
| I/O | `inputBytes`, `inputRecords`, `outputBytes`, `outputRecords` | `Input 2.3 GB (18.4M records) -> Output 1.1 GB (9.2M records)` |
| Shuffle | `shuffleReadBytes`, `shuffleReadRecords`, `shuffleWriteBytes`, `shuffleWriteRecords` | `Read 1.1 GB (8.1M records) / Write 890 MB (7.3M records)` |
| Spill | `memoryBytesSpilled`, `diskBytesSpilled` | `Memory 256 MB / Disk 128 MB` (red if > 0) |
| Execution | `executorRunTime`, `executorCpuTime`, `jvmGcTime` | `CPU 4.2 min / Run 6.8 min / GC 12.3 s` |

### 7.9 Empty State

```
No Spark stages found.
Stages appear when Spark jobs decompose work into parallel tasks.
```

---

## 8. View 4: Executors

### 8.1 Overview

Displays all active Spark executors (including the driver) with memory utilization, GC pressure, and task throughput. This is the primary view for diagnosing resource issues.

**Data source:** `SPARK:/api/v1/applications/{app_id}/allexecutors`
**Poll tier:** Fast (3s)

### 8.2 Summary Bar

```
Total: 5 (1 driver + 4 workers)    Cores: 20    Memory: 150 GB
```

### 8.3 DataTable Columns

| # | Column | Type | Width | Source Field | Format | Sortable |
|---|--------|------|-------|-------------|--------|----------|
| 1 | **Executor ID** | string | 8 | `id` | `driver` or numeric ID. Driver row is always first and visually distinguished (bold). | Yes |
| 2 | **Host:Port** | string | 22 | `hostPort` | e.g., `10.0.1.101:4041` | Yes |
| 3 | **Status** | string | 8 | `isActive` | `Active` (green) or `Dead` (red) | Yes |
| 4 | **Cores** | int | 5 | `totalCores` | Right-aligned integer | Yes |
| 5 | **Memory Used/Max** | bar | 20 | `memoryUsed`, `maxMemory` | `[██████░░] 22/30 GB` (ProgressCell widget) | Yes (by memoryUsed) |
| 6 | **Disk Used** | bytes | 8 | `diskUsed` | Human-readable | Yes |
| 7 | **GC Time** | duration | 8 | `totalGCTime` | Human-readable | Yes |
| 8 | **GC%** | float | 6 | Computed: `totalGCTime / totalDuration * 100` | `3.4%`. Red if > 10%, yellow if > 5% | Yes |
| 9 | **Active Tasks** | int | 6 | `activeTasks` | Right-aligned integer | Yes |
| 10 | **Completed Tasks** | int | 8 | `completedTasks` | Right-aligned integer with commas | Yes |
| 11 | **Failed Tasks** | int | 6 | `failedTasks` | Right-aligned. Red if > 0 | Yes |
| 12 | **Input Size** | bytes | 8 | `totalInputBytes` | Human-readable | Yes |
| 13 | **Shuffle Read** | bytes | 8 | `totalShuffleRead` | Human-readable | Yes |
| 14 | **Shuffle Write** | bytes | 8 | `totalShuffleWrite` | Human-readable | Yes |

**Total columns: 14**

### 8.4 GC% Calculation and Thresholds

```python
gc_pct = (totalGCTime / totalDuration) * 100 if totalDuration > 0 else 0.0
```

| GC% Range | Cell Color | Row Effect |
|-----------|------------|------------|
| 0 - 5% | Normal (white) | None |
| 5 - 10% | Yellow (#ffff00) | None |
| > 10% | Red (#ff0000) | Entire row gets dim red background |

### 8.5 Memory Bar Specification

The Memory Used/Max column renders a `ProgressCell` widget:

- Format: `[{bar}] {used}/{max} {unit}`
- Color thresholds:
  - < 70%: Blue (#0087ff) fill
  - 70-85%: Yellow (#ffff00) fill
  - > 85%: Red (#ff0000) fill
- Example: `[██████░░] 22/30 GB`

### 8.6 Driver Row Treatment

The executor with `id == "driver"` is always rendered first (pinned to top), regardless of sort order. It is visually distinguished with:
- Bold text for the Executor ID column
- A subtle background color difference (slightly lighter than other rows)
- Label: `driver` instead of a numeric ID

### 8.7 Summary/Totals Row

A fixed row at the bottom of the table (below the scrollable area) showing aggregates:

| Column | Aggregation |
|--------|------------|
| Executor ID | `TOTAL` |
| Host:Port | -- |
| Status | `{active}/{total}` |
| Cores | Sum of all `totalCores` |
| Memory | Sum used / sum max with bar |
| Disk Used | Sum |
| GC Time | Sum |
| GC% | Weighted average: `sum(gcTime) / sum(duration) * 100` |
| Active Tasks | Sum |
| Completed Tasks | Sum |
| Failed Tasks | Sum |
| Input Size | Sum |
| Shuffle Read | Sum |
| Shuffle Write | Sum |

### 8.8 Sorting

Default sort: Memory Used descending.

Sort cycle (`s` key): Memory Used -> GC% -> Active Tasks -> Completed Tasks -> Shuffle Write -> Executor ID -> (back to Memory Used).

Note: Driver row remains pinned at top regardless of sort order.

### 8.9 Detail Panel

Expands when `Enter` is pressed on a selected executor row.

**Detail panel fields:**

| Field | Source | Format |
|-------|--------|--------|
| Host | `hostPort` | Full host:port |
| Status | `isActive` | `Active (since {addTime})` or `Dead (removed at {removeTime}: {removeReason})` |
| Cores / Max Tasks | `totalCores`, `maxTasks` | `4 cores / 4 max tasks` |
| Memory | `memoryUsed`, `maxMemory` | `22.1 / 30.0 GB (73.7%)` with color |
| Disk | `diskUsed` | Human-readable |
| GC | `totalGCTime`, `totalDuration` | `{gcTime} ({gc_pct}% of runtime)` with WARNING label if > 5% |
| Tasks | `activeTasks`, `completedTasks`, `failedTasks` | `{active} active / {completed} completed / {failed} failed` |
| Shuffle | `totalShuffleRead`, `totalShuffleWrite` | `Read: {read}    Write: {write}` |
| Input | `totalInputBytes` | Human-readable |
| RDD Blocks | `rddBlocks` | Integer |
| Peak Memory | `peakMemoryMetrics.JVMHeapMemory`, `peakMemoryMetrics.JVMOffHeapMemory` | `JVM Heap: 28.2 GB    JVM Off-Heap: 1.8 GB` |

### 8.10 Empty State

```
No executors found.
Executors appear when the cluster has an active Spark context.
```

---

## 9. View 5: SQL Queries

### 9.1 Overview

Displays Spark SQL query executions with timing, status, and data volume metrics. Provides quick access to the full SQL text of each query.

**Data source:** `SPARK:/api/v1/applications/{app_id}/sql`
**Poll tier:** Slow (15s) -- SQL queries change less frequently than task-level metrics

### 9.2 Summary Bar

```
Running: 2    Completed: 31    Failed: 1
```

### 9.3 DataTable Columns

| # | Column | Type | Width | Source Field | Format | Sortable |
|---|--------|------|-------|-------------|--------|----------|
| 1 | **Execution ID** | int | 6 | `id` | Right-aligned integer | Yes (default: desc) |
| 2 | **Description** | string | 60 | `description` | Left-aligned, first 60 chars, truncated with `..` | Yes |
| 3 | **Status** | string | 10 | `status` | Color-coded (see 9.4) | Yes |
| 4 | **Duration** | duration | 10 | Computed from `submissionTime` and `completionTime` (or `now`) | Human-readable | Yes |
| 5 | **Scan Size** | bytes | 10 | Extracted from `nodes[*].metrics` where `name == "number of output rows"` on Scan nodes. If unavailable, show `--` | Human-readable | Yes |
| 6 | **Rows Scanned** | int | 12 | Extracted from SQL plan metrics (scan node output rows). If unavailable, show `--` | Comma-formatted integer | Yes |
| 7 | **Shuffle Size** | bytes | 10 | Extracted from exchange node metrics. If unavailable, show `--` | Human-readable | Yes |

**Total columns: 7**

### 9.4 Status Colors

| Status | Text Color | Row Style |
|--------|------------|-----------|
| `RUNNING` | Green (#00ff00) | Normal weight |
| `COMPLETED` | Dim | Dim entire row |
| `FAILED` | Red (#ff0000) | Red status text |

### 9.5 Sorting

Default sort: Execution ID descending (newest first).

Sort cycle (`s` key): Execution ID -> Duration -> Scan Size -> Status -> (back to Execution ID).

### 9.6 Query Detail Popup

When `Enter` is pressed on a selected row, a **modal overlay** (not a split panel) appears centered in the view, showing the full query text.

**Popup specification:**
- Title: `Query Detail (ID: {execution_id})`
- Width: 80% of terminal width, max 100 chars
- Height: Up to 60% of terminal height, scrollable if query is longer
- Background: Slightly lighter than main background (panel color)
- Border: Single-line box drawing

**Popup fields:**

| Field | Source | Format |
|-------|--------|--------|
| Execution ID | `id` | Integer |
| Status | `status` | Color-coded, with duration |
| Submitted | `submissionTime` | `YYYY-MM-DD HH:MM:SS` |
| Running Jobs | `runningJobIds` | Comma-separated list or `--` |
| Succeeded Jobs | `successJobIds` | Comma-separated list or `--` |
| Failed Jobs | `failedJobIds` | Comma-separated list or `--` |
| Full Query | `description` | Full text, syntax-highlighted if possible (SQL keywords in blue), monospace font, scrollable |

Press `Escape` to close the popup.

### 9.7 Scan Size and Shuffle Size Extraction

These metrics are derived from the SQL execution plan's physical operators:

**Scan Size:** Sum of `bytesRead` metrics from all Scan/FileScan nodes in the plan.
**Rows Scanned:** Sum of `output rows` metrics from all Scan/FileScan nodes.
**Shuffle Size:** Sum of `data size` metrics from all Exchange nodes.

If the SQL execution does not contain these metrics (e.g., DDL statements), show `--`.

Implementation note: The `/sql/{id}` endpoint with `?details=true` returns the node-level metrics. For the table view, the list endpoint's summary metrics are sufficient. The detail popup may lazy-fetch the full plan.

### 9.8 Empty State

```
No SQL queries found.
SQL queries appear when Spark SQL statements are executed.
```

---

## 10. View 6: Storage / RDD

### 10.1 Overview

Displays cached RDDs and DataFrames with their storage level, partition distribution, and memory/disk usage.

**Data source:** `SPARK:/api/v1/applications/{app_id}/storage/rdd`
**Poll tier:** Slow (15s)

### 10.2 Summary Bar

```
Cached RDDs: 3     Total Memory: 4.2 GB     Total Disk: 0 B
```

### 10.3 DataTable Columns

| # | Column | Type | Width | Source Field | Format | Sortable |
|---|--------|------|-------|-------------|--------|----------|
| 1 | **RDD ID** | int | 6 | `id` | Right-aligned integer | Yes (default: desc) |
| 2 | **Name** | string | 30 | `name` | Left-aligned, truncated to 30 chars | Yes |
| 3 | **Storage Level** | string | 14 | `storageLevel` | Human-readable (see 10.4) | Yes |
| 4 | **Partitions** | string | 10 | `numCachedPartitions`, `numPartitions` | `{cached}/{total}` | Yes (by cached count) |
| 5 | **Memory Size** | bytes | 10 | `memoryUsed` | Human-readable | Yes |
| 6 | **Disk Size** | bytes | 10 | `diskUsed` | Human-readable | Yes |
| 7 | **Serialized** | bool | 4 | `storageLevel` contains `SER` | `Yes` / `No` | Yes |

**Total columns: 7**

### 10.4 Storage Level Display

Map raw storage level strings to human-readable labels:

| Raw Value | Display |
|-----------|---------|
| `MEMORY_ONLY` | `Memory Only` |
| `MEMORY_ONLY_SER` | `Memory Only (Ser)` |
| `MEMORY_AND_DISK` | `Memory+Disk` |
| `MEMORY_AND_DISK_SER` | `Memory+Disk (Ser)` |
| `DISK_ONLY` | `Disk Only` |
| `MEMORY_ONLY_2` | `Memory Only x2` |
| `MEMORY_AND_DISK_2` | `Memory+Disk x2` |
| `OFF_HEAP` | `Off-Heap` |
| Other | Raw string |

### 10.5 Sorting

Default sort: RDD ID descending.

Sort cycle (`s` key): RDD ID -> Memory Size -> Disk Size -> Partitions -> Name -> (back to RDD ID).

### 10.6 Detail Panel

Expands when `Enter` is pressed on a selected RDD row.

**Detail panel fields:**

| Field | Source | Format |
|-------|--------|--------|
| Name | `name` | Full text |
| RDD ID | `id` | Integer |
| Storage Level | `storageLevel` | Human-readable |
| Partitions | `numCachedPartitions`, `numPartitions` | `{cached} cached / {total} total ({pct}%)` |
| Memory Used | `memoryUsed` | Human-readable |
| Disk Used | `diskUsed` | Human-readable |
| Distribution | `dataDistribution` (if available from `/storage/rdd/{id}`) | Per-executor breakdown: `Executor 0: 320 MB (40 partitions)` |

### 10.7 Empty State

```
No RDDs currently cached.
Use .persist() or .cache() to cache data.
```

---

## 11. Keyboard Bindings

### 11.1 Global Bindings (Active in All Views)

| Key | Action | Description |
|-----|--------|-------------|
| `1` | `switch_view("cluster")` | Switch to Cluster Overview |
| `2` | `switch_view("jobs")` | Switch to Spark Jobs |
| `3` | `switch_view("stages")` | Switch to Stages |
| `4` | `switch_view("executors")` | Switch to Executors |
| `5` | `switch_view("sql")` | Switch to SQL Queries |
| `6` | `switch_view("storage")` | Switch to Storage/RDD |
| `Tab` | `next_view()` | Cycle to next view (wraps around) |
| `Shift+Tab` | `prev_view()` | Cycle to previous view (wraps around) |
| `r` | `force_refresh()` | Force immediate poll of all data sources |
| `q` | `quit()` | Exit dbxtop |
| `Ctrl+C` | `quit()` | Exit dbxtop (alternative) |
| `?` | `toggle_help()` | Toggle help overlay |
| `Escape` | `dismiss()` | Close help overlay, detail panel, or filter input |

### 11.2 Table View Bindings (Active in Jobs, Stages, Executors, SQL, Storage)

| Key | Action | Description |
|-----|--------|-------------|
| `Up` / `k` | `cursor_up()` | Move selection up one row |
| `Down` / `j` | `cursor_down()` | Move selection down one row |
| `Home` / `g` | `cursor_top()` | Jump to first row |
| `End` / `G` | `cursor_bottom()` | Jump to last row |
| `PageUp` | `page_up()` | Scroll up one page |
| `PageDown` | `page_down()` | Scroll down one page |
| `Enter` | `toggle_detail()` | Toggle detail panel/popup for selected row |
| `s` | `cycle_sort()` | Cycle to next sort column |
| `S` | `toggle_sort_dir()` | Reverse current sort direction |
| `/` | `open_filter()` | Open text filter input |

### 11.3 View-Specific Bindings

| View | Key | Action | Description |
|------|-----|--------|-------------|
| Jobs | `f` | `cycle_status_filter()` | Cycle: All -> Running -> Succeeded -> Failed |
| Stages | `f` | `cycle_status_filter()` | Cycle: All -> Active -> Complete -> Pending -> Failed |
| Executors | `d` | `toggle_dead()` | Toggle showing dead/removed executors |
| SQL | `c` | `copy_query()` | Copy selected query's full SQL text to system clipboard |

### 11.4 Filter Input Mode Bindings

When the filter input is active (after pressing `/`):

| Key | Action |
|-----|--------|
| Any printable character | Append to filter string |
| `Backspace` | Delete last character |
| `Enter` | Apply filter and return to table navigation |
| `Escape` | Clear filter text and return to table navigation |

### 11.5 Total Binding Count

- Global: 13 bindings (1-6, Tab, Shift+Tab, r, q, Ctrl+C, ?, Escape)
- Table: 10 bindings (Up, Down, k, j, Home, End, g, G, PageUp, PageDown, Enter, s, S, /)
  - Note: Up/k and Down/j are aliases, so 10 unique actions mapped to 14 keys
- View-specific: 4 bindings (f in Jobs, f in Stages, d in Executors, c in SQL)
- Filter mode: 3 bindings (printable, Backspace, Enter, Escape)
  - Note: Escape already counted in global

**Total unique keyboard bindings: 30** (counting aliases as separate bindings)

### 11.6 Help Overlay Content

The `?` overlay displays all bindings in a centered modal:

```
+--- dbxtop Help -------------------------------------------+
|                                                            |
|  NAVIGATION                                                |
|  1-6          Switch to view by number                     |
|  Tab/S-Tab    Cycle views forward/back                     |
|                                                            |
|  TABLE                                                     |
|  j/k, Up/Down   Move cursor                               |
|  g/G, Home/End  Jump to top/bottom                         |
|  PageUp/Down    Scroll page                                |
|  Enter           Toggle detail panel                       |
|  s / S           Sort column / reverse direction           |
|  /               Text filter                               |
|  f               Status filter (Jobs/Stages)               |
|                                                            |
|  CONTROL                                                   |
|  r  Force refresh                                          |
|  q  Quit                                                   |
|  ?  This help                                              |
|                                                            |
|  VIEW-SPECIFIC                                             |
|  d  Show dead executors (Executors view)                   |
|  c  Copy query text (SQL view)                             |
|                                                            |
|                  [Press ? or Esc to close]                  |
+------------------------------------------------------------+
```

---

## 12. Color Scheme

### 12.1 Semantic Colors

| Name | Hex | Usage |
|------|-----|-------|
| `green` | `#00ff00` | Healthy, running, success, RUNNING state |
| `dim-green` | `#006600` | Completed/succeeded items (dimmed) |
| `yellow` | `#ffff00` | Pending, warning, high spill, GC 5-10% |
| `dim-yellow` | `#666600` | Stale data indicator, mild warnings |
| `red` | `#ff0000` | Failed, error, terminated, GC > 10% |
| `blue` | `#0087ff` | Info, headers, memory bars (normal range) |
| `white` | `#ffffff` | Default text |
| `dim` | `#666666` | Inactive, completed items, labels |
| `dark-bg` | `#1a1a2e` | Main background (dark theme) |
| `panel-bg` | `#16213e` | Panel/card background |
| `header-bg` | `#0f3460` | Header bar background |
| `surface` | `#222244` | Table alternating row background |

### 12.2 Dark Theme (Default)

```tcss
Screen {
    background: #1a1a2e;
    color: #ffffff;
}

#cluster-header {
    background: #0f3460;
    color: #ffffff;
    text-style: bold;
}

#keyboard-footer {
    background: #0f3460;
    color: #aaaaaa;
}

DataTable > .datatable--header {
    background: #16213e;
    color: #ffffff;
    text-style: bold;
}

DataTable > .datatable--cursor {
    background: #0087ff 30%;
}

DataTable > .datatable--even-row {
    background: #222244;
}
```

### 12.3 Light Theme

```tcss
Screen {
    background: #f5f5f5;
    color: #1a1a2e;
}

#cluster-header {
    background: #0f3460;
    color: #ffffff;
}

#keyboard-footer {
    background: #e0e0e0;
    color: #333333;
}
```

### 12.4 State-to-Color Mapping (Complete Reference)

| Entity | State/Condition | Foreground | Background |
|--------|----------------|------------|------------|
| Cluster | RUNNING | green | -- |
| Cluster | PENDING / RESIZING / RESTARTING | yellow | -- |
| Cluster | TERMINATED / TERMINATING / ERROR | red | -- |
| Spark Job | RUNNING | green | -- |
| Spark Job | SUCCEEDED | dim-green | dim row |
| Spark Job | FAILED | red | -- |
| Stage | ACTIVE | green | -- |
| Stage | COMPLETE | dim-green | dim row |
| Stage | PENDING | yellow | -- |
| Stage | FAILED | red | -- |
| Stage | High spill | -- | dim-yellow bg |
| Executor | Active | green | -- |
| Executor | Dead | red | -- |
| Executor | GC 5-10% | yellow (GC% cell) | -- |
| Executor | GC > 10% | red (GC% cell) | dim-red row |
| Executor | Memory > 85% | red (memory bar) | -- |
| Executor | Memory 70-85% | yellow (memory bar) | -- |
| SQL Query | RUNNING | green | -- |
| SQL Query | COMPLETED | dim | dim row |
| SQL Query | FAILED | red | -- |
| Library | INSTALLED | green | -- |
| Library | PENDING / INSTALLING | yellow | -- |
| Library | FAILED | red | -- |
| Connection | Connected | green | -- |
| Connection | Reconnecting | yellow | -- |
| Connection | Disconnected | red | -- |

---

## 13. Error States

### 13.1 Cluster TERMINATED

**Header:** Red state badge `○ TERMINATED`, uptime shows `--`, countdown shows slow-refresh interval only.

**Cluster View (View 1):**
- Section A (Identity): Full metadata displayed. State shows `TERMINATED` in red with `state_message` (termination reason) below.
- Section B (Resources): Node types and autoscale config shown. Gauges display `--` (no live metrics).
- Section C (Activity): Replaced with static text: `Cluster not running. Start the cluster to see activity metrics.`
- Section D (Events): Fully functional. Shows events leading up to termination.
- Section E (Libraries): Shows `Libraries unavailable while cluster is terminated.`

**Spark REST Views (Views 2-6: Jobs, Stages, Executors, SQL, Storage):**
Each shows a centered message box:
```
+----------------------------------------------------+
|                                                    |
|   Cluster is not running                           |
|                                                    |
|   The Spark REST API is unavailable when the       |
|   cluster is terminated. This view requires an     |
|   active Spark context.                            |
|                                                    |
|   Terminated at: 2026-03-16 12:45:00               |
|   Reason: Inactivity (auto-termination after 60m)  |
|                                                    |
|   Press 1 to switch to Cluster view for details.   |
|                                                    |
+----------------------------------------------------+
```

**Polling behavior:** Fast poll disabled. Slow poll continues at normal interval (SDK only). Spark REST availability is re-checked every slow-poll cycle. When the cluster transitions to RUNNING, fast poll re-enables automatically.

### 13.2 Authentication Expired / Invalid

**Detection:** SDK raises `NotAuthenticated` (HTTP 401) or `PermissionDenied` (HTTP 403) on any API call.

**Display:** Full-screen error modal, blocking all interaction except `q` to quit.

```
+------------------------------------------------------------+
|                                                            |
|  Authentication Failed                                     |
|                                                            |
|  Your Databricks token has expired or is invalid.          |
|                                                            |
|  To fix:                                                   |
|    1. Run: databricks auth login --profile {profile}       |
|    2. Restart dbxtop                                       |
|                                                            |
|  Profile: {profile_name}                                   |
|  Workspace: {workspace_url}                                |
|  Error: {error_message}                                    |
|                                                            |
|  Press q to quit.                                          |
|                                                            |
+------------------------------------------------------------+
```

**Polling behavior:** All polling stops immediately. No auto-retry (token refresh requires user action).

### 13.3 API Timeout (Request Timeout)

**Detection:** `httpx.TimeoutException` or SDK timeout on individual requests.

**Header:** Append yellow stale indicator: `stale {X}s` where X is seconds since last successful update for the most stale slot.

**Views:** Continue displaying last-known data. Stale data slots show a subtle dim-yellow timestamp label: `(updated 30s ago)` appended to the view title or summary bar.

**Polling behavior:** No immediate retry. The failed slot is marked stale in `DataCache`. On the next scheduled poll cycle, the request is retried normally. If 3 consecutive polls fail for the same slot, the staleness indicator turns from yellow to red.

**Footer:** Connection status shows `Connected` (green) if at least some calls succeed, `Reconnecting...` (yellow) if all calls in the last cycle failed.

### 13.4 Rate Limited (HTTP 429)

**Detection:** HTTP 429 response with optional `Retry-After` header.

**Footer:** Shows `Rate limited -- backing off` in yellow.

**Polling behavior:**
1. Respect `Retry-After` header if present.
2. Otherwise, apply exponential backoff: double the poll interval.
3. Backoff schedule: `1x -> 2x -> 4x -> 8x` (max 8x the configured interval, capped at 60s for fast poll and 120s for slow poll).
4. Recovery: After 3 consecutive successful polls with no 429, halve the interval. Continue halving until back to the configured interval.
5. Backoff applies per-tier (fast and slow independently).

**Header:** Normal. No additional indicator beyond footer status.

**Views:** Continue showing last-known data. No special visual treatment beyond the footer status.

### 13.5 Spark Application Not Found

**Detection:** `GET /api/v1/applications` returns an empty list, or returns applications but none match the expected cluster.

This occurs when the cluster is RUNNING but the Spark context has not yet initialized (e.g., cluster just started, or Spark context was restarted).

**Header:** Shows `RUNNING` (green) state badge, with a yellow `Spark: connecting...` indicator appended.

**Spark REST Views (Views 2-6):**
```
+----------------------------------------------------+
|                                                    |
|   Waiting for Spark application...                 |
|                                                    |
|   The cluster is running but no Spark application  |
|   has been detected yet. This usually means:       |
|                                                    |
|   - The cluster just started                       |
|   - The Spark context is initializing              |
|   - No Spark workload has been submitted yet       |
|                                                    |
|   dbxtop will automatically connect when the       |
|   Spark application becomes available.             |
|                                                    |
|   Checking every {slow_refresh}s...                |
|                                                    |
+----------------------------------------------------+
```

**Cluster View (View 1):** Fully functional (SDK data available). Activity panel sparklines show flat zeros.

**Polling behavior:** Spark REST calls are skipped (no app_id to query). Every slow-poll cycle, `discover_app_id()` is retried. Once an app_id is found, fast polling begins immediately.

---

## 14. Data Formatting Standards

### 14.1 Byte Size Formatting

All byte values are displayed in human-readable format using binary units (1 KB = 1024 bytes).

| Range | Format | Example |
|-------|--------|---------|
| 0 | `0 B` | `0 B` |
| 1 - 1023 | `{n} B` | `512 B` |
| 1 KiB - 999.9 KiB | `{n:.1f} KB` | `256.0 KB` |
| 1 MiB - 999.9 MiB | `{n:.1f} MB` | `1.2 MB` |
| 1 GiB - 999.9 GiB | `{n:.1f} GB` | `22.1 GB` |
| >= 1 TiB | `{n:.1f} TB` | `1.3 TB` |

Implementation:
```python
def format_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024.0 or unit == "TB":
            return f"{b:.1f} {unit}" if unit != "B" else f"{int(b)} B"
        b /= 1024.0
```

### 14.2 Duration Formatting

All durations are displayed in human-readable format.

| Range | Format | Example |
|-------|--------|---------|
| < 1 second | `{n}ms` | `450ms` |
| 1s - 59s | `{n}s` | `45s` |
| 1m - 59m | `{m}m {s}s` | `2m 14s` |
| 1h - 23h | `{h}h {m}m` | `4h 23m` |
| >= 24h | `{d}d {h}h` | `2d 14h` |

### 14.3 Integer Formatting

- Integers >= 1000 use comma separators: `1,847`
- Integers < 1000: no separator: `847`
- Zero: `0`

### 14.4 Percentage Formatting

- 1 decimal place: `73.4%`
- If exactly 0: `0.0%`
- If exactly 100: `100%` (no decimal)

### 14.5 Timestamp Formatting

| Context | Format | Example |
|---------|--------|---------|
| Same day as now | `HH:MM:SS` | `14:30:02` |
| Different day, same year | `MM-DD HH:MM` | `03-15 14:30` |
| Different year | `YYYY-MM-DD HH:MM` | `2025-12-31 14:30` |
| Full (detail panels) | `YYYY-MM-DD HH:MM:SS` | `2026-03-16 14:30:02` |

### 14.6 Record Count Formatting

Large record counts use abbreviated format in inline contexts:

| Range | Format | Example |
|-------|--------|---------|
| < 1000 | `{n}` | `847` |
| 1K - 999.9K | `{n:.1f}K` | `18.4K` |
| 1M - 999.9M | `{n:.1f}M` | `9.2M` |
| >= 1B | `{n:.1f}B` | `1.3B` |

In table cells where there is enough width, use comma-separated integers instead.

---

## 15. Widget Specifications

### 15.1 ProgressCell

**Purpose:** Renders an inline progress bar within a DataTable cell.

**Parameters:**
- `value: float` -- current value (e.g., completed tasks)
- `maximum: float` -- maximum value (e.g., total tasks)
- `width: int` -- total character width of the rendered bar (default: 16)
- `color: str` -- bar fill color (default: green)
- `show_text: bool` -- whether to show `{value}/{max}` or `{pct}%` after the bar

**Rendering:**
```
[=========>   ] 75%     # show_text=True, pct mode
[======>      ] 847/1.2K  # show_text=True, count mode
[=============] 100%    # full bar
[>            ]   0%    # empty bar
[====X        ]  42%    # failed state (red X at progress head)
```

**Character mapping:**
- `[` and `]`: bar delimiters
- `=`: filled portion
- `>`: progress head (current position)
- ` `: unfilled portion
- `X`: failure indicator (replaces `>` when status is FAILED)

### 15.2 MiniSparkline

**Purpose:** Renders a compact time-series chart using Unicode block characters, for embedding in the Cluster view.

**Parameters:**
- `values: list[float]` -- data points (ring buffer, typically 60 values)
- `width: int` -- character width (default: 30)
- `height: int` -- always 1 row
- `label: str` -- prefix label (e.g., `Active Tasks`)

**Rendering:**
```
Active Tasks ▁▂▄▇▅▃▂▁▃▅▇█▆▃▁▁▂▃▄▅▆▇█▇▆▅▄▃▂▁
```

**Block characters used:** `▁▂▃▄▅▆▇█` (Unicode block elements U+2581 through U+2588).

The values are normalized to the range [min, max] within the current buffer, then mapped to 8 levels:
- Level 0: `▁` (lowest)
- Level 7: `█` (highest)

If all values are equal: render all as `▄` (mid-level).
If buffer is empty: render spaces.

**Color:** Green by default. Can be overridden per instance (e.g., red for GC ratio when average > 5%).

### 15.3 StatusIndicator

**Purpose:** Renders a colored circle + text label for state display.

**Parameters:**
- `state: str` -- the state string (e.g., `RUNNING`, `TERMINATED`)
- `color_map: dict[str, str]` -- maps state to color name

**Rendering:**
- Active/filled circle: `●` (U+25CF) -- used for all states except TERMINATED
- Hollow circle: `○` (U+25CB) -- used for TERMINATED
- Followed by space and state text

Examples:
```
● RUNNING          (green)
● PENDING          (yellow)
○ TERMINATED       (red)
● ERROR            (red)
```

---

## 16. Polling and Data Flow

### 16.1 Two-Tier Polling Schedule

| Tier | Default Interval | CLI Override | Data Sources | Endpoints |
|------|-----------------|-------------|-------------|-----------|
| **Fast** | 3.0s | `--refresh` | Spark REST API | `/jobs`, `/stages`, `/allexecutors` |
| **Slow** | 15.0s | `--slow-refresh` | Databricks SDK + Spark REST (less volatile) | `clusters.get`, `clusters.events`, `libraries.cluster_status`, `/sql`, `/storage/rdd` |

### 16.2 Fast Poll Cycle

Executes every `--refresh` seconds. All calls run in parallel via `asyncio.gather()`.

```
1. GET /api/v1/applications/{app_id}/allexecutors  -> cache.executors
2. GET /api/v1/applications/{app_id}/jobs           -> cache.spark_jobs
3. GET /api/v1/applications/{app_id}/stages?status=active  -> cache.stages
```

If any call fails, the individual cache slot is marked stale; other slots still update.

After writing to cache, post `DataUpdated(source="fast")` message to the Textual app.

### 16.3 Slow Poll Cycle

Executes every `--slow-refresh` seconds. All calls run in parallel.

```
1. SDK: clusters.get(cluster_id)                   -> cache.cluster
2. SDK: clusters.events(cluster_id, order=DESC)     -> cache.events
3. SDK: libraries.cluster_status(cluster_id)        -> cache.libraries
4. GET /api/v1/applications/{app_id}/sql            -> cache.sql_queries
5. GET /api/v1/applications/{app_id}/storage/rdd    -> cache.storage
```

After writing to cache, post `DataUpdated(source="slow")` message.

### 16.4 Spark REST API URL Construction

```
{workspace_url}/driver-proxy-api/o/{org_id}/{cluster_id}/{port}/api/v1/applications/{app_id}/{endpoint}
```

- `workspace_url`: From SDK config (`WorkspaceClient.config.host`)
- `org_id`: Parsed from workspace URL or retrieved via API
- `cluster_id`: From CLI `--cluster-id`
- `port`: `40001` (Spark UI port on Databricks driver)
- `app_id`: Discovered via `GET .../api/v1/applications` (cached, refreshed on 404)

### 16.5 Cache Slot Structure

Each data category has an independent cache slot:

```python
@dataclass
class CacheSlot(Generic[T]):
    data: T | None = None           # Current data (preserved across failed polls)
    last_updated: datetime | None   # Timestamp of last successful update
    error: str | None = None        # Error message from last failed poll
    stale: bool = False             # True if last poll failed but data exists
    consecutive_failures: int = 0   # Count of consecutive failures
```

### 16.6 View Update Protocol

When `DataUpdated` is posted:
1. The `DbxtopApp` forwards the event to the currently active view.
2. The active view reads relevant cache slots.
3. The view applies current sort order and filters.
4. The view re-renders its DataTable/widgets.
5. The header updates its countdown timer and stale indicators.
6. The footer updates its last-update timestamp.

Inactive views do NOT re-render. When a user switches to a view, it immediately reads from the cache and renders current data.

---

## Appendix A: Column Count Summary

| View | Columns |
|------|---------|
| 1. Cluster Overview | N/A (card layout, not a DataTable) |
| 2. Spark Jobs | 7 |
| 3. Stages | 11 |
| 4. Executors | 14 |
| 5. SQL Queries | 7 |
| 6. Storage/RDD | 7 |
| **Total across table views** | **46** |

## Appendix B: Data Source Mapping

| View | Primary Data Source | Poll Tier | Cache Slot |
|------|-------------------|-----------|------------|
| Cluster Overview | `SDK:clusters.get`, `SDK:clusters.events`, `SDK:libraries.cluster_status` | Slow | `cluster`, `events`, `libraries` |
| Spark Jobs | `SPARK:/jobs` | Fast | `spark_jobs` |
| Stages | `SPARK:/stages` | Fast | `stages` |
| Executors | `SPARK:/allexecutors` | Fast | `executors` |
| SQL Queries | `SPARK:/sql` | Slow | `sql_queries` |
| Storage/RDD | `SPARK:/storage/rdd` | Slow | `storage` |

## Appendix C: Sort Column Cycles per View

| View | Sort Cycle (pressing `s` repeatedly) |
|------|--------------------------------------|
| Jobs | Job ID (desc) -> Status -> Duration -> Submitted -> Tasks -> Job ID... |
| Stages | Stage ID (desc) -> Status -> Duration -> Shuffle Write -> Spill (Disk) -> Input Size -> Stage ID... |
| Executors | Memory Used (desc) -> GC% -> Active Tasks -> Completed Tasks -> Shuffle Write -> Executor ID -> Memory Used... |
| SQL | Execution ID (desc) -> Duration -> Scan Size -> Status -> Execution ID... |
| Storage | RDD ID (desc) -> Memory Size -> Disk Size -> Partitions -> Name -> RDD ID... |
