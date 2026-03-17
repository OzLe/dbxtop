# dbxtop

[![PyPI](https://img.shields.io/pypi/v/dbxtop)](https://pypi.org/project/dbxtop/)
[![Python](https://img.shields.io/pypi/pyversions/dbxtop)](https://pypi.org/project/dbxtop/)
[![CI](https://github.com/ozlevi/dbxtop/actions/workflows/ci.yml/badge.svg)](https://github.com/ozlevi/dbxtop/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Real-time terminal dashboard for Databricks/Spark clusters. Think `htop`, but for your Spark jobs.

<!-- TODO: Add demo GIF once recorded with vhs/asciinema -->

## Features

**6 live-updating views**, all from a single terminal session:

| View | What it shows |
|------|---------------|
| **Cluster** | State, node counts, uptime, Spark config, events, libraries |
| **Jobs** | Spark jobs with progress bars, stage counts, duration |
| **Stages** | Stages with task progress, shuffle/spill metrics, I/O bytes |
| **Executors** | Memory bars, GC%, disk, shuffle, sparkline trends per executor |
| **SQL** | SQL query status, duration, job counts |
| **Storage** | Cached RDDs/DataFrames, memory/disk usage, partition counts |

**More highlights:**

- Two-tier polling: fast (3s) for executors/jobs/stages, slow (15s) for cluster metadata
- Graceful degradation: works in SDK-only mode when Spark REST is unavailable
- OAuth token auto-refresh: handles token expiry transparently
- Dark and light themes
- Read-only: never mutates cluster state

## Install

```bash
pip install dbxtop
```

Or with pipx for isolated installation:

```bash
pipx install dbxtop
```

## Quick Start

```bash
# Requires a configured Databricks CLI profile (~/.databrickscfg)
dbxtop --cluster-id <CLUSTER_ID>

# With a specific profile
dbxtop --profile my-workspace --cluster-id 0123-456789-abcdefgh

# Short flags
dbxtop -p my-workspace -c 0123-456789-abcdefgh

# Light theme
dbxtop -c <CLUSTER_ID> --theme light
```

### Finding your cluster ID

- **Databricks UI:** Compute > click cluster > copy ID from URL or details
- **Databricks CLI:** `databricks clusters list --profile <profile>`
- **Format:** `XXXX-XXXXXX-XXXXXXXX` (e.g., `0123-456789-abcdefgh`)

## Configuration

### CLI Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--profile` | `-p` | `DEFAULT` | Databricks CLI profile name |
| `--cluster-id` | `-c` | *(required)* | Target cluster ID |
| `--refresh` | | `3.0` | Fast poll interval (seconds) |
| `--slow-refresh` | | `15.0` | Slow poll interval (seconds) |
| `--theme` | | `dark` | Color theme (`dark` or `light`) |

### Environment Variables

All CLI flags can also be set via environment variables:

| Variable | Maps to |
|----------|---------|
| `DBXTOP_PROFILE` | `--profile` |
| `DBXTOP_CLUSTER_ID` | `--cluster-id` |
| `DBXTOP_REFRESH` | `--refresh` |
| `DBXTOP_SLOW_REFRESH` | `--slow-refresh` |
| `DBXTOP_THEME` | `--theme` |

### Connection Setup

dbxtop uses the standard Databricks CLI profile system (`~/.databrickscfg`):

```ini
[my-workspace]
host = https://adb-1234567890.1.azuredatabricks.net
token = dapi...
```

OAuth and U2M authentication are also supported. Run `databricks auth login --profile <name>` to set up.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `1`-`6` | Switch views (Cluster, Jobs, Stages, Executors, SQL, Storage) |
| `Tab` / `Shift+Tab` | Next / previous tab |
| `r` | Force refresh all data |
| `s` | Cycle sort column |
| `/` | Filter rows (type to search, Enter to apply) |
| `Escape` | Clear filter |
| `Enter` | Open detail popup (Jobs, Stages, SQL views) |
| `?` | Toggle help overlay |
| `q` | Quit |

## Requirements

- Python 3.9+
- A Databricks workspace with a configured CLI profile
- A running (or recently terminated) cluster to monitor

## Development

```bash
git clone https://github.com/ozlevi/dbxtop.git
cd dbxtop
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -v --ignore=tests/test_integration.py

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Live reload during development
textual-dev run --dev "dbxtop -c CLUSTER_ID -p PROFILE"
```

## License

[MIT](LICENSE)
