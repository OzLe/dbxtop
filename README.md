# dbxtop

Real-time terminal dashboard for Databricks/Spark clusters. Think `htop`, but for your Spark jobs.

## Features

- Live cluster status, node counts, and uptime
- Spark jobs, stages, and task progress
- Executor resource utilization (memory, disk, GC)
- SQL query monitoring
- Storage/RDD cache overview
- Keyboard-driven tab navigation

## Install

```bash
pip install dbxtop
```

Or with pipx for isolated installation:

```bash
pipx install dbxtop
```

## Usage

```bash
# Basic usage
dbxtop --cluster-id 0311-160038-vjuv0ah9

# With a specific Databricks CLI profile
dbxtop --profile my-workspace --cluster-id 0311-160038-vjuv0ah9

# Custom refresh intervals
dbxtop --cluster-id 0311-160038-vjuv0ah9 --refresh 2.0 --slow-refresh 30.0
```

You can also set environment variables instead of passing flags:

```bash
export DATABRICKS_CLUSTER_ID=0311-160038-vjuv0ah9
export DATABRICKS_CONFIG_PROFILE=my-workspace
dbxtop
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `1`-`6` | Switch tabs (Cluster, Jobs, Stages, Executors, SQL, Storage) |
| `r` | Force refresh |
| `q` | Quit |

## Development

```bash
git clone https://github.com/ozlevi/dbxtop.git
cd dbxtop
pip install -e ".[dev]"
pytest
```

## License

MIT
