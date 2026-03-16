# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-17

### Added

- **Cluster view** with state card, resources, Spark config, events, and libraries
- **Jobs view** with progress bars, stage counts, and pinned running jobs
- **Stages view** with task progress, shuffle/spill metrics, and I/O bytes
- **Executors view** with memory bars, GC% highlighting, sparkline trends, and summary row
- **SQL view** with query status, duration, and job counts
- **Storage view** with cached RDD/DataFrame details, memory/disk usage
- **Two-tier polling**: fast (3s) for executors/jobs/stages, slow (15s) for cluster metadata
- **Graceful degradation**: SDK-only mode when Spark REST API is unavailable
- **OAuth token auto-refresh** via token provider callable
- **Detail popups** for Jobs, Stages, and SQL views (Enter key)
- **Live filter input** (`/` key) with real-time row filtering
- **Sort cycling** (`s` key) across all table views
- **Help overlay** (`?` key) with all keyboard shortcuts
- **Dark and light themes** (`--theme` flag)
- **Sparkline trends** for executor memory and GC metrics
- **Three-state footer**: Spark connected / SDK only / disconnected
- **Spark context ID change detection** for automatic reconnection
- **Poll serialization** via asyncio.Lock to prevent race conditions
- **Slot-based view refresh** to avoid unnecessary re-renders
- **CLI** with Click: `--profile`, `--cluster-id`, `--refresh`, `--theme`
- **Environment variable** support for all CLI flags
- **GitHub Actions CI** with Python 3.9-3.13 matrix
- **PyPI publishing** workflow via trusted publisher (OIDC)

[0.1.0]: https://github.com/ozlevi/dbxtop/releases/tag/v0.1.0
