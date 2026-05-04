# CLAUDE.md

Guidance for Claude Code when working with the `dbxtop` repository.

**dbxtop** is a real-time terminal dashboard for monitoring Databricks/Spark
clusters — `htop` for Spark.

For deep architectural detail, design rationale, API reference, and
implementation gotchas, see **[ARCHITECTURE.md](ARCHITECTURE.md)**. For
end-user docs, see the **[README](README.md)**.

## Commands

```bash
# Setup (once)
pip install -e ".[dev]"

# Run
dbxtop -c <CLUSTER_ID> -p <PROFILE>            # standard
textual-dev run --dev "dbxtop -c ID -p PROF"   # live reload
textual console                                 # debug output (separate terminal)

# Tests
pytest tests/ --ignore=tests/test_integration.py     # unit
pytest tests/test_snapshots.py                        # snapshot
pytest tests/test_snapshots.py --snapshot-update      # refresh snapshots
DBXTOP_TEST_CLUSTER_ID=... DATABRICKS_CONFIG_PROFILE=... \
    pytest tests/test_integration.py                  # integration

# Lint, format, type check
ruff check src/ tests/
ruff format src/ tests/        # ALWAYS run before committing — CI rejects unformatted code
mypy src/dbxtop/                # strict mode
```

Demo screenshots:
`python scripts/generate_demo_screenshots.py` — writes SVGs to
`docs/screenshots/` from synthetic data, no live cluster needed.

## Conventions

### Code style
- **Formatter:** `ruff format`. **Always run before committing** — CI enforces
  `ruff format --check`.
- **Line length:** 120. **Target Python:** 3.9+
  (`from __future__ import annotations`).
- **Type hints:** strict mypy. All public functions need complete annotations.
- **Imports:** absolute (`from dbxtop.api.models import ClusterInfo`).
- **Docstrings:** Google-style on public classes and functions.

### Naming
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Private: leading underscore

## Critical gotchas

- **Wrap SDK calls with `asyncio.to_thread()`.** The Databricks SDK is
  synchronous; calling it directly from an async function blocks the Textual
  event loop. See ARCHITECTURE.md → Implementation Notes.
- **Escape `[` in DataTable cell content and `Static.update()` strings.**
  Textual parses Rich markup; unescaped `[` raises `MarkupError`. Use `\\[`
  — see `widgets/progress_cell.py:render_progress` for the canonical pattern.
- **Views never call APIs directly.** They read from `DataCache` in response
  to `DataUpdated` messages from the poller.

## Git & branching (Gitflow)

- **`main`** — production, protected. Only PRs from `develop`, CI must pass.
- **`develop`** — integration, protected. Only PRs from feature/fix branches.
- Feature: `feature/{ticket-or-description}` (e.g. `feature/sem2-168-markup-fix`)
- Fix: `fix/{ticket-or-description}` (e.g. `fix/sem2-144-executors-memory`)

**Workflow:** create branch from `develop` → PR → merge to `develop` → release
PR `develop → main`. On merge to `main`, version bumps automatically (default
patch; add a `minor` or `major` PR label to override). Versions are derived
from git tags via `hatch-vcs` — no version file to edit.

**Commit style:** imperative mood, <72-char first line, optional body.
**Never commit directly to `main` or `develop`.**

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
