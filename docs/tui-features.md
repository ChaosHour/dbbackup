# TUI Features Reference

> Production-hardened interactive terminal UI for dbbackup v6.30.0

## Overview

The `dbbackup interactive` command launches a full-screen terminal UI built with [Bubbletea](https://github.com/charmbracelet/bubbletea). The TUI includes bulletproofing features that prevent common production mistakes, engine-aware adaptive job sizing, and I/O scheduler governor selection.

## Connection Health Indicator

**What:** Real-time database connection status displayed in the menu header.

**Why:** Prevents users from spending time configuring a backup/restore operation only to discover the database is unreachable.

### Behavior

| Indicator | Meaning |
|-----------|---------|
| `[OK] Connected` | Database is reachable, queries succeed |
| `[FAIL] Disconnected` | Connection failed (timeout, auth error, host down) |
| `[WAIT] Checking...` | Health check in progress |

- **Timeout:** 5 seconds per check
- **Auto-retry:** Every 30 seconds after failure
- **Check method:** `SELECT 1` query via the configured connection

### CLI Flags

| Flag | Description |
|------|-------------|
| `--host` | Database host (default: localhost) |
| `--port` | Database port (default: 5432/3306) |
| `--user` | Database user |
| `--db-type` | Engine type: `postgres`, `mysql`, `mariadb` |

### Troubleshooting

If you see `[FAIL] Disconnected`:

1. Verify the database is running: `psql -U postgres -c "SELECT 1"`
2. Check connection parameters: `./dbbackup status`
3. For Unix socket auth: use `--host /var/run/postgresql` instead of `--host localhost`
4. Test CLI mode to bypass the TUI: `./dbbackup backup single testdb`

---

## Pre-Restore Validation Screen

**What:** A sequential preflight check screen that runs before any restore operation begins.

**Why:** Catches problems (corrupted archives, insufficient disk space, missing privileges) BEFORE the time-consuming extraction phase — not halfway through a 2-hour restore.

### Checks Performed

| # | Check | What It Does | Failure Impact |
|---|-------|-------------|----------------|
| 1 | Archive exists | Verifies the backup file/directory exists | Blocks restore |
| 2 | Archive integrity | Runs `gzip -t` / format-specific validation | Blocks restore |
| 3 | Disk space | Calculates required space using metadata-aware multiplier | Blocks restore |
| 4 | Required tools | Checks for `pg_restore`/`mysql` (if using external engine) | Blocks restore |
| 5 | Target DB status | Checks if target database already exists | Warning only |
| 6 | User privileges | Verifies CREATEDB / superuser privileges | Blocks restore |
| 7 | Lock capacity | Checks `max_locks_per_transaction` vs table count | Warning only |

### Progress Display

```
Pre-Restore Validation

✓ Archive exists                          /backups/prod_2026-02-10.sql.gz
✓ Archive integrity verified              gzip OK (98.7 GB)
✓ Disk space sufficient                   Need 148 GB, have 312 GB (2.1× ratio)
✓ Required tools available                pg_restore 16.2
⚠ Target database exists                  'production' will be DROPPED
✓ User has required privileges            postgres (superuser)
✓ Lock capacity adequate                  max_locks = 64, tables = 42

6 passed, 1 warning, 0 failed

[Enter] Continue  |  [Esc] Cancel
```

### Disk Space Calculation

The multiplier is determined in priority order:

1. **Metadata** — reads `.meta.json` sidecar for actual `total_size_bytes`, calculates exact ratio
2. **Format detection** — identifies compression via magic bytes (`.tar.gz` → 3×, `.custom` → 2×, `.sql.gz` → 4×)
3. **Config override** — `--disk-space-multiplier` CLI flag
4. **Safe fallback** — 3× when nothing else is available

---

## Two-Stage Abort with Full Cleanup

**What:** A safe cancellation mechanism for long-running backup and restore operations.

**Why:** A raw `kill -9` leaves behind orphaned `pg_dump`/`pg_restore` processes, temporary extraction directories consuming disk space, and potentially boosted PostgreSQL settings (e.g., `maintenance_work_mem`) that never get reset.

### Behavior

```
Stage 1 — First Ctrl+C:
┌─────────────────────────────────────────┐
│  ⚠ Abort operation?                     │
│                                         │
│  Backup in progress: production (34%)   │
│                                         │
│  [Y] Abort and clean up                 │
│  [N] Continue operation                 │
│  [Ctrl+C] Force abort                   │
└─────────────────────────────────────────┘

Stage 2 — Y or second Ctrl+C:
  Cancelling context...
  Terminating pg_dump (PID 12345)...
  Removing /tmp/.dbbackup_extract_a1b2c3/...
  Resetting maintenance_work_mem...
  Cleanup complete.
```

### Cleanup Actions (LIFO order)

1. Cancel Go context (stops in-progress queries)
2. Kill child processes (`pg_dump`, `pg_restore`, `pg_basebackup` PIDs)
3. Remove temporary extraction directories (`/tmp/.dbbackup_extract_*`, `/tmp/.restore_*`)
4. Reset boosted PostgreSQL settings (if turbo/balanced mode changed them)
5. Release advisory locks (if held)

### Important Notes

- The cleanup stack uses LIFO (Last-In-First-Out) order to ensure proper resource release
- A third Ctrl+C will force-kill the process immediately (no cleanup)
- `--auto-confirm` mode skips the confirmation prompt — first Ctrl+C triggers cleanup directly

---

## Destructive Operation Warnings

**What:** A type-to-confirm prompt that appears when a restore operation would overwrite an existing database.

**Why:** Prevents accidental production data loss. A simple "Are you sure? [Y/n]" is too easy to confirm by muscle memory. Typing the database name forces conscious acknowledgment.

### Trigger Conditions

The warning appears when ALL of these are true:
1. Operation is a restore (single or cluster)
2. Target database already exists
3. `--auto-confirm` is NOT set

### Display

```
┌─────────────────────────────────────────────────┐
│  ⚠  DESTRUCTIVE OPERATION                       │
│                                                  │
│  Database 'production' already exists.           │
│  Restoring will DROP the existing database       │
│  and all its data.                               │
│                                                  │
│  Source: /backups/prod_2026-02-10.sql.gz         │
│  Size:   98.7 GB (estimated restore: ~45 min)   │
│                                                  │
│  Type 'production' to confirm:                   │
│  > produc█                                       │
│                                                  │
│  [Esc] Cancel                                    │
└─────────────────────────────────────────────────┘
```

### Behavior

- User must type the **exact** database name (case-sensitive)
- Partial matches are not accepted
- `Esc` cancels at any time and returns to the menu
- `--auto-confirm` bypasses this prompt entirely (for CI/CD use)

---

## I/O Governor Selector

**What:** A setting in Configuration Settings to choose the I/O scheduling governor for BLOB operations.

**Why:** Different BLOB strategies benefit from different I/O scheduling patterns. Auto-selection maps the BLOB strategy to the optimal governor, but manual override is available for tuning.

### Governors

| Governor | Strategy | Description |
|----------|----------|-------------|
| `auto` | Auto-select | Maps BLOB strategy to optimal governor automatically |
| `noop` | Standard/no BLOBs | Simple FIFO, zero overhead |
| `bfq` | Bundled BLOBs | Budget Fair Queueing with per-class budgets |
| `mq-deadline` | Parallel-stream BLOBs | Multi-queue deadline with round-robin distribution |
| `deadline` | Large-object (lo_*) | Single-queue deadline with starvation prevention |

### TUI Behavior

In Configuration Settings, the I/O Governor entry cycles through `auto -> noop -> bfq -> mq-deadline -> deadline` on each press. The selected governor is persisted to the config file.

### CLI Override

```bash
dbbackup restore cluster backup.tar.gz --io-governor=bfq --confirm
```

Or via environment variable:

```bash
IO_GOVERNOR=mq-deadline dbbackup restore cluster backup.tar.gz --confirm
```

---

## Adaptive Jobs (v6.14.0)

**What:** Engine-aware adaptive worker calculation that automatically sizes parallel jobs per database.

**Why:** Different databases have different restore characteristics. A 50MB database should not get the same parallelism as a 50GB database. The adaptive system uses a 3-stage pipeline to determine optimal worker count.

### 3-Stage Pipeline

1. **BLOB adjustment** -- adjusts base worker count based on BLOB strategy:
   - `parallel-stream`: halves workers (BLOB engine handles I/O internally)
   - `bundle`: boosts 1.5x (bundled small BLOBs = low overhead)
   - `large-object`: reduces 25% (lo_ API contention)

2. **Native engine boost** -- 1.3x for native Go restore, 1.5x when combined with BLOBs

3. **Memory ceiling** -- caps workers based on available RAM:
   - 8GB: max 16 workers
   - 16GB: max 24 workers
   - 32GB: max 28 workers
   - 64GB: max 32 workers

### TUI Behavior

Adaptive Jobs is enabled by default. Toggle ON/OFF in Configuration Settings. When enabled, the adaptive system overlays resource profiles. Full debug trace is logged with `[ADAPTIVE-V2]` prefix when debug logging is active.

---

## CLI Automation Flags

For CI/CD pipelines and scripted use, these flags bypass interactive prompts:

| Flag | Description |
|------|-------------|
| `--auto-select N` | Auto-select menu item N (1-based) |
| `--auto-database DB` | Auto-select database by name |
| `--auto-confirm` | Skip all confirmation prompts (including destructive warnings) |
| `--no-save-config` | Don't persist configuration changes |
| `--tui-debug` | Enable TUI debug logging |

### Example: Automated Backup

```bash
# Backup 'production' DB without any prompts
./dbbackup interactive \
    --auto-select 1 \
    --auto-database production \
    --auto-confirm \
    --no-save-config
```

### Example: Automated Restore

```bash
# Restore from archive, skip destructive warning
./dbbackup interactive \
    --auto-select 5 \
    --auto-confirm \
    --backup-dir /backups \
    --no-save-config
```

---

## Testing

### Automated Tests

```bash
# Run the Phase 1 test suite (15 tests)
DBBACKUP=./dbbackup ./tests/tui_phase1_test.sh
```

### Manual Testing Guide

See [testing/phase1-manual-tests.md](testing/phase1-manual-tests.md) for step-by-step manual test procedures.

### Test Categories

| Category | Tests | Coverage |
|----------|-------|----------|
| Build & Unit | 2 | Compilation, 23 Go unit tests |
| Connection Health | 3 | Valid, timeout, auto-retry |
| Preflight Checks | 5 | Missing/corrupted archive, disk, privileges, locks |
| Destructive Warnings | 1 | Existing database detection |
| Abort & Cleanup | 3 | Backup abort, restore cleanup, auto-confirm bypass |

---

## Detailed Completion Summary (v6.18.0)

**What:** After backup or restore completes, press `D` to toggle a detailed DBA-focused statistics panel directly on the completion screen.

### Displayed Statistics

| Category | Metrics |
|----------|---------|
| Exact Metrics | Duration (seconds), size (bytes + human-readable), compression ratio |
| Per-Database | Individual duration, size, throughput (MB/s), start/end times |
| Resource Usage | CPU workers used vs available, memory usage %, parallel worker count |
| Warnings | Low compression ratio detection, recommendations |

### Key Bindings

| Key | Action |
|-----|--------|
| `D` | Toggle detailed summary panel on/off |
| `Enter` | Continue to next screen |

### JSON Export

Set `SAVE_DETAILED_SUMMARY=true` to auto-save a `.stats.json` file alongside each backup/restore:

```bash
export SAVE_DETAILED_SUMMARY=true
export DETAILED_SUMMARY_PATH=/var/log/dbbackup/  # optional custom path
```

The JSON output includes all metrics with machine-readable field names, suitable for ingestion by Prometheus, Grafana, or custom monitoring scripts.

### Design

- **Non-breaking** — the default completion screen is unchanged; `D` is opt-in
- Backup summary uses `BackupDetailedSummary` struct with JSON tags
- Restore summary uses `RestoreDetailedSummary` struct with JSON tags
- Resource stats reuse `ResourceUsageStats` (shared between backup/restore)

---

## Settings Pagination (v6.18.0)

**What:** The settings screen (32 items) is split across 2 pages to fit standard 40-line terminals without scrolling.

### Page Layout

| Page | Title | Items |
|------|-------|-------|
| 1 | Core Configuration | First 16 settings (DB type, host, port, compression, jobs, etc.) |
| 2 | Advanced & Cloud Settings | Remaining settings (cloud, encryption, retention, etc.) |

### Key Bindings

| Key | Action |
|-----|--------|
| `→` / `PgDn` | Next page |
| `←` / `h` / `PgUp` | Previous page |
| `↑` / `↓` | Navigate items (wraps across pages at boundaries) |

### Display

- Header shows "Page X of Y" indicator
- Each page shows a category header ("Core Configuration" / "Advanced & Cloud Settings")
- Footer dynamically shows available page navigation keys based on current page

---

## Architecture

### File Layout

| File | LOC | Purpose |
|------|-----|---------|
| `internal/tui/preflight.go` | 533 | Pre-restore validation screen |
| `internal/tui/destructive_warning.go` | 141 | Type-to-confirm prompt |
| `internal/tui/menu.go` | +87 | Connection health indicator |
| `internal/tui/backup_exec.go` | +222 | Detailed summary, D-key toggle, JSON export |
| `internal/tui/restore_exec.go` | +147 | Restore detailed summary and JSON export |
| `internal/tui/settings.go` | +107 | 2-page pagination with page navigation |

### Dependencies

- [Bubbletea](https://github.com/charmbracelet/bubbletea) — Terminal UI framework
- [Lipgloss](https://github.com/charmbracelet/lipgloss) — Terminal styling
- [pgx](https://github.com/jackc/pgx) — PostgreSQL driver (for health checks)

---

*Last updated: 2026-02-16 -- v6.50.10*
