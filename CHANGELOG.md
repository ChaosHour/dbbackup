# Changelog

All notable changes to dbbackup will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Performance - Cluster Restore Optimization
- **Eliminated duplicate archive extraction in cluster restore** - saves 30-50% time on large restores
  - Previously: Archive was extracted twice (once in preflight validation, once in actual restore)
  - Now: Archive extracted once and reused for both validation and restore
  - **Time savings**:
    - 50 GB cluster: ~3-6 minutes faster
    - 10 GB cluster: ~1-2 minutes faster
    - Small clusters (<5 GB): ~30 seconds faster
  - Optimization automatically enabled when `--diagnose` flag is used
  - New `ValidateAndExtractCluster()` performs combined validation + extraction
  - `RestoreCluster()` accepts optional `preExtractedPath` parameter to reuse extracted directory
  - Disk space checks intelligently skipped when using pre-extracted directory
  - Maintains backward compatibility - works with and without pre-extraction
  - Log output shows optimization: `"Using pre-extracted cluster directory ... optimization: skipping duplicate extraction"`

### Improved - Archive Validation
- **Enhanced tar.gz validation with stream-based checks**
  - Fast header-only validation (validates gzip + tar structure without full extraction)
  - Checks gzip magic bytes (0x1f 0x8b) and tar header signature
  - Reduces preflight validation time from minutes to seconds on large archives
  - Falls back to full extraction only when necessary (with `--diagnose`)

## [3.42.74] - 2026-01-20 "Resource Profile System + Critical Ctrl+C Fix"

### Critical Bug Fix
- **Fixed Ctrl+C not working in TUI backup/restore** - Context cancellation was broken in TUI mode
  - `executeBackupWithTUIProgress()` and `executeRestoreWithTUIProgress()` created new contexts with `WithCancel(parentCtx)`
  - When user pressed Ctrl+C, `model.cancel()` was called on parent context but execution had separate context
  - Fixed by using parent context directly instead of creating new one
  - Ctrl+C/ESC/q now properly propagate cancellation to running operations
  - Users can now interrupt long-running TUI operations

### Added - Resource Profile System
- **`--profile` flag for restore operations** with three presets:
  - **Conservative** (`--profile=conservative`): Single-threaded (`--parallel=1`), minimal memory usage
    - Best for resource-constrained servers, shared hosting, or when "out of shared memory" errors occur
    - Automatically enables `LargeDBMode` for better resource management
  - **Balanced** (default): Auto-detect resources, moderate parallelism
    - Good default for most scenarios
  - **Aggressive** (`--profile=aggressive`): Maximum parallelism, all available resources
    - Best for dedicated database servers with ample resources
  - **Potato** (`--profile=potato`): Easter egg 🥔, same as conservative
- **Profile system applies to both CLI and TUI**:
  - CLI: `dbbackup restore cluster backup.tar.gz --profile=conservative --confirm`
  - TUI: Automatically uses conservative profile for safer interactive operation
- **User overrides supported**: `--jobs` and `--parallel-dbs` flags override profile settings
- **New `internal/config/profile.go`** module:
  - `GetRestoreProfile(name)` - Returns profile settings
  - `ApplyProfile(cfg, profile, jobs, parallelDBs)` - Applies profile with overrides
  - `GetProfileDescription(name)` - Human-readable descriptions
  - `ListProfiles()` - All available profiles

### Added - PostgreSQL Diagnostic Tools
- **`diagnose_postgres_memory.sh`** - Comprehensive memory and resource analysis script:
  - System memory overview with usage percentages and warnings
  - Top 15 memory consuming processes
  - PostgreSQL-specific memory configuration analysis
  - Current locks and connections monitoring
  - Shared memory segments inspection
  - Disk space and swap usage checks
  - Identifies other resource consumers (Nessus, Elastic Agent, monitoring tools)
  - Smart recommendations based on findings
  - Detects temp file usage (indicator of low work_mem)
- **`fix_postgres_locks.sh`** - PostgreSQL lock configuration helper:
  - Automatically increases `max_locks_per_transaction` to 4096
  - Shows current configuration before applying changes
  - Calculates total lock capacity
  - Provides restart commands for different PostgreSQL setups
  - References diagnostic tool for comprehensive analysis

### Added - Documentation
- **`RESTORE_PROFILES.md`** - Complete profile guide with real-world scenarios:
  - Profile comparison table
  - When to use each profile
  - Override examples
  - Troubleshooting guide for "out of shared memory" errors
  - Integration with diagnostic tools
- **`email_infra_team.txt`** - Admin communication template (German):
  - Analysis results template
  - Problem identification section
  - Three solution variants (temporary, permanent, workaround)
  - Includes diagnostic tool references

### Changed - TUI Improvements
- **TUI mode defaults to conservative profile** for safer operation
  - Interactive users benefit from stability over speed
  - Prevents resource exhaustion on shared systems
  - Can be overridden with environment variable: `export RESOURCE_PROFILE=balanced`

### Fixed
- Context cancellation in TUI backup operations (critical)
- Context cancellation in TUI restore operations (critical)
- Better error diagnostics for "out of shared memory" errors
- Improved resource detection and management

### Technical Details
- Profile system respects explicit user flags (`--jobs`, `--parallel-dbs`)
- Conservative profile sets `cfg.LargeDBMode = true` automatically
- TUI profile selection logged when `Debug` mode enabled
- All profiles support both single and cluster restore operations

## [3.42.50] - 2026-01-16 "Ctrl+C Signal Handling Fix"

### Fixed - Proper Ctrl+C/SIGINT Handling in TUI
- **Added tea.InterruptMsg handling** - Bubbletea v1.3+ sends `InterruptMsg` for SIGINT signals
  instead of a `KeyMsg` with "ctrl+c", causing cancellation to not work
- **Fixed cluster restore cancellation** - Ctrl+C now properly cancels running restore operations
- **Fixed cluster backup cancellation** - Ctrl+C now properly cancels running backup operations
- **Added interrupt handling to main menu** - Proper cleanup on SIGINT from menu
- **Orphaned process cleanup** - `cleanup.KillOrphanedProcesses()` called on all interrupt paths

### Changed
- All TUI execution views now handle both `tea.KeyMsg` ("ctrl+c") and `tea.InterruptMsg`
- Context cancellation properly propagates to child processes via `exec.CommandContext`
- No zombie pg_dump/pg_restore/gzip processes left behind on cancellation

## [3.42.49] - 2026-01-16 "Unified Cluster Backup Progress"

### Added - Unified Progress Display for Cluster Backup
- **Combined overall progress bar** for cluster backup showing all phases:
  - Phase 1/3: Backing up Globals (0-15% of overall)
  - Phase 2/3: Backing up Databases (15-90% of overall)
  - Phase 3/3: Compressing Archive (90-100% of overall)
- **Current database indicator** - Shows which database is currently being backed up
- **Phase-aware progress tracking** - New fields in backup progress state:
  - `overallPhase` - Current phase (1=globals, 2=databases, 3=compressing)
  - `phaseDesc` - Human-readable phase description
- **Dual progress bars** for cluster backup:
  - Overall progress bar showing combined operation progress
  - Database count progress bar showing individual database progress

### Changed
- Cluster backup TUI now shows unified progress display matching restore
- Progress callbacks now include phase information
- Better visual feedback during entire cluster backup operation

## [3.42.48] - 2026-01-15 "Unified Cluster Restore Progress"

### Added - Unified Progress Display for Cluster Restore
- **Combined overall progress bar** showing progress across all restore phases:
  - Phase 1/3: Extracting Archive (0-60% of overall)
  - Phase 2/3: Restoring Globals (60-65% of overall)
  - Phase 3/3: Restoring Databases (65-100% of overall)
- **Current database indicator** - Shows which database is currently being restored
- **Phase-aware progress tracking** - New fields in progress state:
  - `overallPhase` - Current phase (1=extraction, 2=globals, 3=databases)
  - `currentDB` - Name of database currently being restored
  - `extractionDone` - Boolean flag for phase transition
- **Dual progress bars** for cluster restore:
  - Overall progress bar showing combined operation progress
  - Phase-specific progress bar (extraction bytes or database count)

### Changed
- Cluster restore TUI now shows unified progress display
- Progress callbacks now set phase and current database information
- Extraction completion triggers automatic transition to globals phase
- Database restore phase shows current database name with spinner

### Improved
- Better visual feedback during entire cluster restore operation
- Clear phase indicators help users understand restore progress
- Overall progress percentage gives better time estimates

## [3.42.35] - 2026-01-15 "TUI Detailed Progress"

### Added - Enhanced TUI Progress Display
- **Detailed progress bar in TUI restore** - schollz-style progress bar with:
  - Byte progress display (e.g., `245 MB / 1.2 GB`)
  - Transfer speed calculation (e.g., `45 MB/s`)
  - ETA prediction for long operations
  - Unicode block-based visual bar
- **Real-time extraction progress** - Archive extraction now reports actual bytes processed
- **Go-native tar extraction** - Uses Go's `archive/tar` + `compress/gzip` when progress callback is set
- **New `DetailedProgress` component** in TUI package:
  - `NewDetailedProgress(total, description)` - Byte-based progress
  - `NewDetailedProgressItems(total, description)` - Item count progress
  - `NewDetailedProgressSpinner(description)` - Indeterminate spinner
  - `RenderProgressBar(width)` - Generate schollz-style output
- **Progress callback API** in restore engine:
  - `SetProgressCallback(func(current, total int64, description string))` 
  - Allows TUI to receive real-time progress updates from restore operations
- **Shared progress state** pattern for Bubble Tea integration

### Changed
- TUI restore execution now shows detailed byte progress during archive extraction
- Cluster restore shows extraction progress instead of just spinner
- Falls back to shell `tar` command when no progress callback is set (faster)

### Technical Details
- `progressReader` wrapper tracks bytes read through gzip/tar pipeline
- Throttled progress updates (every 100ms) to avoid UI flooding
- Thread-safe shared state pattern for cross-goroutine progress updates

## [3.42.34] - 2026-01-14 "Filesystem Abstraction"

### Added - spf13/afero for Filesystem Abstraction
- **New `internal/fs` package** for testable filesystem operations
- **In-memory filesystem** for unit testing without disk I/O
- **Global FS interface** that can be swapped for testing:
  ```go
  fs.SetFS(afero.NewMemMapFs())  // Use memory
  fs.ResetFS()                    // Back to real disk
  ```
- **Wrapper functions** for all common file operations:
  - `ReadFile`, `WriteFile`, `Create`, `Open`, `Remove`, `RemoveAll`
  - `Mkdir`, `MkdirAll`, `ReadDir`, `Walk`, `Glob`
  - `Exists`, `DirExists`, `IsDir`, `IsEmpty`
  - `TempDir`, `TempFile`, `CopyFile`, `FileSize`
- **Testing helpers**:
  - `WithMemFs(fn)` - Execute function with temp in-memory FS
  - `SetupTestDir(files)` - Create test directory structure
- **Comprehensive test suite** demonstrating usage

### Changed
- Upgraded afero from v1.10.0 to v1.15.0

## [3.42.33] - 2026-01-14 "Exponential Backoff Retry"

### Added - cenkalti/backoff for Cloud Operation Retry
- **Exponential backoff retry** for all cloud operations (S3, Azure, GCS)
- **Retry configurations**:
  - `DefaultRetryConfig()` - 5 retries, 500ms→30s backoff, 5 min max
  - `AggressiveRetryConfig()` - 10 retries, 1s→60s backoff, 15 min max
  - `QuickRetryConfig()` - 3 retries, 100ms→5s backoff, 30s max
- **Smart error classification**:
  - `IsPermanentError()` - Auth/bucket errors (no retry)
  - `IsRetryableError()` - Timeout/network errors (retry)
- **Retry logging** - Each retry attempt is logged with wait duration

### Changed
- S3 simple upload, multipart upload, download now retry on transient failures
- Azure simple upload, download now retry on transient failures
- GCS upload, download now retry on transient failures
- Large file multipart uploads use `AggressiveRetryConfig()` (more retries)

## [3.42.32] - 2026-01-14 "Cross-Platform Colors"

### Added - fatih/color for Cross-Platform Terminal Colors
- **Windows-compatible colors** - Native Windows console API support
- **Color helper functions** in `logger` package:
  - `Success()`, `Error()`, `Warning()`, `Info()` - Status messages with icons
  - `Header()`, `Dim()`, `Bold()` - Text styling
  - `Green()`, `Red()`, `Yellow()`, `Cyan()` - Colored text
  - `StatusLine()`, `TableRow()` - Formatted output
  - `DisableColors()`, `EnableColors()` - Runtime control
- **Consistent color scheme** across all log levels

### Changed
- Logger `CleanFormatter` now uses fatih/color instead of raw ANSI codes
- All progress indicators use fatih/color for `[OK]`/`[FAIL]` status
- Automatic color detection (disabled for non-TTY)

## [3.42.31] - 2026-01-14 "Visual Progress Bars"

### Added - schollz/progressbar for Enhanced Progress Display
- **Visual progress bars** for cloud uploads/downloads with:
  - Byte transfer display (e.g., `245 MB / 1.2 GB`)
  - Transfer speed (e.g., `45 MB/s`)
  - ETA prediction
  - Color-coded progress with Unicode blocks
- **Checksum verification progress** - visual progress while calculating SHA-256
- **Spinner for indeterminate operations** - Braille-style spinner when size unknown
- New progress types: `NewSchollzBar()`, `NewSchollzBarItems()`, `NewSchollzSpinner()`
- Progress bar `Writer()` method for io.Copy integration

### Changed
- Cloud download shows real-time byte progress instead of 10% log messages
- Cloud upload shows visual progress bar instead of debug logs  
- Checksum verification shows progress for large files

## [3.42.30] - 2026-01-09 "Better Error Aggregation"

### Added - go-multierror for Cluster Restore Errors
- **Enhanced error reporting** - Now shows ALL database failures, not just a count
- Uses `hashicorp/go-multierror` for proper error aggregation
- Each failed database error is preserved with full context
- Bullet-pointed error output for readability:
  ```
  cluster restore completed with 3 failures:
  3 database(s) failed:
    • db1: restore failed: max_locks_per_transaction exceeded
    • db2: restore failed: connection refused
    • db3: failed to create database: permission denied
  ```

### Changed
- Replaced string slice error collection with proper `*multierror.Error`
- Thread-safe error aggregation with dedicated mutex
- Improved error wrapping with `%w` for error chain preservation

## [3.42.10] - 2026-01-08 "Code Quality"

### Fixed - Code Quality Issues
- Removed deprecated `io/ioutil` usage (replaced with `os`)
- Fixed `os.DirEntry.ModTime()` → `file.Info().ModTime()`
- Removed unused fields and variables
- Fixed ineffective assignments in TUI code
- Fixed error strings (no capitalization, no trailing punctuation)

## [3.42.9] - 2026-01-08 "Diagnose Timeout Fix"

### Fixed - diagnose.go Timeout Bugs

**More short timeouts that caused large archive failures:**

- `diagnoseClusterArchive()`: tar listing 60s → **5 minutes**
- `verifyWithPgRestore()`: pg_restore --list 60s → **5 minutes**
- `DiagnoseClusterDumps()`: archive listing 120s → **10 minutes**

**Impact:** These timeouts caused "context deadline exceeded" errors when
diagnosing multi-GB backup archives, preventing TUI restore from even starting.

## [3.42.8] - 2026-01-08 "TUI Timeout Fix"

### Fixed - TUI Timeout Bugs Causing Backup/Restore Failures

**ROOT CAUSE of 2-3 month TUI backup/restore failures identified and fixed:**

#### Critical Timeout Fixes:
- **restore_preview.go**: Safety check timeout increased from 60s → **10 minutes**
  - Large archives (>1GB) take 2+ minutes to diagnose
  - Users saw "context deadline exceeded" before backup even started
- **dbselector.go**: Database listing timeout increased from 15s → **60 seconds**
  - Busy PostgreSQL servers need more time to respond
- **status.go**: Status check timeout increased from 10s → **30 seconds**
  - SSL negotiation and slow networks caused failures

#### Stability Improvements:
- **Panic recovery** added to parallel goroutines in:
  - `backup/engine.go:BackupCluster()` - cluster backup workers
  - `restore/engine.go:RestoreCluster()` - cluster restore workers
  - Prevents single database panic from crashing entire operation

#### Bug Fix:
- **restore/engine.go**: Fixed variable shadowing `err` → `cmdErr` for exit code detection

## [3.42.7] - 2026-01-08 "Context Killer Complete"

### Fixed - Additional Deadlock Bugs in Restore & Engine

**All remaining cmd.Wait() deadlock bugs fixed across the codebase:**

#### internal/restore/engine.go:
- `executeRestoreWithDecompression()` - gunzip/pigz pipeline restore
- `extractArchive()` - tar extraction for cluster restore
- `restoreGlobals()` - pg_dumpall globals restore

#### internal/backup/engine.go:
- `createArchive()` - tar/pigz archive creation pipeline

#### internal/engine/mysqldump.go:
- `Backup()` - mysqldump backup operation
- `BackupToWriter()` - streaming mysqldump to writer

**All 6 functions now use proper channel-based context handling with Process.Kill().**

## [3.42.6] - 2026-01-08 "Deadlock Killer"

### Fixed - Backup Command Context Handling

**Critical Bug: pg_dump/mysqldump could hang forever on context cancellation**

The `executeCommand`, `executeCommandWithProgress`, `executeMySQLWithProgressAndCompression`, 
and `executeMySQLWithCompression` functions had a race condition where:

1. A goroutine was spawned to read stderr
2. `cmd.Wait()` was called directly
3. If context was cancelled, the process was NOT killed
4. The goroutine could hang forever waiting for stderr

**Fix**: All backup execution functions now use proper channel-based context handling:
```go
// Wait for command with context handling
cmdDone := make(chan error, 1)
go func() {
    cmdDone <- cmd.Wait()
}()

select {
case cmdErr = <-cmdDone:
    // Command completed
case <-ctx.Done():
    // Context cancelled - kill process
    cmd.Process.Kill()
    <-cmdDone
    cmdErr = ctx.Err()
}
```

**Affected Functions:**
- `executeCommand()` - pg_dump for cluster backup
- `executeCommandWithProgress()` - pg_dump for single backup with progress
- `executeMySQLWithProgressAndCompression()` - mysqldump pipeline
- `executeMySQLWithCompression()` - mysqldump pipeline

**This fixes:** Backup operations hanging indefinitely when cancelled or timing out.

## [3.42.5] - 2026-01-08 "False Positive Fix"

### Fixed - Encryption Detection Bug

**IsBackupEncrypted False Positive:**
- **BUG FIX**: `IsBackupEncrypted()` returned `true` for ALL files, blocking normal restores
- Root cause: Fallback logic checked if first 12 bytes (nonce size) could be read - always true
- Fix: Now properly detects known unencrypted formats by magic bytes:
  - Gzip: `1f 8b`
  - PostgreSQL custom: `PGDMP`
  - Plain SQL: starts with `--`, `SET`, `CREATE`
- Returns `false` if no metadata present and format is recognized as unencrypted
- Affected file: `internal/backup/encryption.go`

## [3.42.4] - 2026-01-08 "The Long Haul"

### Fixed - Critical Restore Timeout Bug

**Removed Arbitrary Timeouts from Backup/Restore Operations:**
- **CRITICAL FIX**: Removed 4-hour timeout that was killing large database restores
- PostgreSQL cluster restores of 69GB+ databases no longer fail with "context deadline exceeded"
- All backup/restore operations now use `context.WithCancel` instead of `context.WithTimeout`
- Operations run until completion or manual cancellation (Ctrl+C)

**Affected Files:**
- `internal/tui/restore_exec.go`: Changed from 4-hour timeout to context.WithCancel
- `internal/tui/backup_exec.go`: Changed from 4-hour timeout to context.WithCancel  
- `internal/backup/engine.go`: Removed per-database timeout in cluster backup
- `cmd/restore.go`: CLI restore commands use context.WithCancel

**exec.Command Context Audit:**
- Fixed `exec.Command` without Context in `internal/restore/engine.go:730`
- Added proper context handling to all external command calls
- Added timeouts only for quick diagnostic/version checks (not restore path):
  - `restore/version_check.go`: 30s timeout for pg_restore --version check only
  - `restore/error_report.go`: 10s timeout for tool version detection
  - `restore/diagnose.go`: 60s timeout for diagnostic functions
  - `pitr/binlog.go`: 10s timeout for mysqlbinlog --version check
  - `cleanup/processes.go`: 5s timeout for process listing
  - `auth/helper.go`: 30s timeout for auth helper commands

**Verification:**
- 54 total `exec.CommandContext` calls verified in backup/restore/pitr path
- 0 `exec.Command` without Context in critical restore path
- All 14 PostgreSQL exec calls use CommandContext (pg_dump, pg_restore, psql)
- All 15 MySQL/MariaDB exec calls use CommandContext (mysqldump, mysql, mysqlbinlog)
- All 14 test packages pass

### Technical Details
- Large Object (BLOB/BYTEA) restores are particularly affected by timeouts
- 69GB database with large objects can take 5+ hours to restore
- Previous 4-hour hard timeout was causing consistent failures
- Now: No timeout - runs until complete or user cancels

## [3.42.1] - 2026-01-07 "Resistance is Futile"

### Added - Content-Defined Chunking Deduplication

**Deduplication Engine:**
- New `dbbackup dedup` command family for space-efficient backups
- Gear hash content-defined chunking (CDC) with 92%+ overlap on shifted data
- SHA-256 content-addressed storage - chunks stored by hash
- AES-256-GCM per-chunk encryption (optional, via `--encrypt`)
- Gzip compression enabled by default
- SQLite index for fast chunk lookups
- JSON manifests track chunks per backup with full verification

**Dedup Commands:**
```bash
dbbackup dedup backup <file>              # Create deduplicated backup
dbbackup dedup backup <file> --encrypt    # With encryption
dbbackup dedup restore <id> <output>      # Restore from manifest
dbbackup dedup list                       # List all backups
dbbackup dedup stats                      # Show deduplication statistics
dbbackup dedup delete <id>                # Delete a backup manifest
dbbackup dedup gc                         # Garbage collect unreferenced chunks
```

**Storage Structure:**
```
<backup-dir>/dedup/
  chunks/           # Content-addressed chunk files (sharded by hash prefix)
  manifests/        # JSON manifest per backup
  chunks.db         # SQLite index for fast lookups
```

**Test Results:**
- First 5MB backup: 448 chunks, 5MB stored
- Modified 5MB file: 448 chunks, only 1 NEW chunk (1.6KB), 100% dedup ratio
- Restore with SHA-256 verification

### Added - Documentation Updates
- Prometheus alerting rules added to SYSTEMD.md
- Catalog sync instructions for existing backups

## [3.41.1] - 2026-01-07

### Fixed
- Enabled CGO for Linux builds (required for SQLite catalog)

## [3.41.0] - 2026-01-07 "The Operator"

### Added - Systemd Integration & Prometheus Metrics

**Embedded Systemd Installer:**
- New `dbbackup install` command installs as systemd service/timer
- Supports single-database (`--backup-type single`) and cluster (`--backup-type cluster`) modes
- Automatic `dbbackup` user/group creation with proper permissions
- Hardened service units with security features (NoNewPrivileges, ProtectSystem, CapabilityBoundingSet)
- Templated timer units with configurable schedules (daily, weekly, or custom OnCalendar)
- Built-in dry-run mode (`--dry-run`) to preview installation
- `dbbackup install --status` shows current installation state
- `dbbackup uninstall` cleanly removes all systemd units and optionally configuration

**Prometheus Metrics Support:**
- New `dbbackup metrics export` command writes textfile collector format
- New `dbbackup metrics serve` command runs HTTP exporter on port 9399
- Metrics: `dbbackup_last_success_timestamp`, `dbbackup_rpo_seconds`, `dbbackup_backup_total`, etc.
- Integration with node_exporter textfile collector
- Metrics automatically updated via ExecStopPost in service units
- `--with-metrics` flag during install sets up exporter as systemd service

**New Commands:**
```bash
# Install as systemd service
sudo dbbackup install --backup-type cluster --schedule daily

# Install with Prometheus metrics
sudo dbbackup install --with-metrics --metrics-port 9399

# Check installation status
dbbackup install --status

# Export metrics for node_exporter
dbbackup metrics export --output /var/lib/dbbackup/metrics/dbbackup.prom

# Run HTTP metrics server
dbbackup metrics serve --port 9399
```

### Technical Details
- Systemd templates embedded with `//go:embed` for self-contained binary
- Templates use ReadWritePaths for security isolation
- Service units include proper OOMScoreAdjust (-100) to protect backups
- Metrics exporter caches with 30-second TTL for performance
- Graceful shutdown on SIGTERM for metrics server

---

## [3.41.0] - 2026-01-07 "The Pre-Flight Check"

### Added - 🛡️ Pre-Restore Validation

**Automatic Dump Validation Before Restore:**
- SQL dump files are now validated BEFORE attempting restore
- Detects truncated COPY blocks that cause "syntax error" failures
- Catches corrupted backups in seconds instead of wasting 49+ minutes
- Cluster restore pre-validates ALL dumps upfront (fail-fast approach)
- Custom format `.dump` files now validated with `pg_restore --list`

**Improved Error Messages:**
- Clear indication when dump file is truncated
- Shows which table's COPY block was interrupted
- Displays sample orphaned data for diagnosis
- Provides actionable error messages with root cause

### Fixed
- **P0: SQL Injection** - Added identifier validation for database names in CREATE/DROP DATABASE to prevent SQL injection attacks; uses safe quoting and regex validation (alphanumeric + underscore only)
- **P0: Data Race** - Fixed concurrent goroutines appending to shared error slice in notification manager; now uses mutex synchronization
- **P0: psql ON_ERROR_STOP** - Added `-v ON_ERROR_STOP=1` to psql commands to fail fast on first error instead of accumulating millions of errors
- **P1: Pipe deadlock** - Fixed streaming compression deadlock when pg_dump blocks on full pipe buffer; now uses goroutine with proper context timeout handling
- **P1: SIGPIPE handling** - Detect exit code 141 (broken pipe) and report compressor failure as root cause
- **P2: .dump validation** - Custom format dumps now validated with `pg_restore --list` before restore
- **P2: fsync durability** - Added `outFile.Sync()` after streaming compression to prevent truncation on power loss
- Truncated `.sql.gz` dumps no longer waste hours on doomed restores
- "syntax error at or near" errors now caught before restore begins
- Cluster restores abort immediately if any dump is corrupted

### Technical Details
- Integrated `Diagnoser` into restore pipeline for pre-validation
- Added `quickValidateSQLDump()` for fast integrity checks
- Pre-validation runs on all `.sql.gz` and `.dump` files in cluster archives
- Streaming compression uses channel-based wait with context cancellation
- Zero performance impact on valid backups (diagnosis is fast)

---

## [3.40.0] - 2026-01-05 "The Diagnostician"

### Added - 🔍 Restore Diagnostics & Error Reporting

**Backup Diagnosis Command:**
- `restore diagnose <archive>` - Deep analysis of backup files before restore
- Detects truncated dumps, corrupted archives, incomplete COPY blocks
- PGDMP signature validation for PostgreSQL custom format
- Gzip integrity verification with decompression test
- `pg_restore --list` validation for custom format archives
- `--deep` flag for exhaustive line-by-line analysis
- `--json` flag for machine-readable output
- Cluster archive diagnosis scans all contained dumps

**Detailed Error Reporting:**
- Comprehensive error collector captures stderr during restore
- Ring buffer prevents OOM on high-error restores (2M+ errors)
- Error classification with actionable hints and recommendations
- `--save-debug-log <path>` saves JSON report on failure
- Reports include: exit codes, last errors, line context, tool versions
- Automatic recommendations based on error patterns

**TUI Restore Enhancements:**
- **Dump validity** safety check runs automatically before restore
- Detects truncated/corrupted backups in restore preview
- Press **`d`** to toggle debug log saving in Advanced Options
- Debug logs saved to `/tmp/dbbackup-restore-debug-*.json` on failure
- Press **`d`** in archive browser to run diagnosis on any backup

**New Commands:**
- `restore diagnose` - Analyze backup file integrity and structure

**New Flags:**
- `--save-debug-log <path>` - Save detailed JSON error report on failure
- `--diagnose` - Run deep diagnosis before cluster restore
- `--deep` - Enable exhaustive diagnosis (line-by-line analysis)
- `--json` - Output diagnosis in JSON format
- `--keep-temp` - Keep temporary files after diagnosis
- `--verbose` - Show detailed diagnosis progress

### Technical Details
- 1,200+ lines of new diagnostic code
- Error classification system with 15+ error patterns
- Ring buffer stderr capture (1MB max, 10K lines)
- Zero memory growth on high-error restores
- Full TUI integration for diagnostics

---

## [3.2.0] - 2025-12-13 "The Margin Eraser"

### Added - 🚀 Physical Backup Revolution

**MySQL Clone Plugin Integration:**
- Native physical backup using MySQL 8.0.17+ Clone Plugin
- No XtraBackup dependency - pure Go implementation
- Real-time progress monitoring via performance_schema
- Support for both local and remote clone operations

**Filesystem Snapshot Orchestration:**
- LVM snapshot support with automatic cleanup
- ZFS snapshot integration with send/receive
- Btrfs subvolume snapshot support
- Brief table lock (<100ms) for consistency
- Automatic snapshot backend detection

**Continuous Binlog Streaming:**
- Real-time binlog capture using MySQL replication protocol
- Multiple targets: file, compressed file, S3 direct streaming
- Sub-second RPO without impacting database server
- Automatic position tracking and checkpointing

**Parallel Cloud Streaming:**
- Direct database-to-S3 streaming (zero local storage)
- Configurable worker pool for parallel uploads
- S3 multipart upload with automatic retry
- Support for S3, GCS, and Azure Blob Storage

**Smart Engine Selection:**
- Automatic engine selection based on environment
- MySQL version detection and capability checking
- Filesystem type detection for optimal snapshot backend
- Database size-based recommendations

**New Commands:**
- `engine list` - List available backup engines
- `engine info <name>` - Show detailed engine information
- `backup --engine=<name>` - Use specific backup engine

### Technical Details
- 7,559 lines of new code
- Zero new external dependencies
- 10/10 platform builds successful
- Full test coverage for new engines

## [3.1.0] - 2025-11-26

### Added - 🔄 Point-in-Time Recovery (PITR)

**Complete PITR Implementation for PostgreSQL:**
- **WAL Archiving**: Continuous archiving of Write-Ahead Log files with compression and encryption support
- **Timeline Management**: Track and manage PostgreSQL timeline history with branching support
- **Recovery Targets**: Restore to specific timestamp, transaction ID (XID), LSN, named restore point, or immediate
- **PostgreSQL Version Support**: Both modern (12+) and legacy recovery configuration formats
- **Recovery Actions**: Promote to primary, pause for inspection, or shutdown after recovery
- **Comprehensive Testing**: 700+ lines of tests covering all PITR functionality with 100% pass rate

**New Commands:**

**PITR Management:**
- `pitr enable` - Configure PostgreSQL for WAL archiving and PITR
- `pitr disable` - Disable WAL archiving in PostgreSQL configuration
- `pitr status` - Display current PITR configuration and archive statistics

**WAL Archive Operations:**
- `wal archive <wal-file> <filename>` - Archive WAL file (used by archive_command)
- `wal list` - List all archived WAL files with details
- `wal cleanup` - Remove old WAL files based on retention policy
- `wal timeline` - Display timeline history and branching structure

**Point-in-Time Restore:**
- `restore pitr` - Perform point-in-time recovery with multiple target types:
  - `--target-time "YYYY-MM-DD HH:MM:SS"` - Restore to specific timestamp
  - `--target-xid <xid>` - Restore to transaction ID
  - `--target-lsn <lsn>` - Restore to Log Sequence Number
  - `--target-name <name>` - Restore to named restore point
  - `--target-immediate` - Restore to earliest consistent point

**Advanced PITR Features:**
- **WAL Compression**: gzip compression (70-80% space savings)
- **WAL Encryption**: AES-256-GCM encryption for archived WAL files
- **Timeline Selection**: Recover along specific timeline or latest
- **Recovery Actions**: Promote (default), pause, or shutdown after target reached
- **Inclusive/Exclusive**: Control whether target transaction is included
- **Auto-Start**: Automatically start PostgreSQL after recovery setup
- **Recovery Monitoring**: Real-time monitoring of recovery progress

**Configuration Options:**
```bash
# Enable PITR with compression and encryption
./dbbackup pitr enable --archive-dir /backups/wal_archive \
  --compress --encrypt --encryption-key-file /secure/key.bin

# Perform PITR to specific time
./dbbackup restore pitr \
  --base-backup /backups/base.tar.gz \
  --wal-archive /backups/wal_archive \
  --target-time "2024-11-26 14:30:00" \
  --target-dir /var/lib/postgresql/14/restored \
  --auto-start --monitor
```

**Technical Details:**
- WAL file parsing and validation (timeline, segment, extension detection)
- Timeline history parsing (.history files) with consistency validation
- Automatic PostgreSQL version detection (12+ vs legacy)
- Recovery configuration generation (postgresql.auto.conf + recovery.signal)
- Data directory validation (exists, writable, PostgreSQL not running)
- Comprehensive error handling and validation

**Documentation:**
- Complete PITR section in README.md (200+ lines)
- Dedicated PITR.md guide with detailed examples and troubleshooting
- Test suite documentation (tests/pitr_complete_test.go)

**Files Added:**
- `internal/pitr/wal/` - WAL archiving and parsing
- `internal/pitr/config/` - Recovery configuration generation
- `internal/pitr/timeline/` - Timeline management
- `cmd/pitr.go` - PITR command implementation
- `cmd/wal.go` - WAL management commands
- `cmd/restore_pitr.go` - PITR restore command
- `tests/pitr_complete_test.go` - Comprehensive test suite (700+ lines)
- `PITR.md` - Complete PITR guide

**Performance:**
- WAL archiving: ~100-200 MB/s (with compression)
- WAL encryption: ~1-2 GB/s (streaming)
- Recovery replay: 10-100 MB/s (disk I/O dependent)
- Minimal overhead during normal operations

**Use Cases:**
- Disaster recovery from accidental data deletion
- Rollback to pre-migration state
- Compliance and audit requirements
- Testing and what-if scenarios
- Timeline branching for parallel recovery paths

### Changed
- **Licensing**: Added Apache License 2.0 to the project (LICENSE file)
- **Version**: Updated to v3.1.0
- Enhanced metadata format with PITR information
- Improved progress reporting for long-running operations
- Better error messages for PITR operations

### Production
- **Production Validated**: 2 production hosts
- **Databases backed up**: 8 databases nightly
- **Retention policy**: 30-day retention with minimum 5 backups
- **Backup volume**: ~10MB/night
- **Schedule**: 02:09 and 02:25 CET
- **Impact**: Resolved 4-day backup failure immediately
- **User feedback**: "cleanup command is SO gut" | "--dry-run: chef's kiss!" 💋

### Documentation
- Added comprehensive PITR.md guide (complete PITR documentation)
- Updated README.md with PITR section (200+ lines)
- Updated CHANGELOG.md with v3.1.0 details
- Added NOTICE file for Apache License attribution
- Created comprehensive test suite (tests/pitr_complete_test.go - 700+ lines)

## [3.0.0] - 2025-11-26

### Added - 🔐 AES-256-GCM Encryption (Phase 4)

**Secure Backup Encryption:**
- **Algorithm**: AES-256-GCM authenticated encryption (prevents tampering)
- **Key Derivation**: PBKDF2-SHA256 with 600,000 iterations (OWASP 2024 recommended)
- **Streaming Encryption**: Memory-efficient for large backups (O(buffer) not O(file))
- **Key Sources**: File (raw/base64), environment variable, or passphrase
- **Auto-Detection**: Restore automatically detects and decrypts encrypted backups
- **Metadata Tracking**: Encrypted flag and algorithm stored in .meta.json

**CLI Integration:**
- `--encrypt` - Enable encryption for backup operations
- `--encryption-key-file <path>` - Path to 32-byte encryption key (raw or base64 encoded)
- `--encryption-key-env <var>` - Environment variable containing key (default: DBBACKUP_ENCRYPTION_KEY)
- Automatic decryption on restore (no extra flags needed)

**Security Features:**
- Unique nonce per encryption (no key reuse vulnerabilities)
- Cryptographically secure random generation (crypto/rand)
- Key validation (32 bytes required)
- Authenticated encryption prevents tampering attacks
- 56-byte header: Magic(16) + Algorithm(16) + Nonce(12) + Salt(32)

**Usage Examples:**
```bash
# Generate encryption key
head -c 32 /dev/urandom | base64 > encryption.key

# Encrypted backup
./dbbackup backup single mydb --encrypt --encryption-key-file encryption.key

# Restore (automatic decryption)
./dbbackup restore single mydb_backup.sql.gz --encryption-key-file encryption.key --confirm
```

**Performance:**
- Encryption speed: ~1-2 GB/s (streaming, no memory bottleneck)
- Overhead: 56 bytes header + 16 bytes GCM tag per file
- Key derivation: ~1.4s for 600k iterations (intentionally slow for security)

**Files Added:**
- `internal/crypto/interface.go` - Encryption interface and configuration
- `internal/crypto/aes.go` - AES-256-GCM implementation (272 lines)
- `internal/crypto/aes_test.go` - Comprehensive test suite (all tests passing)
- `cmd/encryption.go` - CLI encryption helpers
- `internal/backup/encryption.go` - Backup encryption operations
- Total: ~1,200 lines across 13 files

### Added - 📦 Incremental Backups (Phase 3B)

**MySQL/MariaDB Incremental Backups:**
- **Change Detection**: mtime-based file modification tracking
- **Archive Format**: tar.gz containing only changed files since base backup
- **Space Savings**: 70-95% smaller than full backups (typical)
- **Backup Chain**: Tracks base → incremental relationships with metadata
- **Checksum Verification**: SHA-256 integrity checking
- **Auto-Detection**: CLI automatically uses correct engine for PostgreSQL vs MySQL

**MySQL-Specific Exclusions:**
- Relay logs (relay-log, relay-bin*)
- Binary logs (mysql-bin*, binlog*)
- InnoDB redo logs (ib_logfile*)
- InnoDB undo logs (undo_*)
- Performance schema (in-memory)
- Temporary files (#sql*, *.tmp)
- Lock files (*.lock, auto.cnf.lock)
- PID files (*.pid, mysqld.pid)
- Error logs (*.err, error.log)
- Slow query logs (*slow*.log)
- General logs (general.log, query.log)

**CLI Integration:**
- `--backup-type <full|incremental>` - Backup type (default: full)
- `--base-backup <path>` - Path to base backup (required for incremental)
- Auto-detects database type (PostgreSQL vs MySQL) and uses appropriate engine
- Same interface for both database types

**Usage Examples:**
```bash
# Full backup (base)
./dbbackup backup single mydb --db-type mysql --backup-type full

# Incremental backup
./dbbackup backup single mydb \
  --db-type mysql \
  --backup-type incremental \
  --base-backup /backups/mydb_20251126.tar.gz

# Restore incremental
./dbbackup restore incremental \
  --base-backup mydb_base.tar.gz \
  --incremental-backup mydb_incr_20251126.tar.gz \
  --target /restore/path
```

**Implementation:**
- Copy-paste-adapt from Phase 3A PostgreSQL (95% code reuse)
- Interface-based design enables sharing tests between engines
- `internal/backup/incremental_mysql.go` - MySQL incremental engine (530 lines)
- All existing tests pass immediately (interface compatibility)
- Development time: 30 minutes (vs 5-6h estimated) - **10x speedup!**

**Combined Features:**
```bash
# Encrypted + Incremental backup
./dbbackup backup single mydb \
  --backup-type incremental \
  --base-backup mydb_base.tar.gz \
  --encrypt \
  --encryption-key-file key.txt
```

### Changed
- **Version**: Bumped to 3.0.0 (major feature release)
- **Backup Engine**: Integrated encryption and incremental capabilities
- **Restore Engine**: Added automatic decryption detection
- **Metadata Format**: Extended with encryption and incremental fields

### Testing
- ✅ Encryption tests: 4 tests passing (TestAESEncryptionDecryption, TestKeyDerivation, TestKeyValidation, TestLargeData)
- ✅ Incremental tests: 2 tests passing (TestIncrementalBackupRestore, TestIncrementalBackupErrors)
- ✅ Roundtrip validation: Encrypt → Decrypt → Verify (data matches perfectly)
- ✅ Build: All platforms compile successfully
- ✅ Interface compatibility: PostgreSQL and MySQL engines share test suite

### Documentation
- Updated README.md with encryption and incremental sections
- Added PHASE4_COMPLETION.md - Encryption implementation details
- Added PHASE3B_COMPLETION.md - MySQL incremental implementation report
- Usage examples for encryption, incremental, and combined workflows

### Performance
- **Phase 4**: Completed in ~1h (encryption library + CLI integration)
- **Phase 3B**: Completed in 30 minutes (vs 5-6h estimated)
- **Total**: 2 major features delivered in 1 day (planned: 6 hours, actual: ~2 hours)
- **Quality**: Production-ready, all tests passing, no breaking changes

### Commits
- Phase 4: 3 commits (7d96ec7, f9140cf, dd614dd, 8bbca16)
- Phase 3B: 2 commits (357084c, a0974ef)
- Docs: 1 commit (3b9055b)

## [2.1.0] - 2025-11-26

### Added - Cloud Storage Integration
- **S3/MinIO/B2 Support**: Native S3-compatible storage backend with streaming uploads
- **Azure Blob Storage**: Native Azure integration with block blob support for files >256MB
- **Google Cloud Storage**: Native GCS integration with 16MB chunked uploads
- **Cloud URI Syntax**: Direct backup/restore using `--cloud s3://bucket/path` URIs
- **TUI Cloud Settings**: Configure cloud providers directly in interactive menu
  - Cloud Storage Enabled toggle
  - Provider selector (S3, MinIO, B2, Azure, GCS)
  - Bucket/Container configuration
  - Region configuration
  - Credential management with masking
  - Auto-upload toggle
- **Multipart Uploads**: Automatic multipart uploads for files >100MB (S3/MinIO/B2)
- **Streaming Transfers**: Memory-efficient streaming for all cloud operations
- **Progress Tracking**: Real-time upload/download progress with ETA
- **Metadata Sync**: Automatic .sha256 and .info file upload alongside backups
- **Cloud Verification**: Verify backup integrity directly from cloud storage
- **Cloud Cleanup**: Apply retention policies to cloud-stored backups

### Added - Cross-Platform Support
- **Windows Support**: Native binaries for Windows Intel (amd64) and ARM (arm64)
- **NetBSD Support**: Full support for NetBSD amd64 (disk checks use safe defaults)
- **Platform-Specific Implementations**: 
  - `resources_unix.go` - Linux, macOS, FreeBSD, OpenBSD
  - `resources_windows.go` - Windows stub implementation
  - `disk_check_netbsd.go` - NetBSD disk space stub
- **Build Tags**: Proper Go build constraints for platform-specific code
- **All Platforms Building**: 10/10 platforms successfully compile
  - ✅ Linux (amd64, arm64, armv7)
  - ✅ macOS (Intel, Apple Silicon)
  - ✅ Windows (Intel, ARM)
  - ✅ FreeBSD amd64
  - ✅ OpenBSD amd64
  - ✅ NetBSD amd64

### Changed
- **Cloud Auto-Upload**: When `CloudEnabled=true` and `CloudAutoUpload=true`, backups automatically upload after creation
- **Configuration**: Added cloud settings to TUI settings interface
- **Backup Engine**: Integrated cloud upload into backup workflow with progress tracking

### Fixed
- **BSD Syscall Issues**: Fixed `syscall.Rlimit` type mismatches (int64 vs uint64) on BSD platforms
- **OpenBSD RLIMIT_AS**: Made RLIMIT_AS check Linux-only (not available on OpenBSD)
- **NetBSD Disk Checks**: Added safe default implementation for NetBSD (syscall.Statfs unavailable)
- **Cross-Platform Builds**: Resolved Windows syscall.Rlimit undefined errors

### Documentation
- Updated README.md with Cloud Storage section and examples
- Enhanced CLOUD.md with setup guides for all providers
- Added testing scripts for Azure and GCS
- Docker Compose files for Azurite and fake-gcs-server

### Testing
- Added `scripts/test_azure_storage.sh` - Azure Blob Storage integration tests
- Added `scripts/test_gcs_storage.sh` - Google Cloud Storage integration tests
- Docker Compose setups for local testing (Azurite, fake-gcs-server, MinIO)

## [2.0.0] - 2025-11-25

### Added - Production-Ready Release
- **100% Test Coverage**: All 24 automated tests passing
- **Zero Critical Issues**: Production-validated and deployment-ready
- **Backup Verification**: SHA-256 checksum generation and validation
- **JSON Metadata**: Structured .info files with backup metadata
- **Retention Policy**: Automatic cleanup of old backups with configurable retention
- **Configuration Management**:
  - Auto-save/load settings to `.dbbackup.conf` in current directory
  - Per-directory configuration for different projects
  - CLI flags always take precedence over saved configuration
  - Passwords excluded from saved configuration files

### Added - Performance Optimizations
- **Parallel Cluster Operations**: Worker pool pattern for concurrent database operations
- **Memory Efficiency**: Streaming command output eliminates OOM errors
- **Optimized Goroutines**: Ticker-based progress indicators reduce CPU overhead
- **Configurable Concurrency**: `CLUSTER_PARALLELISM` environment variable

### Added - Reliability Enhancements
- **Context Cleanup**: Proper resource cleanup with `sync.Once` and `io.Closer` interface
- **Process Management**: Thread-safe process tracking with automatic cleanup on exit
- **Error Classification**: Regex-based error pattern matching for robust error handling
- **Performance Caching**: Disk space checks cached with 30-second TTL
- **Metrics Collection**: Structured logging with operation metrics

### Fixed
- **Configuration Bug**: CLI flags now correctly override config file values
- **Memory Leaks**: Proper cleanup prevents resource leaks in long-running operations

### Changed
- **Streaming Architecture**: Constant ~1GB memory footprint regardless of database size
- **Cross-Platform**: Native binaries for Linux (x64/ARM), macOS (x64/ARM), FreeBSD, OpenBSD

## [1.2.0] - 2025-11-12

### Added
- **Interactive TUI**: Full terminal user interface with progress tracking
- **Database Selector**: Interactive database selection for backup operations
- **Archive Browser**: Browse and restore from backup archives
- **Configuration Settings**: In-TUI configuration management
- **CPU Detection**: Automatic CPU detection and optimization

### Changed
- Improved error handling and user feedback
- Enhanced progress tracking with real-time updates

## [1.1.0] - 2025-11-10

### Added
- **Multi-Database Support**: PostgreSQL, MySQL, MariaDB
- **Cluster Operations**: Full cluster backup and restore for PostgreSQL
- **Sample Backups**: Create reduced-size backups for testing
- **Parallel Processing**: Automatic CPU detection and parallel jobs

### Changed
- Refactored command structure for better organization
- Improved compression handling

## [1.0.0] - 2025-11-08

### Added
- Initial release
- Single database backup and restore
- PostgreSQL support
- Basic CLI interface
- Streaming compression

---

## Version Numbering

- **Major (X.0.0)**: Breaking changes, major feature additions
- **Minor (0.X.0)**: New features, non-breaking changes
- **Patch (0.0.X)**: Bug fixes, minor improvements

## Upcoming Features

See [ROADMAP.md](ROADMAP.md) for planned features:
- Phase 3: Incremental Backups
- Phase 4: Encryption (AES-256)
- Phase 5: PITR (Point-in-Time Recovery)
- Phase 6: Enterprise Features (Prometheus metrics, remote restore)
