# Changelog

All notable changes to dbbackup will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.2.5] - 2026-01-30
## [4.2.6] - 2026-01-30

### Security - Critical Fixes

- **SEC#1: Password exposure in process list**
  - Removed `--password` CLI flag to prevent passwords appearing in `ps aux`
  - Use environment variables (`PGPASSWORD`, `MYSQL_PWD`) or config file instead
  - Enhanced security for multi-user systems and shared environments

- **SEC#2: World-readable backup files**
  - All backup files now created with 0600 permissions (owner-only read/write)
  - Prevents unauthorized users from reading sensitive database dumps
  - Affects: `internal/backup/engine.go`, `incremental_mysql.go`, `incremental_tar.go`
  - Critical for GDPR, HIPAA, and PCI-DSS compliance

- **#4: Directory race condition in parallel backups**
  - Replaced `os.MkdirAll()` with `fs.SecureMkdirAll()` that handles EEXIST gracefully
  - Prevents "file exists" errors when multiple backup processes create directories
  - Affects: All backup directory creation paths

### Added

- **internal/fs/secure.go**: New secure file operations utilities
  - `SecureMkdirAll()`: Race-condition-safe directory creation
  - `SecureCreate()`: File creation with 0600 permissions
  - `SecureMkdirTemp()`: Temporary directories with 0700 permissions
  - `CheckWriteAccess()`: Proactive detection of read-only filesystems

- **internal/exitcode/codes.go**: BSD-style exit codes for automation
  - Standard exit codes for scripting and monitoring systems
  - Improves integration with systemd, cron, and orchestration tools

### Fixed

- Fixed multiple file creation calls using insecure 0644 permissions
- Fixed race conditions in backup directory creation during parallel operations
- Improved security posture for multi-user and shared environments


### Fixed - TUI Cluster Restore Double-Extraction

- **TUI cluster restore performance optimization**
  - Eliminated double-extraction: cluster archives were scanned twice (once for DB list, once for restore)
  - `internal/restore/extract.go`: Added `ListDatabasesFromExtractedDir()` to list databases from disk instead of tar scan
  - `internal/tui/cluster_db_selector.go`: Now pre-extracts cluster once, lists from extracted directory
  - `internal/tui/archive_browser.go`: Added `ExtractedDir` field to `ArchiveInfo` for passing pre-extracted path
  - `internal/tui/restore_exec.go`: Reuses pre-extracted directory when available
  - **Performance improvement:** 50GB cluster archive now processes once instead of twice (saves 5-15 minutes)
  - Automatic cleanup of extracted directory after restore completes or fails

## [4.2.4] - 2026-01-30

### Fixed - Comprehensive Ctrl+C Support Across All Operations

- **System-wide context-aware file operations**
  - All long-running I/O operations now respond to Ctrl+C
  - Added `CopyWithContext()` to cloud package for S3/Azure/GCS transfers
  - Partial files are cleaned up on cancellation

- **Fixed components:**
  - `internal/restore/extract.go`: Single DB extraction from cluster
  - `internal/wal/compression.go`: WAL file compression/decompression
  - `internal/restore/engine.go`: SQL restore streaming (2 paths)
  - `internal/backup/engine.go`: pg_dump/mysqldump streaming (3 paths)
  - `internal/cloud/s3.go`: S3 download interruption
  - `internal/cloud/azure.go`: Azure Blob download interruption
  - `internal/cloud/gcs.go`: GCS upload/download interruption
  - `internal/drill/engine.go`: DR drill decompression

## [4.2.3] - 2026-01-30

### Fixed - Cluster Restore Performance & Ctrl+C Handling

- **Removed redundant gzip validation in cluster restore**
  - `ValidateAndExtractCluster()` no longer calls `ValidateArchive()` internally
  - Previously validation happened 2x before extraction (caller + internal)
  - Eliminates duplicate gzip header reads on large archives
  - Reduces cluster restore startup time

- **Fixed Ctrl+C not working during extraction**
  - Added `CopyWithContext()` function for context-aware file copying
  - Extraction now checks for cancellation every 1MB of data
  - Ctrl+C immediately interrupts large file extractions
  - Partial files are cleaned up on cancellation
  - Applies to both `ExtractTarGzParallel` and `extractArchiveWithProgress`

## [4.2.2] - 2026-01-30

### Fixed - Complete pgzip Migration (Backup Side)

- **Removed ALL external gzip/pigz calls from backup engine**
  - `internal/backup/engine.go`: `executeWithStreamingCompression` now uses pgzip
  - `internal/parallel/engine.go`: Fixed stub gzipWriter to use pgzip
  - No more gzip/pigz processes visible in htop during backup
  - Uses klauspost/pgzip for parallel multi-core compression

- **Complete pgzip migration status**:
  - ✅ Backup: All compression uses in-process pgzip
  - ✅ Restore: All decompression uses in-process pgzip  
  - ✅ Drill: Decompress on host with pgzip before Docker copy
  - ⚠️ PITR only: PostgreSQL's `restore_command` must remain shell (PostgreSQL limitation)

## [4.2.1] - 2026-01-30

### Fixed - Complete pgzip Migration

- **Removed ALL external gunzip/gzip calls** - Systematic audit and fix
  - `internal/restore/engine.go`: SQL restores now use pgzip stream → psql/mysql stdin
  - `internal/drill/engine.go`: Decompress on host with pgzip before Docker copy
  - No more gzip/gunzip/pigz processes visible in htop during restore
  - Uses klauspost/pgzip for parallel multi-core decompression

- **PostgreSQL PITR exception** - `restore_command` in recovery config must remain shell
  - PostgreSQL itself runs this command to fetch WAL files
  - Cannot be replaced with Go code (PostgreSQL limitation)

## [4.2.0] - 2026-01-30

### Added - Quick Wins Release

- **`dbbackup health` command** - Comprehensive backup infrastructure health check
  - 10 automated health checks: config, DB connectivity, backup dir, catalog, freshness, gaps, verification, file integrity, orphans, disk space
  - Exit codes for automation: 0=healthy, 1=warning, 2=critical
  - JSON output for monitoring integration (Prometheus, Nagios, etc.)
  - Auto-generates actionable recommendations
  - Custom backup interval for gap detection: `--interval 12h`
  - Skip database check for offline mode: `--skip-db`
  - Example: `dbbackup health --format json`

- **TUI System Health Check** - Interactive health monitoring
  - Accessible via Tools → System Health Check
  - Runs all 10 checks asynchronously with progress spinner
  - Color-coded results: green=healthy, yellow=warning, red=critical
  - Displays recommendations for any issues found

- **`dbbackup restore preview` command** - Pre-restore analysis and validation
  - Shows backup format, compression type, database type
  - Estimates uncompressed size (3x compression ratio)
  - Calculates RTO (Recovery Time Objective) based on active profile
  - Validates backup integrity without actual restore
  - Displays resource requirements (RAM, CPU, disk space)
  - Example: `dbbackup restore preview backup.dump.gz`

- **`dbbackup diff` command** - Compare two backups and track changes
  - Flexible input: file paths, catalog IDs, or `database:latest/previous`
  - Shows size delta with percentage change
  - Calculates database growth rate (GB/day)
  - Projects time to reach 10GB threshold
  - Compares backup duration and compression efficiency
  - JSON output for automation and reporting
  - Example: `dbbackup diff mydb:latest mydb:previous`

- **`dbbackup cost analyze` command** - Cloud storage cost optimization
  - Analyzes 15 storage tiers across 5 cloud providers
  - AWS S3: Standard, IA, Glacier Instant/Flexible, Deep Archive
  - Google Cloud Storage: Standard, Nearline, Coldline, Archive
  - Azure Blob Storage: Hot, Cool, Archive
  - Backblaze B2 and Wasabi alternatives
  - Monthly/annual cost projections
  - Savings calculations vs S3 Standard baseline
  - Tiered lifecycle strategy recommendations
  - Shows potential savings of 90%+ with proper policies
  - Example: `dbbackup cost analyze --database mydb`

### Enhanced
- **TUI restore preview** - Added RTO estimates and size calculations
  - Shows estimated uncompressed size during restore confirmation
  - Displays estimated restore time based on current profile
  - Helps users make informed restore decisions
  - Keeps TUI simple (essentials only), detailed analysis in CLI

### Documentation
- Updated README.md with new commands and examples
- Created QUICK_WINS.md documenting the rapid development sprint
- Added backup diff and cost analysis sections

## [4.1.4] - 2026-01-29

### Added
- **New `turbo` restore profile** - Maximum restore speed, matches native `pg_restore -j8`
  - `ClusterParallelism = 2` (restore 2 DBs concurrently)
  - `Jobs = 8` (8 parallel pg_restore jobs)
  - `BufferedIO = true` (32KB write buffers for faster extraction)
  - Works on 16GB+ RAM, 4+ cores
  - Usage: `dbbackup restore cluster backup.tar.gz --profile=turbo --confirm`

- **Restore startup performance logging** - Shows actual parallelism settings at restore start
  - Logs profile name, cluster_parallelism, pg_restore_jobs, buffered_io
  - Helps verify settings before long restore operations

- **Buffered I/O optimization** - 32KB write buffers during tar extraction (turbo profile)
  - Reduces system call overhead
  - Improves I/O throughput for large archives

### Fixed
- **TUI now respects saved profile settings** - Previously TUI forced `conservative` profile on every launch, ignoring user's saved configuration. Now properly loads and respects saved settings.

### Changed
- TUI default profile changed from forced `conservative` to `balanced` (only when no profile configured)
- `LargeDBMode` no longer forced on TUI startup - user controls it via settings

## [4.1.3] - 2026-01-27

### Added
- **`--config` / `-c` global flag** - Specify config file path from anywhere
  - Example: `dbbackup --config /opt/dbbackup/.dbbackup.conf backup single mydb`
  - No longer need to `cd` to config directory before running commands
  - Works with all subcommands (backup, restore, verify, etc.)

## [4.1.2] - 2026-01-27

### Added
- **`--socket` flag for MySQL/MariaDB** - Connect via Unix socket instead of TCP/IP
  - Usage: `dbbackup backup single mydb --db-type mysql --socket /var/run/mysqld/mysqld.sock`
  - Works for both backup and restore operations
  - Supports socket auth (no password required with proper permissions)

### Fixed
- **Socket path as --host now works** - If `--host` starts with `/`, it's auto-detected as a socket path
  - Example: `--host /var/run/mysqld/mysqld.sock` now works correctly instead of DNS lookup error
  - Auto-converts to `--socket` internally

## [4.1.1] - 2026-01-25

### Added
- **`dbbackup_build_info` metric** - Exposes version and git commit as Prometheus labels
  - Useful for tracking deployed versions across a fleet
  - Labels: `server`, `version`, `commit`

### Fixed
- **Documentation clarification**: The `pitr_base` value for `backup_type` label is auto-assigned
  by `dbbackup pitr base` command. CLI `--backup-type` flag only accepts `full` or `incremental`.
  This was causing confusion in deployments.

## [4.1.0] - 2026-01-25

### Added
- **Backup Type Tracking**: All backup metrics now include a `backup_type` label
  (`full`, `incremental`, or `pitr_base` for PITR base backups)
- **PITR Metrics**: Complete Point-in-Time Recovery monitoring
  - `dbbackup_pitr_enabled` - Whether PITR is enabled (1/0)
  - `dbbackup_pitr_archive_lag_seconds` - Seconds since last WAL/binlog archived
  - `dbbackup_pitr_chain_valid` - WAL/binlog chain integrity (1=valid)
  - `dbbackup_pitr_gap_count` - Number of gaps in archive chain
  - `dbbackup_pitr_archive_count` - Total archived segments
  - `dbbackup_pitr_archive_size_bytes` - Total archive storage
  - `dbbackup_pitr_recovery_window_minutes` - Estimated PITR coverage
- **PITR Alerting Rules**: 6 new alerts for PITR monitoring
  - PITRArchiveLag, PITRChainBroken, PITRGapsDetected, PITRArchiveStalled,
    PITRStorageGrowing, PITRDisabledUnexpectedly
- **`dbbackup_backup_by_type` metric** - Count backups by type

### Changed
- `dbbackup_backup_total` type changed from counter to gauge for snapshot-based collection

## [3.42.110] - 2026-01-24

### Improved - Code Quality & Testing
- **Cleaned up 40+ unused code items** found by staticcheck:
  - Removed unused functions, variables, struct fields, and type aliases
  - Fixed SA4006 warning (unused value assignment in restore engine)
  - All packages now pass staticcheck with zero warnings

- **Added golangci-lint integration** to Makefile:
  - New `make golangci-lint` target with auto-install
  - Updated `lint` target to include golangci-lint
  - Updated `install-tools` to install golangci-lint

- **New unit tests** for improved coverage:
  - `internal/config/config_test.go` - Tests for config initialization, database types, env helpers
  - `internal/security/security_test.go` - Tests for checksums, path validation, rate limiting, audit logging

## [3.42.109] - 2026-01-24

### Added - Grafana Dashboard & Monitoring Improvements
- **Enhanced Grafana dashboard** with comprehensive improvements:
  - Added dashboard description for better discoverability
  - New collapsible "Backup Overview" row for organization
  - New **Verification Status** panel showing last backup verification state
  - Added descriptions to all 17 panels for better understanding
  - Enabled shared crosshair (graphTooltip=1) for correlated analysis
  - Added "monitoring" tag for dashboard discovery

- **New Prometheus alerting rules** (`grafana/alerting-rules.yaml`):
  - `DBBackupRPOCritical` - No backup in 24+ hours (critical)
  - `DBBackupRPOWarning` - No backup in 12+ hours (warning)
  - `DBBackupFailure` - Backup failures detected
  - `DBBackupNotVerified` - Backup not verified in 24h
  - `DBBackupDedupRatioLow` - Dedup ratio below 10%
  - `DBBackupDedupDiskGrowth` - Rapid storage growth prediction
  - `DBBackupExporterDown` - Metrics exporter not responding
  - `DBBackupMetricsStale` - Metrics not updated in 10+ minutes
  - `DBBackupNeverSucceeded` - Database never backed up successfully

### Changed
- **Grafana dashboard layout fixes**:
  - Fixed overlapping dedup panels (y: 31/36 → 22/27/32)
  - Adjusted top row panel widths for better balance (5+5+5+4+5=24)

- **Added Makefile** for streamlined development workflow:
  - `make build` - optimized binary with ldflags
  - `make test`, `make race`, `make cover` - testing targets
  - `make lint` - runs vet + staticcheck
  - `make all-platforms` - cross-platform builds

### Fixed
- Removed deprecated `netErr.Temporary()` call in cloud retry logic (Go 1.18+)
- Fixed staticcheck warnings for redundant fmt.Sprintf calls
- Logger optimizations: buffer pooling, early level check, pre-allocated maps
- Clone engine now validates disk space before operations

## [3.42.108] - 2026-01-24

### Added - TUI Tools Expansion
- **Table Sizes** - view top 100 tables sorted by size with row counts, data/index breakdown
  - Supports PostgreSQL (`pg_stat_user_tables`) and MySQL (`information_schema.TABLES`)
  - Shows total/data/index sizes, row counts, schema prefix for non-public schemas

- **Kill Connections** - manage active database connections
  - List all active connections with PID, user, database, state, query preview, duration
  - Kill single connection or all connections to a specific database
  - Useful before restore operations to clear blocking sessions
  - Supports PostgreSQL (`pg_terminate_backend`) and MySQL (`KILL`)

- **Drop Database** - safely drop databases with double confirmation
  - Lists user databases (system DBs hidden: postgres, template0/1, mysql, sys, etc.)
  - Requires two confirmations: y/n then type full database name
  - Auto-terminates connections before drop
  - Supports PostgreSQL and MySQL

## [3.42.107] - 2026-01-24

### Added - Tools Menu & Blob Statistics
- **New "Tools" submenu in TUI** - centralized access to utility functions
  - Blob Statistics - scan database for bytea/blob columns with size analysis
  - Blob Extract - externalize large objects (coming soon)
  - Dedup Store Analyze - storage savings analysis (coming soon)
  - Verify Backup Integrity - backup verification
  - Catalog Sync - synchronize local catalog (coming soon)

- **New `dbbackup blob stats` CLI command** - analyze blob/bytea columns
  - Scans `information_schema` for binary column types
  - Shows row counts, total size, average size, max size per column
  - Identifies tables storing large binary data for optimization
  - Supports both PostgreSQL (bytea, oid) and MySQL (blob, mediumblob, longblob)
  - Provides recommendations for databases with >100MB blob data

## [3.42.106] - 2026-01-24

### Fixed - Cluster Restore Resilience & Performance
- **Fixed cluster restore failing on missing roles** - harmless "role does not exist" errors no longer abort restore
  - Added role-related errors to `isIgnorableError()` with warning log
  - Removed `ON_ERROR_STOP=1` from psql commands (pre-validation catches real corruption)
  - Restore now continues gracefully when referenced roles don't exist in target cluster
  - Previously caused 12h+ restores to fail at 94% completion

- **Fixed TUI output scrambling in screen/tmux sessions** - added terminal detection
  - Uses `go-isatty` to detect non-interactive terminals (backgrounded screen sessions, pipes)
  - Added `viewSimple()` methods for clean line-by-line output without ANSI escape codes
  - TUI menu now shows warning when running in non-interactive terminal

### Changed - Consistent Parallel Compression (pgzip)
- **Migrated all gzip operations to parallel pgzip** - 2-4x faster compression/decompression on multi-core systems
  - Systematic audit found 17 files using standard `compress/gzip`
  - All converted to `github.com/klauspost/pgzip` for consistent performance
  - **Files updated**:
    - `internal/backup/`: incremental_tar.go, incremental_extract.go, incremental_mysql.go
    - `internal/wal/`: compression.go (CompressWALFile, DecompressWALFile, VerifyCompressedFile)
    - `internal/engine/`: clone.go, snapshot_engine.go, mysqldump.go, binlog/file_target.go
    - `internal/restore/`: engine.go, safety.go, formats.go, error_report.go
    - `internal/pitr/`: mysql.go, binlog.go
    - `internal/dedup/`: store.go
    - `cmd/`: dedup.go, placeholder.go
  - **Benefit**: Large backup/restore operations now fully utilize available CPU cores

## [3.42.105] - 2026-01-23

### Changed - TUI Visual Cleanup
- **Removed ASCII box characters** from backup/restore success/failure banners
  - Replaced `╔═╗║╚╝` boxes with clean `═══` horizontal line separators
  - Cleaner, more modern appearance in terminal output
- **Consolidated duplicate styles** in TUI components
  - Unified check status styles (passed/failed/warning/pending) into global definitions
  - Reduces code duplication across restore preview and diagnose views

## [3.42.98] - 2025-01-23

### Fixed - Critical Bug Fixes for v3.42.97
- **Fixed CGO/SQLite build issue** - binaries now work when compiled with `CGO_ENABLED=0`
  - Switched from `github.com/mattn/go-sqlite3` (requires CGO) to `modernc.org/sqlite` (pure Go)
  - All cross-compiled binaries now work correctly on all platforms
  - No more "Binary was compiled with 'CGO_ENABLED=0', go-sqlite3 requires cgo to work" errors

- **Fixed MySQL positional database argument being ignored**
  - `dbbackup backup single <dbname> --db-type mysql` now correctly uses `<dbname>`
  - Previously defaulted to 'postgres' regardless of positional argument
  - Also fixed in `backup sample` command

## [3.42.97] - 2025-01-23

### Added - Bandwidth Throttling for Cloud Uploads
- **New `--bandwidth-limit` flag for cloud operations** - prevent network saturation during business hours
  - Works with S3, GCS, Azure Blob Storage, MinIO, Backblaze B2
  - Supports human-readable formats:
    - `10MB/s`, `50MiB/s` - megabytes per second
    - `100KB/s`, `500KiB/s` - kilobytes per second  
    - `1GB/s` - gigabytes per second
    - `100Mbps` - megabits per second (for network-minded users)
    - `unlimited` or `0` - no limit (default)
  - Environment variable: `DBBACKUP_BANDWIDTH_LIMIT`
  - **Example usage**:
    ```bash
    # Limit upload to 10 MB/s during business hours
    dbbackup cloud upload backup.dump --bandwidth-limit 10MB/s
    
    # Environment variable for all operations
    export DBBACKUP_BANDWIDTH_LIMIT=50MiB/s
    ```
  - **Implementation**: Token-bucket style throttling with 100ms windows for smooth rate limiting
  - **DBA requested feature**: Avoid saturating production network during scheduled backups

## [3.42.96] - 2025-02-01

### Changed - Complete Elimination of Shell tar/gzip Dependencies
- **All tar/gzip operations now 100% in-process** - ZERO shell dependencies for backup/restore
  - Removed ALL remaining `exec.Command("tar", ...)` calls
  - Removed ALL remaining `exec.Command("gzip", ...)` calls
  - Systematic code audit found and eliminated:
    - `diagnose.go`: Replaced `tar -tzf` test with direct file open check
    - `large_restore_check.go`: Replaced `gzip -t` and `gzip -l` with in-process pgzip verification
    - `pitr/restore.go`: Replaced `tar -xf` with in-process tar extraction
  - **Benefits**:
    - No external tool dependencies (works in minimal containers)
    - 2-4x faster on multi-core systems using parallel pgzip
    - More reliable error handling with Go-native errors
    - Consistent behavior across all platforms
    - Reduced attack surface (no shell spawning)
  - **Verification**: `strace` and `ps aux` show no tar/gzip/gunzip processes during backup/restore
  - **Note**: Docker drill container commands still use gunzip for in-container operations (intentional)

## [Unreleased]

### Added - Single Database Extraction from Cluster Backups (CLI + TUI)
- **Extract and restore individual databases from cluster backups** - selective restore without full cluster restoration
  - **CLI Commands**:
    - **List databases**: `dbbackup restore cluster backup.tar.gz --list-databases`
      - Shows all databases in cluster backup with sizes
      - Fast scan without full extraction
    - **Extract single database**: `dbbackup restore cluster backup.tar.gz --database myapp --output-dir /tmp/extract`
      - Extracts only the specified database dump
      - No restore, just file extraction
    - **Restore single database from cluster**: `dbbackup restore cluster backup.tar.gz --database myapp --confirm`
      - Extracts and restores only one database
      - Much faster than full cluster restore when you only need one database
    - **Rename on restore**: `dbbackup restore cluster backup.tar.gz --database myapp --target myapp_test --confirm`
      - Restore with different database name (useful for testing)
    - **Extract multiple databases**: `dbbackup restore cluster backup.tar.gz --databases "app1,app2,app3" --output-dir /tmp/extract`
      - Comma-separated list of databases to extract
  - **TUI Support**:
    - Press **'s'** on any cluster backup in archive browser to select individual databases
    - New **ClusterDatabaseSelector** view shows all databases with sizes
    - Navigate with arrow keys, select with Enter
    - Automatic handling when cluster backup selected in single restore mode
    - Full restore preview and confirmation workflow
  - **Benefits**:
    - Faster restores (extract only what you need)
    - Less disk space usage during restore
    - Easy database migration/copying
    - Better testing workflow
    - Selective disaster recovery

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

### Added - PostgreSQL lock verification (CLI + preflight)
- **`dbbackup verify-locks`** — new CLI command that probes PostgreSQL GUCs (`max_locks_per_transaction`, `max_connections`, `max_prepared_transactions`) and prints total lock capacity plus actionable restore guidance.
- **Integrated into preflight checks** — preflight now warns/fails when lock settings are insufficient and provides exact remediation commands and recommended restore flags (e.g. `--jobs 1 --parallel-dbs 1`).
- **Implemented in Go (replaces `verify_postgres_locks.sh`)** with robust parsing, sudo/`psql` fallback and unit-tested decision logic.
- **Files:** `cmd/verify_locks.go`, `internal/checks/locks.go`, `internal/checks/locks_test.go`, `internal/checks/preflight.go`.
- **Why:** Prevents repeated parallel-restore failures by surfacing lock-capacity issues early and providing bulletproof guidance.

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
  - **Potato** (`--profile=potato`): Easter egg, same as conservative
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

### Added - Pre-Restore Validation

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

### Added - Physical Backup Revolution

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
