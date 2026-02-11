# dbbackup

Database backup and restore utility for PostgreSQL, MySQL, and MariaDB.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://golang.org/)
[![Release](https://img.shields.io/badge/Release-v6.16.0-green.svg)](https://github.com/PlusOne/dbbackup/releases/latest)

**Repository:** https://git.uuxo.net/UUXO/dbbackup  
**Mirror:** https://github.com/PlusOne/dbbackup

## Table of Contents

- [Quick Start](#quick-start-30-seconds)
- [Production-Hardened](#production-hardened)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Interactive Mode](#interactive-mode)
  - [Command Line](#command-line)
- [Commands](#commands)
- [Global Flags](#global-flags)
- [Encryption](#encryption)
- [Incremental Backups](#incremental-backups)
- [Cloud Storage](#cloud-storage)
- [Point-in-Time Recovery](#point-in-time-recovery)
- [Backup Cleanup](#backup-cleanup)
- [Dry-Run Mode](#dry-run-mode)
- [Backup Diagnosis](#backup-diagnosis)
- [Notifications](#notifications)
- [Backup Catalog](#backup-catalog)
- [Cost Analysis](#cost-analysis)
- [Health Check](#health-check)
- [DR Drill Testing](#dr-drill-testing)
- [Compliance Reports](#compliance-reports)
- [RTO/RPO Analysis](#rtorpo-analysis)
- [Systemd Integration](#systemd-integration)
- [Prometheus Metrics](#prometheus-metrics)
- [Configuration](#configuration)
- [Performance](#performance)
- [Requirements](#requirements)
- [Documentation](#documentation)
- [License](#license)

## Quick Start (30 seconds)

```bash
# Download
wget https://github.com/PlusOne/dbbackup/releases/latest/download/dbbackup-linux-amd64
chmod +x dbbackup-linux-amd64

# Backup your database
./dbbackup-linux-amd64 backup single mydb --db-type postgres
# Or for MySQL
./dbbackup-linux-amd64 backup single mydb --db-type mysql --user root

# Interactive mode (recommended for first-time users)
./dbbackup-linux-amd64 interactive
```

**That's it!** Backups are stored in `./backups/` by default. See [QUICK.md](QUICK.md) for more real-world examples.

## Production-Hardened

The interactive TUI includes **bulletproof reliability features**:

- **Connection health check** -- Know immediately if your DB is reachable
- **Pre-restore validation** -- Catch issues BEFORE extraction (corrupted archives, disk space, privileges)
- **Safe abort** -- Ctrl+C properly cleans up processes and temp files
- **Destructive warnings** -- Type-to-confirm prevents accidental data loss
- **I/O Governor selector** -- Choose I/O scheduling strategy per workload (noop/bfq/mq-deadline/deadline)
- **Adaptive Jobs toggle** -- Auto-sizes parallel workers per database with engine-aware 3-stage pipeline

**Try it:** `./dbbackup interactive` -- safer, smarter, faster.

See [docs/tui-features.md](docs/tui-features.md) for full details.

## Features

### Enterprise Physical Backup and Operations

**Major enterprise features for production DBAs:**

- **pg_basebackup Integration**: Physical backup via streaming replication for 100GB+ databases
- **WAL Archiving Manager**: pg_receivewal integration with replication slot management for true PITR
- **Table-Level Backup**: Selective backup by table pattern, schema, or row count
- **Pre/Post Hooks**: Run VACUUM ANALYZE, notify Slack, or custom scripts before/after backups
- **Bandwidth Throttling**: Rate-limit backup and upload operations (e.g., `--max-bandwidth 100M`)
- **Intelligent Compression**: Detects blob types (JPEG, PDF, archives) and recommends optimal compression
- **ZFS/Btrfs Detection**: Auto-detects filesystem compression and adjusts recommendations

### Native Database Engines

**We built our own database engines -- no external tools required.**

- **Pure Go Implementation**: Direct PostgreSQL (pgx) and MySQL (go-sql-driver) protocol communication
- **No External Dependencies**: No pg_dump, mysqldump, pg_restore, mysql, psql, mysqlbinlog
- **Full Control**: Our code generates SQL, handles types, manages connections, and processes binary data
- **Production Ready**: Advanced data type handling, proper escaping, binary support, batch processing

### Core Database Features

- Multi-database support: PostgreSQL, MySQL, MariaDB
- Backup modes: Single database, cluster, sample data
- **Dry-run mode**: Preflight checks before backup execution
- AES-256-GCM encryption
- Incremental backups
- Cloud storage: S3, MinIO, B2, Azure Blob, Google Cloud Storage
- Point-in-Time Recovery (PITR) for PostgreSQL and MySQL/MariaDB
- **GFS retention policies**: Grandfather-Father-Son backup rotation
- **Notifications**: SMTP email and webhook alerts
- **Systemd integration**: Install as service with scheduled timers
- **Prometheus metrics**: Textfile collector and HTTP exporter
- Interactive terminal UI
- Cross-platform binaries

### Enterprise DBA Features

- **Backup Catalog**: SQLite-based catalog tracking all backups with gap detection
- **Catalog Dashboard**: Interactive TUI for browsing and managing backups
- **DR Drill Testing**: Automated disaster recovery testing in Docker containers
- **Smart Notifications**: Batched alerts with escalation policies
- **Progress Webhooks**: Real-time backup/restore progress notifications
- **Compliance Reports**: SOC2, GDPR, HIPAA, PCI-DSS, ISO27001 report generation
- **RTO/RPO Calculator**: Recovery objective analysis and recommendations
- **Replica-Aware Backup**: Automatic backup from replicas to reduce primary load
- **Parallel Table Backup**: Concurrent table dumps for faster backups
- **Retention Simulator**: Preview retention policy effects before applying
- **Cross-Region Sync**: Sync backups between cloud regions for disaster recovery
- **Encryption Key Rotation**: Secure key management with rotation support

### Performance Features

- **5-10x faster parallel restore** vs pg_dump/pg_restore baseline
- **90% RTO reduction** with tiered restore (critical tables first)
- **Engine-aware adaptive job sizing** -- 3-stage pipeline: BLOB adjustment, native engine boost, memory ceiling
- **I/O scheduler governors** -- noop, bfq, mq-deadline, deadline (auto-selected per BLOB strategy)
- **BLOB Pipeline Matrix** -- native large object backup/restore with parallel streaming
- **Nuclear Restore Engine** -- pure Go streaming SQL parser, global index builder, transaction batcher
- **Streaming I/O** with 256KB batch pipeline (constant memory usage)
- **UNLOGGED table optimization** during COPY phase (PostgreSQL balanced/turbo mode)
- **Bulk load optimizations** for MySQL/MariaDB (FOREIGN_KEY_CHECKS, UNIQUE_CHECKS, sql_log_bin, innodb_flush_log)
- **Index type detection** -- GIN/GIST indexes get 4GB RAM allocation and 8 parallel workers
- **Adaptive worker allocation** based on table size metadata with physical CPU core detection

### Restore Modes

| Mode | Speed | Safety | Use Case |
|------|-------|--------|----------|
| **Safe** (default) | Baseline | Full WAL logging | Production with replication |
| **Balanced** | 2–3× faster | UNLOGGED during COPY, LOGGED before indexes | Production standalone |
| **Turbo** | 3–4× faster | Minimal WAL, async commit | Dev/test only |

```bash
dbbackup restore single dump.sql.gz \
    --restore-mode=balanced \
    --tiered-restore \
    --critical-tables="user*,session*,payment*"
```

### Multi-Database Support

- **PostgreSQL 10+** -- Fully optimized (UNLOGGED tables, parallel DDL, adaptive workers)
- **MySQL 5.7+** -- Native engine with bulk load optimizations
- **MariaDB 10.3+** -- Full parity with MySQL engine

TUI shows database type indicator and adapts features automatically.

### MariaDB Galera Cluster Support

- **Auto-detection**: Automatic Galera cluster detection via `wsrep_*` status variables
- **Health validation**: Pre-backup checks (sync state, flow control, cluster size)
- **Desync mode**: `--galera-desync` to temporarily isolate node during backup
- **Zero-downtime**: Backup from donor node without impacting cluster performance

```bash
# Backup from Galera cluster with desync mode
dbbackup backup single mydb \
    --db-type mariadb \
    --host galera-node2 \
    --galera-desync \
    --galera-min-cluster-size 3

# Health check only (auto-detected)
dbbackup backup single mydb --db-type mariadb --galera-health-check
```

### Quality Assurance

- **1358-line pre-release test suite** covering 10 categories with 43+ checks
- **GitHub Actions CI** with race detector, leak tests, multi-DB validation
- **27 TUI screens** with comprehensive Ctrl+C handling on every screen
- **Backwards compatible** with all v5.x backup formats

## Installation

### Docker

```bash
docker pull git.uuxo.net/UUXO/dbbackup:latest

# PostgreSQL backup
docker run --rm \
  -v $(pwd)/backups:/backups \
  -e PGHOST=your-host \
  -e PGUSER=postgres \
  -e PGPASSWORD=secret \
  git.uuxo.net/UUXO/dbbackup:latest backup single mydb
```

### Binary Download

Download from [releases](https://git.uuxo.net/UUXO/dbbackup/releases):

```bash
# Linux x86_64
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v6.15.0/dbbackup-linux-amd64
chmod +x dbbackup-linux-amd64
sudo mv dbbackup-linux-amd64 /usr/local/bin/dbbackup
```

Available platforms: Linux (amd64, arm64, armv7), macOS (amd64, arm64).

### Build from Source

```bash
git clone https://git.uuxo.net/UUXO/dbbackup.git
cd dbbackup
go build
```

## Usage

### Interactive Mode

**Recommended for first-time users and production safety.**

```bash
# PostgreSQL with peer authentication
sudo -u postgres dbbackup interactive

# MySQL/MariaDB (use MYSQL_PWD env var for password)
export MYSQL_PWD='secret'
dbbackup interactive --db-type mysql --user root
```

**TUI Features:**
- **Real-time connection status** in menu header ([OK] / [FAIL])
- **Pre-restore validation screen** catches issues early (7 automated checks)
- **Safe abort** with Ctrl+C (no orphaned processes or temp files)
- **Type-to-confirm warnings** for destructive operations (database overwrites)
- **Adaptive Jobs toggle** in settings -- auto-sizes parallel workers per database
- **I/O Governor selector** -- cycle through auto/noop/bfq/mq-deadline/deadline
- **Resource profile presets** -- conservative, balanced, performance, max-performance

The TUI automatically:
- Tests database connectivity on startup (5s timeout)
- Validates backup archives before restore
- Checks disk space, privileges, and PostgreSQL lock capacity
- Prevents common mistakes (overwriting production DBs)

**For automation/CI:** Use `--auto-select`, `--auto-database`, `--auto-confirm` flags.

**Main Menu:**
```
Database Backup Tool - Interactive Menu

Target Engine: PostgreSQL  |  MySQL  |  MariaDB
Database: postgres@localhost:5432 (PostgreSQL)  [OK] Connected

> Single Database Backup
  Sample Database Backup (with ratio)
  Cluster Backup (all databases)
  ────────────────────────────────
  Restore Single Database
  Restore Cluster Backup
  Diagnose Backup File
  List & Manage Backups
  ────────────────────────────────
  Tools
  View Active Operations
  Show Operation History
  Database Status & Health Check
  Configuration Settings
  Clear Operation History
  Quit
```

**Tools Menu:**
```
Tools

Advanced utilities for database backup management

> Blob Statistics
  Blob Extract (externalize LOBs)
  ────────────────────────────────
  Dedup Store Analyze
  Verify Backup Integrity
  Catalog Sync
  ────────────────────────────────
  Back to Main Menu
```

**Database Selection:**
```
Single Database Backup

Select database to backup:

> production_db      (245 MB)
  analytics_db       (1.2 GB)
  users_db           (89 MB)
  inventory_db       (456 MB)

Enter: Select | Esc: Back
```

**Backup Execution:**
```
Backup Execution

  Type:      Single Database
  Database:  production_db
  Duration:  2m 35s

  Backing up database 'production_db'...
```

**Backup Complete:**
```
Backup Execution

  Type:      Cluster Backup
  Duration:  8m 12s

  Backup completed successfully!

  Backup created: cluster_20251128_092928.tar.gz
  Size: 22.5 GB (compressed)
  Location: /var/backups/postgres/
  Databases: 7
  Checksum: SHA-256 verified
```

**Restore Preview:**
```
Cluster Restore Preview

Archive Information
  File: cluster_20251128_092928.tar.gz
  Format: PostgreSQL Cluster (tar.gz)
  Size: 22.5 GB

Cluster Restore Options
  Host: localhost:5432
  Existing Databases: 5 found
  Clean All First: true

Safety Checks
  [OK] Archive integrity verified
  [OK] Dump validity verified
  [OK] Disk space: 140 GB available
  [OK] Required tools found
  [OK] Target database accessible

Advanced Options
  ✗ Debug Log: false (press 'd' to toggle)

c: Toggle cleanup | d: Debug log | Enter: Proceed | Esc: Cancel
```

**Backup Manager:**
```
Backup Archive Manager

Total Archives: 15  |  Total Size: 156.8 GB

FILENAME                              FORMAT                SIZE        MODIFIED
─────────────────────────────────────────────────────────────────────────────────
> [OK] cluster_20250115.tar.gz        PostgreSQL Cluster    18.5 GB     2025-01-15
  [OK] myapp_prod_20250114.dump.gz    PostgreSQL Custom     12.3 GB     2025-01-14
  [!!] users_db_20241220.dump.gz      PostgreSQL Custom     850 MB      2024-12-20

r: Restore | v: Verify | i: Info | d: Diagnose | D: Delete | R: Refresh | Esc: Back
```

**Configuration Settings:**
```
Configuration Settings

[SYSTEM] Detected Resources
  CPU: 8 physical cores, 16 logical cores
  Memory: 32GB total, 28GB available
  Recommended Profile: balanced
    → 8 cores and 32GB RAM supports moderate parallelism

[CONFIG] Current Settings
  Target DB: PostgreSQL (postgres)
  Database: postgres@localhost:5432
  Backup Dir: /var/backups/postgres
  Compression: Level 6
  Profile: balanced | Cluster: 2 parallel | Jobs: 4
  I/O Governor: auto | Adaptive Jobs: ON

> Database Type: postgres
  CPU Workload Type: balanced
  Resource Profile: balanced (P:2 J:4)
  Cluster Parallelism: 2
  I/O Governor: auto
  Adaptive Jobs: ON
  Backup Directory: /var/backups/postgres
  Work Directory: (system temp)
  Compression Level: 6
  Parallel Jobs: 4
  Dump Jobs: 4
  Database Host: localhost
  Database Port: 5432
  Database User: postgres
  SSL Mode: prefer

[KEYS]  ↑↓ navigate | Enter edit | 'l' toggle LargeDB | 'c' conservative | 'p' recommend | 's' save | 'q' menu
```

**Resource Profiles for Large Databases:**

When restoring large databases on VMs with limited resources, use the resource profile settings to prevent "out of shared memory" errors:

| Profile | Cluster Parallel | Jobs | Best For |
|---------|------------------|------|----------|
| conservative | 1 | 1 | Small VMs (<16GB RAM) |
| balanced | 2 | 2-4 | Medium VMs (16-32GB RAM) |
| performance | 4 | 4-8 | Large servers (32GB+ RAM) |
| max-performance | 8 | 8-16 | High-end servers (64GB+) |

**Large DB Mode:** Toggle with `l` key. Reduces parallelism by 50% and sets max_locks_per_transaction=8192 for complex databases with many tables/LOBs.

**Quick shortcuts:** Press `l` to toggle Large DB Mode, `c` for conservative, `p` to show recommendation.

**Troubleshooting Tools:**

For PostgreSQL restore issues ("out of shared memory" errors), diagnostic scripts are available:
- **diagnose_postgres_memory.sh** - Comprehensive system memory, PostgreSQL configuration, and resource analysis
- **fix_postgres_locks.sh** - Automatically increase max_locks_per_transaction to 4096

See [RESTORE_PROFILES.md](RESTORE_PROFILES.md) for detailed troubleshooting guidance.

**Database Status:**
```
Database Status & Health Check

Connection Status: Connected

Database Type: PostgreSQL
Host: localhost:5432
User: postgres
Version: PostgreSQL 17.2
Databases Found: 5

All systems operational
```

### Command Line

```bash
# Single database backup
dbbackup backup single myapp_db

# Cluster backup (PostgreSQL)
dbbackup backup cluster

# Sample backup (reduced data for testing)
dbbackup backup sample myapp_db --sample-strategy percent --sample-value 10

# Encrypted backup
dbbackup backup single myapp_db --encrypt --encryption-key-file key.txt

# Incremental backup
dbbackup backup single myapp_db --backup-type incremental --base-backup base.tar.gz

# Restore single database
dbbackup restore single backup.dump --target myapp_db --create --confirm

# Restore cluster (adaptive job sizing is enabled by default)
dbbackup restore cluster cluster_backup.tar.gz --confirm

# Restore with specific I/O governor (for BLOB-heavy databases)
dbbackup restore cluster backup.tar.gz --io-governor=bfq --confirm

# Restore with resource profile (for resource-constrained servers)
dbbackup restore cluster backup.tar.gz --profile=conservative --confirm

# Restore with debug logging (saves detailed error report on failure)
dbbackup restore cluster backup.tar.gz --save-debug-log /tmp/restore-debug.json --confirm

# Diagnose backup before restore
dbbackup restore diagnose backup.dump.gz --deep

# Check PostgreSQL lock configuration (preflight for large restores)
# - warns/fails when `max_locks_per_transaction` is insufficient and prints exact remediation
# - safe to run before a restore to determine whether single-threaded restore is required
# Example:
# dbbackup verify-locks

# Cloud backup
dbbackup backup single mydb --cloud s3://my-bucket/backups/

# Dry-run mode (preflight checks without execution)
dbbackup backup single mydb --dry-run
```

## Commands

| Command | Description |
|---------|-------------|
| `backup single` | Backup single database |
| `backup cluster` | Backup all databases (PostgreSQL) |
| `backup sample` | Backup with reduced data |
| `restore single` | Restore single database |
| `restore cluster` | Restore full cluster |
| `restore pitr` | Point-in-Time Recovery |
| `restore diagnose` | Diagnose backup file integrity |
| `verify-backup` | Verify backup integrity |
| `verify-locks` | Check PostgreSQL lock settings and get restore guidance |
| `cleanup` | Remove old backups |
| `status` | Check connection status |
| `preflight` | Run pre-backup checks |
| `list` | List databases and backups |
| `cpu` | Show CPU optimization settings |
| `cloud` | Cloud storage operations |
| `pitr` | PITR management |
| `wal` | WAL archive operations |
| `interactive` | Start interactive UI |
| `catalog` | Backup catalog management |
| `drill` | DR drill testing |
| `report` | Compliance report generation |
| `rto` | RTO/RPO analysis |
| `blob stats` | Analyze blob/bytea columns in database |
| `install` | Install as systemd service |
| `uninstall` | Remove systemd service |
| `metrics export` | Export Prometheus metrics to textfile |
| `metrics serve` | Run Prometheus HTTP exporter |

## Global Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-d, --db-type` | Database type (postgres, mysql, mariadb) | postgres |
| `--host` | Database host | localhost |
| `--port` | Database port | 5432/3306 |
| `--user` | Database user | current user |
| `MYSQL_PWD` / `PGPASSWORD` | Database password (env var) | - |
| `--backup-dir` | Backup directory | ~/db_backups |
| `--compression` | Compression level (0-9) | 6 |
| `--jobs` | Parallel jobs | 8 |
| `--profile` | Resource profile (conservative/balanced/aggressive) | balanced |
| `--adaptive` | Adaptive per-DB job sizing (overlays profile) | true |
| `--io-governor` | I/O scheduler governor (auto/noop/bfq/mq-deadline/deadline) | auto |
| `--cloud` | Cloud storage URI | - |
| `--encrypt` | Enable encryption | false |
| `--dry-run, -n` | Run preflight checks only | false |
| `--debug` | Enable debug logging (activates DEBUG log level) | false |
| `--save-debug-log` | Save error report to file on failure | - |
| `--native-engine` | Use native Go restore engine | true |

## Encryption

AES-256-GCM encryption for secure backups:

```bash
# Generate key
head -c 32 /dev/urandom | base64 > encryption.key

# Backup with encryption
dbbackup backup single mydb --encrypt --encryption-key-file encryption.key

# Restore (decryption is automatic)
dbbackup restore single mydb_encrypted.sql.gz --encryption-key-file encryption.key --target mydb --confirm
```

## Incremental Backups

Space-efficient incremental backups:

```bash
# Full backup (base)
dbbackup backup single mydb --backup-type full

# Incremental backup
dbbackup backup single mydb --backup-type incremental --base-backup mydb_base.tar.gz
```

## Cloud Storage

Supported providers: AWS S3, MinIO, Backblaze B2, Azure Blob Storage, Google Cloud Storage.

```bash
# AWS S3
export AWS_ACCESS_KEY_ID="key"
export AWS_SECRET_ACCESS_KEY="secret"
dbbackup backup single mydb --cloud s3://bucket/path/

# Azure Blob
export AZURE_STORAGE_ACCOUNT="account"
export AZURE_STORAGE_KEY="key"
dbbackup backup single mydb --cloud azure://container/path/

# Google Cloud Storage
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
dbbackup backup single mydb --cloud gcs://bucket/path/
```

See [CLOUD.md](CLOUD.md) for detailed configuration.

## Point-in-Time Recovery

PITR for PostgreSQL allows restoring to any specific point in time:

```bash
# Enable PITR
dbbackup pitr enable --archive-dir /backups/wal_archive

# Restore to timestamp
dbbackup restore pitr \
  --base-backup /backups/base.tar.gz \
  --wal-archive /backups/wal_archive \
  --target-time "2024-11-26 12:00:00" \
  --target-dir /var/lib/postgresql/14/restored
```

See [PITR.md](PITR.md) for detailed documentation.

## Backup Cleanup

Automatic retention management:

```bash
# Delete backups older than 30 days, keep minimum 5
dbbackup cleanup /backups --retention-days 30 --min-backups 5

# Preview deletions
dbbackup cleanup /backups --retention-days 7 --dry-run
```

### GFS Retention Policy

Grandfather-Father-Son (GFS) retention provides tiered backup rotation:

```bash
# GFS retention: 7 daily, 4 weekly, 12 monthly, 3 yearly
dbbackup cleanup /backups --gfs \
  --gfs-daily 7 \
  --gfs-weekly 4 \
  --gfs-monthly 12 \
  --gfs-yearly 3

# Custom weekly day (Saturday) and monthly day (15th)
dbbackup cleanup /backups --gfs \
  --gfs-weekly-day Saturday \
  --gfs-monthly-day 15

# Preview GFS deletions
dbbackup cleanup /backups --gfs --dry-run
```

**GFS Tiers:**
- **Daily**: Most recent N daily backups
- **Weekly**: Best backup from each week (configurable day)
- **Monthly**: Best backup from each month (configurable day)
- **Yearly**: Best backup from January each year

## Dry-Run Mode

Preflight checks validate backup readiness without execution:

```bash
# Run preflight checks only
dbbackup backup single mydb --dry-run
dbbackup backup cluster -n  # Short flag
```

**Checks performed:**
- Database connectivity (connect + ping)
- Required tools availability (pg_dump, mysqldump, etc.)
- Storage target accessibility and permissions
- Backup size estimation
- Encryption configuration validation
- Cloud storage credentials (if configured)

**Example output:**
```
╔══════════════════════════════════════════════════════════════╗
║             [DRY RUN] Preflight Check Results                ║
╚══════════════════════════════════════════════════════════════╝

  Database: PostgreSQL PostgreSQL 15.4
  Target:   postgres@localhost:5432/mydb

  Checks:
  ─────────────────────────────────────────────────────────────
  Database Connectivity: Connected successfully
  Required Tools:        pg_dump 15.4 available
  Storage Target:        /backups writable (45 GB free)
  Size Estimation:       ~2.5 GB required
  ─────────────────────────────────────────────────────────────

  All checks passed

  Ready to backup. Remove --dry-run to execute.
```

## Backup Diagnosis

Diagnose backup files before restore to detect corruption or truncation:

```bash
# Diagnose a backup file
dbbackup restore diagnose backup.dump.gz

# Deep analysis (line-by-line COPY block verification)
dbbackup restore diagnose backup.dump.gz --deep

# JSON output for automation
dbbackup restore diagnose backup.dump.gz --json

# Diagnose cluster archive (checks all contained dumps)
dbbackup restore diagnose cluster_backup.tar.gz --deep
```

**Checks performed:**
- PGDMP signature validation (PostgreSQL custom format)
- Gzip integrity verification
- COPY block termination (detects truncated dumps)
- `pg_restore --list` validation
- Archive structure analysis

**Example output:**
```
Backup Diagnosis Report
══════════════════════════════════════════════════════════════

File: mydb_20260105.dump.gz
   Format: PostgreSQL Custom (gzip)
   Size: 2.5 GB

Analysis Results:
   Gzip integrity: Valid
   PGDMP signature: Valid
   pg_restore --list: Success (245 objects)
   COPY block check: TRUNCATED

Issues Found:
   - COPY block for table 'orders' not terminated
   - Dump appears truncated at line 1,234,567

Recommendations:
   - Re-run the backup for this database
   - Check disk space on backup server
   - Verify network stability during backup
```

**In Interactive Mode:**
- Press `d` in archive browser to diagnose any backup
- Automatic dump validity check in restore preview
- Toggle debug logging with `d` in restore options

## Notifications

Get alerted on backup events via email or webhooks. Configure via environment variables.

### SMTP Email

```bash
# Environment variables
export NOTIFY_SMTP_HOST="smtp.example.com"
export NOTIFY_SMTP_PORT="587"
export NOTIFY_SMTP_USER="alerts@example.com"
export NOTIFY_SMTP_PASSWORD="secret"
export NOTIFY_SMTP_FROM="dbbackup@example.com"
export NOTIFY_SMTP_TO="admin@example.com,dba@example.com"

# Run backup (notifications triggered when SMTP is configured)
dbbackup backup single mydb
```

### Webhooks

```bash
# Generic webhook
export NOTIFY_WEBHOOK_URL="https://api.example.com/webhooks/backup"
export NOTIFY_WEBHOOK_SECRET="signing-secret"  # Optional HMAC signing

# Slack webhook
export NOTIFY_WEBHOOK_URL="https://hooks.slack.com/services/T00/B00/XXX"

# Run backup (notifications triggered when webhook is configured)
dbbackup backup single mydb
```

**Webhook payload:**
```json
{
  "version": "1.0",
  "event": {
    "type": "backup_completed",
    "severity": "info",
    "timestamp": "2025-01-15T10:30:00Z",
    "database": "mydb",
    "message": "Backup completed successfully",
    "backup_file": "/backups/mydb_20250115.dump.gz",
    "backup_size": 2684354560,
    "hostname": "db-server-01"
  },
  "subject": "[dbbackup] Backup Completed: mydb"
}
```

**Supported events:**
- `backup_started`, `backup_completed`, `backup_failed`
- `restore_started`, `restore_completed`, `restore_failed`
- `cleanup_completed`
- `verify_completed`, `verify_failed`
- `pitr_recovery`
- `dr_drill_passed`, `dr_drill_failed`
- `gap_detected`, `rpo_violation`

### Testing Notifications

```bash
# Test notification configuration
export NOTIFY_SMTP_HOST="localhost"
export NOTIFY_SMTP_PORT="25"
export NOTIFY_SMTP_FROM="dbbackup@myserver.local"
export NOTIFY_SMTP_TO="admin@example.com"

dbbackup notify test --verbose
# [OK] Notification sent successfully

# For servers using STARTTLS with self-signed certs
export NOTIFY_SMTP_STARTTLS="false"
```

## Backup Catalog

Track all backups in a SQLite catalog with gap detection and search:

```bash
# Sync backups from directory to catalog
dbbackup catalog sync /backups

# List recent backups
dbbackup catalog list --database mydb --limit 10

# Show catalog statistics
dbbackup catalog stats

# Detect backup gaps (missing scheduled backups)
dbbackup catalog gaps --interval 24h --database mydb

# Search backups by date range
dbbackup catalog search --database mydb --after 2024-01-01 --before 2024-12-31

# Get backup info by path
dbbackup catalog info /backups/mydb_20240115.dump.gz

# Compare two backups to see what changed
dbbackup diff /backups/mydb_20240115.dump.gz /backups/mydb_20240120.dump.gz

# Compare using catalog IDs
dbbackup diff 123 456

# Compare latest two backups for a database
dbbackup diff mydb:latest mydb:previous
```

## Cost Analysis

Analyze and optimize cloud storage costs:

```bash
# Analyze current backup costs
dbbackup cost analyze

# Specific database
dbbackup cost analyze --database mydb

# Compare providers and tiers
dbbackup cost analyze --provider aws --format table

# Get JSON for automation/reporting
dbbackup cost analyze --format json
```

**Providers analyzed:**
- AWS S3 (Standard, IA, Glacier, Deep Archive)
- Google Cloud Storage (Standard, Nearline, Coldline, Archive)
- Azure Blob (Hot, Cool, Archive)
- Backblaze B2
- Wasabi

Shows tiered storage strategy recommendations with potential annual savings.

## Health Check

Comprehensive backup infrastructure health monitoring:

```bash
# Quick health check
dbbackup health

# Detailed output
dbbackup health --verbose

# JSON for monitoring integration (Prometheus, Nagios, etc.)
dbbackup health --format json

# Custom backup interval for gap detection
dbbackup health --interval 12h

# Skip database connectivity (offline check)
dbbackup health --skip-db
```

**Checks performed:**
- Configuration validity
- Database connectivity
- Backup directory accessibility
- Catalog integrity
- Backup freshness (is last backup recent?)
- Gap detection (missed scheduled backups)
- Verification status (% of backups verified)
- File integrity (do files exist and match metadata?)
- Orphaned entries (catalog entries for missing files)
- Disk space

**Exit codes for automation:**
- `0` = healthy (all checks passed)
- `1` = warning (some checks need attention)
- `2` = critical (immediate action required)

## DR Drill Testing

Automated disaster recovery testing restores backups to Docker containers:

```bash
# Run full DR drill
dbbackup drill run /backups/mydb_latest.dump.gz \
  --database mydb \
  --type postgresql \
  --timeout 1800

# Quick drill (restore + basic validation)
dbbackup drill quick /backups/mydb_latest.dump.gz --database mydb

# List running drill containers
dbbackup drill list

# Cleanup all drill containers
dbbackup drill cleanup

# Display a saved drill report
dbbackup drill report drill_20240115_120000_report.json --format json
```

**Drill phases:**
1. Container creation
2. Backup download (if cloud)
3. Restore execution
4. Database validation
5. Custom query checks
6. Cleanup

## Compliance Reports

Generate compliance reports for regulatory frameworks:

```bash
# Generate SOC2 report
dbbackup report generate --type soc2 --days 90 --format html --output soc2-report.html

# HIPAA compliance report
dbbackup report generate --type hipaa --format markdown

# Show compliance summary
dbbackup report summary --type gdpr --days 30

# List available frameworks
dbbackup report list

# Show controls for a framework
dbbackup report controls soc2
```

**Supported frameworks:**
- SOC2 Type II (Trust Service Criteria)
- GDPR (General Data Protection Regulation)
- HIPAA (Health Insurance Portability and Accountability Act)
- PCI-DSS (Payment Card Industry Data Security Standard)
- ISO 27001 (Information Security Management)

## RTO/RPO Analysis

Calculate and monitor Recovery Time/Point Objectives:

```bash
# Analyze RTO/RPO for a database
dbbackup rto analyze --database mydb

# Show status for all databases
dbbackup rto status

# Check against targets
dbbackup rto check --target-rto 4h --target-rpo 1h
```

**Analysis includes:**
- Current RPO (time since last backup)
- Estimated RTO (detection + download + restore + validation)
- RTO breakdown by phase
- Compliance status
- Recommendations for improvement

## Systemd Integration

Install dbbackup as a systemd service for automated scheduled backups:

```bash
# Install with Prometheus metrics exporter
sudo dbbackup install --backup-type cluster --with-metrics

# Preview what would be installed
dbbackup install --dry-run --backup-type cluster

# Check installation status
dbbackup install --status

# Uninstall
sudo dbbackup uninstall cluster --purge
```

**Schedule options:**
```bash
--schedule daily                    # Every day at midnight (default)
--schedule weekly                   # Every Monday at midnight  
--schedule "*-*-* 02:00:00"         # Every day at 2am
--schedule "Mon *-*-* 03:00"        # Every Monday at 3am
```

**What gets installed:**
- Systemd service and timer units
- Dedicated `dbbackup` user with security hardening
- Directories: `/var/lib/dbbackup/`, `/etc/dbbackup/`
- Optional: Prometheus HTTP exporter on port 9399

**Full documentation:** [SYSTEMD.md](SYSTEMD.md) - Manual setup, security hardening, multiple instances, troubleshooting

## Prometheus Metrics

Export backup metrics for monitoring with Prometheus:

> **Migration Note (v1.x → v2.x):** The `--instance` flag was renamed to `--server` to avoid collision with Prometheus's reserved `instance` label. Update your cronjobs and scripts accordingly.

### Textfile Collector

For integration with node_exporter:

```bash
# Export metrics to textfile
dbbackup metrics export --output /var/lib/node_exporter/textfile_collector/dbbackup.prom

# Export for specific server
dbbackup metrics export --server production --output /var/lib/dbbackup/metrics/production.prom
```

Configure node_exporter:
```bash
node_exporter --collector.textfile.directory=/var/lib/node_exporter/textfile_collector/
```

### HTTP Exporter

Run a dedicated metrics HTTP server:

```bash
# Start metrics server on default port 9399
dbbackup metrics serve

# Custom port
dbbackup metrics serve --port 9100

# Run as systemd service (installed via --with-metrics)
sudo systemctl start dbbackup-exporter
```

**Endpoints:**
- `/metrics` - Prometheus exposition format
- `/health` - Health check (returns 200 OK)

**Available metrics:**
| Metric | Type | Description |
|--------|------|-------------|
| `dbbackup_last_success_timestamp` | gauge | Unix timestamp of last successful backup |
| `dbbackup_last_backup_duration_seconds` | gauge | Duration of last backup |
| `dbbackup_last_backup_size_bytes` | gauge | Size of last backup |
| `dbbackup_backup_total` | counter | Total backups by status (success/failure) |
| `dbbackup_rpo_seconds` | gauge | Seconds since last successful backup |
| `dbbackup_backup_verified` | gauge | Whether last backup was verified (1/0) |
| `dbbackup_scrape_timestamp` | gauge | When metrics were collected |

**Labels:** `instance`, `database`, `engine`

**Example Prometheus query:**
```promql
# Alert if RPO exceeds 24 hours
dbbackup_rpo_seconds{instance="production"} > 86400

# Backup success rate
sum(rate(dbbackup_backup_total{status="success"}[24h])) / sum(rate(dbbackup_backup_total[24h]))
```

## Configuration

### PostgreSQL Authentication

```bash
# Peer authentication
sudo -u postgres dbbackup backup cluster

# Password file
echo "localhost:5432:*:postgres:password" > ~/.pgpass
chmod 0600 ~/.pgpass

# Environment variable
export PGPASSWORD=password
```

### MySQL/MariaDB Authentication

```bash
# Environment variable (recommended)
export MYSQL_PWD='secret'
dbbackup backup single mydb --db-type mysql --user root

# Socket authentication (no password needed)
dbbackup backup single mydb --db-type mysql --socket /var/run/mysqld/mysqld.sock

# Configuration file
cat > ~/.my.cnf << EOF
[client]
user=root
password=secret
EOF
chmod 0600 ~/.my.cnf
```

> **Note:** The `--password` command-line flag is not supported for security reasons
> (passwords would be visible in `ps aux` output). Use environment variables or config files.

### Configuration Persistence

Settings are saved to `.dbbackup.conf` in the current directory:

```bash
--no-config       # Skip loading saved configuration
--no-save-config  # Prevent saving configuration
```

## Performance

### Restore Performance

| Scenario | Standard | Optimized | Improvement |
|----------|----------|-----------|-------------|
| 100GB uniform data | 2h30m | 1h39m | **1.5×** |
| 100GB fragmented | 12h+ | 3h20m | **3.6×** |
| 500GB + tiered restore | 8h full | 8 min (critical tables) | **60× RTO** |

### Throughput Benchmarks

| Operation | Throughput |
|-----------|------------|
| Dump (pgzip, 8 workers) | 2,048 MB/s |
| Restore (pgzip decompress) | 1,673 MB/s |
| Standard gzip baseline | 422 MB/s |

### Memory Usage

Streaming architecture maintains constant memory usage regardless of database size:

| Database Size | Memory Usage |
|---------------|--------------|
| 1–100+ GB | < 1 GB |

### Optimization

```bash
# High-performance backup
dbbackup backup cluster \
  --max-cores 32 \
  --jobs 32 \
  --cpu-workload cpu-intensive \
  --compression 3

# High-performance restore
dbbackup restore single dump.sql.gz \
  --restore-mode=balanced \
  --workers=16 \
  --tiered-restore \
  --critical-tables="user*,payment*"
```

Workload types:
- `balanced` — Default, suitable for most workloads
- `cpu-intensive` — Higher parallelism for fast storage
- `io-intensive` — Lower parallelism to avoid I/O contention

See [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) for advanced tuning.

### Automatic Performance Optimizations (v6.1+)

The following optimizations are applied automatically and degrade gracefully on older systems:

| Optimization | Improvement | How It Works |
|-------------|-------------|--------------|
| pgx Batch Pipeline | 15–30% faster DDL | Sends all batch statements in one network round-trip |
| WAL Compression | 10–20% less I/O | Compresses WAL during write-heavy restore phases |
| Unix Socket Auto-detect | 10–30% lower latency | Prefers local socket over TCP for localhost connections |
| BLOB-Aware Buffers | 20–40% faster BLOBs | Dynamically scales buffer size based on detected BLOB characteristics |
| Prepared Statement Cache | 5–10% faster init | Reuses server-side prepared statements for metadata queries |

**Expected combined impact by workload:**

| Workload | Improvement |
|----------|-------------|
| Small tables (< 100MB, many DDL) | **+25–35%** |
| Large tables (> 1GB, data-heavy) | **+30–60%** |
| BLOB-heavy (images, PDFs) | **+40–100%** |
| Mixed production workload | **+30–45%** |

All optimizations are hardware-independent and adapt to VPS (2 vCPU) through bare metal (64+ cores).

## Testing

dbbackup is tested daily on dedicated infrastructure:

- 5 production database servers running automated nightly backups
- Dedicated 16-core test node running the full test suite against real PostgreSQL and MySQL instances
- Every release is deployed via Ansible and validated across the fleet before tagging
- Prometheus monitoring with RPO/failure alerts on all nodes
- CI pipeline with race detection on every commit

## Troubleshooting

### TUI Connection Issues

If the TUI shows `[FAIL] Disconnected`:

1. **Check PostgreSQL is running:** `psql -U postgres -c "SELECT 1"`
2. **Verify connection settings:** `./dbbackup status`
3. **Test with CLI mode:** `./dbbackup backup single testdb` (bypasses TUI)

Connection health checks timeout after 5 seconds.

### Pre-Restore Failures

The pre-restore validation screen will block restore if:
- **Archive integrity fails** → Archive may be corrupted, try re-downloading
- **Insufficient disk space** → Free up space or use `--work-dir`
- **Missing privileges** → User needs CREATEDB: `ALTER USER youruser CREATEDB;`

See [docs/testing/phase1-manual-tests.md](docs/testing/phase1-manual-tests.md) for detailed troubleshooting.

## Requirements

**System:**
- Linux, macOS, FreeBSD, OpenBSD, NetBSD
- 1 GB RAM minimum
- Disk space: 30–50% of database size

**Native Engine (default — no external tools required):**
- PostgreSQL 10+ (via pgx protocol)
- MySQL 5.7+ / MariaDB 10.3+ (via go-sql-driver)

**External Tools (optional, used as fallback):**
- PostgreSQL: psql, pg_dump, pg_dumpall, pg_restore
- MySQL/MariaDB: mysql, mysqldump

## Documentation

**Getting Started:**
- [QUICK.md](QUICK.md) — Real-world examples cheat sheet
- [docs/MIGRATION_FROM_V5.md](docs/MIGRATION_FROM_V5.md) — Upgrade guide (v5.x → v6.0)
- [docs/DATABASE_COMPATIBILITY.md](docs/DATABASE_COMPATIBILITY.md) — Feature matrix (PG/MySQL/MariaDB)

**Performance & Restore:**
- [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) — Advanced optimization guide
- [docs/RESTORE_PERFORMANCE.md](docs/RESTORE_PERFORMANCE.md) — Restore performance analysis
- [docs/RESTORE_PROFILES.md](docs/RESTORE_PROFILES.md) — Restore resource profiles
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) — Common issues & solutions

**Database Engines:**
- [docs/ENGINES.md](docs/ENGINES.md) — Database engine configuration
- [docs/PITR.md](docs/PITR.md) — Point-in-Time Recovery (PostgreSQL)
- [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) — Point-in-Time Recovery (MySQL)

**Cloud Storage:**
- [docs/CLOUD.md](docs/CLOUD.md) — Cloud storage overview
- [docs/AZURE.md](docs/AZURE.md) — Azure Blob Storage
- [docs/GCS.md](docs/GCS.md) — Google Cloud Storage

**Deployment:**
- [docs/DOCKER.md](docs/DOCKER.md) — Docker deployment
- [docs/SYSTEMD.md](docs/SYSTEMD.md) — Systemd installation & scheduling

**Reference:**
- [SECURITY.md](SECURITY.md) -- Security considerations
- [CONTRIBUTING.md](CONTRIBUTING.md) -- Contribution guidelines
- [CHANGELOG.md](CHANGELOG.md) -- Version history
- [RELEASE_NOTES_v6.0.md](RELEASE_NOTES_v6.0.md) -- v6.0.0 release notes
- [docs/tui-features.md](docs/tui-features.md) -- TUI feature reference
- [docs/LOCK_DEBUGGING.md](docs/LOCK_DEBUGGING.md) -- Lock troubleshooting

## License

Apache License 2.0 - see [LICENSE](LICENSE).

Copyright 2025 dbbackup Project
