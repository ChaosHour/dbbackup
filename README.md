# dbbackup

Database backup and restore utility for PostgreSQL, MySQL, and MariaDB.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://golang.org/)
[![Release](https://img.shields.io/badge/Release-v6.45.0-green.svg)](https://github.com/PlusOne/dbbackup/releases/latest)

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
- [Restore Verification](#restore-verification)
- [Backup Status Dashboard](#backup-status-dashboard)
- [Percona XtraBackup / MariaBackup](#percona-xtrabackup--mariabackup)
- [DR Drill Testing](#dr-drill-testing)
- [Compliance Reports](#compliance-reports)
- [RTO/RPO Analysis](#rtorpo-analysis)
- [Systemd Integration](#systemd-integration)
- [Prometheus Metrics](#prometheus-metrics)
- [Configuration](#configuration)
- [Benchmarking](#benchmarking)
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

- **Percona XtraBackup / MariaBackup**: Physical hot backup engine for MySQL/MariaDB — full and incremental backups, streaming output, AES encryption, Galera cluster support, automatic binary detection
- **pg_basebackup Integration**: Physical backup via streaming replication for 100GB+ databases
- **WAL Archiving Manager**: pg_receivewal integration with replication slot management for true PITR
- **Table-Level Backup**: Selective backup by table pattern, schema, or row count
- **Pre/Post Hooks**: Run VACUUM ANALYZE, notify Slack, or custom scripts before/after backups
- **Bandwidth Throttling**: Rate-limit backup and upload operations (e.g., `--max-bandwidth 100M`)
- **Intelligent Compression**: Detects blob types (JPEG, PDF, archives) and recommends optimal compression
- **ZFS/Btrfs Detection**: Auto-detects filesystem compression and adjusts recommendations
- **PostgreSQL Large Object Vacuum**: Optional `--lo-vacuum` pre-backup cleanup of orphaned large objects (PG ≥14 uses native autovacuum; PG <14 uses `lo_unlink()` on orphans)

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
- **Automated Restore Verification**: `--verify-restore` creates temp DB, restores, compares every table's row count, prints color-coded report, always cleans up
- **Backup Status Dashboard**: `dbbackup status` shows per-database health table with age, size, encryption, and color-coded staleness indicators

### Performance Features

- **5-10x faster parallel restore** vs pg_dump/pg_restore baseline
- **90% RTO reduction** with tiered restore (critical tables first)
- **Engine-aware adaptive job sizing** -- 3-stage pipeline: BLOB adjustment, native engine boost, memory ceiling
- **I/O scheduler governors** -- noop, bfq, mq-deadline, deadline (auto-selected per BLOB strategy)
- **BLOB Pipeline Matrix** -- native large object backup/restore with parallel streaming
- **BLOB Type Detection** -- 30+ magic byte signatures + Shannon entropy; skips compressing JPEG/PNG/MP4/ZIP
- **Content-Addressed Dedup** -- bloom filter (2.74 MB for 2.4M BLOBs) + SHA-256 exact match
- **Split Backup Mode** -- schema + data + BLOBs in separate files for parallel restore
- **Nuclear Restore Engine** -- pure Go streaming SQL parser, global index builder, transaction batcher
- **Streaming I/O** with 256KB batch pipeline (constant memory usage)
- **UNLOGGED table optimization** during COPY phase (PostgreSQL balanced/turbo mode)
- **Bulk load optimizations** for MySQL/MariaDB (FOREIGN_KEY_CHECKS, UNIQUE_CHECKS, sql_log_bin, innodb_flush_log)
- **MySQL/MariaDB dump speedups** -- `--quick`, `--extended-insert`, `--disable-keys`, `--net-buffer-length=1MB`, `--max-allowed-packet=256M`, `--order-by-primary` (all on by default except order-by-primary)
- **MySQL/MariaDB fast restore** -- `--init-command` sets `fk_checks=0, unique_checks=0, autocommit=0, sql_log_bin=0` before import (2-5× faster)
- **Configurable INSERT batch size** -- Native engine groups 5,000 rows per INSERT (was 1,000), tunable via `--mysql-batch-size`
- **Index type detection** -- GIN/GIST indexes get 4GB RAM allocation and 8 parallel workers
- **Adaptive worker allocation** based on table size metadata with physical CPU core detection
- **Intel/AMD vendor-aware CPU tuning** -- auto-detects ISA features (AVX2, AVX-512, AES-NI, SHA-NI, NEON, SVE), hybrid P/E-core topology, cache hierarchy, NUMA layout, and memory bandwidth; selects optimal parallelism, compression algorithm, and buffer sizes per vendor
- **GOAMD64=v3 optimized build** -- dedicated Linux binary compiled with AVX2/BMI2 for 2015+ Intel/AMD CPUs (5-15% faster compression and hashing)

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
- **MySQL 5.7+** -- Native engine with bulk load optimizations and dump/restore speed flags; Percona XtraBackup physical backup support
- **MariaDB 10.3+** -- Full parity with MySQL engine plus Galera cluster support; MariaBackup physical backup support

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

### MySQL/MariaDB Performance Tuning (v6.43.0+)

All flags default to optimal values. Override only when needed:

| Flag | Default | Effect |
|------|---------|--------|
| `--mysql-quick` | true | Row-by-row fetch from server (constant memory) |
| `--mysql-extended-insert` | true | Multi-row INSERT statements (5-20× faster import) |
| `--mysql-order-by-primary` | false | Sort rows by primary key (better InnoDB import locality) |
| `--mysql-disable-keys` | true | DISABLE KEYS around data load (MyISAM/Aria) |
| `--mysql-net-buffer-length` | 1048576 | Network buffer size (1MB, max mysqldump allows) |
| `--mysql-max-packet` | 256M | max-allowed-packet for dump and restore |
| `--mysql-fast-restore` | true | SET fk_checks=0, unique_checks=0 before restore |
| `--mysql-batch-size` | 5000 | Rows per extended INSERT in native engine |

```bash
# MariaDB backup with maximum speed (all defaults already optimal)
dbbackup backup single mydb --db-type mariadb --user root

# With PK-sorted rows for InnoDB-heavy databases
dbbackup backup single mydb --db-type mariadb --mysql-order-by-primary

# Disable speed optimizations for conservative backup
dbbackup backup single mydb --db-type mysql \
    --mysql-quick=false \
    --mysql-extended-insert=false \
    --mysql-fast-restore=false

# Tune native engine batch size for very wide tables
dbbackup backup single mydb --db-type mariadb --mysql-batch-size=2000
```

All settings persist to `.dbbackup.conf` under `[performance]`.

### Percona XtraBackup / MariaBackup (v6.45.0+)

Physical hot backup engine for MySQL and MariaDB using Percona XtraBackup or MariaBackup. Produces consistent full or incremental backups without locking tables (InnoDB).

**Key capabilities:**
- **Auto-detection**: Automatically selects `xtrabackup` (Percona Server / MySQL) or `mariabackup` (MariaDB) based on detected database flavor
- **Full & incremental backups**: LSN-based incremental with checkpoint parsing
- **Streaming output**: xbstream or tar format for pipe-to-cloud workflows
- **AES encryption**: 128/192/256-bit encryption with key file support
- **Compression**: Built-in qpress/LZ4 compression with configurable thread count
- **Galera cluster support**: `--xtrabackup-galera-info` captures wsrep position
- **Replica-safe**: `--xtrabackup-slave-info` and `--xtrabackup-safe-slave` for replica backups
- **Progress monitoring**: Real-time progress parsing from xtrabackup output
- **Engine selector integration**: Automatically scored and recommended when xtrabackup/mariabackup is available

| Flag | Default | Description |
|------|---------|-------------|
| `--xtrabackup` | false | Enable XtraBackup engine |
| `--xtrabackup-parallel` | 1 | Parallel copy threads |
| `--xtrabackup-use-memory` | 128M | Memory for --prepare phase |
| `--xtrabackup-throttle` | 0 | I/O throttle (pairs/sec, 0=unlimited) |
| `--xtrabackup-no-lock` | false | Skip FTWRL (InnoDB only) |
| `--xtrabackup-slave-info` | false | Record replica position |
| `--xtrabackup-safe-slave` | false | Stop replica SQL thread during backup |
| `--xtrabackup-galera-info` | false | Record Galera wsrep position |
| `--xtrabackup-stream-format` | "" | Stream format (xbstream or tar) |
| `--xtrabackup-compress` | false | Enable built-in compression |
| `--xtrabackup-compress-threads` | 1 | Compression threads |
| `--xtrabackup-encrypt` | "" | Encryption algorithm (AES128/AES192/AES256) |
| `--xtrabackup-encrypt-key-file` | "" | Encryption key file path |
| `--xtrabackup-incr-basedir` | "" | Base directory for incremental backup |
| `--xtrabackup-incr-lsn` | "" | LSN for incremental backup |
| `--xtrabackup-extra-args` | "" | Additional xtrabackup arguments |

```bash
# Full physical backup with Percona XtraBackup
dbbackup backup single mydb --db-type mysql --xtrabackup

# Incremental backup based on previous LSN
dbbackup backup single mydb --db-type mysql --xtrabackup \
    --xtrabackup-incr-lsn="12345678"

# MariaDB with compression and parallel threads
dbbackup backup single mydb --db-type mariadb --xtrabackup \
    --xtrabackup-parallel=4 --xtrabackup-compress --xtrabackup-compress-threads=4

# Streaming to cloud storage
dbbackup backup single mydb --db-type mysql --xtrabackup \
    --xtrabackup-stream-format=xbstream --cloud s3://bucket/backups/

# Galera cluster node backup
dbbackup backup single mydb --db-type mariadb --xtrabackup \
    --xtrabackup-galera-info --xtrabackup-no-lock

# Encrypted backup
dbbackup backup single mydb --db-type mysql --xtrabackup \
    --xtrabackup-encrypt=AES256 --xtrabackup-encrypt-key-file=/path/to/keyfile
```

See [docs/ENGINES.md](docs/ENGINES.md) for full XtraBackup engine documentation.

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
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v6.30.0/dbbackup-linux-amd64
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
- **Detailed completion summary** -- press `D` after backup/restore for DBA-focused stats (exact bytes, per-DB throughput, resource usage, JSON export)
- **Paginated settings** -- 2-page layout fits 40-line terminals; navigate with `←`/`→`

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

# Single database backup — PostgreSQL custom format (.dump)
dbbackup backup single myapp_db --dump-format custom

# Cluster backup (all databases — PostgreSQL, MySQL, MariaDB)
dbbackup backup cluster

# MariaDB/MySQL cluster backup
dbbackup backup cluster --db-type mariadb --user root
dbbackup backup cluster --db-type mysql --user root --socket /var/run/mysqld/mysqld.sock

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

# Restore MariaDB/MySQL cluster
dbbackup restore cluster cluster_backup.tar.gz --db-type mariadb --user root --confirm

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
| **Backup** | |
| `backup single` | Backup single database |
| `backup cluster` | Backup all databases (PostgreSQL, MySQL, MariaDB) |
| `backup sample` | Backup with reduced data |
| **Restore** | |
| `restore single` | Restore single database |
| `restore cluster` | Restore full cluster |
| `restore pitr` | Point-in-Time Recovery restore |
| `restore diagnose` | Diagnose backup file integrity |
| `restore preview` | Preview backup contents before restoring |
| **Dedup** | |
| `dedup backup` | Create deduplicated backup of a file |
| `dedup backup-db` | Direct database dump with deduplication |
| `dedup restore` | Restore from deduplicated backup |
| `dedup list` | List deduplicated backups |
| `dedup stats` | Show deduplication statistics |
| `dedup gc` | Garbage collect unreferenced chunks |
| `dedup verify` | Verify chunk integrity against manifests |
| `dedup prune` | Apply retention policy to manifests |
| `dedup delete` | Delete a backup manifest |
| `dedup metrics` | Export dedup stats as Prometheus metrics |
| **Verification** | |
| `verify-backup` | Verify backup integrity (updates catalog for Prometheus metrics) |
| `verify-locks` | Check PostgreSQL lock settings and get restore guidance |
| `verify-restore` | Systematic verification for large database restores (also `--verify-restore` flag on backup) |
| **PITR** | |
| `pitr enable` | Enable PostgreSQL PITR (WAL archiving) |
| `pitr disable` | Disable PostgreSQL PITR |
| `pitr status` | Show PITR/WAL archive status |
| `pitr mysql-enable` | Enable MySQL/MariaDB PITR (binary logging) |
| `pitr mysql-status` | Show MySQL/MariaDB PITR status |
| **WAL / Binlog** | |
| `wal archive` | Archive a WAL file |
| `wal list` | List archived WAL files |
| `wal cleanup` | Remove old WAL archives |
| `wal timeline` | Show timeline branching history |
| `binlog list` | List binary log files |
| `binlog archive` | Archive binary log files |
| `binlog watch` | Watch and auto-archive new binlog files |
| `binlog validate` | Validate binlog chain integrity |
| `binlog position` | Show current binary log position |
| **Cloud** | |
| `cloud upload` | Upload backup to cloud storage |
| `cloud download` | Download backup from cloud storage |
| `cloud list` | List backups in cloud storage |
| `cloud delete` | Delete backup from cloud storage |
| `cloud status` | Check cloud storage connectivity |
| `cloud sync` | Sync local backups to cloud storage |
| `cloud cross-region-sync` | Sync backups between cloud regions |
| **Catalog** | |
| `catalog sync` | Sync backups from directory into catalog |
| `catalog list` | List backups in catalog |
| `catalog stats` | Show catalog statistics |
| `catalog gaps` | Detect gaps in backup schedule |
| `catalog search` | Search backups in catalog |
| `catalog info` | Show detailed info for a backup |
| `catalog export` | Export catalog to CSV/HTML/JSON |
| `catalog dashboard` | Interactive catalog browser (TUI) |
| `catalog check` | Verify backup archives and metadata |
| **Analysis** | |
| `diff` | Compare two backups and show differences |
| `estimate single` | Estimate single database backup size |
| `estimate cluster` | Estimate full cluster backup size |
| `forecast` | Predict future disk space requirements |
| `cost analyze` | Analyze cloud storage costs |
| `compression analyze` | Analyze database for optimal compression |
| `retention-simulator` | Simulate retention policy effects |
| **Infrastructure** | |
| `status` | Check connection status + backup dashboard (age, size, encryption, health) |
| `preflight` | Run pre-backup checks |
| `health` | Check backup system health |
| `list` | List databases and backups |
| `cpu` | Show CPU optimization settings |
| `profile` | Profile system and show recommended settings |
| `diagnose` | Troubleshoot common backup issues with auto-fix |
| `validate` | Validate configuration and environment |
| `chain` | Show backup chain (full → incremental) |
| **Monitoring** | |
| `rto analyze` | Analyze RTO/RPO for databases |
| `rto status` | Show RTO/RPO status summary |
| `rto check` | Check RTO/RPO compliance |
| `metrics export` | Export Prometheus metrics to textfile |
| `metrics serve` | Run Prometheus HTTP exporter |
| `report generate` | Generate compliance report |
| `notify test` | Test notification integrations |
| **DR & Migration** | |
| `drill run` | Run DR drill on a backup |
| `drill quick` | Quick restore test with minimal validation |
| `drill list` | List DR drill containers |
| `drill cleanup` | Cleanup DR drill containers |
| `migrate single` | Migrate single database to target server |
| `migrate cluster` | Migrate entire cluster to target server |
| **Benchmarking** | |
| `benchmark run` | Benchmark a single database (backup + restore + verify) |
| `benchmark matrix` | Cross-engine comparison across all databases |
| `benchmark history` | Show past benchmark results from the catalog |
| `benchmark show` | Display the full report for a specific run |
| **Operations** | |
| `cleanup` | Remove old backups (supports GFS retention) |
| `install` | Install as systemd service |
| `uninstall` | Remove systemd service |
| `schedule` | Show scheduled backup times |
| `interactive` | Start interactive TUI |
| `blob stats` | Analyze blob/bytea columns in database |
| `blob backup` | Backup BLOBs with parallel pipeline |
| `blob restore` | Restore BLOBs from blob archive |
| `encryption rotate` | Rotate encryption keys |
| `engine list` | List available backup engines |
| `parallel-restore` | Configure and test parallel restore |
| `completion` | Generate shell completion scripts |
| `version` | Show version and system information |
| `man` | Generate man pages |

## Global Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-d, --db-type` | Database type (postgres, mysql, mariadb) | postgres |
| `--host` | Database host | localhost |
| `--port` | Database port | 5432/3306 |
| `--user` | Database user | current user |
| `--socket` | Unix socket path (MySQL/MariaDB) | - |
| `MYSQL_PWD` / `PGPASSWORD` | Database password (env var) | - |
| `--backup-dir` | Backup directory | ~/db_backups |
| `--compression` | Compression level (0-9) | 6 |
| `--jobs` | Parallel jobs | 8 |
| `--dump-jobs` | Parallel dump jobs | (jobs) |
| `--max-cores` | Maximum CPU cores to use | all |
| `--cpu-workload` | Workload type (cpu-intensive/io-intensive/balanced) | balanced |
| `--profile` | Resource profile (conservative/balanced/aggressive) | balanced |
| `--adaptive` | Adaptive per-DB job sizing (overlays profile) | true |
| `--io-governor` | I/O scheduler governor (auto/noop/bfq/mq-deadline/deadline) | auto |
| `--cloud` | Cloud storage URI | - |
| `--encrypt` | Enable encryption | false |
| `--dry-run, -n` | Run preflight checks only | false |
| `--verify-restore` | After backup, restore to temp DB and compare row counts | false |
| `--debug` | Enable debug logging (activates DEBUG log level) | false |
| `--debug-locks` | Enable detailed lock debugging | false |
| `--save-debug-log` | Save error report to file on failure | - |
| `--native, --native-engine` | Use native Go backup/restore engine | true |
| `--fallback-tools` | Fallback to external tools if native engine fails | false |
| `--native-debug` | Enable detailed native engine debugging | false |
| `--ssl-mode` | SSL mode for connections | - |
| `--insecure` | Disable SSL (shortcut for --ssl-mode=disable) | false |
| `--retention-days` | Backup retention period in days (0=disabled) | 0 |
| `--min-backups` | Minimum number of backups to keep | 0 |
| `--max-retries` | Maximum connection retry attempts | 3 |
| `--no-config` | Skip loading saved configuration | false |
| `--no-save-config` | Prevent saving configuration | false |
| `--no-color` | Disable colored output | false |
| `--allow-root` | Allow running as root/Administrator | false |
| `--check-resources` | Check system resource limits | false |
| `--cpu-auto-tune` | Vendor-aware CPU auto-tuning (Intel/AMD/ARM) | true |
| `--cpu-boost` | Set CPU governor to 'performance' during backup | false |

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

## PostgreSQL Custom Format

PostgreSQL custom format (`.dump`) enables parallel restore and selective table recovery:

```bash
# Single database — custom format
dbbackup backup single mydb --dump-format custom

# Cluster backup uses custom format by default for all PG databases
dbbackup backup cluster

# Available formats: custom | plain | directory | tar
dbbackup backup single mydb --dump-format tar

# Restore from custom format (parallel restore with -j)
dbbackup restore single mydb.dump --target mydb --confirm --jobs 8
```

**Benefits of custom format:**
- **2-3× faster restores** via TOC-based parallel processing
- **Selective restore**: restore specific tables with `--include-table` / `--exclude-table`
- **Built-in compression**: no separate gzip step needed
- **Random access**: seek directly to any table's data block

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

## Deduplicated Backups

Content-addressed deduplication for storage-efficient backups. Uses SHA-256 chunking with a Bloom filter to eliminate duplicate data across backups.

```bash
# Deduplicated backup of a file
dbbackup dedup backup /backups/mydb_20250601.dump.gz

# Direct database dump with deduplication
dbbackup dedup backup-db mydb --db-type mariadb --user root

# List deduplicated backups
dbbackup dedup list

# Show deduplication statistics (savings, chunk counts)
dbbackup dedup stats

# Restore from dedup backup
dbbackup dedup restore <manifest-id> /tmp/restored.dump.gz

# Verify chunk integrity
dbbackup dedup verify

# Garbage collect unreferenced chunks
dbbackup dedup gc

# Apply retention policy to manifests
dbbackup dedup prune --retention-days 90

# Export dedup metrics for Prometheus
dbbackup dedup metrics --output /var/lib/node_exporter/textfile_collector/dedup.prom
```

**Storage savings** depend on data similarity between backups. Typical results:
- Daily backups of slowly-changing databases: **50–80%** reduction
- Databases with large static BLOBs: **60–90%** reduction
- Highly volatile databases: **20–40%** reduction

## Point-in-Time Recovery

PITR allows restoring databases to any specific point in time.

### PostgreSQL PITR

```bash
# Enable PITR (WAL archiving)
dbbackup pitr enable --archive-dir /backups/wal_archive

# Check PITR status
dbbackup pitr status

# Restore to timestamp
dbbackup restore pitr \
  --base-backup /backups/base.tar.gz \
  --wal-archive /backups/wal_archive \
  --target-time "2024-11-26 12:00:00" \
  --target-dir /var/lib/postgresql/14/restored
```

### MySQL/MariaDB PITR

```bash
# Enable binary logging for PITR
dbbackup pitr mysql-enable --db-type mariadb --user root

# Check MySQL/MariaDB PITR status
dbbackup pitr mysql-status --db-type mariadb --user root

# List binary log files
dbbackup binlog list --db-type mariadb --user root

# Archive binary logs
dbbackup binlog archive --db-type mariadb --user root --archive-dir /backups/binlogs

# Watch for new binlogs and auto-archive
dbbackup binlog watch --db-type mariadb --user root --archive-dir /backups/binlogs

# Validate binlog chain integrity
dbbackup binlog validate --db-type mariadb --user root
```

See [PITR.md](PITR.md) for PostgreSQL and [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) for MySQL/MariaDB.

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

## Restore Verification

Automated post-backup verification creates a temporary database, restores the backup, and compares every table's row count against the source:

```bash
# Backup with automatic restore verification
dbbackup backup single mydb --verify-restore

# Also works with cluster and sample backups
dbbackup backup cluster --verify-restore
dbbackup backup sample mydb --verify-restore --ratio 10

# Persist the setting (always verify)
dbbackup backup single mydb --verify-restore
# Next run will auto-verify from .dbbackup.conf
```

**What happens:**
1. Backup completes normally
2. Creates temporary database `_dbbackup_verify_<timestamp>`
3. Restores the backup into the temp database
4. Compares every table's row count (source vs. restored)
5. Prints color-coded verification report
6. Drops the temporary database (always, even on failure)

**Example output:**
```
╔═══════════════════════════════════════════════════════════╗
║              Restore Verification Report                  ║
╠═══════════════════════════════════════════════════════════╣
║ Database: mydb → _dbbackup_verify_20260216_123456         ║
╠════════════════════╦══════════╦══════════╦════════════════╣
║ TABLE              ║ SOURCE   ║ RESTORED ║ STATUS         ║
╠════════════════════╬══════════╬══════════╬════════════════╣
║ public.users       ║   50,000 ║   50,000 ║ ✓ PASS         ║
║ public.orders      ║   25,000 ║   25,000 ║ ✓ PASS         ║
║ public.products    ║    1,200 ║    1,200 ║ ✓ PASS         ║
║ public.sessions    ║   30,000 ║   30,000 ║ ✓ PASS         ║
╠════════════════════╬══════════╬══════════╬════════════════╣
║ TOTAL ROWS         ║  106,200 ║  106,200 ║ ALL PASS       ║
╚════════════════════╩══════════╩══════════╩════════════════╝
  Duration: 4.2s
```

**Notes:**
- Works with PostgreSQL, MySQL, and MariaDB
- Compatible with both native Go engine and tool-based (pg_dump/mysqldump) backups
- Temp database is always cleaned up, even if verification fails
- Setting persists to `.dbbackup.conf` as `verify_restore = true`

## Backup Status Dashboard

The `status` command displays a rich overview of all backups in the backup directory:

```bash
# Show backup dashboard
dbbackup status

# With explicit backup directory
dbbackup status --backup-dir /mnt/backups
```

**Example output:**
```
╔═══════════════════════════════════════════════════════════════════════════╗
║                        Backup Status Dashboard                           ║
╠══════════════════╦═══════════════════╦════════╦════════╦═════╦═══════════╣
║ DATABASE         ║ LAST BACKUP       ║ AGE    ║ SIZE   ║ ENC ║ STATUS    ║
╠══════════════════╬═══════════════════╬════════╬════════╬═════╬═══════════╣
║ production_db    ║ 2026-02-16 08:00  ║ 4h     ║ 245 MB ║ Yes ║ ✓ OK      ║
║ analytics_db     ║ 2026-02-16 08:00  ║ 4h     ║ 1.2 GB ║ Yes ║ ✓ OK      ║
║ users_db         ║ 2026-02-14 08:00  ║ 2d     ║ 89 MB  ║ No  ║ ⚠ AGING   ║
║ legacy_db        ║ 2026-02-01 03:00  ║ 15d    ║ 456 MB ║ No  ║ ✗ STALE   ║
╚══════════════════╩═══════════════════╩════════╩════════╩═════╩═══════════╝

  Total Backups: 4
  Summary: 2 OK | 1 AGING | 1 STALE
```

**Color coding:**
- **Green (OK)**: Last backup within 24 hours
- **Yellow (AGING)**: Last backup 1–7 days old
- **Red (STALE)**: Last backup older than 7 days

The dashboard scans `.meta.json` metadata files for accurate backup information including timestamps, file sizes, and encryption status.

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

## Benchmarking

Built-in benchmark framework for measuring backup, restore, and verify performance with statistical rigor. Runs multiple iterations and reports min/avg/median/p95/stddev, throughput in MB/s, peak RSS, and compression ratios. Results are saved to the catalog DB and to JSON/Markdown files.

### Quick Start

```bash
# Benchmark PostgreSQL with 3 iterations
dbbackup benchmark run --db-type postgres --database mydb --iterations 3

# Benchmark MySQL with zstd compression
dbbackup benchmark run --db-type mysql --database mydb --compression zstd

# Cross-engine comparison (auto-detects qa_*/bench_* databases)
dbbackup benchmark matrix --iterations 2

# View past results
dbbackup benchmark history --last 10

# Drill into a specific run (JSON output)
dbbackup benchmark show <run-id> --json
```

### Bash Wrappers

Convenience scripts in `scripts/`:

```bash
./scripts/bench_postgres.sh mydb 3      # PostgreSQL, 3 iterations
./scripts/bench_mysql.sh mydb 3         # MySQL
./scripts/bench_mariadb.sh mydb 3       # MariaDB
./scripts/bench_all.sh 2                # Cross-engine matrix
```

Environment variables: `BENCH_HOST`, `BENCH_PORT`, `BENCH_USER`, `BENCH_WORKERS`, `BENCH_COMP`, `BENCH_SOCKET`.

### Makefile Targets

```bash
make bench                           # Full cross-engine matrix
make bench-pg BENCH_DB=mydb          # PostgreSQL only
make bench-mysql BENCH_DB=mydb       # MySQL only
make bench-maria BENCH_DB=mydb       # MariaDB only
make bench-history                   # View past results
```

### Sample Output

```
╔══════════════════════════════════════════════════════════════╗
║                    BENCHMARK RESULTS                        ║
╠══════════════════════════════════════════════════════════════╣
║  Engine:     postgres                                       ║
║  Database:   mydb                                           ║
║  DB Size:    5319.0 MB                                      ║
║  Iterations: 3                                              ║
╠══════════════════════════════════════════════════════════════╣
║  Phase     Min       Avg       Median    P95       MB/s     ║
║  backup     40.12s    42.47s    42.19s    44.80s   125.3    ║
║  restore   148.50s   152.66s   151.90s   156.20s    34.8   ║
║  verify      4.20s     4.55s     4.50s     4.90s     —     ║
╚══════════════════════════════════════════════════════════════╝
```

Results are persisted to:
- **Catalog DB** (`~/.config/dbbackup/catalog.db`) — queryable via `benchmark history` / `benchmark show`
- **JSON** (`reports/<run_id>.json`) — machine-readable, includes per-iteration detail and peak RSS
- **Markdown** (`reports/<run_id>.md`) — human-readable summary table

### Benchmark Flags

| Flag | Description | Default |
|------|-------------|--------|
| `--iterations` | Number of benchmark iterations | 3 |
| `--workers` | Parallel workers | auto (CPU count) |
| `--compression` | Compression algorithm (gzip/zstd/none) | gzip |
| `--compression-level` | Compression level (0-9) | 6 |
| `--profile` | Resource profile | balanced |
| `--verify` | Run verify phase after backup | true |
| `--clean` | Remove backups between iterations | true |
| `--dump-format` | PostgreSQL dump format (custom/plain/directory) | — |
| `--native` | Use native Go engine | true |
| `--json` | Print JSON to stdout | false |
| `--catalog-db` | Custom catalog DB path | auto |

### Automatic Performance Optimizations (v6.1+)

The following optimizations are applied automatically and degrade gracefully on older systems:

| Optimization | Improvement | How It Works |
|-------------|-------------|--------------|
| pgx Batch Pipeline | 15–30% faster DDL | Sends all batch statements in one network round-trip |
| WAL Compression | 10–20% less I/O | Compresses WAL during write-heavy restore phases |
| Unix Socket Auto-detect | 10–30% lower latency | Prefers local socket over TCP for localhost connections |
| BLOB-Aware Buffers | 20–40% faster BLOBs | Dynamically scales buffer size based on detected BLOB characteristics |
| BLOB Type Detection | 20–50% less I/O | Skips compressing pre-compressed BLOBs (JPEG, PNG, GZIP, ZSTD, MP4) |
| Content-Addressed Dedup | Up to 60% less storage | Bloom filter + SHA-256 eliminates duplicate BLOBs across tables |
| Split Backup Mode | 2–5× faster BLOB restore | Schema/data/BLOBs in separate files for parallel phase restore |
| Prepared Statement Cache | 5–10% faster init | Reuses server-side prepared statements for metadata queries |
| MySQL `--quick` + `--extended-insert` | 2–5× faster dump | Row-by-row transfer + multi-row INSERTs reduce overhead |
| MySQL `--init-command` fast restore | 2–5× faster import | Disables FK/unique checks and binary logging during bulk load |
| MySQL 5000-row batch INSERTs | 20–40% faster native | Larger INSERT batches reduce per-statement overhead |
| Vendor-Aware CPU Tuning | 10–25% faster backup | AMD gets aggressive parallelism, Intel uses 75% cores with larger batches, ARM optimized for NEON/SVE |
| ISA-Based Compression Selection | 5–15% faster compress | Auto-selects zstd when AVX2/AVX-512/SVE detected, gzip otherwise |
| Cache-Aware Buffer Sizing | 5–10% less cache thrash | Buffer and batch sizes derived from L2/L3 cache topology per core |
| Hybrid P/E-Core Detection | 10–20% better throughput | Intel 12th+ gen: schedules work on P-cores, avoids E-core bottleneck |
| NUMA-Aware Worker Distribution | 5–15% on multi-socket | Workers distributed 70/30 toward preferred NUMA node to reduce cross-node traffic |
| Frequency Governor Warning | Avoids 2–3× slowdown | Warns when CPU governor is 'powersave' and recommends 'performance' |

**Expected combined impact by workload:**

| Workload | Improvement |
|----------|-------------|
| Small tables (< 100MB, many DDL) | **+25–35%** |
| Large tables (> 1GB, data-heavy) | **+30–60%** |
| BLOB-heavy (images, PDFs) | **+40–100%** |
| Mixed production workload | **+30–45%** |

All optimizations are hardware-independent and adapt to VPS (2 vCPU) through bare metal (64+ cores).

### CPU Architecture Optimization (v6.46.0+)

dbbackup now performs deep hardware introspection to squeeze maximum performance from your specific CPU:

```bash
# View your system's CPU optimization report
dbbackup cpu

# Example output on AMD EPYC:
#   Vendor: AuthenticAMD (AMD Server/Desktop)
#   ISA Features: SSE4.2 AVX AVX2 AES-NI PCLMULQDQ
#   GOAMD64 Level: v3 (AVX2+BMI2 — use _v3 binary for best performance)
#   Recommended Compression: zstd (hardware-accelerated)
#   Cache: L1d=32KB L2=512KB L3=16MB → Buffer=256KB, Batch=7500
#   Jobs: 16 (aggressive AMD tuning), DumpJobs: 12
#   Governor: performance ✓
#   NUMA: 1 node, 16 CPUs, 32GB
```

**Key capabilities:**

| Feature | Intel | AMD | ARM |
|---------|-------|-----|-----|
| ISA detection (AVX2/512/NEON/SVE) | ✓ | ✓ | ✓ |
| Hybrid P/E-core topology | ✓ (12th gen+) | — | — |
| Vendor-tuned parallelism | 75% cores | All cores | All cores |
| AVX-512 throttle warning | ✓ (pre-Sapphire Rapids) | — | — |
| Cache-aware buffer sizing | ✓ | ✓ | ✓ |
| NUMA worker distribution | ✓ | ✓ | — |
| Frequency governor check | ✓ | ✓ | ✓ |
| Memory bandwidth estimate | ✓ | ✓ | ✓ |

**CLI flags:**

| Flag | Description | Default |
|------|-------------|---------|
| `--cpu-auto-tune` | Enable vendor-aware auto-tuning | `true` |
| `--cpu-boost` | Set governor to 'performance' during backup | `false` |

**GOAMD64=v3 optimized binary:**

For 2015+ Intel (Broadwell) and AMD (Excavator/Zen) CPUs, a dedicated binary compiled with `GOAMD64=v3` enables AVX2 and BMI2 instructions for 5–15% faster compression and hashing:

```bash
# Download the v3 binary (Linux only)
wget https://github.com/PlusOne/dbbackup/releases/latest/download/dbbackup_linux_amd64_v3
chmod +x dbbackup_linux_amd64_v3

# Check if your CPU supports it
dbbackup cpu  # Look for "GOAMD64 Level: v3" or higher
```

**TUI Configuration Panel (v6.47.0+):**

The interactive TUI (`dbbackup interactive`) now includes a dedicated **CPU Optimization** panel where you can:

- View detected hardware (vendor, ISA features, cache, NUMA, governor)
- Toggle individual auto-tune subsystems on/off (vendor tuning, ISA compression, cache-aware buffers, NUMA, governor boost)
- Override detected values (jobs, dump jobs, batch size, buffer KB, compression)
- Save settings to `.dbbackup.conf` or reset all to auto-detected defaults

### HugePages Integration (v6.17+)

On Linux systems with HugePages configured, dbbackup automatically detects and recommends optimal `shared_buffers` settings for PostgreSQL. HugePages reduce TLB misses and can provide a **30–50% improvement** in shared-buffer-heavy workloads.

**What dbbackup does:**

1. **Detects HugePages** — reads `/proc/meminfo` for `HugePages_Total`, `HugePages_Free`, and `Hugepagesize`
2. **Recommends shared_buffers** — calculates 75% of total HugePages memory as the optimal `shared_buffers` value
3. **Warns on Connect** — if HugePages are available but PostgreSQL's `huge_pages` setting is `off`, a warning is logged with the recommended configuration
4. **Displays in TUI & CLI** — the system profile view shows HugePages status, page count, and recommended settings

**Setting up HugePages (example for 1 GB shared_buffers):**

```bash
# 1. Calculate pages needed: shared_buffers / 2MB + overhead
#    1 GB / 2 MB = 512 + ~88 overhead = 600
sudo sysctl -w vm.nr_hugepages=600

# Make persistent in /etc/sysctl.d/99-postgresql.conf
echo "vm.nr_hugepages = 600" >> /etc/sysctl.d/99-postgresql.conf

# 2. Allow the postgres group to use HugePages
PG_GID=$(id -g postgres)
echo "vm.hugetlb_shm_group = $PG_GID" >> /etc/sysctl.d/99-postgresql.conf
sudo sysctl -w vm.hugetlb_shm_group=$PG_GID

# 3. Set memlock limit for PostgreSQL (systemd overrides /etc/security/limits.conf)
mkdir -p /etc/systemd/system/postgresql@17-main.service.d
cat > /etc/systemd/system/postgresql@17-main.service.d/hugepages.conf << 'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
sudo systemctl daemon-reload

# 4. Disable Transparent HugePages (causes latency spikes with PostgreSQL)
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
# Make persistent:
cat > /etc/tmpfiles.d/thp.conf << 'EOF'
w /sys/kernel/mm/transparent_hugepage/enabled - - - - madvise
w /sys/kernel/mm/transparent_hugepage/defrag - - - - defer+madvise
EOF

# 5. Enable in PostgreSQL and restart
sed -i 's/^#huge_pages = try/huge_pages = on/' /etc/postgresql/*/main/postgresql.conf
sudo systemctl restart postgresql

# 6. Verify HugePages are in use
grep HugePages /proc/meminfo
# HugePages_Rsvd should be > 0 (pages reserved by PostgreSQL)
```

> **Note:** If `nr_hugepages` allocates fewer pages than requested, drop caches first:
> `sync && echo 3 > /proc/sys/vm/drop_caches && sysctl -w vm.nr_hugepages=600`

**Scaling HugePages by shared_buffers size:**

| `shared_buffers` | `nr_hugepages` | Reserved Memory |
|-----------------|----------------|----------------|
| 256 MB | 200 | 400 MB |
| 1 GB | 600 | 1.2 GB |
| 4 GB | 2200 | 4.4 GB |
| 8 GB | 4200 | 8.4 GB |

**Verify with dbbackup:**

```bash
./dbbackup profile --dsn "postgres://user:pass@localhost/mydb"
# Look for the 📐 HugePages section in the output
```

### Linux Kernel Tuning (sysctl)

For database servers running dbbackup, tuning `sysctl` can significantly improve backup and restore throughput. Below is a production-tested example for a **32 GB / 16-core** PostgreSQL server:

```bash
# /etc/sysctl.d/99-postgresql.conf
# PostgreSQL / Database Server Tuning — 32 GB RAM, 16 cores

# --- Memory / VM ---
vm.swappiness = 1                       # Minimize swap usage (or 0 if no swap)
vm.overcommit_memory = 2                # PostgreSQL recommended: don't overcommit
vm.overcommit_ratio = 80                # Allow up to 80% RAM commitment
vm.vfs_cache_pressure = 50              # Keep dentries/inodes cached longer

# Dirty page tuning — smooth out large write bursts during backup/restore
vm.dirty_background_ratio = 5           # Start flushing at 5% dirty pages
vm.dirty_ratio = 40                     # Block writers at 40% dirty pages
vm.dirty_expire_centisecs = 3000        # Expire dirty pages after 30s
vm.dirty_writeback_centisecs = 500      # Flush daemon wakes every 5s

# --- Shared Memory (PostgreSQL) ---
kernel.shmmax = 26843545600             # 25 GB — must be >= shared_buffers
kernel.shmall = 6553600                 # shmmax / PAGE_SIZE (4096)
kernel.sem = 250 32000 100 128          # Semaphores for PostgreSQL

# HugePages for PostgreSQL shared_buffers (see HugePages section above)
# vm.nr_hugepages = 600                 # shared_buffers / 2MB + overhead
# vm.hugetlb_shm_group = 104            # GID of postgres group

# --- Network (backup to SMB / NFS / cloud) ---
net.core.rmem_max = 16777216            # 16 MB receive buffer max
net.core.wmem_max = 16777216            # 16 MB send buffer max
net.core.rmem_default = 1048576         # 1 MB receive default
net.core.wmem_default = 1048576         # 1 MB send default
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216
net.ipv4.tcp_max_syn_backlog = 8192

# Faster connection recycling (helps dbbackup parallel workers)
net.ipv4.tcp_fin_timeout = 15           # Default 60 — release sockets faster
net.ipv4.tcp_tw_reuse = 1               # Reuse TIME_WAIT for outgoing

# Shorter keepalive for database connections
net.ipv4.tcp_keepalive_time = 600       # Default 7200 — detect dead peers in 10m
net.ipv4.tcp_keepalive_intvl = 30       # Probe interval
net.ipv4.tcp_keepalive_probes = 5       # Give up after 5 failed probes

# --- Filesystem ---
fs.file-max = 2097152                   # 2M open file handles
```

Apply with:

```bash
sudo sysctl --system
```

**Scaling guidelines:**

| RAM | `shmmax` | `overcommit_ratio` | `dirty_ratio` |
|-----|----------|--------------------|---------------|
| 8 GB | 6 GB | 80 | 30 |
| 16 GB | 12 GB | 80 | 35 |
| 32 GB | 25 GB | 80 | 40 |
| 64 GB+ | 50 GB | 80 | 40 |

> **Tip:** Pair these settings with HugePages (above) for maximum performance. On a 32 GB server with `shared_buffers=8GB`, set `vm.nr_hugepages=4200` (8 GB / 2 MB + ~100 overhead).

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
- Percona XtraBackup: xtrabackup 8.0+ (for MySQL/Percona Server physical backups)
- MariaBackup: mariabackup 10.3+ (for MariaDB physical backups)

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
- [docs/ENGINES.md](docs/ENGINES.md) — Database engine configuration (includes XtraBackup/MariaBackup)
- [docs/PITR.md](docs/PITR.md) — Point-in-Time Recovery (PostgreSQL)
- [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) — Point-in-Time Recovery (MySQL/MariaDB)

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

Copyright 2025-2026 dbbackup Project
