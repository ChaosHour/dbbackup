# dbbackup

High-performance database backup and restore utility for PostgreSQL, MySQL, and MariaDB. Built in Go with hardware introspection, streaming architecture, and parallel processing.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://golang.org/)
[![Release](https://img.shields.io/badge/Release-v6.50.23-green.svg)](https://github.com/PlusOne/dbbackup/releases/latest)

**Repository:** https://git.uuxo.net/UUXO/dbbackup
**Mirror:** https://github.com/PlusOne/dbbackup

## Table of Contents

- [Quick Start](#quick-start-30-seconds)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Commands](#commands)
- [Global Flags](#global-flags)
- [Encryption](#encryption)
- [PostgreSQL Custom Format](#postgresql-custom-format)
- [Incremental Backups](#incremental-backups)
- [Cloud Storage](#cloud-storage)
- [Deduplicated Backups](#deduplicated-backups)
- [Point-in-Time Recovery](#point-in-time-recovery)
- [Backup Cleanup & GFS Retention](#backup-cleanup--gfs-retention)
- [Dry-Run Mode](#dry-run-mode)
- [Notifications](#notifications)
- [Backup Catalog](#backup-catalog)
- [Monitoring & Compliance](#monitoring--compliance)
- [Systemd Integration](#systemd-integration)
- [Prometheus Metrics](#prometheus-metrics)
- [Configuration](#configuration)
- [Performance](#performance)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
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

**That's it!** Backups are stored in `./backups/` by default.

## Features

### Native Database Engines

**Pure Go implementation -- no external tools required.**

- Direct PostgreSQL (pgx) and MySQL (go-sql-driver) protocol communication
- No pg_dump, mysqldump, pg_restore, mysql, psql, mysqlbinlog needed
- Full SQL generation, type handling, connection management, and binary data support
- Falls back to external tools if needed (`--fallback-tools`)

### Database Support

- **PostgreSQL 10+** -- UNLOGGED tables, parallel DDL, adaptive workers
- **MySQL 5.7+** -- Native engine with bulk load optimizations; Percona XtraBackup physical backup
- **MariaDB 10.3+** -- Full MySQL parity plus Galera cluster support; MariaBackup physical backup

### Core Capabilities

- **Backup modes**: Single database, cluster (all DBs), sample data
- **AES-256-GCM encryption** with key rotation support
- **Incremental backups** (LSN-based for XtraBackup)
- **Cloud storage**: S3, MinIO, B2, Azure Blob, Google Cloud Storage, SFTP
- **Point-in-Time Recovery**: WAL archiving (PostgreSQL), binlog archiving (MySQL/MariaDB)
- **GFS retention policies**: Grandfather-Father-Son backup rotation
- **Content-addressed dedup**: Bloom filter + SHA-256 chunking (50-90% storage reduction)
- **Dry-run mode**: Preflight checks before backup execution

### Enterprise Features

- **Percona XtraBackup / MariaBackup**: Physical hot backup engine -- see [docs/ENGINES.md](docs/ENGINES.md)
- **pg_basebackup / WAL archiving**: Streaming replication backup for 100GB+ databases
- **Backup catalog**: SQLite-based tracking with gap detection and search
- **DR drill testing**: Automated restore testing in Docker containers
- **Compliance reports**: SOC2, GDPR, HIPAA, PCI-DSS, ISO27001 generation
- **RTO/RPO calculator**: Recovery objective analysis and recommendations
- **Restore verification**: `--verify-restore` creates temp DB, restores, compares row counts
- **Status dashboard**: `dbbackup status` shows per-database health with age, size, encryption
- **Notifications**: SMTP email and webhook alerts with batching and escalation
- **Prometheus metrics**: Textfile collector and HTTP exporter (port 9399)
- **Systemd integration**: Install as service with scheduled timers
- **Replica-aware backup**: Automatic backup from replicas to reduce primary load
- **Bandwidth throttling**: Rate-limit backup and upload operations
- **Pre/Post hooks**: Run VACUUM ANALYZE, notify Slack, or custom scripts

### Performance

- **5-10x faster parallel restore** vs pg_dump/pg_restore baseline
- **90% RTO reduction** with tiered restore (critical tables first)
- **Engine-aware adaptive job sizing** -- 3-stage pipeline with I/O governor selection
- **BLOB pipeline**: Type detection (30+ signatures), content-addressed dedup, split backup mode
- **Intel/AMD/ARM vendor-aware CPU tuning** -- ISA features, cache hierarchy, NUMA, hybrid P/E-core
- **Streaming I/O** with 256KB batch pipeline (constant memory usage regardless of DB size)

### Restore Modes

| Mode | Speed | Safety | Use Case |
|------|-------|--------|----------|
| **Safe** (default) | Baseline | Full WAL logging | Production with replication |
| **Balanced** | 2-3x faster | UNLOGGED during COPY | Production standalone |
| **Turbo** | 3-4x faster | Minimal WAL, async commit | Dev/test only |

### Quality Assurance

- 1358-line pre-release test suite covering 10 categories with 43+ checks
- GitHub Actions CI with race detector, leak tests, multi-DB validation
- 27 TUI screens with comprehensive Ctrl+C handling
- Backwards compatible with all v5.x backup formats

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
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v6.50.23/dbbackup-linux-amd64
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

The interactive TUI provides connection health checks, pre-restore validation (7 automated checks), safe abort with Ctrl+C, type-to-confirm warnings for destructive operations, adaptive job sizing, I/O governor selection, and resource profile presets.

**For automation/CI:** Use `--auto-select`, `--auto-database`, `--auto-confirm` flags.

See [docs/tui-features.md](docs/tui-features.md) for full TUI documentation with screenshots.

### Command Line

```bash
# Single database backup
dbbackup backup single myapp_db

# PostgreSQL custom format (.dump)
dbbackup backup single myapp_db --dump-format custom

# Cluster backup (all databases)
dbbackup backup cluster
dbbackup backup cluster --db-type mariadb --user root

# Sample backup (reduced data for testing)
dbbackup backup sample myapp_db --sample-strategy percent --sample-value 10

# Encrypted backup
dbbackup backup single myapp_db --encrypt --encryption-key-file key.txt

# Incremental backup
dbbackup backup single myapp_db --backup-type incremental --base-backup base.tar.gz

# Restore single database
dbbackup restore single backup.dump --target myapp_db --create --confirm

# Restore cluster
dbbackup restore cluster cluster_backup.tar.gz --confirm

# Restore with resource profile and I/O governor
dbbackup restore cluster backup.tar.gz --profile=conservative --io-governor=bfq --confirm

# Diagnose backup before restore
dbbackup restore diagnose backup.dump.gz --deep

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
| `dedup list` / `stats` / `gc` / `verify` / `prune` | Manage dedup store |
| **Verification** | |
| `verify-backup` | Verify backup integrity (updates catalog) |
| `verify-locks` | Check PostgreSQL lock settings for restore |
| `verify-restore` | Systematic verification for large restores |
| **PITR** | |
| `pitr enable` / `disable` / `status` | PostgreSQL PITR management |
| `pitr mysql-enable` / `mysql-status` | MySQL/MariaDB PITR management |
| **WAL / Binlog** | |
| `wal archive` / `list` / `cleanup` / `timeline` | PostgreSQL WAL management |
| `binlog list` / `archive` / `watch` / `validate` / `position` | MySQL binlog management |
| **Cloud** | |
| `cloud upload` / `download` / `list` / `delete` / `status` / `sync` | Cloud storage operations |
| `cloud cross-region-sync` | Sync backups between cloud regions |
| **Catalog** | |
| `catalog sync` / `list` / `stats` / `gaps` / `search` / `info` | Catalog operations |
| `catalog export` / `dashboard` / `check` | Export, browse, verify |
| **Analysis** | |
| `diff` | Compare two backups |
| `estimate single` / `cluster` | Estimate backup size |
| `forecast` | Predict future disk space |
| `cost analyze` | Analyze cloud storage costs |
| `compression analyze` | Optimal compression analysis |
| `retention-simulator` | Simulate retention policy effects |
| **Infrastructure** | |
| `status` | Backup dashboard (age, size, encryption, health) |
| `preflight` / `health` / `diagnose` / `validate` | System checks |
| `list` / `cpu` / `profile` / `chain` | Info and diagnostics |
| **Monitoring** | |
| `rto analyze` / `status` / `check` | RTO/RPO analysis |
| `metrics export` / `serve` | Prometheus metrics |
| `report generate` / `summary` / `list` / `controls` | Compliance reports |
| `notify test` | Test notifications |
| **DR & Migration** | |
| `drill run` / `quick` / `list` / `cleanup` | DR drill testing |
| `migrate single` / `cluster` | Database migration |
| **Benchmarking** | |
| `benchmark run` / `matrix` / `history` / `show` | Performance benchmarking |
| **Operations** | |
| `cleanup` | Remove old backups (supports GFS retention) |
| `install` / `uninstall` / `schedule` | Systemd management |
| `interactive` | Start interactive TUI |
| `blob stats` / `backup` / `restore` | BLOB operations |
| `encryption rotate` | Rotate encryption keys |
| `engine list` | List available backup engines |
| `completion` / `version` / `man` | Shell completion, version info, man pages |

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
| `--max-cores` | Maximum CPU cores to use | all |
| `--profile` | Resource profile (conservative/balanced/aggressive) | balanced |
| `--adaptive` | Adaptive per-DB job sizing | true |
| `--io-governor` | I/O scheduler (auto/noop/bfq/mq-deadline/deadline) | auto |
| `--cloud` | Cloud storage URI | - |
| `--encrypt` | Enable encryption | false |
| `--dry-run, -n` | Run preflight checks only | false |
| `--verify-restore` | After backup, restore to temp DB and compare | false |
| `--native` | Use native Go engine | true |
| `--debug` | Enable debug logging | false |
| `--retention-days` | Backup retention period in days (0=disabled) | 0 |
| `--cpu-auto-tune` | Vendor-aware CPU auto-tuning | true |

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
# Single database -- custom format
dbbackup backup single mydb --dump-format custom

# Cluster backup uses custom format by default
dbbackup backup cluster

# Restore from custom format (parallel restore with -j)
dbbackup restore single mydb.dump --target mydb --confirm --jobs 8
```

**Benefits:** 2-3x faster restores via TOC-based parallel processing, selective table restore, built-in compression, random access to any table's data block.

## Incremental Backups

```bash
# Full backup (base)
dbbackup backup single mydb --backup-type full

# Incremental backup
dbbackup backup single mydb --backup-type incremental --base-backup mydb_base.tar.gz
```

## Cloud Storage

Supported providers: AWS S3, MinIO, Backblaze B2, Azure Blob Storage, Google Cloud Storage, SFTP.

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

# SFTP (SSH key or password)
dbbackup backup single mydb --cloud sftp://user@host/backups/ --sftp-key ~/.ssh/id_ed25519 --sftp-insecure
dbbackup cloud list --cloud sftp://user@host:2222/backups/ --sftp-password secret --sftp-insecure
```

See [docs/CLOUD.md](docs/CLOUD.md) for detailed configuration.

## Deduplicated Backups

Content-addressed deduplication using SHA-256 chunking with Bloom filter:

```bash
# Deduplicated backup of a file
dbbackup dedup backup /backups/mydb_20250601.dump.gz

# Direct database dump with deduplication
dbbackup dedup backup-db mydb --db-type mariadb --user root

# List / stats / restore / verify / garbage collect / prune
dbbackup dedup list
dbbackup dedup stats
dbbackup dedup restore <manifest-id> /tmp/restored.dump.gz
dbbackup dedup gc
dbbackup dedup prune --retention-days 90
```

**Typical storage savings:** 50-80% for daily backups of slowly-changing databases, 60-90% for databases with large static BLOBs.

## Point-in-Time Recovery

### PostgreSQL PITR

```bash
# Enable PITR (WAL archiving)
dbbackup pitr enable --archive-dir /backups/wal_archive

# Check status
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
# Enable binary logging
dbbackup pitr mysql-enable --db-type mariadb --user root

# Archive / watch / validate binlogs
dbbackup binlog archive --db-type mariadb --user root --archive-dir /backups/binlogs
dbbackup binlog watch --db-type mariadb --user root --archive-dir /backups/binlogs
dbbackup binlog validate --db-type mariadb --user root
```

See [docs/PITR.md](docs/PITR.md) for PostgreSQL and [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) for MySQL/MariaDB.

## Backup Cleanup & GFS Retention

```bash
# Delete backups older than 30 days, keep minimum 5
dbbackup cleanup /backups --retention-days 30 --min-backups 5

# GFS retention: 7 daily, 4 weekly, 12 monthly, 3 yearly
dbbackup cleanup /backups --gfs \
  --gfs-daily 7 --gfs-weekly 4 --gfs-monthly 12 --gfs-yearly 3

# Preview deletions
dbbackup cleanup /backups --retention-days 7 --dry-run
```

## Dry-Run Mode

Preflight checks validate backup readiness without execution:

```bash
dbbackup backup single mydb --dry-run
```

Checks: database connectivity, required tools, storage permissions, size estimation, encryption config, cloud credentials.

## Notifications

Get alerted on backup events via email or webhooks:

```bash
# SMTP Email
export NOTIFY_SMTP_HOST="smtp.example.com"
export NOTIFY_SMTP_PORT="587"
export NOTIFY_SMTP_USER="alerts@example.com"
export NOTIFY_SMTP_PASSWORD="secret"
export NOTIFY_SMTP_FROM="dbbackup@example.com"
export NOTIFY_SMTP_TO="admin@example.com"

# Webhook (Slack, generic)
export NOTIFY_WEBHOOK_URL="https://hooks.slack.com/services/T00/B00/XXX"
export NOTIFY_WEBHOOK_SECRET="signing-secret"  # Optional HMAC signing

# Test notification configuration
dbbackup notify test --verbose
```

**Supported events:** `backup_started/completed/failed`, `restore_started/completed/failed`, `cleanup_completed`, `verify_completed/failed`, `pitr_recovery`, `dr_drill_passed/failed`, `gap_detected`, `rpo_violation`.

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

# Compare two backups to see what changed
dbbackup diff /backups/mydb_20240115.dump.gz /backups/mydb_20240120.dump.gz
```

## Monitoring & Compliance

### Cost Analysis

```bash
dbbackup cost analyze                      # All databases
dbbackup cost analyze --database mydb      # Specific database
dbbackup cost analyze --format json        # JSON for automation
```

Analyzes AWS S3, GCS, Azure Blob, Backblaze B2, and Wasabi with tiered storage recommendations.

### Health Check

```bash
dbbackup health                            # Quick check
dbbackup health --format json              # For monitoring integration
```

Checks: config validity, DB connectivity, backup freshness, gap detection, verification status, disk space. Exit codes: 0=healthy, 1=warning, 2=critical.

### Restore Verification

```bash
dbbackup backup single mydb --verify-restore
```

Creates temp DB, restores, compares every table's row count, prints color-coded report, always cleans up.

### Backup Status Dashboard

```bash
dbbackup status
```

Per-database health table with age, size, encryption, and color-coded staleness (green=OK, yellow=aging, red=stale).

### Backup Diagnosis

```bash
dbbackup restore diagnose backup.dump.gz --deep
```

Validates PGDMP signatures, gzip integrity, COPY block termination, pg_restore listing, and archive structure.

### DR Drill Testing

```bash
dbbackup drill run /backups/mydb_latest.dump.gz --database mydb --type postgresql
dbbackup drill quick /backups/mydb_latest.dump.gz --database mydb
```

Automated restore testing in Docker containers. See [docs/DRILL.md](docs/DRILL.md).

### Compliance Reports

```bash
dbbackup report generate --type soc2 --days 90 --format html --output soc2-report.html
dbbackup report summary --type gdpr --days 30
dbbackup report list
```

Frameworks: SOC2 Type II, GDPR, HIPAA, PCI-DSS, ISO 27001.

### RTO/RPO Analysis

```bash
dbbackup rto analyze --database mydb
dbbackup rto status
dbbackup rto check --target-rto 4h --target-rpo 1h
```

Calculates current RPO/RTO, compliance status, breakdown by phase, and improvement recommendations. See [docs/RTO.md](docs/RTO.md).

## Systemd Integration

```bash
# Install with Prometheus metrics exporter
sudo dbbackup install --backup-type cluster --with-metrics

# Preview / check status / uninstall
dbbackup install --dry-run --backup-type cluster
dbbackup install --status
sudo dbbackup uninstall cluster --purge
```

**Schedule options:**
```bash
--schedule daily                    # Every day at midnight (default)
--schedule weekly                   # Every Monday at midnight
--schedule "*-*-* 02:00:00"         # Every day at 2am
```

Installs systemd service/timer units, dedicated `dbbackup` user, directories, and optional Prometheus exporter on port 9399.

See [docs/SYSTEMD.md](docs/SYSTEMD.md) for manual setup, security hardening, and troubleshooting.

## Prometheus Metrics

> **Migration Note (v1.x -> v2.x):** The `--instance` flag was renamed to `--server` to avoid collision with Prometheus's reserved `instance` label.

### Textfile Collector

```bash
dbbackup metrics export --output /var/lib/node_exporter/textfile_collector/dbbackup.prom
```

### HTTP Exporter

```bash
dbbackup metrics serve                    # Default port 9399
dbbackup metrics serve --port 9100        # Custom port
```

**Endpoints:** `/metrics` (Prometheus format), `/health` (returns 200 OK).

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dbbackup_build_info` | gauge | Build info (version, commit) |
| `dbbackup_last_success_timestamp` | gauge | Unix timestamp of last successful backup |
| `dbbackup_last_backup_duration_seconds` | gauge | Duration of last backup |
| `dbbackup_last_backup_size_bytes` | gauge | Size of last backup |
| `dbbackup_backup_total` | gauge | Total backups by status (success/failure) |
| `dbbackup_backup_by_type` | gauge | Total backups by backup type |
| `dbbackup_rpo_seconds` | gauge | Seconds since last successful backup |
| `dbbackup_backup_verified` | gauge | Whether last backup was verified (1/0) |
| `dbbackup_restore_total` | counter | Total restore operations by status |
| `dbbackup_restore_duration_seconds` | gauge | Duration of last restore |
| `dbbackup_dedup_database_last_backup_timestamp` | gauge | Last dedup backup timestamp |

**Labels:** `server`, `database`, `engine`, `backup_type`, `status`

See [docs/METRICS.md](docs/METRICS.md) and [docs/EXPORTER.md](docs/EXPORTER.md) for alerting rules, PromQL examples, and production deployment.

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
```

> **Note:** The `--password` command-line flag is not supported for security reasons (passwords would be visible in `ps aux` output). Use environment variables or config files.

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
| 100GB uniform data | 2h30m | 1h39m | **1.5x** |
| 100GB fragmented | 12h+ | 3h20m | **3.6x** |
| 500GB + tiered restore | 8h full | 8 min (critical tables) | **60x RTO** |

### Throughput

| Operation | Throughput |
|-----------|------------|
| Dump (pgzip, 8 workers) | 2,048 MB/s |
| Restore (pgzip decompress) | 1,673 MB/s |
| Standard gzip baseline | 422 MB/s |

### Memory Usage

Streaming architecture maintains constant memory regardless of database size: **< 1 GB** for databases of 1-100+ GB.

### Optimization

```bash
# High-performance backup
dbbackup backup cluster --max-cores 32 --jobs 32 --cpu-workload cpu-intensive --compression 3

# High-performance restore
dbbackup restore single dump.sql.gz --restore-mode=balanced --workers=16 \
  --tiered-restore --critical-tables="user*,payment*"
```

Workload types: `balanced` (default), `cpu-intensive` (fast storage), `io-intensive` (avoid I/O contention).

See [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) for advanced tuning and [docs/BENCHMARKING.md](docs/BENCHMARKING.md) for the full benchmark framework, CPU architecture optimization, HugePages, and Linux kernel tuning.

## Testing

dbbackup is tested daily on dedicated infrastructure:

- 6 production database servers running automated nightly backups
- Dedicated 16-core test node running the full test suite against real PostgreSQL and MySQL instances
- Every release is deployed via Ansible and validated across the fleet before tagging
- Prometheus monitoring with RPO/failure alerts on all nodes
- CI pipeline with race detection on every commit

## Troubleshooting

### TUI Connection Issues

If the TUI shows `[FAIL] Disconnected`:

1. **Check database is running:** `psql -U postgres -c "SELECT 1"`
2. **Verify connection settings:** `./dbbackup status`
3. **Test with CLI mode:** `./dbbackup backup single testdb`

### Backup Works But Restore Fails

Common causes:
- **Peer auth mismatch** -- Backup ran as `postgres`, restore as `root`. Fix: `sudo -u postgres dbbackup restore ...`
- **PostgreSQL crashed** after heavy backup I/O. Check: `sudo systemctl status postgresql`
- **max_connections exhausted** -- Check: `sudo -u postgres psql -c "SELECT count(*) FROM pg_stat_activity;"`
- **Password not set** -- Re-export `PGPASSWORD` for the restore session

### Pre-Restore Failures

- **Archive integrity fails** -- Archive may be corrupted, try re-downloading
- **Insufficient disk space** -- Free up space or use `--work-dir`
- **Missing privileges** -- User needs CREATEDB: `ALTER USER youruser CREATEDB;`

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for extended diagnostics.

## Requirements

**System:**
- Linux, macOS, FreeBSD, OpenBSD, NetBSD
- 1 GB RAM minimum
- Disk space: 30-50% of database size

**Native Engine (default -- no external tools required):**
- PostgreSQL 10+ (via pgx protocol)
- MySQL 5.7+ / MariaDB 10.3+ (via go-sql-driver)

**External Tools (optional, used as fallback):**
- PostgreSQL: psql, pg_dump, pg_dumpall, pg_restore
- MySQL/MariaDB: mysql, mysqldump
- Percona XtraBackup 8.0+ / MariaBackup 10.3+

## Documentation

**Getting Started:**
- [docs/MIGRATION_FROM_V5.md](docs/MIGRATION_FROM_V5.md) -- Upgrade guide (v5.x -> v6.0)
- [docs/DATABASE_COMPATIBILITY.md](docs/DATABASE_COMPATIBILITY.md) -- Feature matrix (PG/MySQL/MariaDB)

**Performance & Restore:**
- [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) -- Advanced optimization guide
- [docs/BENCHMARKING.md](docs/BENCHMARKING.md) -- Benchmark framework, CPU optimization, HugePages, kernel tuning
- [docs/RESTORE_PERFORMANCE.md](docs/RESTORE_PERFORMANCE.md) -- Restore performance analysis
- [docs/RESTORE_PROFILES.md](docs/RESTORE_PROFILES.md) -- Restore resource profiles

**Database Engines:**
- [docs/ENGINES.md](docs/ENGINES.md) -- Engine configuration (includes XtraBackup/MariaBackup)
- [docs/PITR.md](docs/PITR.md) -- Point-in-Time Recovery (PostgreSQL)
- [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) -- Point-in-Time Recovery (MySQL/MariaDB)

**Cloud Storage:**
- [docs/CLOUD.md](docs/CLOUD.md) -- Cloud storage overview
- [docs/AZURE.md](docs/AZURE.md) -- Azure Blob Storage
- [docs/GCS.md](docs/GCS.md) -- Google Cloud Storage

**Monitoring & Operations:**
- [docs/METRICS.md](docs/METRICS.md) -- Prometheus metrics reference
- [docs/EXPORTER.md](docs/EXPORTER.md) -- Prometheus exporter setup
- [docs/RTO.md](docs/RTO.md) -- RTO/RPO analysis
- [docs/DRILL.md](docs/DRILL.md) -- DR drill testing
- [docs/CATALOG.md](docs/CATALOG.md) -- Backup catalog

**Deployment:**
- [docs/DOCKER.md](docs/DOCKER.md) -- Docker deployment
- [docs/SYSTEMD.md](docs/SYSTEMD.md) -- Systemd installation & scheduling
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) -- Common issues & solutions

**Reference:**
- [SECURITY.md](SECURITY.md) -- Security considerations
- [CONTRIBUTING.md](CONTRIBUTING.md) -- Contribution guidelines
- [CHANGELOG.md](CHANGELOG.md) -- Version history
- [docs/tui-features.md](docs/tui-features.md) -- TUI feature reference

## License

Apache License 2.0 - see [LICENSE](LICENSE).
