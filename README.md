# dbbackup

Database backup and restore utility for PostgreSQL, MySQL, and MariaDB.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org/)

**Repository:** https://git.uuxo.net/UUXO/dbbackup  
**Mirror:** https://github.com/PlusOne/dbbackup

## Features

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
- **DR Drill Testing**: Automated disaster recovery testing in Docker containers
- **Smart Notifications**: Batched alerts with escalation policies
- **Compliance Reports**: SOC2, GDPR, HIPAA, PCI-DSS, ISO27001 report generation
- **RTO/RPO Calculator**: Recovery objective analysis and recommendations
- **Replica-Aware Backup**: Automatic backup from replicas to reduce primary load
- **Parallel Table Backup**: Concurrent table dumps for faster backups

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
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v3.42.74/dbbackup-linux-amd64
chmod +x dbbackup-linux-amd64
sudo mv dbbackup-linux-amd64 /usr/local/bin/dbbackup
```

Available platforms: Linux (amd64, arm64, armv7), macOS (amd64, arm64), FreeBSD, OpenBSD, NetBSD.

### Build from Source

```bash
git clone https://git.uuxo.net/UUXO/dbbackup.git
cd dbbackup
go build
```

## Usage

### Interactive Mode

```bash
# PostgreSQL with peer authentication
sudo -u postgres dbbackup interactive

# MySQL/MariaDB
dbbackup interactive --db-type mysql --user root --password secret
```

**Main Menu:**
```
Database Backup Tool - Interactive Menu

Target Engine: PostgreSQL  |  MySQL  |  MariaDB
Database: postgres@localhost:5432 (PostgreSQL)

> Single Database Backup
  Sample Database Backup (with ratio)
  Cluster Backup (all databases)
  ────────────────────────────────
  Restore Single Database
  Restore Cluster Backup
  Diagnose Backup File
  List & Manage Backups
  ────────────────────────────────
  View Active Operations
  Show Operation History
  Database Status & Health Check
  Configuration Settings
  Clear Operation History
  Quit
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

> Database Type: postgres
  CPU Workload Type: balanced
  Resource Profile: balanced (P:2 J:4)
  Cluster Parallelism: 2
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

# Restore cluster
dbbackup restore cluster cluster_backup.tar.gz --confirm

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
| `--password` | Database password | - |
| `--backup-dir` | Backup directory | ~/db_backups |
| `--compression` | Compression level (0-9) | 6 |
| `--jobs` | Parallel jobs | 8 |
| `--profile` | Resource profile (conservative/balanced/aggressive) | balanced |
| `--cloud` | Cloud storage URI | - |
| `--encrypt` | Enable encryption | false |
| `--dry-run, -n` | Run preflight checks only | false |
| `--debug` | Enable debug logging | false |
| `--save-debug-log` | Save error report to file on failure | - |

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
  ✅ Database Connectivity: Connected successfully
  ✅ Required Tools:        pg_dump 15.4 available
  ✅ Storage Target:        /backups writable (45 GB free)
  ✅ Size Estimation:       ~2.5 GB required
  ─────────────────────────────────────────────────────────────

  ✅ All checks passed

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
🔍 Backup Diagnosis Report
══════════════════════════════════════════════════════════════

📁 File: mydb_20260105.dump.gz
   Format: PostgreSQL Custom (gzip)
   Size: 2.5 GB

🔬 Analysis Results:
   ✅ Gzip integrity: Valid
   ✅ PGDMP signature: Valid
   ✅ pg_restore --list: Success (245 objects)
   ❌ COPY block check: TRUNCATED

⚠️  Issues Found:
   - COPY block for table 'orders' not terminated
   - Dump appears truncated at line 1,234,567

💡 Recommendations:
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
  "subject": "✅ [dbbackup] Backup Completed: mydb"
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

# Search backups
dbbackup catalog search --database mydb --start 2024-01-01 --end 2024-12-31

# Get backup info
dbbackup catalog info 42
```

## DR Drill Testing

Automated disaster recovery testing restores backups to Docker containers:

```bash
# Run full DR drill
dbbackup drill run /backups/mydb_latest.dump.gz \
  --database mydb \
  --db-type postgres \
  --timeout 30m

# Quick drill (restore + basic validation)
dbbackup drill quick /backups/mydb_latest.dump.gz --database mydb

# List running drill containers
dbbackup drill list

# Cleanup old drill containers
dbbackup drill cleanup --age 24h

# Generate drill report
dbbackup drill report --format html --output drill-report.html
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
dbbackup rto analyze mydb

# Show status for all databases
dbbackup rto status

# Check against targets
dbbackup rto check --rto 4h --rpo 1h

# Set target objectives
dbbackup rto analyze mydb --target-rto 4h --target-rpo 1h
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

📖 **Full documentation:** [SYSTEMD.md](SYSTEMD.md) - Manual setup, security hardening, multiple instances, troubleshooting

## Prometheus Metrics

Export backup metrics for monitoring with Prometheus:

### Textfile Collector

For integration with node_exporter:

```bash
# Export metrics to textfile
dbbackup metrics export --output /var/lib/node_exporter/textfile_collector/dbbackup.prom

# Export for specific instance
dbbackup metrics export --instance production --output /var/lib/dbbackup/metrics/production.prom
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
# Command line
dbbackup backup single mydb --db-type mysql --user root --password secret

# Configuration file
cat > ~/.my.cnf << EOF
[client]
user=root
password=secret
EOF
chmod 0600 ~/.my.cnf
```

### Configuration Persistence

Settings are saved to `.dbbackup.conf` in the current directory:

```bash
--no-config       # Skip loading saved configuration
--no-save-config  # Prevent saving configuration
```

## Performance

### Memory Usage

Streaming architecture maintains constant memory usage regardless of database size:

| Database Size | Memory Usage |
|---------------|--------------|
| 1-100+ GB | < 1 GB |

### Optimization

```bash
# High-performance backup
dbbackup backup cluster \
  --max-cores 32 \
  --jobs 32 \
  --cpu-workload cpu-intensive \
  --compression 3
```

Workload types:
- `balanced` - Default, suitable for most workloads
- `cpu-intensive` - Higher parallelism for fast storage
- `io-intensive` - Lower parallelism to avoid I/O contention

## Requirements

**System:**
- Linux, macOS, FreeBSD, OpenBSD, NetBSD
- 1 GB RAM minimum
- Disk space: 30-50% of database size

**PostgreSQL:**
- psql, pg_dump, pg_dumpall, pg_restore
- PostgreSQL 10+

**MySQL/MariaDB:**
- mysql, mysqldump
- MySQL 5.7+ or MariaDB 10.3+

## Documentation

**Quick Start:**
- [QUICK.md](QUICK.md) - Real-world examples cheat sheet

**Guides:**
- [docs/PITR.md](docs/PITR.md) - Point-in-Time Recovery (PostgreSQL)
- [docs/MYSQL_PITR.md](docs/MYSQL_PITR.md) - Point-in-Time Recovery (MySQL)
- [docs/ENGINES.md](docs/ENGINES.md) - Database engine configuration
- [docs/RESTORE_PROFILES.md](docs/RESTORE_PROFILES.md) - Restore resource profiles

**Cloud Storage:**
- [docs/CLOUD.md](docs/CLOUD.md) - Cloud storage overview
- [docs/AZURE.md](docs/AZURE.md) - Azure Blob Storage
- [docs/GCS.md](docs/GCS.md) - Google Cloud Storage

**Deployment:**
- [docs/DOCKER.md](docs/DOCKER.md) - Docker deployment
- [docs/SYSTEMD.md](docs/SYSTEMD.md) - Systemd installation & scheduling

**Reference:**
- [SECURITY.md](SECURITY.md) - Security considerations
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [CHANGELOG.md](CHANGELOG.md) - Version history
- [docs/LOCK_DEBUGGING.md](docs/LOCK_DEBUGGING.md) - Lock troubleshooting

## License

Apache License 2.0 - see [LICENSE](LICENSE).

Copyright 2025 dbbackup Project
