# dbbackup

Database backup and restore utility for PostgreSQL, MySQL, and MariaDB.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org/)

**Repository:** https://git.uuxo.net/UUXO/dbbackup  
**Mirror:** https://github.com/PlusOne/dbbackup

## Features

- Multi-database support: PostgreSQL, MySQL, MariaDB
- Backup modes: Single database, cluster, sample data
- AES-256-GCM encryption
- Incremental backups
- Cloud storage: S3, MinIO, B2, Azure Blob, Google Cloud Storage
- Point-in-Time Recovery (PITR) for PostgreSQL
- Interactive terminal UI
- Cross-platform binaries

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
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v3.1.0/dbbackup-linux-amd64
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
  Location: /u01/dba/dumps/
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
  [OK] Disk space: 140 GB available
  [OK] Required tools found
  [OK] Target database accessible

c: Toggle cleanup | Enter: Proceed | Esc: Cancel
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

r: Restore | v: Verify | i: Info | d: Delete | R: Refresh | Esc: Back
```

**Configuration Settings:**
```
Configuration Settings

> Database Type: postgres
  CPU Workload Type: balanced
  Backup Directory: /root/db_backups
  Compression Level: 6
  Parallel Jobs: 16
  Dump Jobs: 8
  Database Host: localhost
  Database Port: 5432
  Database User: root
  SSL Mode: prefer

s: Save | r: Reset | q: Menu
```

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

# Cloud backup
dbbackup backup single mydb --cloud s3://my-bucket/backups/
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
| `verify-backup` | Verify backup integrity |
| `cleanup` | Remove old backups |
| `status` | Check connection status |
| `preflight` | Run pre-backup checks |
| `list` | List databases and backups |
| `cpu` | Show CPU optimization settings |
| `cloud` | Cloud storage operations |
| `pitr` | PITR management |
| `wal` | WAL archive operations |
| `interactive` | Start interactive UI |

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
| `--cloud` | Cloud storage URI | - |
| `--encrypt` | Enable encryption | false |
| `--debug` | Enable debug logging | false |

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

- [DOCKER.md](DOCKER.md) - Docker deployment
- [CLOUD.md](CLOUD.md) - Cloud storage configuration
- [PITR.md](PITR.md) - Point-in-Time Recovery
- [AZURE.md](AZURE.md) - Azure Blob Storage
- [GCS.md](GCS.md) - Google Cloud Storage
- [SECURITY.md](SECURITY.md) - Security considerations
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [CHANGELOG.md](CHANGELOG.md) - Version history

## License

Apache License 2.0 - see [LICENSE](LICENSE).

Copyright 2025 dbbackup Project
