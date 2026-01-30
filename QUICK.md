# dbbackup Quick Reference

Real examples, no fluff.

## Basic Backups

```bash
# PostgreSQL cluster (all databases + globals)
dbbackup backup cluster

# Single database
dbbackup backup single myapp

# MySQL
dbbackup backup single gitea --db-type mysql --host 127.0.0.1 --port 3306

# MySQL/MariaDB with Unix socket
dbbackup backup single myapp --db-type mysql --socket /var/run/mysqld/mysqld.sock

# With compression level (0-9, default 6)
dbbackup backup cluster --compression 9

# As root (requires flag)
sudo dbbackup backup cluster --allow-root
```

## PITR (Point-in-Time Recovery)

```bash
# Enable WAL archiving for a database
dbbackup pitr enable myapp /mnt/backups/wal

# Take base backup (required before PITR works)
dbbackup pitr base myapp /mnt/backups/wal

# Check PITR status
dbbackup pitr status myapp /mnt/backups/wal

# Restore to specific point in time
dbbackup pitr restore myapp /mnt/backups/wal --target-time "2026-01-23 14:30:00"

# Restore to latest available
dbbackup pitr restore myapp /mnt/backups/wal --target-time latest

# Disable PITR
dbbackup pitr disable myapp
```

## Deduplication

```bash
# Backup with dedup (saves ~60-80% space on similar databases)
dbbackup backup all /mnt/backups/databases --dedup

# Check dedup stats
dbbackup dedup stats /mnt/backups/databases

# Prune orphaned chunks (after deleting old backups)
dbbackup dedup prune /mnt/backups/databases

# Verify chunk integrity
dbbackup dedup verify /mnt/backups/databases
```

## Blob Statistics

```bash
# Analyze blob/binary columns in a database (plan extraction strategies)
dbbackup blob stats --database myapp

# Output shows tables with blob columns, row counts, and estimated sizes
# Helps identify large binary data for separate extraction

# With explicit connection
dbbackup blob stats --database myapp --host dbserver --user admin

# MySQL blob analysis
dbbackup blob stats --database shopdb --db-type mysql
```

## Blob Statistics

```bash
# Analyze blob/binary columns in a database (plan extraction strategies)
dbbackup blob stats --database myapp

# Output shows tables with blob columns, row counts, and estimated sizes
# Helps identify large binary data for separate extraction

# With explicit connection
dbbackup blob stats --database myapp --host dbserver --user admin

# MySQL blob analysis
dbbackup blob stats --database shopdb --db-type mysql
```

## Engine Management

```bash
# List available backup engines for MySQL/MariaDB
dbbackup engine list

# Get detailed info on a specific engine
dbbackup engine info clone

# Get current environment info
dbbackup engine info
```

## Cloud Storage

```bash
# Upload to S3
dbbackup cloud upload /mnt/backups/databases/myapp_2026-01-23.sql.gz \
  --cloud-provider s3 \
  --cloud-bucket my-backups

# Upload to MinIO (self-hosted)
dbbackup cloud upload backup.sql.gz \
  --cloud-provider minio \
  --cloud-bucket backups \
  --cloud-endpoint https://minio.internal:9000

# Upload to Backblaze B2
dbbackup cloud upload backup.sql.gz \
  --cloud-provider b2 \
  --cloud-bucket my-b2-bucket

# With bandwidth limit (don't saturate the network)
dbbackup cloud upload backup.sql.gz --cloud-provider s3 --cloud-bucket backups --bandwidth-limit 10MB/s

# List remote backups
dbbackup cloud list --cloud-provider s3 --cloud-bucket my-backups

# Download
dbbackup cloud download myapp_2026-01-23.sql.gz /tmp/ --cloud-provider s3 --cloud-bucket my-backups

# Delete old backup from cloud
dbbackup cloud delete myapp_2026-01-01.sql.gz --cloud-provider s3 --cloud-bucket my-backups
```

### Cloud Environment Variables

```bash
# S3/MinIO
export AWS_ACCESS_KEY_ID=AKIAXXXXXXXX
export AWS_SECRET_ACCESS_KEY=xxxxxxxx
export AWS_REGION=eu-central-1

# GCS
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Azure
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=xxxxxxxx
```

## Encryption

```bash
# Backup with encryption (AES-256-GCM)
dbbackup backup single myapp --encrypt

# Use environment variable for key (recommended)
export DBBACKUP_ENCRYPTION_KEY="my-secret-passphrase"
dbbackup backup cluster --encrypt

# Or use key file
dbbackup backup single myapp --encrypt --encryption-key-file /path/to/keyfile

# Restore encrypted backup (key from environment)
dbbackup restore single myapp_2026-01-23.dump.gz.enc --confirm
```

## Catalog (Backup Inventory)

```bash
# Sync local backups to catalog
dbbackup catalog sync /mnt/backups/databases

# List all backups
dbbackup catalog list

# Show catalog statistics
dbbackup catalog stats

# Show gaps (missing daily backups)
dbbackup catalog gaps mydb --interval 24h

# Search backups
dbbackup catalog search --database myapp --after 2026-01-01

# Show detailed info for a backup
dbbackup catalog info myapp_2026-01-23.dump.gz
```

## Restore

```bash
# Preview restore (dry-run by default)
dbbackup restore single myapp_2026-01-23.dump.gz

# Restore to new database
dbbackup restore single myapp_2026-01-23.dump.gz --target myapp_restored --confirm

# Restore to existing database (clean first)
dbbackup restore single myapp_2026-01-23.dump.gz --clean --confirm

# Restore MySQL
dbbackup restore single gitea_2026-01-23.sql.gz --target gitea_restored \
  --db-type mysql --host 127.0.0.1 --confirm

# Verify restore (restores to temp db, runs checks, drops it)
dbbackup verify-restore myapp_2026-01-23.dump.gz
```

## Retention & Cleanup

```bash
# Delete backups older than 30 days (keep at least 5)
dbbackup cleanup /mnt/backups/databases --retention-days 30 --min-backups 5

# GFS retention: 7 daily, 4 weekly, 12 monthly
dbbackup cleanup /mnt/backups/databases --gfs --gfs-daily 7 --gfs-weekly 4 --gfs-monthly 12

# Dry run (show what would be deleted)
dbbackup cleanup /mnt/backups/databases --retention-days 7 --dry-run
```

## Disaster Recovery Drill

```bash
# Full DR test (restores random backup, verifies, cleans up)
dbbackup drill /mnt/backups/databases

# Test specific database
dbbackup drill /mnt/backups/databases --database myapp

# With email notification (configure via environment variables)
export NOTIFY_SMTP_HOST="smtp.example.com"
export NOTIFY_SMTP_TO="admin@example.com"
dbbackup drill /mnt/backups/databases --database myapp
```

## Monitoring & Metrics

```bash
# Prometheus metrics endpoint
dbbackup metrics serve --port 9101

# One-shot status check (for scripts)
dbbackup status /mnt/backups/databases
echo $?  # 0 = OK, 1 = warnings, 2 = critical

# Generate HTML report
dbbackup report /mnt/backups/databases --output backup-report.html
```

## Systemd Timer (Recommended)

```bash
# Install systemd units
sudo dbbackup install systemd --backup-path /mnt/backups/databases --schedule "02:00"

# Creates:
#   /etc/systemd/system/dbbackup.service
#   /etc/systemd/system/dbbackup.timer

# Check timer
systemctl status dbbackup.timer
systemctl list-timers dbbackup.timer
```

## Common Combinations

```bash
# Full production setup: encrypted, with cloud auto-upload
dbbackup backup cluster \
  --encrypt \
  --compression 9 \
  --cloud-auto-upload \
  --cloud-provider s3 \
  --cloud-bucket prod-backups

# Quick MySQL backup to S3
dbbackup backup single shopdb --db-type mysql && \
dbbackup cloud upload shopdb_*.sql.gz --cloud-provider s3 --cloud-bucket backups

# PITR-enabled PostgreSQL with cloud upload
dbbackup pitr enable proddb /mnt/wal
dbbackup pitr base proddb /mnt/wal
dbbackup cloud upload /mnt/wal/*.gz --cloud-provider s3 --cloud-bucket wal-archive
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DBBACKUP_ENCRYPTION_KEY` | Encryption passphrase |
| `DBBACKUP_BANDWIDTH_LIMIT` | Cloud upload limit (e.g., `10MB/s`) |
| `DBBACKUP_CLOUD_PROVIDER` | Cloud provider (s3, minio, b2) |
| `DBBACKUP_CLOUD_BUCKET` | Cloud bucket name |
| `DBBACKUP_CLOUD_ENDPOINT` | Custom endpoint (for MinIO) |
| `AWS_ACCESS_KEY_ID` | S3/MinIO credentials |
| `AWS_SECRET_ACCESS_KEY` | S3/MinIO secret key |
| `PGHOST`, `PGPORT`, `PGUSER` | PostgreSQL connection |
| `MYSQL_HOST`, `MYSQL_TCP_PORT` | MySQL connection |

## Quick Checks

```bash
# What version?
dbbackup --version

# Connection status
dbbackup status

# Test database connection (dry-run)
dbbackup backup single testdb --dry-run

# Verify a backup file
dbbackup verify /mnt/backups/databases/myapp_2026-01-23.dump.gz

# Run preflight checks
dbbackup preflight
```
