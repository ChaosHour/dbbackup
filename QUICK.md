# dbbackup Quick Reference

Real examples, no fluff.

## Basic Backups

```bash
# PostgreSQL (auto-detects all databases)
dbbackup backup all /mnt/backups/databases

# Single database
dbbackup backup single myapp /mnt/backups/databases

# MySQL
dbbackup backup single gitea --db-type mysql --db-host 127.0.0.1 --db-port 3306 /mnt/backups/databases

# With compression level (1-9, default 6)
dbbackup backup all /mnt/backups/databases --compression-level 9

# As root (requires flag)
sudo dbbackup backup all /mnt/backups/databases --allow-root
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

## Cloud Storage

```bash
# Upload to S3/MinIO
dbbackup cloud upload /mnt/backups/databases/myapp_2026-01-23.sql.gz \
  --provider s3 \
  --bucket my-backups \
  --endpoint https://s3.amazonaws.com

# Upload to MinIO (self-hosted)
dbbackup cloud upload backup.sql.gz \
  --provider s3 \
  --bucket backups \
  --endpoint https://minio.internal:9000

# Upload to Google Cloud Storage
dbbackup cloud upload backup.sql.gz \
  --provider gcs \
  --bucket my-gcs-bucket

# Upload to Azure Blob
dbbackup cloud upload backup.sql.gz \
  --provider azure \
  --bucket mycontainer

# With bandwidth limit (don't saturate the network)
dbbackup cloud upload backup.sql.gz --provider s3 --bucket backups --bandwidth-limit 10MB/s

# List remote backups
dbbackup cloud list --provider s3 --bucket my-backups

# Download
dbbackup cloud download myapp_2026-01-23.sql.gz /tmp/ --provider s3 --bucket my-backups

# Sync local backup dir to cloud
dbbackup cloud sync /mnt/backups/databases --provider s3 --bucket my-backups
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
dbbackup backup all /mnt/backups/databases --encrypt --encrypt-key "my-secret-passphrase"

# Or use environment variable
export DBBACKUP_ENCRYPT_KEY="my-secret-passphrase"
dbbackup backup all /mnt/backups/databases --encrypt

# Restore encrypted backup
dbbackup restore /mnt/backups/databases/myapp_2026-01-23.sql.gz.enc myapp_restored \
  --encrypt-key "my-secret-passphrase"
```

## Catalog (Backup Inventory)

```bash
# Sync local backups to catalog
dbbackup catalog sync /mnt/backups/databases

# List all backups
dbbackup catalog list

# Show gaps (missing daily backups)
dbbackup catalog gaps

# Search backups
dbbackup catalog search myapp

# Export catalog to JSON
dbbackup catalog export --format json > backups.json
```

## Restore

```bash
# Restore to new database
dbbackup restore /mnt/backups/databases/myapp_2026-01-23.sql.gz myapp_restored

# Restore to existing database (overwrites!)
dbbackup restore /mnt/backups/databases/myapp_2026-01-23.sql.gz myapp --force

# Restore MySQL
dbbackup restore /mnt/backups/databases/gitea_2026-01-23.sql.gz gitea_restored \
  --db-type mysql --db-host 127.0.0.1

# Verify restore (restores to temp db, runs checks, drops it)
dbbackup verify-restore /mnt/backups/databases/myapp_2026-01-23.sql.gz
```

## Retention & Cleanup

```bash
# Delete backups older than 30 days
dbbackup cleanup /mnt/backups/databases --older-than 30d

# Keep 7 daily, 4 weekly, 12 monthly (GFS)
dbbackup cleanup /mnt/backups/databases --keep-daily 7 --keep-weekly 4 --keep-monthly 12

# Dry run (show what would be deleted)
dbbackup cleanup /mnt/backups/databases --older-than 30d --dry-run
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
# Full production setup: encrypted, deduplicated, uploaded to S3
dbbackup backup all /mnt/backups/databases \
  --dedup \
  --encrypt \
  --compression-level 9

dbbackup cloud sync /mnt/backups/databases \
  --provider s3 \
  --bucket prod-backups \
  --bandwidth-limit 50MB/s

# Quick MySQL backup to S3
dbbackup backup single shopdb --db-type mysql /tmp/backup && \
dbbackup cloud upload /tmp/backup/shopdb_*.sql.gz --provider s3 --bucket backups

# PITR-enabled PostgreSQL with cloud sync
dbbackup pitr enable proddb /mnt/wal
dbbackup pitr base proddb /mnt/wal
dbbackup cloud sync /mnt/wal --provider gcs --bucket wal-archive
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DBBACKUP_ENCRYPT_KEY` | Encryption passphrase |
| `DBBACKUP_BANDWIDTH_LIMIT` | Cloud upload limit (e.g., `10MB/s`) |
| `PGHOST`, `PGPORT`, `PGUSER` | PostgreSQL connection |
| `MYSQL_HOST`, `MYSQL_TCP_PORT` | MySQL connection |
| `AWS_ACCESS_KEY_ID` | S3/MinIO credentials |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCS service account JSON path |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account name |

## Quick Checks

```bash
# What version?
dbbackup --version

# What's installed?
dbbackup status

# Test database connection
dbbackup backup single testdb /tmp --dry-run

# Verify a backup file
dbbackup verify /mnt/backups/databases/myapp_2026-01-23.sql.gz
```
