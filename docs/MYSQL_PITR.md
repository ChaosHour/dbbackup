# MySQL/MariaDB Point-in-Time Recovery (PITR)

This guide explains how to use dbbackup for Point-in-Time Recovery with MySQL and MariaDB databases.

## Overview

Point-in-Time Recovery (PITR) allows you to restore your database to any specific moment in time, not just to when a backup was taken. This is essential for:

- Recovering from accidental data deletion or corruption
- Restoring to a state just before a problematic change
- Meeting regulatory compliance requirements for data recovery

### How MySQL PITR Works

MySQL PITR uses binary logs (binlogs) which record all changes to the database:

1. **Base Backup**: A full database backup with the binlog position recorded
2. **Binary Log Archiving**: Continuous archiving of binlog files
3. **Recovery**: Restore base backup, then replay binlogs up to the target time

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Base Backup │ --> │ binlog.00001 │ --> │ binlog.00002 │ --> │ binlog.00003 │
│ (pos: 1234) │     │              │     │              │     │  (current)   │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                   │                    │                    │
       ▼                   ▼                    ▼                    ▼
   10:00 AM            10:30 AM             11:00 AM            11:30 AM
                                                 ↑
                                          Target: 11:15 AM
```

## Prerequisites

### MySQL Configuration

Binary logging must be enabled in MySQL. Add to `my.cnf`:

```ini
[mysqld]
# Enable binary logging
log_bin = mysql-bin
server_id = 1

# Recommended: Use ROW format for PITR
binlog_format = ROW

# Optional but recommended: Enable GTID for easier replication and recovery
gtid_mode = ON
enforce_gtid_consistency = ON

# Keep binlogs for at least 7 days (adjust as needed)
expire_logs_days = 7
# Or for MySQL 8.0+:
# binlog_expire_logs_seconds = 604800
```

After changing configuration, restart MySQL:
```bash
sudo systemctl restart mysql
```

### MariaDB Configuration

MariaDB configuration is similar:

```ini
[mysqld]
log_bin = mariadb-bin
server_id = 1
binlog_format = ROW

# MariaDB uses different GTID implementation (auto-enabled with log_slave_updates)
log_slave_updates = ON
```

## Quick Start

### 1. Check PITR Status

```bash
# Check if MySQL is properly configured for PITR
dbbackup pitr mysql-status
```

Example output:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  MySQL/MariaDB PITR Status (mysql)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PITR Status:     NOT CONFIGURED
Binary Logging:  ENABLED
Binlog Format:   ROW
GTID Mode:       ON
Current Position: mysql-bin.000042:1234

PITR Requirements:
  [OK] Binary logging enabled
  [OK] Row-based logging (recommended)
```

### 2. Enable PITR

```bash
# Enable PITR and configure archive directory
dbbackup pitr mysql-enable --archive-dir /backups/binlog_archive
```

### 3. Create a Base Backup

```bash
# Create a backup - binlog position is automatically recorded
dbbackup backup single mydb
```

> **Note:** All backups automatically capture the current binlog position when PITR is enabled at the MySQL level. This position is stored in the backup metadata and used as the starting point for binlog replay during recovery.

### 4. Start Binlog Archiving

```bash
# Run binlog archiver in the background
dbbackup binlog watch --binlog-dir /var/lib/mysql --archive-dir /backups/binlog_archive --interval 30s
```

Or set up a cron job for periodic archiving:
```bash
# Archive new binlogs every 5 minutes
*/5 * * * * dbbackup binlog archive --binlog-dir /var/lib/mysql --archive-dir /backups/binlog_archive
```

### 5. Restore to Point in Time

```bash
# Restore to a specific time
dbbackup restore pitr mydb_backup.sql.gz --target-time '2024-01-15 14:30:00'
```

## Commands Reference

### PITR Commands

#### `pitr mysql-status`
Show MySQL/MariaDB PITR configuration and status.

```bash
dbbackup pitr mysql-status
```

#### `pitr mysql-enable`
Enable PITR for MySQL/MariaDB.

```bash
dbbackup pitr mysql-enable \
  --archive-dir /backups/binlog_archive \
  --retention-days 7 \
  --require-row-format \
  --require-gtid
```

Options:
- `--archive-dir`: Directory to store archived binlogs (required)
- `--retention-days`: Days to keep archived binlogs (default: 7)
- `--require-row-format`: Require ROW binlog format (default: true)
- `--require-gtid`: Require GTID mode enabled (default: false)

### Binlog Commands

#### `binlog list`
List available binary log files.

```bash
# List binlogs from MySQL data directory
dbbackup binlog list --binlog-dir /var/lib/mysql

# List archived binlogs
dbbackup binlog list --archive-dir /backups/binlog_archive
```

#### `binlog archive`
Archive binary log files.

```bash
dbbackup binlog archive \
  --binlog-dir /var/lib/mysql \
  --archive-dir /backups/binlog_archive \
  --compress
```

Options:
- `--binlog-dir`: MySQL binary log directory
- `--archive-dir`: Destination for archived binlogs (required)
- `--compress`: Compress archived binlogs with gzip
- `--encrypt`: Encrypt archived binlogs
- `--encryption-key-file`: Path to encryption key file

#### `binlog watch`
Continuously monitor and archive new binlog files.

```bash
dbbackup binlog watch \
  --binlog-dir /var/lib/mysql \
  --archive-dir /backups/binlog_archive \
  --interval 30s \
  --compress
```

Options:
- `--interval`: How often to check for new binlogs (default: 30s)

#### `binlog validate`
Validate binlog chain integrity.

```bash
dbbackup binlog validate --binlog-dir /var/lib/mysql
```

Output shows:
- Whether the chain is complete (no missing files)
- Any gaps in the sequence
- Server ID changes (indicating possible failover)
- Total size and file count

#### `binlog position`
Show current binary log position.

```bash
dbbackup binlog position
```

Output:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Current Binary Log Position
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

File:     mysql-bin.000042
Position: 123456
GTID Set: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000

Position String: mysql-bin.000042:123456
```

## Restore Scenarios

### Restore to Specific Time

```bash
# Restore to January 15, 2024 at 2:30 PM
dbbackup restore pitr mydb_backup.sql.gz \
  --target-time '2024-01-15 14:30:00'
```

### Restore to Specific Position

```bash
# Restore to a specific binlog position
dbbackup restore pitr mydb_backup.sql.gz \
  --target-position 'mysql-bin.000042:12345'
```

### Dry Run (Preview)

```bash
# See what SQL would be replayed without applying
dbbackup restore pitr mydb_backup.sql.gz \
  --target-time '2024-01-15 14:30:00' \
  --dry-run
```

### Restore to Backup Point Only

```bash
# Restore just the base backup without replaying binlogs
dbbackup restore pitr mydb_backup.sql.gz --immediate
```

## Best Practices

### 1. Archiving Strategy

- Archive binlogs frequently (every 5-30 minutes)
- Use compression to save disk space
- Store archives on separate storage from the database

### 2. Retention Policy

- Keep archives for at least as long as your oldest valid base backup
- Consider regulatory requirements for data retention
- Use the cleanup command to purge old archives:

```bash
dbbackup binlog cleanup --archive-dir /backups/binlog_archive --retention-days 30
```

### 3. Validation

- Regularly validate your binlog chain:
```bash
dbbackup binlog validate --binlog-dir /var/lib/mysql
```

- Test restoration periodically on a test environment

### 4. Monitoring

- Monitor the `dbbackup binlog watch` process
- Set up alerts for:
  - Binlog archiver failures
  - Gaps in binlog chain
  - Low disk space on archive directory

### 5. GTID Mode

Enable GTID for:
- Easier tracking of replication position
- Automatic failover in replication setups
- Simpler point-in-time recovery

## Troubleshooting

### Binary Logging Not Enabled

**Error**: "Binary logging appears to be disabled"

**Solution**: Add to my.cnf and restart MySQL:
```ini
[mysqld]
log_bin = mysql-bin
server_id = 1
```

### Missing Binlog Files

**Error**: "Gaps detected in binlog chain"

**Causes**:
- `RESET MASTER` was executed
- `expire_logs_days` is too short
- Binlogs were manually deleted

**Solution**:
- Take a new base backup immediately
- Adjust retention settings to prevent future gaps

### Permission Denied

**Error**: "Failed to read binlog directory"

**Solution**:
```bash
# Add dbbackup user to mysql group
sudo usermod -aG mysql dbbackup_user

# Or set appropriate permissions
sudo chmod g+r /var/lib/mysql/mysql-bin.*
```

### Wrong Binlog Format

**Warning**: "binlog_format = STATEMENT (ROW recommended)"

**Impact**: STATEMENT format may not capture all changes accurately

**Solution**: Change to ROW format (requires restart):
```ini
[mysqld]
binlog_format = ROW
```

### Server ID Changes

**Warning**: "server_id changed from X to Y (possible master failover)"

This warning indicates the binlog chain contains events from different servers, which may happen during:
- Failover in a replication setup
- Restoring from a different server's backup

This is usually informational but review your topology if unexpected.

## MariaDB-Specific Notes

### GTID Format

MariaDB uses a different GTID format than MySQL:
- **MySQL**: `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5`
- **MariaDB**: `0-1-100` (domain-server_id-sequence)

### Tool Detection

dbbackup automatically detects MariaDB and uses:
- `mariadb-binlog` if available (MariaDB 10.4+)
- Falls back to `mysqlbinlog` for older versions

### Encrypted Binlogs

MariaDB supports binlog encryption. If enabled, ensure the key is available during archive and restore operations.

## See Also

- [PITR.md](PITR.md) - PostgreSQL PITR documentation
- [DOCKER.md](DOCKER.md) - Running in Docker environments
- [CLOUD.md](CLOUD.md) - Cloud storage for archives
