# Troubleshooting Guide

Common issues and solutions for dbbackup.

---

## Restore Issues

### Restore Stuck at 85%

**Symptoms:** Progress stops, no CPU activity, database appears unresponsive.

**Diagnosis:**

```bash
# PostgreSQL: Check what the database is doing
psql -c "SELECT pid, state, query, wait_event_type, wait_event
         FROM pg_stat_activity
         WHERE datname = 'yourdb' AND state != 'idle';"

# Check for lock contention
psql -c "SELECT * FROM pg_locks WHERE NOT granted;"

# Enable TUI debug mode to trace internal state
dbbackup restore --tui-debug single dump.sql.gz
```

**Common causes:**
1. **Long-running index build on fragmented data** — GIN/GIST indexes on large text columns can take 30+ minutes.
2. **Foreign key validation with missing index** — Constraint checks scan the entire referenced table.
3. **Lock contention** — Another session holds a conflicting lock.
4. **Disk I/O saturation** — Use `iostat -x 1` to check.

**Solutions:**
- Use `--restore-mode=turbo` for dev/test environments (3–4× faster).
- Use `--tiered-restore` to bring critical tables online first while the rest continues.
- Increase parallelism: `--workers=16` for better throughput on multi-core systems.
- Check disk: `df -h` — ensure at least 20% free space.

---

### Connection Pool Exhausted

**Error:** `failed to acquire connection: timeout`

**Cause:** More workers than the database's `max_connections` allows.

**Solution:**

```bash
# Check current PostgreSQL connection limit
psql -c "SHOW max_connections;"

# Check active connections
psql -c "SELECT count(*) FROM pg_stat_activity;"

# Reduce workers (connection pool = workers × 2)
dbbackup restore single dump.sql.gz --workers=8
```

**Permanent fix:**
```sql
-- PostgreSQL: Increase max connections
ALTER SYSTEM SET max_connections = 200;
-- Requires restart
```

For MySQL:
```sql
SHOW VARIABLES LIKE 'max_connections';
SET GLOBAL max_connections = 200;
```

---

### Restore Fails with "Permission Denied"

**Error:** `permission denied for schema public` or `must be owner of table`

**Solutions:**

```bash
# PostgreSQL: Restore as superuser
sudo -u postgres dbbackup restore single dump.sql.gz --database mydb

# Or grant permissions first
psql -c "GRANT ALL ON SCHEMA public TO myuser;"
psql -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO myuser;"
```

For MySQL:
```bash
# Ensure user has all privileges
mysql -e "GRANT ALL PRIVILEGES ON mydb.* TO 'myuser'@'localhost';"
```

---

### Corrupt or Truncated Backup

**Symptoms:** Restore fails with `unexpected EOF`, `invalid gzip header`, or `syntax error in SQL`.

**Diagnosis:**

```bash
# Check backup file integrity
dbbackup verify /path/to/backup.sql.gz

# Test gzip integrity
gzip -t backup.sql.gz

# Check file size (0 bytes = failed backup)
ls -lh backup.sql.gz

# View the end of the backup
zcat backup.sql.gz | tail -20
```

**Solutions:**
1. Re-run the backup: `dbbackup backup single mydb`
2. Check disk space on the backup server: `df -h`
3. If using cloud storage, verify the upload completed: `dbbackup cloud status`

---

## Monitoring / Alert Issues

### DBBackupNotVerified Alert Firing

**Symptoms:** Prometheus alert `DBBackupNotVerified` fires, `dbbackup_backup_verified` metric shows 0.

**Diagnosis:**

```bash
# Check if verify has been run recently
dbbackup catalog list --limit 5
# Look for verified_at and verify_valid columns

# Check the Prometheus metric directly
curl -s localhost:9399/metrics | grep dbbackup_backup_verified
```

**Common causes:**
1. **Backup scripts don't include a verify step** — add `dbbackup verify <backup-file>` after each backup
2. **Verify runs as wrong user** — the catalog is per-user (`~/.dbbackup/catalog.db`); ensure verify runs as the same user whose catalog the exporter reads
3. **File glob pattern mismatch** — native engine backups produce `*_native.sql.gz`, not `*.dump`; adjust verify patterns accordingly
4. **Version < 6.17.1** — prior versions had a bug where `verify` did not update the catalog (fixed in v6.17.1)

**Solution:**

```bash
# Run verify on the latest backup (updates catalog automatically)
dbbackup verify /path/to/latest_backup.sql.gz --allow-root

# Confirm the metric updated
curl -s localhost:9399/metrics | grep dbbackup_backup_verified
# Should show: dbbackup_backup_verified{server="...",database="..."} 1
```

---

## Backup Issues

### Backup Takes Too Long

**Tuning options:**

```bash
# Use higher parallelism
dbbackup backup single mydb --jobs=16 --max-cores=16

# Lower compression for speed
dbbackup backup single mydb --compression=1

# Use CPU-intensive workload type
dbbackup backup single mydb --cpu-workload=cpu-intensive

# Skip large binary columns (if applicable)
dbbackup backup single mydb --sample-ratio=0.1
```

### Backup Fails with "Too Many Connections"

```bash
# Reduce parallel dump jobs
dbbackup backup single mydb --dump-jobs=4

# Use single-threaded mode
dbbackup backup single mydb --dump-jobs=1
```

---

## TUI Issues

### TUI Freezes or Doesn't Respond

**Diagnosis:**

```bash
# Run with debug logging enabled
TUI_DEBUG=1 dbbackup interactive 2>/tmp/tui_debug.log

# Check the debug log
tail -f /tmp/tui_debug.log
```

**Common causes:**
1. Terminal doesn't support required escape sequences — try `TERM=xterm-256color`.
2. SSH connection with slow latency — TUI renders at 250ms intervals.
3. Database connection dropped — check `dbconnect.go` debug output.

**Workarounds:**
- Use CLI mode instead: `dbbackup restore single dump.sql.gz --no-tui --confirm`
- Press `Ctrl+C` — all 27 TUI screens handle interrupts gracefully.

### TUI Shows Wrong Database Type

The TUI auto-detects database type from the config. To override:

```bash
dbbackup interactive --db-type mysql
dbbackup interactive --db-type postgres
```

---

## Connection Issues

### Cannot Connect to PostgreSQL

```bash
# Test connection directly
psql -h localhost -U postgres -d postgres -c "SELECT 1;"

# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Check peer authentication
# If using local socket, you may need:
sudo -u postgres dbbackup interactive

# Or use password authentication
export PGPASSWORD='yourpassword'
dbbackup interactive --host localhost --user postgres
```

### Cannot Connect to MySQL

```bash
# Test connection directly
mysql -h localhost -u root -p -e "SELECT 1;"

# Check if MySQL is running
mysqladmin -h localhost -u root -p ping

# Using socket connection
dbbackup interactive --db-type mysql --socket /var/run/mysqld/mysqld.sock

# Using TCP
dbbackup interactive --db-type mysql --host 127.0.0.1 --port 3306 --user root
```

---

## Cloud Storage Issues

### Upload Fails with Timeout

```bash
# Increase timeout
dbbackup cloud sync --timeout=600

# Use bandwidth throttling for unreliable connections
dbbackup backup single mydb --max-bandwidth=50M

# Check connectivity
dbbackup cloud status
```

### AWS S3 Credentials

```bash
# Ensure credentials are configured
aws sts get-caller-identity

# Or use environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

---

## Performance Issues

### High Memory Usage

dbbackup uses streaming architecture with constant memory. If memory is high:

```bash
# Check for memory leaks (shouldn't happen)
ps aux | grep dbbackup

# Use lower buffer sizes
dbbackup restore single dump.sql.gz --workers=4
```

### Slow Compression

```bash
# Use fastest compression
dbbackup backup single mydb --compression=1

# Or disable compression for local backups
dbbackup backup single mydb --compression=0
```

---

## Debug Mode

For any issue, enable debug logging:

```bash
# CLI debug
dbbackup --debug restore single dump.sql.gz

# Native engine debug
dbbackup --native-debug restore single dump.sql.gz

# Lock debugging
dbbackup --debug-locks restore single dump.sql.gz

# TUI debug
TUI_DEBUG=1 dbbackup interactive 2>/tmp/tui.log
```

Debug logs include:
- SQL statement classification
- Connection pool state
- Worker allocation decisions
- Lock acquisition/release events
- Phase transitions (tiered restore)

---

## Getting Help

If your issue isn't covered here:

1. Check [CHANGELOG.md](../CHANGELOG.md) for recent fixes
2. Check [docs/LOCK_DEBUGGING.md](LOCK_DEBUGGING.md) for lock-specific issues
3. Run the diagnostic: `dbbackup health --verbose`
4. File an issue: https://github.com/PlusOne/dbbackup/issues
