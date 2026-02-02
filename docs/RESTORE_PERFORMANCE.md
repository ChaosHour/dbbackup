# Restore Performance Optimization Guide

## Quick Start: Fastest Restore Command

```bash
# For single database (matches pg_restore -j8 speed)
dbbackup restore single backup.dump.gz \
  --confirm \
  --profile turbo \
  --jobs 8

# For cluster restore (maximum speed)
dbbackup restore cluster backup.tar.gz \
  --confirm \
  --profile max-performance \
  --jobs 16 \
  --parallel-dbs 8 \
  --no-tui \
  --quiet
```

## Performance Profiles

| Profile | Jobs | Parallel DBs | Best For |
|---------|------|--------------|----------|
| `conservative` | 1 | 1 | Resource-constrained servers, production with other services |
| `balanced` | auto | auto | Default, most scenarios |
| `turbo` | 8 | 4 | Fast restores, matches `pg_restore -j8` |
| `max-performance` | 16 | 8 | Dedicated restore operations, benchmarking |

## New Performance Flags (v5.4.0+)

### `--no-tui`
Disables the Terminal User Interface completely for maximum performance.
Use this for scripted/automated restores where visual progress isn't needed.

```bash
dbbackup restore single backup.dump.gz --confirm --no-tui
```

### `--quiet`
Suppresses all output except errors. Combine with `--no-tui` for minimal overhead.

```bash
dbbackup restore single backup.dump.gz --confirm --no-tui --quiet
```

### `--jobs N`
Sets the number of parallel pg_restore workers. Equivalent to `pg_restore -jN`.

```bash
# 8 parallel restore workers
dbbackup restore single backup.dump.gz --confirm --jobs 8
```

### `--parallel-dbs N`
For cluster restores only. Sets how many databases to restore simultaneously.

```bash
# 4 databases restored in parallel, each with 8 jobs
dbbackup restore cluster backup.tar.gz --confirm --parallel-dbs 4 --jobs 8
```

## Benchmarking Your Restore Performance

Use the included benchmark script to identify bottlenecks:

```bash
./scripts/benchmark_restore.sh backup.dump.gz test_database
```

This will test:
1. `dbbackup` with TUI (default)
2. `dbbackup` without TUI (`--no-tui --quiet`)
3. `dbbackup` max performance profile
4. Native `pg_restore -j8` baseline

## Expected Performance

With optimal settings, `dbbackup restore` should match native `pg_restore -j8`:

| Database Size | pg_restore -j8 | dbbackup turbo |
|---------------|----------------|----------------|
| 1 GB | ~2 min | ~2 min |
| 10 GB | ~15 min | ~15-17 min |
| 100 GB | ~2.5 hr | ~2.5-3 hr |
| 500 GB | ~12 hr | ~12-13 hr |

If `dbbackup` is significantly slower (>2x), check:
1. TUI overhead: Test with `--no-tui --quiet`
2. Profile setting: Use `--profile turbo` or `--profile max-performance`
3. PostgreSQL config: See optimization section below

## PostgreSQL Configuration for Bulk Restore

Add these settings to `postgresql.conf` for faster restores:

```ini
# Memory
maintenance_work_mem = 2GB        # Faster index builds
work_mem = 256MB                  # Faster sorts

# WAL
max_wal_size = 10GB               # Less frequent checkpoints
checkpoint_timeout = 30min        # Less frequent checkpoints
wal_buffers = 64MB                # Larger WAL buffer

# For restore operations only (revert after!)
synchronous_commit = off          # Async commits (safe for restore)
full_page_writes = off            # Skip for bulk load
autovacuum = off                  # Skip during restore
```

Or apply temporarily via session:
```sql
SET maintenance_work_mem = '2GB';
SET work_mem = '256MB';
SET synchronous_commit = off;
```

## Troubleshooting Slow Restores

### Symptom: 3x slower than pg_restore

**Likely causes:**
1. Using `conservative` profile (default for cluster restores)
2. Large objects detected, forcing sequential mode
3. TUI refresh causing overhead

**Fix:**
```bash
# Force turbo profile with explicit parallelism
dbbackup restore cluster backup.tar.gz \
  --confirm \
  --profile turbo \
  --jobs 8 \
  --parallel-dbs 4 \
  --no-tui
```

### Symptom: Lock exhaustion errors

Error: `out of shared memory` or `max_locks_per_transaction`

**Fix:**
```sql
-- Increase lock limit (requires restart)
ALTER SYSTEM SET max_locks_per_transaction = 4096;
SELECT pg_reload_conf();
```

### Symptom: High CPU but slow restore

**Likely cause:** Single-threaded restore (jobs=1)

**Check:** Look for `--jobs=1` or `--jobs=0` in logs

**Fix:**
```bash
dbbackup restore single backup.dump.gz --confirm --jobs 8
```

### Symptom: Low CPU but slow restore

**Likely cause:** I/O bottleneck or PostgreSQL waiting on disk

**Check:** 
```bash
iostat -x 1    # Check disk utilization
```

**Fix:**
- Use SSD storage
- Increase `wal_buffers` and `max_wal_size`
- Use `--parallel-dbs 1` to reduce I/O contention

## Architecture: How Restore Works

```
dbbackup restore
    │
    ├── Archive Detection (format, compression)
    │
    ├── Pre-flight Checks
    │   ├── Disk space verification
    │   ├── PostgreSQL version compatibility
    │   └── Lock limit checking
    │
    ├── Extraction (for cluster backups)
    │   └── Parallel pgzip decompression
    │
    ├── Database Restore (parallel)
    │   ├── Worker pool (--parallel-dbs)
    │   └── Each worker runs pg_restore -j (--jobs)
    │
    └── Post-restore
        ├── Index rebuilding (if dropped)
        └── ANALYZE tables
```

## TUI vs No-TUI Performance

The TUI adds minimal overhead when using async progress updates (default).
However, for maximum performance:

| Mode | Tick Rate | Overhead |
|------|-----------|----------|
| TUI enabled | 250ms (4Hz) | ~1-3% |
| `--no-tui` | N/A | 0% |
| `--no-tui --quiet` | N/A | 0% |

For production batch restores, always use `--no-tui --quiet`.

## Monitoring Restore Progress

### With TUI
Progress is shown automatically with:
- Phase indicators (Extracting → Globals → Databases)
- Per-database progress with timing
- ETA calculations
- Speed in MB/s

### Without TUI
Monitor via PostgreSQL:
```sql
-- Check active restore connections
SELECT count(*), state 
FROM pg_stat_activity 
WHERE datname = 'your_database' 
GROUP BY state;

-- Check current queries
SELECT pid, now() - query_start as duration, query
FROM pg_stat_activity
WHERE datname = 'your_database'
AND state = 'active'
ORDER BY duration DESC;
```

## Best Practices Summary

1. **Use `--profile turbo` for production restores** - matches `pg_restore -j8`
2. **Use `--no-tui --quiet` for scripted/batch operations** - zero overhead
3. **Set `--jobs 8`** (or number of cores) for maximum parallelism
4. **For cluster restores, use `--parallel-dbs 4`** - balances I/O and speed
5. **Tune PostgreSQL** - `maintenance_work_mem`, `max_wal_size`
6. **Run benchmark script** - identify your specific bottlenecks
