# Performance Tuning Guide

Optimize dbbackup for maximum restore and backup performance.

---

## Quick Wins

### 1. Use Balanced Mode (2–3× speedup)

The default `safe` mode uses full WAL logging. For standalone servers (no replication),
`balanced` mode uses UNLOGGED tables during COPY and switches to LOGGED before index creation:

```bash
dbbackup restore single dump.sql.gz \
    --restore-mode=balanced \
    --workers=16
```

**How it works:**
- Tables set to UNLOGGED during bulk COPY phase (no WAL writes → 2–3× faster)
- Tables switched back to LOGGED before index creation (crash-safe)
- Final state is fully WAL-logged and replication-ready

**When to use:** Standalone PostgreSQL servers, staging environments.

### 2. Enable Tiered Restore (90% RTO reduction)

Bring critical tables online first while the rest restores in the background:

```bash
dbbackup restore single dump.sql.gz \
    --tiered-restore \
    --critical-tables="user*,payment*,session*" \
    --important-tables="order*,product*,inventory*"
```

**Phase order:**
1. **Critical** — User-facing tables restored first (app comes online)
2. **Important** — Business logic tables
3. **Cold** — Analytics, logs, archives (restored last)

**Real-world example:** 500GB database with tiered restore:
- Full restore: 8 hours
- Critical tables online: 8 minutes (60× RTO improvement)

### 3. Turbo Mode for Dev/Test (3–4× speedup)

Maximum speed with minimal safety. **Dev/test only — not for production:**

```bash
dbbackup restore single dump.sql.gz \
    --restore-mode=turbo \
    --workers=32
```

**Turbo optimizations:**
- UNLOGGED tables for entire restore
- `synchronous_commit=off`
- `checkpoint_timeout=1h`
- `max_wal_size=10GB`
- `wal_compression=on`
- All settings restored to defaults after completion

---

## PostgreSQL Server Tuning

### Before a Large Restore

Temporarily adjust PostgreSQL settings for faster bulk loading:

```sql
-- Increase maintenance memory (for index builds)
ALTER SYSTEM SET maintenance_work_mem = '4GB';

-- More parallel workers for index builds
ALTER SYSTEM SET max_parallel_maintenance_workers = 8;

-- Larger WAL for fewer checkpoints
ALTER SYSTEM SET max_wal_size = '16GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';

-- Apply without restart
SELECT pg_reload_conf();
```

### After Restore — Reset to Defaults

```sql
ALTER SYSTEM RESET maintenance_work_mem;
ALTER SYSTEM RESET max_parallel_maintenance_workers;
ALTER SYSTEM RESET max_wal_size;
ALTER SYSTEM RESET checkpoint_timeout;
SELECT pg_reload_conf();

-- Run ANALYZE to update statistics
ANALYZE;
```

### Connection Pool Sizing

dbbackup automatically sizes the connection pool to `workers × 2`. Ensure your
PostgreSQL `max_connections` supports this:

```sql
-- Check current limit
SHOW max_connections;

-- Formula: max_connections >= (workers × 2) + 20 (headroom)
-- For 16 workers: max_connections >= 52
ALTER SYSTEM SET max_connections = 100;
-- Requires restart
```

---

## MySQL / MariaDB Server Tuning

### Automatic Bulk Load Optimizations

dbbackup automatically applies these settings during restore (and resets them after):

| Setting | Value During Restore | Purpose |
|---------|---------------------|---------|
| `FOREIGN_KEY_CHECKS` | 0 | Skip FK validation during load |
| `UNIQUE_CHECKS` | 0 | Skip unique index checks during load |
| `AUTOCOMMIT` | 0 | Batch INSERTs into transactions |
| `sql_log_bin` | 0 | Skip binary logging (if not replicating) |
| `innodb_flush_log_at_trx_commit` | 2 | Flush every second instead of every commit |

### Additional MySQL Server Settings

For very large restores, you can also tune the server:

```sql
-- Larger sort buffer for index builds
SET GLOBAL sort_buffer_size = 268435456;  -- 256MB

-- Larger bulk insert buffer
SET GLOBAL bulk_insert_buffer_size = 268435456;  -- 256MB

-- Larger InnoDB buffer pool (set to ~70% of RAM)
SET GLOBAL innodb_buffer_pool_size = 8589934592;  -- 8GB
```

---

## Worker Allocation

### Adaptive Workers (Automatic)

When `.meta.json` metadata exists (generated during backup), dbbackup automatically
allocates workers based on table size:

| Table Size | Workers | Rationale |
|------------|---------|-----------|
| < 1 MB | 1 | Overhead dominates |
| 1–100 MB | 2 | Light parallelism |
| 100 MB – 1 GB | 4–8 | Moderate parallelism |
| > 1 GB | 8–16 | Maximum throughput |

### Manual Worker Control

```bash
# Explicit worker count
dbbackup restore single dump.sql.gz --workers=16

# Let the system decide (based on CPU cores)
dbbackup restore single dump.sql.gz --jobs=0
```

### Index-Aware Allocation

dbbackup detects index types and allocates resources accordingly:

| Index Type | Memory | Workers | Rationale |
|------------|--------|---------|-----------|
| B-tree | Default | Default | Standard |
| GIN (full-text) | 4 GB | 8 | Memory-intensive |
| GIST (geometric) | 4 GB | 8 | Memory-intensive |
| Hash | Default | Default | Standard |

---

## Backup Performance

### Parallel Dump

```bash
# Maximum parallelism for backup
dbbackup backup single mydb \
    --dump-jobs=16 \
    --max-cores=16 \
    --cpu-workload=cpu-intensive
```

### Compression Levels

| Level | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| 0 | Fastest | None | Local disk, fast network |
| 1 | Very fast | ~50% | Good balance for most |
| 3 | Fast | ~60% | Default |
| 6 | Moderate | ~70% | Default for cloud uploads |
| 9 | Slow | ~75% | Maximum compression, archival |

```bash
# Fast local backup
dbbackup backup single mydb --compression=1

# Maximum compression for cloud
dbbackup backup single mydb --compression=9
```

### Throughput Benchmarks

Using pgzip with 8 workers:

| Operation | Throughput |
|-----------|------------|
| Dump (BestSpeed) | 2,048 MB/s |
| Dump (Default) | 915 MB/s |
| Decompress | 1,673 MB/s |
| Standard gzip (baseline) | 422 MB/s |

---

## Benchmarking Your Setup

Use the built-in benchmark commands to establish baselines:

```bash
# Compare dbbackup vs native pg_restore
dbbackup benchmark run mydb --engine postgres --iterations 3

# Run the full cross-engine benchmark matrix
dbbackup benchmark matrix --iterations 2
```

### Key Metrics to Track

1. **Restore throughput** (MB/s) — `data_size / restore_time`
2. **RTO** — Time until critical tables are queryable
3. **Peak memory** — Should stay under 1 GB regardless of DB size
4. **Worker utilization** — Check with `htop` during restore

---

## Monitoring Restore Performance

### Real-Time Progress

```bash
# TUI mode (default) — shows progress bar, table counts, throughput
dbbackup restore single dump.sql.gz

# CLI progress
dbbackup restore single dump.sql.gz --no-tui --confirm
```

### Prometheus Metrics

If you have Prometheus monitoring:

```bash
# Enable metrics exporter
dbbackup metrics serve --port 9100
```

Key metrics:
- `dbbackup_restore_tables_completed` — Tables restored
- `dbbackup_restore_bytes_processed` — Data throughput
- `dbbackup_restore_duration_seconds` — Total time

See [METRICS.md](METRICS.md) for full metric list.

---

## Common Performance Mistakes

1. **Using safe mode on a standalone server** — Switch to `balanced` for 2–3× speedup.
2. **Too many workers** — More workers than CPU cores causes context switching overhead. Rule: `workers ≤ nproc`.
3. **Not using tiered restore** — If you only need a few tables online quickly, tiered restore is dramatically faster.
4. **High compression for local backups** — Use `--compression=1` when backup stays on local disk.
5. **Ignoring disk I/O** — SSDs give 3–5× better restore performance vs HDDs. Check with `iostat -x 1`.

---

See also:
- [DATABASE_COMPATIBILITY.md](DATABASE_COMPATIBILITY.md) — Feature matrix
- [RESTORE_PROFILES.md](RESTORE_PROFILES.md) — Restore resource profiles
- [RESTORE_PERFORMANCE.md](RESTORE_PERFORMANCE.md) — Detailed performance analysis
