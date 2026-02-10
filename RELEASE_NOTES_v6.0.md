# dbbackup v6.0.0 Release Notes

## Highlights

**Performance:** 2–4× faster restore with 90% RTO reduction capability.
**Stability:** 1358-line test suite with race detector, leak tests, multi-DB validation.
**Parity:** Full PostgreSQL, MySQL, and MariaDB support with database-specific optimizations.

---

## Major Features

### Tiered Restore (v5.8.71)

Restore critical tables first — your application comes online in minutes while background
restoration continues for the remaining data.

```bash
dbbackup restore single 500GB.sql.gz \
    --tiered-restore \
    --critical-tables="user*,payment*" \
    --confirm

# App online in 8 minutes (vs 8+ hours for full restore)
```

Three phases:
1. **Critical** — User-facing tables (restored first)
2. **Important** — Business logic tables
3. **Cold** — Analytics, logs, archives (restored last)

### 3-Tier Restore Modes (v5.8.66)

| Mode | Speedup | Safety | Use Case |
|------|---------|--------|----------|
| **Safe** | Baseline | Full WAL | Production with replication |
| **Balanced** | 2–3× | UNLOGGED during COPY | Standalone production |
| **Turbo** | 3–4× | Minimal WAL | Dev/test only |

```bash
dbbackup restore single dump.sql.gz --restore-mode=balanced --confirm
```

Auto-detection queries `pg_is_in_recovery()` and `pg_stat_replication` to select the
safest high-performance mode automatically.

### Adaptive Worker Allocation (v5.8.68)

Metadata-driven performance: when `.meta.json` exists, dbbackup automatically tunes
worker counts per table:

| Table Size | Workers Assigned |
|------------|-----------------|
| < 1 MB | 1–2 |
| 1–100 MB | 2–4 |
| 100 MB – 1 GB | 4–8 |
| > 1 GB | 8–16 |

GIN/GIST indexes automatically receive 4 GB memory allocation and 8 parallel workers.

### Multi-Database Parity (v5.8.74)

All three database engines are now production-ready:

- **PostgreSQL:** UNLOGGED tables, parallel DDL, connection pool tuning, restore mode auto-detection
- **MySQL/MariaDB:** FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0, sql_log_bin=0, innodb_flush_log_at_trx_commit=2, sort/bulk insert buffers
- **TUI:** Database-type indicators (PostgreSQL / MySQL), DB-appropriate tool names, generic error messages

### Pre-Release Validation Suite (v5.8.75)

1358-line test suite with 10 categories and 43+ checks:

1. Race detector (`go test -race`)
2. Memory & goroutine leak detection
3. Multi-database parity (source analysis + Docker integration)
4. Signal handling (SIGINT, SIGTERM, connection leak checks)
5. Backwards compatibility
6. TUI comprehensive validation (27 screens)
7. Large-scale restore (100-table smoke test)
8. Tiered restore infrastructure verification
9. Error injection (corrupt files, invalid configs, connection failures)
10. Performance baseline (binary size, startup latency, build time)

```bash
# Quick validation (~10s)
bash scripts/pre_release_suite.sh --quick

# Full validation (~5 min)
bash scripts/pre_release_suite.sh --skip-docker
```

---

## Performance

| Scenario | Standard | Optimized | Improvement |
|----------|----------|-----------|-------------|
| 100GB uniform data | 2h30m | 1h39m | **1.5×** |
| 100GB fragmented | 12h+ | 3h20m | **3.6×** |
| 500GB + tiered | 8h | 8 min (critical) | **60× RTO** |

Throughput (pgzip, 8 workers):
- Dump: 2,048 MB/s
- Restore: 1,673 MB/s
- Standard gzip baseline: 422 MB/s

Binary: 55 MB (stripped), 189ms startup latency.

---

## Quality Assurance

- **Race detector:** Clean across all packages (CGO_ENABLED=1)
- **Leak tests:** 50-iteration goroutine stress + unclosed resource scanning
- **Multi-DB:** PostgreSQL 16, MySQL 8.0, MariaDB 10.11 validated
- **Signal handling:** SIGINT/SIGTERM clean shutdown, no connection leaks
- **Backwards compat:** All v5.x backup formats supported
- **TUI:** 27 screens with Ctrl+C handling, 26 debug instrumentation points
- **CI/CD:** GitHub Actions workflow on tag push

---

## Notable Fixes Since v5.0

| Version | Fix |
|---------|-----|
| v5.8.74 | MySQL native engine enabled (was erroring out) |
| v5.8.74 | `dropDatabaseCLI` dispatches to `mysql` for MySQL (was hardcoded to `psql`) |
| v5.8.74 | Lock debug messages made database-agnostic |
| v5.8.73 | InterruptMsg handlers on all 19 TUI Update() methods |
| v5.8.73 | Debug logging on 26 Update() methods |
| v5.8.72 | Centralized DB connector (`openTUIDatabase`) |
| v5.8.72 | TUI tools database connection failures fixed |
| v5.8.69 | Connection pool tuning prevents exhaustion |
| v5.8.61 | Index ordering optimized |
| v5.8.59 | Corrupt `.meta.json` auto-regeneration |

---

## New Documentation

- [docs/DATABASE_COMPATIBILITY.md](docs/DATABASE_COMPATIBILITY.md) — Feature matrix across PG/MySQL/MariaDB
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) — Common issues and solutions
- [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) — Advanced optimization guide
- [docs/MIGRATION_FROM_V5.md](docs/MIGRATION_FROM_V5.md) — Upgrade guide (v5.x → v6.0)

---

## Upgrading

v6.0 is **fully backwards compatible** with v5.x. No configuration changes required.

```bash
# Download
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v6.0.0/dbbackup-linux-amd64
chmod +x dbbackup-linux-amd64
sudo mv dbbackup-linux-amd64 /usr/local/bin/dbbackup

# Verify
dbbackup version

# Test with existing backup
dbbackup restore single old_v5_backup.sql.gz --confirm
```

See [docs/MIGRATION_FROM_V5.md](docs/MIGRATION_FROM_V5.md) for detailed upgrade instructions.

---

## Links

- **Repository:** https://git.uuxo.net/UUXO/dbbackup
- **Mirror:** https://github.com/PlusOne/dbbackup
- **Full Changelog:** [CHANGELOG.md](CHANGELOG.md)
- **License:** Apache License 2.0
