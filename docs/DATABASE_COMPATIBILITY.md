# Database Feature Compatibility Matrix

This document details feature support across all supported database engines.

## Engine Support

| Capability | PostgreSQL | MySQL | MariaDB | Notes |
|------------|-----------|-------|---------|-------|
| Native engine (pure Go) | ✅ | ✅ | ✅ | No external tools required |
| External tool fallback | ✅ pg_dump/pg_restore | ✅ mysqldump/mysql | ✅ mariadump/mariadb | `--fallback-tools` flag |
| Single database backup | ✅ | ✅ | ✅ | |
| Cluster backup | ✅ | ✅ | ✅ | |
| Point-in-Time Recovery | ✅ WAL archiving | ✅ Binary log | ✅ Binary log | See [PITR.md](PITR.md) |
| Incremental backup | ✅ | ✅ | ✅ | |
| AES-256-GCM encryption | ✅ | ✅ | ✅ | Database-agnostic |

## Restore Features

| Feature | PostgreSQL | MySQL | MariaDB | Implementation |
|---------|-----------|-------|---------|----------------|
| Parallel restore | ✅ Full | ❌ | ❌ | pgx connection pool |
| Adaptive workers | ✅ | ❌ | ❌ | Metadata-driven (`SupportsParallel`) |
| Tiered restore | ✅ | ✅ | ✅ | Pattern matching on table names |
| Restore modes (safe/balanced/turbo) | ✅ | ❌ | ❌ | PostgreSQL-specific optimizations |
| UNLOGGED table optimization | ✅ | ❌ | ❌ | Balanced/turbo mode (PG) |
| `sql_log_bin=0` | ❌ | ✅ | ✅ | Bulk load optimization |
| `FOREIGN_KEY_CHECKS=0` | ❌ | ✅ | ✅ | Bulk load optimization |
| `UNIQUE_CHECKS=0` | ❌ | ✅ | ✅ | Bulk load optimization |
| `innodb_flush_log_at_trx_commit=2` | ❌ | ✅ | ✅ | Bulk load optimization |
| Auto-detect restore mode | ✅ | ❌ | ❌ | Queries `pg_is_in_recovery()` |
| Index type detection (GIN/GIST) | ✅ Full | ⚠️ Partial | ⚠️ Partial | btree/gin/gist vs btree/fulltext |
| Connection pool tuning | ✅ | ❌ | ❌ | MaxConns, HealthCheck, IdleTime |

## Backup Features

| Feature | PostgreSQL | MySQL | MariaDB | Notes |
|---------|-----------|-------|---------|-------|
| Streaming backup | ✅ | ✅ | ✅ | Constant memory usage |
| Parallel table backup | ✅ | ❌ | ❌ | `SupportsParallel()` = true (PG) |
| Physical backup (pg_basebackup) | ✅ | ❌ | ❌ | Streaming replication |
| Binary log position tracking | ❌ | ✅ | ✅ | PITR anchor point |
| Schema + data separation | ✅ | ✅ | ✅ | |
| Binary data handling | ✅ bytea | ✅ BLOB | ✅ BLOB | |
| UTF-8 / charset support | ✅ | ✅ utf8mb4 | ✅ utf8mb4 | |

## TUI Support

| Screen | PostgreSQL | MySQL | MariaDB |
|--------|-----------|-------|---------|
| Main menu (DB icon) | ✅ 🐘 | ✅ 🐬 | ✅ 🐬 |
| Table sizes | ✅ | ✅ | ✅ |
| Kill connections | ✅ | ✅ | ✅ |
| Drop database | ✅ psql | ✅ mysql | ✅ mysql |
| Blob statistics | ✅ | ✅ | ✅ |
| Restore preview | ✅ | ✅ | ✅ |
| DB connect (`openTUIDatabase`) | ✅ pgx | ✅ go-sql-driver | ✅ go-sql-driver |

## Cloud Storage

All cloud backends are database-agnostic:

| Provider | Support | Notes |
|----------|---------|-------|
| AWS S3 | ✅ | `--cloud s3` |
| MinIO | ✅ | S3-compatible |
| Azure Blob Storage | ✅ | `--cloud azure` |
| Google Cloud Storage | ✅ | `--cloud gcs` |
| Backblaze B2 | ✅ | `--cloud b2` |

## Minimum Versions

| Database | Minimum | Recommended | Tested |
|----------|---------|-------------|--------|
| PostgreSQL | 10 | 16+ | 16.x, 17.x |
| MySQL | 5.7 | 8.0+ | 8.0.x |
| MariaDB | 10.3 | 10.11+ | 10.11.x |

## Future Roadmap

| Feature | Status | Target |
|---------|--------|--------|
| MySQL parallel restore | Planned | v6.1 |
| MySQL WAL-based incremental | Planned | v6.1 |
| PostgreSQL custom format | Planned | v6.2 |
| MariaDB Galera cluster backup | Under evaluation | TBD |

---

See also:
- [ENGINES.md](ENGINES.md) — Engine configuration
- [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md) — Performance optimization
- [MIGRATION_FROM_V5.md](MIGRATION_FROM_V5.md) — Upgrade guide
