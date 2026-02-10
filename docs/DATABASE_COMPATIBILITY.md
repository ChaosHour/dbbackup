# Database Feature Compatibility Matrix

This document details feature support across all supported database engines.

## Engine Support

| Capability | PostgreSQL | MySQL | MariaDB | Notes |
|------------|-----------|-------|---------|-------|
| Native engine (pure Go) | Yes | Yes | Yes | No external tools required |
| External tool fallback | Yes (pg_dump/pg_restore) | Yes (mysqldump/mysql) | Yes (mariadump/mariadb) | `--fallback-tools` flag |
| Single database backup | Yes | Yes | Yes | |
| Cluster backup | Yes | Yes | Yes | |
| Point-in-Time Recovery | Yes (WAL archiving) | Yes (Binary log) | Yes (Binary log) | See [PITR.md](PITR.md) |
| Incremental backup | Yes | Yes | Yes | |
| AES-256-GCM encryption | Yes | Yes | Yes | Database-agnostic |

## Restore Features

| Feature | PostgreSQL | MySQL | MariaDB | Implementation |
|---------|-----------|-------|---------|----------------|
| Parallel restore | Yes (full) | No | No | pgx connection pool |
| Adaptive workers | Yes | No | No | Metadata-driven (`SupportsParallel`) |
| Tiered restore | Yes | Yes | Yes | Pattern matching on table names |
| Restore modes (safe/balanced/turbo) | Yes | No | No | PostgreSQL-specific optimizations |
| UNLOGGED table optimization | Yes | No | No | Balanced/turbo mode (PG) |
| `sql_log_bin=0` | No | Yes | Yes | Bulk load optimization |
| `FOREIGN_KEY_CHECKS=0` | No | Yes | Yes | Bulk load optimization |
| `UNIQUE_CHECKS=0` | No | Yes | Yes | Bulk load optimization |
| `innodb_flush_log_at_trx_commit=2` | No | Yes | Yes | Bulk load optimization |
| Auto-detect restore mode | Yes | No | No | Queries `pg_is_in_recovery()` |
| Index type detection (GIN/GIST) | Yes (full) | Partial | Partial | btree/gin/gist vs btree/fulltext |
| Connection pool tuning | Yes | No | No | MaxConns, HealthCheck, IdleTime |

## Backup Features

| Feature | PostgreSQL | MySQL | MariaDB | Notes |
|---------|-----------|-------|---------|-------|
| Streaming backup | Yes | Yes | Yes | Constant memory usage |
| Parallel table backup | Yes | No | No | `SupportsParallel()` = true (PG) |
| Physical backup (pg_basebackup) | Yes | No | No | Streaming replication |
| Binary log position tracking | No | Yes | Yes | PITR anchor point |
| Schema + data separation | Yes | Yes | Yes | |
| Binary data handling | Yes (bytea) | Yes (BLOB) | Yes (BLOB) | |
| UTF-8 / charset support | Yes | Yes (utf8mb4) | Yes (utf8mb4) | |

## TUI Support

| Screen | PostgreSQL | MySQL | MariaDB |
|--------|-----------|-------|---------|
| Main menu (DB icon) | Yes (PG) | Yes (MY) | Yes (MY) |
| Table sizes | Yes | Yes | Yes |
| Kill connections | Yes | Yes | Yes |
| Drop database | Yes (psql) | Yes (mysql) | Yes (mysql) |
| Blob statistics | Yes | Yes | Yes |
| Restore preview | Yes | Yes | Yes |
| DB connect (`openTUIDatabase`) | Yes (pgx) | Yes (go-sql-driver) | Yes (go-sql-driver) |

## Cloud Storage

All cloud backends are database-agnostic:

| Provider | Support | Notes |
|----------|---------|-------|
| AWS S3 | Yes | `--cloud s3` |
| MinIO | Yes | S3-compatible |
| Azure Blob Storage | Yes | `--cloud azure` |
| Google Cloud Storage | Yes | `--cloud gcs` |
| Backblaze B2 | Yes | `--cloud b2` |

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
| MariaDB Galera cluster backup | Yes | v5.9 |

## Galera Cluster Support (MariaDB/MySQL)

dbbackup automatically detects Galera cluster nodes and validates health before backup.

| Feature | Status | Notes |
|---------|--------|-------|
| Auto-detection | Yes | Queries `wsrep_on` and `wsrep_*` status variables |
| Health check | Yes | Validates sync state, cluster status, flow control |
| Desync mode | Yes | `--galera-desync` flag (reduces cluster impact) |
| Cluster size check | Yes | `--galera-min-cluster-size` (default: 2) |
| Node preference | Yes | `--galera-prefer-node` for manual donor selection |
| Multi-node backup | No | Single-node backup recommended |

### CLI Examples

```bash
# Auto-detect Galera (no flags needed — detection is automatic)
dbbackup backup single mydb --db-type mariadb --host galera-node1

# With desync mode (reduces cluster impact during heavy backups)
dbbackup backup single mydb --db-type mariadb --galera-desync

# Strict health check with minimum cluster size
dbbackup backup single mydb --db-type mariadb \
    --galera-health-check \
    --galera-min-cluster-size 3

# Prefer a specific node
dbbackup backup single mydb --db-type mariadb --galera-prefer-node node2
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GALERA_DESYNC` | `false` | Enable desync mode during backup |
| `GALERA_MIN_CLUSTER_SIZE` | `2` | Minimum cluster size required |
| `GALERA_PREFER_NODE` | (empty) | Preferred node name |
| `GALERA_HEALTH_CHECK` | `true` | Verify node health before backup |

---

See also:
- [ENGINES.md](ENGINES.md) — Engine configuration
- [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md) — Performance optimization
- [MIGRATION_FROM_V5.md](MIGRATION_FROM_V5.md) — Upgrade guide
