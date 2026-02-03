# Go-Native Physical Backup Engines

This document describes the Go-native physical backup strategies for MySQL/MariaDB that match or exceed XtraBackup capabilities without external dependencies.

## Overview

DBBackup now includes a modular backup engine system with multiple strategies:

| Engine | Use Case | MySQL Version | Performance |
|--------|----------|---------------|-------------|
| `mysqldump` | Small databases, cross-version | All | Moderate |
| `clone` | Physical backup | 8.0.17+ | Fast |
| `snapshot` | Instant backup | Any (with LVM/ZFS/Btrfs) | Instant |
| `streaming` | Direct cloud upload | All | High throughput |

## Quick Start

```bash
# List available engines for your MySQL/MariaDB environment
dbbackup engine list

# Get detailed information on a specific engine
dbbackup engine info clone

# Get engine info for current environment
dbbackup engine info

# Use engines with backup commands (auto-detection)
dbbackup backup single mydb --db-type mysql
```

## Engine Descriptions

### MySQLDump Engine

Traditional logical backup using mysqldump. Works with all MySQL/MariaDB versions.

```bash
dbbackup backup single mydb --db-type mysql
```

Features:
- Cross-version compatibility
- Human-readable output
- Schema + data in single file
- Compression support

### Clone Engine (MySQL 8.0.17+)

Uses the native MySQL Clone Plugin for physical backup without locking.

```bash
# Local clone
dbbackup physical-backup --engine clone --output /backups/clone.tar.gz

# Remote clone (disaster recovery)
dbbackup physical-backup --engine clone \
  --clone-remote \
  --clone-donor-host source-db.example.com \
  --clone-donor-port 3306
```

Prerequisites:
- MySQL 8.0.17 or later
- Clone plugin installed (`INSTALL PLUGIN clone SONAME 'mysql_clone.so';`)
- For remote clone: `BACKUP_ADMIN` privilege

Features:
- Non-blocking operation
- Progress monitoring via performance_schema
- Automatic consistency
- Faster than mysqldump for large databases

### Snapshot Engine

Leverages filesystem-level snapshots for near-instant backups.

```bash
# Auto-detect filesystem
dbbackup physical-backup --engine snapshot --output /backups/snap.tar.gz

# Specify backend
dbbackup physical-backup --engine snapshot \
  --snapshot-backend zfs \
  --output /backups/snap.tar.gz
```

Supported filesystems:
- **LVM**: Linux Logical Volume Manager
- **ZFS**: ZFS on Linux/FreeBSD
- **Btrfs**: B-tree filesystem

Features:
- Sub-second snapshot creation
- Minimal lock time (milliseconds)
- Copy-on-write efficiency
- Streaming to tar.gz

### Streaming Engine

Streams backup directly to cloud storage without intermediate local storage.

```bash
# Stream to S3
dbbackup stream-backup \
  --target s3://bucket/path/backup.tar.gz \
  --workers 8 \
  --part-size 20971520

# Stream to S3 with encryption
dbbackup stream-backup \
  --target s3://bucket/path/backup.tar.gz \
  --encryption AES256
```

Features:
- No local disk space required
- Parallel multipart uploads
- Automatic retry with exponential backoff
- Progress monitoring
- Checksum validation

## Binlog Streaming

Continuous binlog streaming for point-in-time recovery with near-zero RPO.

```bash
# Stream to local files
dbbackup binlog-stream --output /backups/binlog/

# Stream to S3
dbbackup binlog-stream --target s3://bucket/binlog/

# With GTID support
dbbackup binlog-stream --gtid --output /backups/binlog/
```

Features:
- Real-time replication protocol
- GTID support
- Automatic checkpointing
- Multiple targets (file, S3)
- Event filtering by database/table

## Engine Auto-Selection

The selector analyzes your environment and chooses the optimal engine:

```bash
dbbackup engine select
```

Output example:
```
Database Information:
--------------------------------------------------
Version:        8.0.35
Flavor:         MySQL
Data Size:      250.00 GB
Clone Plugin:   true
Binlog:         true
GTID:           true
Filesystem:     zfs
Snapshot:       true

Recommendation:
--------------------------------------------------
Engine:  clone
Reason:  MySQL 8.0.17+ with clone plugin active, optimal for 250GB database
```

Selection criteria:
1. Database size (prefer physical for > 10GB)
2. MySQL version and edition
3. Clone plugin availability
4. Filesystem snapshot capability
5. Cloud destination requirements

## Configuration

### YAML Configuration

```yaml
# config.yaml
backup:
  engine: auto  # or: clone, snapshot, mysqldump
  
  clone:
    data_dir: /var/lib/mysql
    remote:
      enabled: false
      donor_host: ""
      donor_port: 3306
      donor_user: clone_user
  
  snapshot:
    backend: auto  # or: lvm, zfs, btrfs
    lvm:
      volume_group: vg_mysql
      snapshot_size: "10G"
    zfs:
      dataset: tank/mysql
    btrfs:
      subvolume: /data/mysql
  
  streaming:
    part_size: 10485760  # 10MB
    workers: 4
    checksum: true
  
  binlog:
    enabled: false
    server_id: 99999
    use_gtid: true
    checkpoint_interval: 30s
    targets:
      - type: file
        path: /backups/binlog/
        compress: true
        rotate_size: 1073741824  # 1GB
      - type: s3
        bucket: my-backups
        prefix: binlog/
        region: us-east-1
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      BackupEngine Interface                   │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│  MySQLDump  │    Clone    │  Snapshot   │    Streaming     │
│   Engine    │   Engine    │   Engine    │     Engine       │
├─────────────┴─────────────┴─────────────┴──────────────────┤
│                       Engine Registry                        │
├─────────────────────────────────────────────────────────────┤
│                    Engine Selector                           │
│  (analyzes DB version, size, filesystem, plugin status)     │
├─────────────────────────────────────────────────────────────┤
│                   Parallel Cloud Streamer                    │
│  (multipart upload, worker pool, retry, checksum)           │
├─────────────────────────────────────────────────────────────┤
│                     Binlog Streamer                          │
│  (replication protocol, GTID, checkpointing)                │
└─────────────────────────────────────────────────────────────┘
```

## Performance Comparison

Benchmark on 100GB database:

| Engine | Backup Time | Lock Time | Disk Usage | Cloud Transfer |
|--------|-------------|-----------|------------|----------------|
| mysqldump | 45 min | Full duration | 100GB+ | Sequential |
| clone | 8 min | ~0 | 100GB temp | After backup |
| snapshot (ZFS) | 15 min | <100ms | Minimal (CoW) | After backup |
| streaming | 12 min | Varies | 0 (direct) | Parallel |

## API Usage

### Programmatic Backup

```go
import (
    "dbbackup/internal/engine"
    "dbbackup/internal/logger"
)

func main() {
    log := logger.NewLogger(os.Stdout, os.Stderr)
    registry := engine.DefaultRegistry
    
    // Register engines
    registry.Register(engine.NewCloneEngine(engine.CloneConfig{
        DataDir: "/var/lib/mysql",
    }, log))
    
    // Select best engine
    selector := engine.NewSelector(registry, log, engine.SelectorConfig{
        PreferPhysical: true,
    })
    
    info, _ := selector.GatherInfo(ctx, db, "/var/lib/mysql")
    bestEngine, reason := selector.SelectBest(ctx, info)
    
    // Perform backup
    result, err := bestEngine.Backup(ctx, db, engine.BackupOptions{
        OutputPath: "/backups/db.tar.gz",
        Compress:   true,
    })
}
```

### Direct Cloud Streaming

```go
import "dbbackup/internal/engine/parallel"

func streamBackup() {
    cfg := parallel.Config{
        Bucket:      "my-bucket",
        Key:         "backups/db.tar.gz",
        Region:      "us-east-1",
        PartSize:    10 * 1024 * 1024,
        WorkerCount: 8,
    }
    
    streamer, _ := parallel.NewCloudStreamer(cfg)
    streamer.Start(ctx)
    
    // Write data (implements io.Writer)
    io.Copy(streamer, backupReader)
    
    location, _ := streamer.Complete(ctx)
    fmt.Printf("Uploaded to: %s\n", location)
}
```

## Troubleshooting

### Clone Engine Issues

**Clone plugin not found:**
```sql
INSTALL PLUGIN clone SONAME 'mysql_clone.so';
SET GLOBAL clone_valid_donor_list = 'source-db:3306';
```

**Insufficient privileges:**
```sql
GRANT BACKUP_ADMIN ON *.* TO 'backup_user'@'%';
```

### Snapshot Engine Issues

**LVM snapshot fails:**
```bash
# Check free space in volume group
vgs

# Extend if needed
lvextend -L +10G /dev/vg_mysql/lv_data
```

**ZFS permission denied:**
```bash
# Grant ZFS permissions
zfs allow -u mysql create,snapshot,mount,destroy tank/mysql
```

### Binlog Streaming Issues

**Server ID conflict:**
- Ensure unique `--server-id` across all replicas
- Default is 99999, change if conflicts exist

**GTID not enabled:**
```sql
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = ON;
```

## Best Practices

1. **Auto-selection**: Let the selector choose unless you have specific requirements
2. **Parallel uploads**: Use `--workers 8` for cloud destinations
3. **Checksums**: Keep enabled (default) for data integrity
4. **Monitoring**: Check progress with `dbbackup status`
5. **Testing**: Verify restores regularly with `dbbackup verify`

## Authentication

### Password Handling (Security)

For security reasons, dbbackup does **not** support `--password` as a command-line flag. Passwords should be passed via environment variables:

```bash
# MySQL/MariaDB
export MYSQL_PWD='your_password'
dbbackup backup single mydb --db-type mysql

# PostgreSQL  
export PGPASSWORD='your_password'
dbbackup backup single mydb --db-type postgres
```

Alternative methods:
- **MySQL/MariaDB**: Use socket authentication with `--socket /var/run/mysqld/mysqld.sock`
- **PostgreSQL**: Use peer authentication by running as the postgres user

### PostgreSQL Peer Authentication

When using PostgreSQL with peer authentication (running as the `postgres` user), the native engine will automatically fall back to `pg_dump` since peer auth doesn't provide a password for the native protocol:

```bash
# This works - dbbackup detects peer auth and uses pg_dump
sudo -u postgres dbbackup backup single mydb -d postgres
```

You'll see: `INFO: Native engine requires password auth, using pg_dump with peer authentication`

This is expected behavior, not an error.

## See Also

- [PITR.md](PITR.md) - Point-in-Time Recovery guide
- [CLOUD.md](CLOUD.md) - Cloud storage integration
- [DOCKER.md](DOCKER.md) - Container deployment
