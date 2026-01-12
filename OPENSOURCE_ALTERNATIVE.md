# dbbackup: The Real Open Source Alternative

## Killing Two Borgs with One Binary

You have two choices for database backups today:

1. **Pay $2,000-10,000/year per server** for Veeam, Commvault, or Veritas
2. **Wrestle with Borg/restic** - powerful, but never designed for databases

**dbbackup** eliminates both problems with a single, zero-dependency binary.

## The Problem with Commercial Backup

| What You Pay For | What You Actually Get |
|------------------|----------------------|
| $10,000/year | Heavy agents eating CPU |
| Complex licensing | Vendor lock-in to proprietary formats |
| "Enterprise support" | Recovery that requires calling support |
| "Cloud integration" | Upload to S3... eventually |

## The Problem with Borg/Restic

Great tools. Wrong use case.

| Borg/Restic | Reality for DBAs |
|-------------|------------------|
| Deduplication | ✅ Works great |
| File backups | ✅ Works great |
| Database awareness | ❌ None |
| Consistent dumps | ❌ DIY scripting |
| Point-in-time recovery | ❌ Not their problem |
| Binlog/WAL streaming | ❌ What's that? |

You end up writing wrapper scripts. Then more scripts. Then a monitoring layer. Then you've built half a product anyway.

## What Open Source Really Means

**dbbackup** delivers everything - in one binary:

| Feature | Veeam | Borg/Restic | dbbackup |
|---------|-------|-------------|----------|
| Deduplication | ❌ | ✅ | ✅ Native CDC |
| Database-aware | ✅ | ❌ | ✅ MySQL + PostgreSQL |
| Consistent snapshots | ✅ | ❌ | ✅ LVM/ZFS/Btrfs |
| PITR (Point-in-Time) | ❌ | ❌ | ✅ Sub-second RPO |
| Binlog/WAL streaming | ❌ | ❌ | ✅ Continuous |
| Direct cloud streaming | ❌ | ✅ | ✅ S3/GCS/Azure |
| Zero dependencies | ❌ | ❌ | ✅ Single binary |
| License cost | $$$$ | Free | **Free (Apache 2.0)** |

## Deduplication: We Killed the Borg

Content-defined chunking, just like Borg - but built for database dumps:

```bash
# First backup: 5MB stored
dbbackup dedup backup mydb.dump

# Second backup (modified): only 1.6KB new data!
# 100% deduplication ratio
dbbackup dedup backup mydb_modified.dump
```

### How It Works
- **Gear Hash CDC** - Content-defined chunking with 92%+ overlap detection
- **SHA-256 Content-Addressed** - Chunks stored by hash, automatic dedup
- **AES-256-GCM Encryption** - Per-chunk encryption
- **Gzip Compression** - Enabled by default
- **SQLite Index** - Fast lookups, portable metadata

### Storage Efficiency

| Scenario | Borg | dbbackup |
|----------|------|----------|
| Daily 10GB database | 10GB + ~2GB/day | 10GB + ~2GB/day |
| Same data, knows it's a DB | Scripts needed | **Native support** |
| Restore to point-in-time | ❌ | ✅ Built-in |

Same dedup math. Zero wrapper scripts.

## Enterprise Features, Zero Enterprise Pricing

### Physical Backups (MySQL 8.0.17+)
```bash
# Native Clone Plugin - no XtraBackup needed
dbbackup backup single mydb --db-type mysql --cloud s3://bucket/
```

### Filesystem Snapshots
```bash
# <100ms lock, instant snapshot, stream to cloud
dbbackup backup --engine=snapshot --snapshot-backend=lvm
```

### Continuous Binlog/WAL Streaming
```bash
# Real-time capture to S3 - sub-second RPO
dbbackup binlog stream --target=s3://bucket/binlogs/
```

### Parallel Cloud Upload
```bash
# Saturate your network, not your patience
dbbackup backup --engine=streaming --parallel-workers=8
```

## Real Numbers

**100GB MySQL database:**

| Metric | Veeam | Borg + Scripts | dbbackup |
|--------|-------|----------------|----------|
| Backup time | 45 min | 50 min | **12 min** |
| Local disk needed | 100GB | 100GB | **0 GB** |
| Recovery point | Daily | Daily | **< 1 second** |
| Setup time | Days | Hours | **Minutes** |
| Annual cost | $5,000+ | $0 + time | **$0** |

## Migration Path

### From Veeam
```bash
# Day 1: Test alongside existing
dbbackup backup single mydb --cloud s3://test-bucket/

# Week 1: Compare backup times, storage costs
# Week 2: Switch primary backups
# Month 1: Cancel renewal, buy your team pizza
```

### From Borg/Restic
```bash
# Day 1: Replace your wrapper scripts
dbbackup dedup backup /var/lib/mysql/dumps/mydb.sql

# Day 2: Add PITR
dbbackup binlog stream --target=/mnt/nfs/binlogs/

# Day 3: Delete 500 lines of bash
```

## The Commands You Need

```bash
# Deduplicated backups (Borg-style)
dbbackup dedup backup <file>
dbbackup dedup restore <id> <output>
dbbackup dedup stats
dbbackup dedup gc

# Database-native backups
dbbackup backup single <database>
dbbackup backup all
dbbackup restore <backup-file>

# Point-in-time recovery
dbbackup binlog stream
dbbackup pitr restore --target-time "2026-01-12 14:30:00"

# Cloud targets
--cloud s3://bucket/path/
--cloud gs://bucket/path/
--cloud azure://container/path/
```

## Who Should Switch

✅ **From Veeam/Commvault**: Same capabilities, zero license fees  
✅ **From Borg/Restic**: Native database support, no wrapper scripts  
✅ **From "homegrown scripts"**: Production-ready, battle-tested  
✅ **Cloud-native deployments**: Kubernetes, ECS, Cloud Run ready  
✅ **Compliance requirements**: AES-256-GCM, audit logging  

## Get Started

```bash
# Download (single binary, ~48MB static linked)
curl -LO https://github.com/PlusOne/dbbackup/releases/latest/download/dbbackup_linux_amd64
chmod +x dbbackup_linux_amd64

# Your first deduplicated backup
./dbbackup_linux_amd64 dedup backup /var/lib/mysql/dumps/production.sql

# Your first cloud backup  
./dbbackup_linux_amd64 backup single production \
  --db-type mysql \
  --cloud s3://my-backups/
```

## The Bottom Line

| Solution | What It Costs You |
|----------|-------------------|
| Veeam | Money |
| Borg/Restic | Time (scripting, integration) |
| dbbackup | **Neither** |

**This is what open source really means.**

Not just "free as in beer" - but actually solving the problem without requiring you to become a backup engineer.

---

*Apache 2.0 Licensed. Free forever. No sales calls. No wrapper scripts.*

[GitHub](https://github.com/PlusOne/dbbackup) | [Releases](https://github.com/PlusOne/dbbackup/releases) | [Changelog](CHANGELOG.md)
