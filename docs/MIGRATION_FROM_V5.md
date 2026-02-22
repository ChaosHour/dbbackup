# Migration Guide: v5.x → v6.0

## Overview

dbbackup v6.0.0 is **fully backwards compatible** with v5.x. All existing backup files,
configuration, and CLI flags continue to work without modification.

This guide covers what's new and how to take advantage of v6.0 features.

---

## Breaking Changes

**None.** v6.0 is a feature release with no breaking changes.

- All v5.x backup formats (`.sql`, `.sql.gz`, `.dump`, `.dump.gz`, `.tar.gz`) are supported.
- All v5.x CLI flags still work.
- All v5.x configuration files load without changes.
- The `.meta.json` format is backwards compatible (new fields are additive).

---

## Upgrade Steps

### 1. Replace the Binary

```bash
# Download the new binary
wget https://git.uuxo.net/UUXO/dbbackup/releases/download/v6.50.10/dbbackup-linux-amd64
chmod +x dbbackup-linux-amd64

# Replace the existing binary
sudo mv dbbackup-linux-amd64 /usr/local/bin/dbbackup

# Verify
dbbackup version
```

### 2. Test with Existing Backups

```bash
# Restore a v5.x backup (works unchanged)
dbbackup restore single old_v5_backup.sql.gz --confirm

# Run health check
dbbackup health --verbose
```

### 3. Optionally Enable New Features

No configuration changes are required. New features are opt-in via CLI flags.

---

## New Features You Should Use

### 1. Tiered Restore (NEW in v5.8.71)

Reduces RTO from hours to minutes by restoring critical tables first.

```bash
dbbackup restore single dump.sql.gz \
    --tiered-restore \
    --critical-tables="user*,payment*,session*" \
    --important-tables="order*,product*" \
    --confirm
```

**Before (v5.x):** Full restore takes 8 hours. App is offline the entire time.
**After (v6.0):** Critical tables online in 8 minutes. Background restore continues.

### 2. Restore Modes (NEW in v5.8.66)

Three performance tiers for different use cases:

```bash
# Recommended for standalone servers (2–3× faster)
dbbackup restore single dump.sql.gz --restore-mode=balanced --confirm

# Dev/test only (3–4× faster)
dbbackup restore single dump.sql.gz --restore-mode=turbo --confirm
```

| Mode | Speed | Safety | When to Use |
|------|-------|--------|-------------|
| `safe` | Baseline | Full WAL | Replication, PITR, production |
| `balanced` | 2–3× | UNLOGGED during COPY | Standalone production |
| `turbo` | 3–4× | Minimal WAL | Dev/test only |

### 3. Multi-Database Support (IMPROVED in v5.8.74)

MySQL and MariaDB are now fully supported with native engine and bulk load optimizations:

```bash
# Explicit database type
dbbackup backup single mydb --database-type=mysql
dbbackup restore single dump.sql --database-type=mysql --confirm

# TUI auto-detects and shows the database type
dbbackup interactive --db-type mysql
```

### 4. Native Engine (Default since v5.0, enhanced in v6.0)

The native engine uses pure Go for all database operations — no external tools required:

```bash
# Native engine is the default (--native=true)
dbbackup backup single mydb

# Force external tools (pg_dump, mysqldump) if needed
dbbackup backup single mydb --native=false
```

### 5. Pre-Release Validation (NEW in v5.8.75)

Run the comprehensive validation via CLI:

```bash
# Quick validation
dbbackup validate --quick

# Full validation
dbbackup validate --all
```

---

## Configuration File Changes

### v5.x Format (Still Works)

```yaml
restore:
  workers: 8

backup:
  compression: 6
  directory: /var/backups/dbbackup
```

### v6.0 Format (Optional New Fields)

```yaml
restore:
  workers: 8
  restore_mode: balanced         # NEW: safe | balanced | turbo
  tiered_restore: true           # NEW: enable phased restore
  critical_tables:               # NEW: phase 1 tables
    - "user*"
    - "payment*"
  important_tables:              # NEW: phase 2 tables
    - "order*"
    - "product*"

backup:
  compression: 6
  directory: /var/backups/dbbackup
```

All new fields are optional. Omitting them uses safe defaults.

---

## CLI Flag Changes

All v5.x flags still work. New flags are **additive only**:

| New Flag | Default | Description |
|----------|---------|-------------|
| `--restore-mode` | `safe` | Restore performance mode |
| `--tiered-restore` | `false` | Enable phased restore |
| `--critical-tables` | (none) | Comma-separated critical table patterns |
| `--important-tables` | (none) | Comma-separated important table patterns |
| `--cold-tables` | (none) | Comma-separated cold table patterns |
| `--database-type` | auto-detect | Explicit database type |
| `--native-debug` | `false` | Native engine debug logging |
| `--debug-locks` | `false` | Lock debugging |
| `--tui-debug` | `false` | TUI state machine debugging |

---

## Metadata (.meta.json) Changes

### v5.x Format (Still Parsed)

```json
{
  "database": "mydb",
  "size_bytes": 1048576,
  "created_at": "2025-01-01T00:00:00Z"
}
```

### v6.0 Format (Additive Fields)

```json
{
  "database": "mydb",
  "size_bytes": 1048576,
  "created_at": "2025-01-01T00:00:00Z",
  "tables": [
    {
      "name": "users",
      "size_bytes": 524288,
      "row_count": 50000,
      "index_types": ["btree", "gin"]
    }
  ],
  "database_type": "postgresql",
  "engine_version": "16.3"
}
```

The new fields enable adaptive worker allocation and index-aware resource tuning.
Old `.meta.json` files without these fields are parsed normally — the new features
simply won't activate.

---

## Verify Your Upgrade

After upgrading, run:

```bash
# 1. Check version
dbbackup version

# 2. Run health check
dbbackup health --verbose

# 3. Test a backup
dbbackup backup single testdb --compression=1

# 4. Test a restore
dbbackup restore single testdb_backup.sql.gz --restore-mode=balanced --confirm

# 5. Run validation
dbbackup validate --quick
```

---

## Rolling Back

If you need to downgrade back to v5.x:

```bash
# Simply replace the binary with the old version
sudo mv /usr/local/bin/dbbackup.v5-backup /usr/local/bin/dbbackup

# All backups created by v6.0 are compatible with v5.x
dbbackup restore single v6_backup.sql.gz --confirm
```

---

See also:
- [DATABASE_COMPATIBILITY.md](DATABASE_COMPATIBILITY.md) — Feature matrix
- [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md) — Performance optimization
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — Common issues & solutions
