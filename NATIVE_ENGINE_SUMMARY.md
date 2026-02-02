# Native Database Engine Implementation Summary

## Current Status: Full Native Engine Support (v5.5.0+)

**Goal:** Zero dependency on external tools (pg_dump, pg_restore, mysqldump, mysql)

**Reality:** Native engine is **NOW AVAILABLE FOR ALL OPERATIONS** when using `--native` flag!

## Engine Support Matrix

| Operation | Default Mode | With `--native` Flag |
|-----------|-------------|---------------------|
| **Single DB Backup** | ✅ Native Go | ✅ Native Go |
| **Single DB Restore** | ✅ Native Go | ✅ Native Go |
| **Cluster Backup** | pg_dump (custom format) | ✅ **Native Go** (SQL format) |
| **Cluster Restore** | pg_restore | ✅ **Native Go** (for .sql.gz files) |

### NEW: Native Cluster Operations (v5.5.0)

```bash
# Native cluster backup - creates SQL format dumps, no pg_dump needed!
./dbbackup backup cluster --native

# Native cluster restore - restores .sql.gz files with pure Go, no pg_restore!
./dbbackup restore cluster backup.tar.gz --native --confirm
```

### Format Selection

| Format | Created By | Restored By | Size | Speed |
|--------|------------|-------------|------|-------|
| **SQL** (.sql.gz) | Native Go or pg_dump | Native Go or psql | Larger | Medium |
| **Custom** (.dump) | pg_dump -Fc | pg_restore only | Smaller | Fast (parallel) |

### When to Use Native Mode

**Use `--native` when:**
- External tools (pg_dump/pg_restore) are not installed
- Running in minimal containers without PostgreSQL client
- Building a single statically-linked binary deployment
- Simplifying disaster recovery procedures

**Use default mode when:**
- Maximum backup/restore performance is critical
- You need parallel restore with `-j` option
- Backup size is a primary concern

## Architecture Overview

### Core Native Engines

1. **PostgreSQL Native Engine** (`internal/engine/native/postgresql.go`)
   - Pure Go implementation using `pgx/v5` driver
   - Direct PostgreSQL protocol communication
   - Native SQL generation and COPY data export
   - Advanced data type handling with proper escaping

2. **MySQL Native Engine** (`internal/engine/native/mysql.go`)
   - Pure Go implementation using `go-sql-driver/mysql`
   - Direct MySQL protocol communication
   - Batch INSERT generation with proper data type handling
   - Binary data support with hex encoding

3. **Engine Manager** (`internal/engine/native/manager.go`)
   - Pluggable architecture for engine selection
   - Configuration-based engine initialization
   - Unified backup orchestration across engines

4. **Restore Engine Framework** (`internal/engine/native/restore.go`)
   - Parses SQL statements from backup
   - Uses `CopyFrom` for COPY data
   - Progress tracking and status reporting

## Configuration

```bash
# SINGLE DATABASE (native is default for SQL format)
./dbbackup backup single mydb              # Uses native engine
./dbbackup restore backup.sql.gz --native  # Uses native engine

# CLUSTER BACKUP
./dbbackup backup cluster                  # Default: pg_dump custom format
./dbbackup backup cluster --native         # NEW: Native Go, SQL format

# CLUSTER RESTORE
./dbbackup restore cluster backup.tar.gz --confirm          # Default: pg_restore
./dbbackup restore cluster backup.tar.gz --native --confirm # NEW: Native Go for .sql.gz files

# FALLBACK MODE
./dbbackup backup cluster --native --fallback-tools  # Try native, fall back if fails
```

### Config Defaults

```go
// internal/config/config.go
UseNativeEngine: true,   // Native is default for single DB
FallbackToTools: true,   // Fall back to tools if native fails
```

## When Native Engine is Used

### ✅ Native Engine for Single DB (Default)

```bash
# Single DB backup to SQL format
./dbbackup backup single mydb
# → Uses native.PostgreSQLNativeEngine.Backup()
# → Pure Go: pgx COPY TO STDOUT

# Single DB restore from SQL format  
./dbbackup restore mydb_backup.sql.gz --database=mydb
# → Uses native.PostgreSQLRestoreEngine.Restore()
# → Pure Go: pgx CopyFrom()
```

### ✅ Native Engine for Cluster (With --native Flag)

```bash
# Cluster backup with native engine
./dbbackup backup cluster --native
# → For each database: native.PostgreSQLNativeEngine.Backup()
# → Creates .sql.gz files (not .dump)
# → Pure Go: no pg_dump required!

# Cluster restore with native engine
./dbbackup restore cluster backup.tar.gz --native --confirm
# → For each .sql.gz: native.PostgreSQLRestoreEngine.Restore()
# → Pure Go: no pg_restore required!
```

### External Tools (Default for Cluster, or Custom Format)

```bash
# Cluster backup (default - uses custom format for efficiency)
./dbbackup backup cluster
# → Uses pg_dump -Fc for each database
# → Reason: Custom format enables parallel restore

# Cluster restore (default)
./dbbackup restore cluster backup.tar.gz --confirm
# → Uses pg_restore for .dump files
# → Uses native engine for .sql.gz files automatically!

# Single DB restore from .dump file
./dbbackup restore mydb_backup.dump --database=mydb
# → Uses pg_restore
# → Reason: Custom format binary file
```

## Performance Comparison

| Method | Format | Backup Speed | Restore Speed | File Size | External Tools |
|--------|--------|-------------|---------------|-----------|----------------|
| Native Go | SQL.gz | Medium | Medium | Larger | ❌ None |
| pg_dump/restore | Custom | Fast | Fast (parallel) | Smaller | ✅ Required |

### Recommendation

| Scenario | Recommended Mode |
|----------|------------------|
| No PostgreSQL tools installed | `--native` |
| Minimal container deployment | `--native` |
| Maximum performance needed | Default (pg_dump) |
| Large databases (>10GB) | Default with `-j8` |
| Disaster recovery simplicity | `--native` |

## Implementation Details

### Native Backup Flow

```
User → backupCmd → cfg.UseNativeEngine=true → runNativeBackup()
                                                    ↓
                               native.EngineManager.BackupWithNativeEngine()
                                                    ↓
                               native.PostgreSQLNativeEngine.Backup()
                                                    ↓
                               pgx: COPY table TO STDOUT → SQL file
```

### Native Restore Flow

```
User → restoreCmd → cfg.UseNativeEngine=true → runNativeRestore()
                                                     ↓
                                native.EngineManager.RestoreWithNativeEngine()
                                                     ↓
                                native.PostgreSQLRestoreEngine.Restore()
                                                     ↓
                                Parse SQL → pgx CopyFrom / Exec → Database
```

### Native Cluster Flow (NEW in v5.5.0)

```
User → backup cluster --native
                ↓
        For each database:
            native.PostgreSQLNativeEngine.Backup()
                     ↓
            Create .sql.gz file (not .dump)
                     ↓
        Package all .sql.gz into tar.gz archive

User → restore cluster --native --confirm
                ↓
        Extract tar.gz → .sql.gz files
                ↓
        For each .sql.gz:
            native.PostgreSQLRestoreEngine.Restore()
                     ↓
            Parse SQL → pgx CopyFrom → Database
```

### External Tools Flow (Default Cluster)

```
User → restoreClusterCmd → engine.RestoreCluster()
                                    ↓
                           Extract tar.gz → .dump files
                                    ↓
                           For each .dump:
                               cleanup.SafeCommand("pg_restore", args...)
                                    ↓
                           PostgreSQL restores data
```

## CLI Flags

```bash
--native              # Use native engine for backup/restore (works for cluster too!)
--fallback-tools      # Fall back to external if native fails
--native-debug        # Enable native engine debug logging
```

## Future Improvements

1. ~~Add SQL format option for cluster backup~~ ✅ **DONE in v5.5.0**

2. **Implement custom format parser in Go**
   - Very complex (PostgreSQL proprietary format)
   - Would enable native restore of .dump files

3. **Add parallel native restore**
   - Parse SQL file into table chunks
   - Restore multiple tables concurrently

## Summary

| Feature | Default | With `--native` |
|---------|---------|-----------------|
| Single DB backup (SQL) | ✅ Native Go | ✅ Native Go |
| Single DB restore (SQL) | ✅ Native Go | ✅ Native Go |
| Single DB restore (.dump) | pg_restore | pg_restore |
| Cluster backup | pg_dump (.dump) | ✅ **Native Go (.sql.gz)** |
| Cluster restore (.dump) | pg_restore | pg_restore |
| Cluster restore (.sql.gz) | psql | ✅ **Native Go** |
| MySQL backup | ✅ Native Go | ✅ Native Go |
| MySQL restore | ✅ Native Go | ✅ Native Go |

**Bottom Line:** With `--native` flag, dbbackup can now perform **ALL operations** without external tools, as long as you create native-format backups. This enables single-binary deployment with zero PostgreSQL client dependencies.

**Bottom Line:** With `--native` flag, dbbackup can now perform **ALL operations** without external tools, as long as you create native-format backups. This enables single-binary deployment with zero PostgreSQL client dependencies.

**Bottom Line:** Native engine works for SQL format operations. Cluster operations use external tools because PostgreSQL's custom format provides better performance and features.