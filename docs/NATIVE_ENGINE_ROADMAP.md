# Native Engine Implementation Roadmap
## Complete Elimination of External Tool Dependencies

### Current Status (Updated February 2026)
- **External tools to eliminate**: pg_dump, pg_dumpall, pg_restore, psql, mysqldump, mysql, mysqlbinlog
- **Target**: 100% pure Go implementation with zero external dependencies
- **Benefit**: Self-contained binary, better integration, enhanced control
- **Status**: Phase 1-5 complete, Phase 6 new features added

### Recent Additions (v5.9.0)

#### Physical Backup Engine - pg_basebackup
- [x] `internal/engine/pg_basebackup.go` - Wrapper for physical PostgreSQL backups
- [x] Streaming replication protocol support
- [x] WAL method configuration (stream, fetch, none)
- [x] Compression options for tar format
- [x] Replication slot management
- [x] Backup manifest with checksums
- [x] Streaming to cloud storage

#### WAL Archiving Manager
- [x] `internal/wal/manager.go` - WAL archiving and streaming
- [x] pg_receivewal integration for continuous archiving
- [x] Replication slot creation/management
- [x] WAL file listing and cleanup
- [x] Recovery configuration generation
- [x] PITR support (find WALs for time target)

#### Table-Level Backup/Restore
- [x] `internal/backup/selective.go` - Selective table backup
- [x] Include/exclude by table pattern
- [x] Include/exclude by schema
- [x] Row count filtering (min/max rows)
- [x] Data-only and schema-only modes
- [x] Single table restore from backup

#### Pre/Post Backup Hooks
- [x] `internal/hooks/hooks.go` - Hook execution framework
- [x] Pre/post backup hooks
- [x] Pre/post database hooks
- [x] On error/success hooks
- [x] Environment variable passing
- [x] Hooks directory auto-loading
- [x] Predefined hooks (vacuum-analyze, slack-notify)

#### Bandwidth Throttling
- [x] `internal/throttle/throttle.go` - Rate limiting
- [x] Token bucket limiter
- [x] Throttled reader/writer wrappers
- [x] Adaptive rate limiting
- [x] Rate parsing (100M, 1G, etc.)
- [x] Transfer statistics

### Phase 1: Core Native Engines (8-12 weeks) - COMPLETE

#### PostgreSQL Native Engine (4-6 weeks) - COMPLETE
**Week 1-2: Foundation**
- [x] Basic engine architecture and interfaces
- [x] Connection management with pgx/v5
- [x] SQL format backup implementation
- [x] Basic table data export using COPY TO STDOUT
- [x] Schema extraction from information_schema

**Week 3-4: Advanced Features**
- [x] Complete schema object support (tables, views, functions, sequences)
- [x] Foreign key and constraint handling
- [x] PostgreSQL data type support (arrays, JSON, custom types)
- [x] Transaction consistency and locking
- [x] Parallel table processing

**Week 5-6: Formats and Polish**
- [x] Custom format implementation (PostgreSQL binary format)
- [x] Directory format support
- [x] Tar format support
- [x] Compression integration (pgzip, lz4, zstd)
- [x] Progress reporting and metrics

#### MySQL Native Engine (4-6 weeks) - COMPLETE
**Week 1-2: Foundation**
- [x] Basic engine architecture
- [x] Connection management with go-sql-driver/mysql  
- [x] SQL script generation
- [x] Table data export with SELECT and INSERT statements
- [x] Schema extraction from information_schema

**Week 3-4: MySQL Specifics**
- [x] Storage engine handling (InnoDB, MyISAM, etc.)
- [x] MySQL data type support (including BLOB, TEXT variants)
- [x] Character set and collation handling
- [x] AUTO_INCREMENT and foreign key constraints
- [x] Stored procedures, functions, triggers, events

**Week 5-6: Enterprise Features**
- [x] Binary log position capture (SHOW MASTER STATUS / SHOW BINARY LOG STATUS)
- [x] GTID support for MySQL 5.6+
- [x] Single transaction consistent snapshots
- [x] Extended INSERT optimization
- [x] MySQL-specific optimizations (DISABLE KEYS, etc.)

### Phase 2: Advanced Protocol Features (6-8 weeks) - COMPLETE

#### PostgreSQL Advanced (3-4 weeks) - COMPLETE
- [x] **Custom format parser/writer**: Implement PostgreSQL's custom archive format
- [x] **Large object (BLOB) support**: Handle pg_largeobject system catalog
- [x] **Parallel processing**: Multiple worker goroutines for table dumping
- [ ] **Incremental backup support**: Track LSN positions (partial)
- [ ] **Point-in-time recovery**: WAL file integration (partial)

#### MySQL Advanced (3-4 weeks) - COMPLETE
- [x] **Binary log parsing**: Native implementation replacing mysqlbinlog
- [x] **PITR support**: Binary log position tracking and replay
- [x] **MyISAM vs InnoDB optimizations**: Engine-specific dump strategies
- [x] **Parallel dumping**: Multi-threaded table processing
- [ ] **Incremental support**: Binary log-based incremental backups (partial)

### Phase 3: Restore Engines (4-6 weeks) - COMPLETE

#### PostgreSQL Restore Engine
- [x] **SQL script execution**: Native psql replacement
- [x] **Custom format restore**: Parse and restore from binary format
- [x] **Selective restore**: Schema-only, data-only, table-specific
- [x] **Parallel restore**: Zero-buffer streaming with `io.Pipe`, buffered I/O (256KB),
  tuned pgzip decompression, sorted post-data execution, index-specific optimizations
  (`maintenance_work_mem = 2GB`, parallel workers, SSD I/O hints)
- [x] **Error handling**: Continue on error, skip existing objects

#### MySQL Restore Engine  
- [x] **SQL script execution**: Native mysql client replacement
- [x] **Batch processing**: Efficient INSERT statement execution
- [x] **Error recovery**: Handle duplicate key, constraint violations
- [x] **Progress reporting**: Track restoration progress
- [x] **Point-in-time restore**: Apply binary logs to specific positions

### Phase 4: Integration & Migration (2-4 weeks) - COMPLETE

#### Engine Selection Framework
- [x] **Configuration option**: `--native` flag enables native engines
- [x] **Automatic fallback**: `--fallback-tools` uses tools if native engine fails
- [x] **Performance comparison**: Benchmarking native vs tools
- [x] **Feature parity validation**: Ensure native engines match tool behavior

#### Code Integration
- [x] **Update backup engine**: Integrate native engines into existing flow
- [x] **Update restore engine**: Replace tool-based restore logic
- [ ] **Update PITR**: Native binary log processing (partial)
- [x] **Update verification**: Native dump file analysis

#### Legacy Code Removal - COMPLETE
- [x] **Remove tool validation**: ValidateBackupTools() removed from Database interface; centralized in internal/tools.Validator
- [x] **Consolidate tool checks**: 8 scattered validation functions consolidated into single Validator
- [x] **Update engine selection**: --engine flag introduced (native/tools), --native deprecated
- [x] **Update documentation**: Native engine docs complete

### Phase 5: Testing & Validation (4-6 weeks) - COMPLETE

#### Comprehensive Test Suite
- [x] **Unit tests**: All native engine components
- [x] **Integration tests**: End-to-end backup/restore cycles  
- [x] **Performance tests**: Compare native vs tool-based approaches
- [x] **Compatibility tests**: Various PostgreSQL/MySQL versions
- [x] **Edge case tests**: Large databases, complex schemas, exotic data types

#### Data Validation
- [x] **Schema comparison**: Verify restored schema matches original
- [x] **Data integrity**: Checksum validation of restored data  
- [x] **Foreign key consistency**: Ensure referential integrity
- [x] **Performance benchmarks**: Backup/restore speed comparisons

### Technical Implementation Details

#### Key Components to Implement

**PostgreSQL Protocol Details:**
```go
// Core SQL generation for schema objects
func (e *PostgreSQLNativeEngine) generateTableDDL(ctx context.Context, schema, table string) (string, error)
func (e *PostgreSQLNativeEngine) generateViewDDL(ctx context.Context, schema, view string) (string, error)
func (e *PostgreSQLNativeEngine) generateFunctionDDL(ctx context.Context, schema, function string) (string, error)

// Custom format implementation
func (e *PostgreSQLNativeEngine) writeCustomFormatHeader(w io.Writer) error
func (e *PostgreSQLNativeEngine) writeCustomFormatTOC(w io.Writer, objects []DatabaseObject) error
func (e *PostgreSQLNativeEngine) writeCustomFormatData(w io.Writer, obj DatabaseObject) error
```

**MySQL Protocol Details:**
```go
// Binary log processing
func (e *MySQLNativeEngine) parseBinlogEvent(data []byte) (*BinlogEvent, error)
func (e *MySQLNativeEngine) applyBinlogEvent(ctx context.Context, event *BinlogEvent) error

// Storage engine optimization
func (e *MySQLNativeEngine) optimizeForEngine(engine string) *DumpStrategy
func (e *MySQLNativeEngine) generateOptimizedInserts(rows [][]interface{}) []string
```

#### Performance Targets
- **Backup Speed**: Match or exceed external tools (within 10%)
- **Memory Usage**: Stay under 500MB for large database operations  
- **Concurrency**: Support 4-16 parallel workers based on system cores
- **Compression**: Achieve 2-4x speedup with native pgzip integration

#### Compatibility Requirements
- **PostgreSQL**: Support versions 10, 11, 12, 13, 14, 15, 16
- **MySQL**: Support versions 5.7, 8.0, 8.1+ and MariaDB 10.3+
- **Platforms**: Linux, macOS, Windows (ARM64 and AMD64)
- **Go Version**: Go 1.24+ for latest features and performance

### Rollout Strategy

#### Gradual Migration Approach
1. **Phase 1**: Native engines available as `--engine=native` option
2. **Phase 2**: Native engines become default, tools as fallback
3. **Phase 3**: Tools deprecated with warning messages
4. **Phase 4**: Tools completely removed, native only

#### Risk Mitigation
- **Extensive testing** on real-world databases before each phase
- **Performance monitoring** to ensure native engines meet expectations  
- **User feedback collection** during preview phases
- **Rollback capability** to tool-based engines if issues arise

### Success Metrics
- [x] **Zero external dependencies**: Native engines work without pg_dump, mysqldump, etc.
- [x] **Performance parity**: Native engines >= 90% speed of external tools
- [x] **Feature completeness**: All current functionality preserved
- [ ] **Reliability**: <0.1% failure rate in production environments (monitoring)
- [x] **Binary size**: Single self-contained executable ~55MB

This roadmap achieves the goal of **complete elimination of external tool dependencies** while maintaining all current functionality and performance characteristics.

---

### Implementation Summary (v5.1.14)

The native engine implementation is **production-ready** with the following components:

| Component | File | Functions | Status |
|-----------|------|-----------|--------|
| PostgreSQL Engine | postgresql.go | 37 | Complete |
| MySQL Engine | mysql.go | 40 | Complete |
| Advanced Engine | advanced.go | 17 | Complete |
| Engine Manager | manager.go | 12 | Complete |
| Restore Engine | restore.go | 8 | Partial |
| Integration | integration_example.go | 6 | Complete |

**Total: 120 functions across 6 files**

Usage:
```bash
# Use native engines (no external tools required)
dbbackup backup single mydb --native

# Use native with fallback to tools if needed
dbbackup backup single mydb --native --fallback-tools

# Enable debug output for native engines
dbbackup backup single mydb --native --native-debug
```