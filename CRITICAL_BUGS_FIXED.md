# Critical Bug Fixes - January 2026

**Timeline**: Issues existed for 2-3 months before being fixed  
**Impact**: Customer backup/restore operations failing through TUI  
**Fixed in**: v3.42.5 through v3.42.10

---

## Root Cause Analysis

### Bug #1: Encryption Detection False Positive (v3.42.5)
**File**: `internal/backup/encryption.go:92`  
**Issue**: `IsBackupEncrypted()` returned TRUE for ALL files  
**Impact**: Normal (unencrypted) backups could not be restored  
**Root Cause**: Fallback logic checked if 12 bytes could be read (nonce size) - always true for any file  
**Fix**: Properly detect magic bytes (1f 8b for gzip, PGDMP for PostgreSQL custom format)

### Bug #2-13: cmd.Wait() Deadlocks (v3.42.6-v3.42.7)
**Files**: 
- `internal/backup/engine.go` (4 functions)
- `internal/restore/engine.go` (3 functions)  
- `internal/engine/mysqldump.go` (2 functions)
- `internal/backup/engine.go:createArchive` (tar/pigz pipeline)

**Issue**: 12 functions with blocking `cmd.Wait()` calls  
**Impact**: Processes could hang forever when context was cancelled  
**Root Cause**: 
```go
// WRONG - hangs on cancellation
if err := cmd.Wait(); err != nil {
    return err
}
```

**Fix**: Channel-based pattern with Process.Kill()
```go
// CORRECT
cmdDone := make(chan error, 1)
go func() { cmdDone <- cmd.Wait() }()
select {
case err = <-cmdDone:
case <-ctx.Done():
    cmd.Process.Kill()
    <-cmdDone
}
```

### Bug #14-20: TUI Timeout Bugs (v3.42.8-v3.42.9)
**THE PRIMARY ISSUE** - Why backups appeared to fail for months

| File | Function | Old Timeout | New Timeout | Impact |
|------|----------|-------------|-------------|---------|
| restore_preview.go | runSafetyChecks | 60s | 10 min | Large archives timeout before backup even starts |
| dbselector.go | fetchDatabases | 15s | 60s | Database listing fails on busy servers |
| status.go | fetchStatus | 10s | 30s | Status checks fail with SSL/slow networks |
| diagnose.go | diagnoseClusterArchive | 60s | 5 min | tar -tzf times out on multi-GB archives |
| diagnose.go | verifyWithPgRestore | 60s | 5 min | pg_restore --list times out on large dumps |
| diagnose.go | DiagnoseClusterDumps | 120s | 10 min | Archive extraction times out |
| engine.go | detectLargeObjectsInDumps | 10s | 2 min | Large object detection fails |

**Root Cause**: User sees "context deadline exceeded" in TUI, thinks pg_dump failed, but the operation never even started - it timed out during pre-validation checks.

### Bug #21-22: Missing Panic Recovery (v3.42.8)
**Files**: 
- `internal/backup/engine.go:442` (BackupCluster goroutines)
- `internal/restore/engine.go:861` (RestoreCluster goroutines)

**Issue**: No `defer recover()` in parallel goroutines  
**Impact**: Single database panic crashes entire cluster backup/restore  
**Fix**: Added panic recovery with error counting

### Bug #23: Variable Shadowing (v3.42.8)
**File**: `internal/restore/engine.go:416`  
**Issue**: Used `err` instead of `cmdErr` for exit code detection  
**Impact**: Incorrect exit code reported in error messages

---

## Code Quality Issues (v3.42.10)

- Deprecated `io/ioutil` usage (Go 1.19+)
- Duplicate imports
- Unused fields and variables
- Error string formatting violations
- Ineffective assignments

---

## Timeline

- **Bugs introduced**: Unknown (existed 2-3 months)
- **First report**: ~October 2025
- **Investigation started**: January 7, 2026
- **Fixed**: January 8, 2026 (v3.42.5 - v3.42.10)

---

## Customer Impact

**Duration**: 2-3 months of failed backup/restore operations  
**Symptom**: TUI backups appeared to fail immediately with timeout errors  
**Actual Issue**: Pre-validation checks timing out, not the actual backup  
**Business Impact**: Potential data loss risk, support costs, reputation damage

---

## Lessons Learned

1. **Never use arbitrary short timeouts** for operations on potentially large data
2. **Always use channel-based pattern** for cmd.Wait() with context
3. **Add panic recovery** to all goroutines in production code
4. **Test with realistic data sizes** (multi-GB archives)
5. **Systematic code audits** should be first step, not last resort

---

*This document is for internal use and legal purposes only.*
