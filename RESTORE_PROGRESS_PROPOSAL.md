# Restore Progress Bar Enhancement Proposal

## Problem
During Phase 2 cluster restore, the progress bar is not real-time because:
- `pg_restore` subprocess blocks until completion
- Progress updates only happen **before** each database restore starts
- No feedback during actual restore execution (which can take hours)
- Users see frozen progress bar during large database restores

## Root Cause
In `internal/restore/engine.go`:
- `executeRestoreCommand()` blocks on `cmd.Wait()` 
- Progress is only reported at goroutine entry (line ~1315)
- No streaming progress during pg_restore execution

## Proposed Solutions

### Option 1: Parse pg_restore stderr for progress (RECOMMENDED)
**Pros:**
- Real-time feedback during restore
- Works with existing pg_restore
- No external tools needed

**Implementation:**
```go
// In executeRestoreCommand, modify stderr reader:
go func() {
    scanner := bufio.NewScanner(stderr)
    for scanner.Scan() {
        line := scanner.Text()
        
        // Parse pg_restore progress lines
        // Format: "pg_restore: processing item 1234 TABLE public users"
        if strings.Contains(line, "processing item") {
            e.reportItemProgress(line) // Update progress bar
        }
        
        // Capture errors
        if strings.Contains(line, "ERROR:") {
            lastError = line
            errorCount++
        }
    }
}()
```

**Add to RestoreCluster goroutine:**
```go
// Track sub-items within each database
var currentDBItems, totalDBItems int
e.setItemProgressCallback(func(current, total int) {
    currentDBItems = current
    totalDBItems = total
    // Update TUI with sub-progress
    e.reportDatabaseSubProgress(idx, totalDBs, dbName, current, total)
})
```

### Option 2: Verbose mode with line counting
**Pros:**
- More granular progress (row-level)
- Shows exact operation being performed

**Cons:**
- `--verbose` causes massive stderr output (OOM risk on huge DBs)
- Currently disabled for memory safety
- Requires careful memory management

### Option 3: Hybrid approach (BEST)
**Combine both:**
1. **Default**: Parse non-verbose pg_restore output for item counts
2. **Small DBs** (<500MB): Enable verbose for detailed progress
3. **Periodic updates**: Report progress every 5 seconds even without stderr changes

**Implementation:**
```go
// Add periodic progress ticker
progressTicker := time.NewTicker(5 * time.Second)
defer progressTicker.Stop()

go func() {
    for {
        select {
        case <-progressTicker.C:
            // Report heartbeat even if no stderr
            e.reportHeartbeat(dbName, time.Since(dbRestoreStart))
        case <-stderrDone:
            return
        }
    }
}()
```

## Recommended Implementation Plan

### Phase 1: Quick Win (1-2 hours)
1. Add heartbeat ticker in cluster restore goroutines
2. Update TUI to show "Restoring database X... (elapsed: 3m 45s)"
3. No code changes to pg_restore wrapper

### Phase 2: Parse pg_restore Output (4-6 hours)
1. Parse stderr for "processing item" lines
2. Extract current/total item counts
3. Report sub-progress to TUI
4. Update progress bar calculation: 
   ```
   dbProgress = baseProgress + (itemsDone/totalItems) * dbWeightedPercent
   ```

### Phase 3: Smart Verbose Mode (optional)
1. Detect database size before restore
2. Enable verbose for DBs < 500MB
3. Parse verbose output for detailed progress
4. Automatic fallback to item-based for large DBs

## Files to Modify

1. **internal/restore/engine.go**:
   - `executeRestoreCommand()` - add progress parsing
   - `RestoreCluster()` - add heartbeat ticker
   - New: `reportItemProgress()`, `reportHeartbeat()`

2. **internal/tui/restore_exec.go**:
   - Update `RestoreExecModel` to handle sub-progress
   - Add "elapsed time" display during restore
   - Show item counts: "Restoring tables... (234/567)"

3. **internal/progress/indicator.go**:
   - Add `UpdateSubProgress(current, total int)` method
   - Add `ReportHeartbeat(elapsed time.Duration)` method

## Example Output

**Before (current):**
```
[====================] Phase 2/3: Restoring Databases (1/5)
Restoring database myapp...
[frozen for 30 minutes]
```

**After (with heartbeat):**
```
[====================] Phase 2/3: Restoring Databases (1/5)
Restoring database myapp... (elapsed: 4m 32s)
[updates every 5 seconds]
```

**After (with item parsing):**
```
[=========>-----------] Phase 2/3: Restoring Databases (1/5)
Restoring database myapp... (processing item 1,234/5,678) (elapsed: 4m 32s)
[smooth progress bar movement]
```

## Testing Strategy
1. Test with small DB (< 100MB) - verify heartbeat works
2. Test with large DB (> 10GB) - verify no OOM, heartbeat works
3. Test with BLOB-heavy DB - verify phased restore shows progress
4. Test parallel cluster restore - verify multiple heartbeats don't conflict

## Risk Assessment
- **Low risk**: Heartbeat ticker (Phase 1)
- **Medium risk**: stderr parsing (Phase 2) - test thoroughly
- **High risk**: Verbose mode (Phase 3) - can cause OOM

## Estimated Implementation Time
- Phase 1 (heartbeat): 1-2 hours
- Phase 2 (item parsing): 4-6 hours
- Phase 3 (smart verbose): 8-10 hours (optional)

**Total for Phases 1+2: 5-8 hours**
