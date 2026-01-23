# Lock Debugging Feature

## Overview

The `--debug-locks` flag provides complete visibility into the lock protection system introduced in v3.42.82. This eliminates the need for blind troubleshooting when diagnosing lock exhaustion issues.

## Problem

When PostgreSQL lock exhaustion occurs during restore:
- User sees "out of shared memory" error after 7 hours
- No visibility into why Large DB Guard chose conservative mode  
- Unknown whether lock boost attempts succeeded
- Unclear what actions are required to fix the issue
- Requires 14 days of troubleshooting to understand the problem

## Solution

New `--debug-locks` flag captures every decision point in the lock protection system with detailed logging prefixed by 🔍 [LOCK-DEBUG].

## Usage

### CLI
```bash
# Single database restore with lock debugging
dbbackup restore single mydb.dump --debug-locks --confirm

# Cluster restore with lock debugging  
dbbackup restore cluster backup.tar.gz --debug-locks --confirm

# Can also use global flag
dbbackup --debug-locks restore cluster backup.tar.gz --confirm
```

### TUI (Interactive Mode)
```bash
dbbackup  # Start interactive mode
# Navigate to restore operation
# Select your archive
# Press 'l' to toggle lock debugging (🔍 icon appears when enabled)
# Press Enter to proceed
```

## What Gets Logged

### 1. Strategy Analysis Entry Point
```
🔍 [LOCK-DEBUG] Large DB Guard: Starting strategy analysis
    archive=cluster_backup.tar.gz
    dump_count=15
```

### 2. PostgreSQL Configuration Detection
```
🔍 [LOCK-DEBUG] Querying PostgreSQL for lock configuration
    host=localhost
    port=5432
    user=postgres

🔍 [LOCK-DEBUG] Successfully retrieved PostgreSQL lock settings
    max_locks_per_transaction=2048
    max_connections=256
    total_capacity=524288
```

### 3. Guard Decision Logic  
```
🔍 [LOCK-DEBUG] PostgreSQL lock configuration detected
    max_locks_per_transaction=2048
    max_connections=256
    calculated_capacity=524288
    threshold_required=4096
    below_threshold=true

🔍 [LOCK-DEBUG] Guard decision: CONSERVATIVE mode
    jobs=1
    parallel_dbs=1
    reason="Lock threshold not met (max_locks < 4096)"
```

### 4. Lock Boost Attempts
```
🔍 [LOCK-DEBUG] boostPostgreSQLSettings: Starting lock boost procedure
    target_lock_value=4096

🔍 [LOCK-DEBUG] Current PostgreSQL lock configuration
    current_max_locks=2048
    target_max_locks=4096
    boost_required=true

🔍 [LOCK-DEBUG] Executing ALTER SYSTEM to boost locks
    from=2048
    to=4096

🔍 [LOCK-DEBUG] ALTER SYSTEM succeeded - restart required
    setting_saved_to=postgresql.auto.conf
    active_after="PostgreSQL restart"
```

### 5. PostgreSQL Restart Attempts
```
🔍 [LOCK-DEBUG] Attempting PostgreSQL restart to activate new lock setting

# If restart succeeds:
🔍 [LOCK-DEBUG] PostgreSQL restart SUCCEEDED

🔍 [LOCK-DEBUG] Post-restart verification
    new_max_locks=4096
    target_was=4096
    verification=PASS

# If restart fails:
🔍 [LOCK-DEBUG] PostgreSQL restart FAILED
    current_locks=2048
    required_locks=4096
    setting_saved=true
    setting_active=false
    verdict="ABORT - Manual restart required"
```

### 6. Final Verification
```
🔍 [LOCK-DEBUG] Lock boost function returned
    original_max_locks=2048
    target_max_locks=4096
    boost_successful=false

🔍 [LOCK-DEBUG] CRITICAL: Lock verification FAILED
    actual_locks=2048
    required_locks=4096
    delta=2048
    verdict="ABORT RESTORE"
```

## Example Workflow

### Scenario: Lock Exhaustion on New System

```bash
# Step 1: Run restore with lock debugging enabled
dbbackup restore cluster backup.tar.gz --debug-locks --confirm

# Output shows:
# 🔍 [LOCK-DEBUG] Guard decision: CONSERVATIVE mode
#     current_locks=2048, required=4096
#     verdict="ABORT - Manual restart required"

# Step 2: Follow the actionable instructions
sudo -u postgres psql -c "ALTER SYSTEM SET max_locks_per_transaction = 4096;"
sudo systemctl restart postgresql

# Step 3: Verify the change
sudo -u postgres psql -c "SHOW max_locks_per_transaction;"
# Output: 4096

# Step 4: Retry restore (can disable debug now)
dbbackup restore cluster backup.tar.gz --confirm

# Success! Restore proceeds with verified lock protection
```

## When to Use

### Enable Lock Debugging When:
- Diagnosing lock exhaustion failures
- Understanding why conservative mode was triggered
- Verifying lock boost attempts worked
- Troubleshooting "out of shared memory" errors  
- Setting up restore on new systems with unknown lock config
- Documenting lock requirements for compliance/security

### Leave Disabled For:
- Normal production restores (cleaner logs)
- Scripted/automated restores (less noise)
- When lock config is known to be sufficient
- When restore performance is critical

## Integration Points

### Configuration
- **Config Field:** `cfg.DebugLocks` (bool)
- **CLI Flag:** `--debug-locks` (persistent flag on root command)
- **TUI Toggle:** Press 'l' in restore preview screen
- **Default:** `false` (opt-in only)

### Files Modified
- `internal/config/config.go` - Added DebugLocks field
- `cmd/root.go` - Added --debug-locks persistent flag
- `cmd/restore.go` - Wired flag to single/cluster restore commands
- `internal/restore/large_db_guard.go` - 20+ debug log points
- `internal/restore/engine.go` - 15+ debug log points in boost logic
- `internal/tui/restore_preview.go` - 'l' key toggle with 🔍 icon

### Log Locations
All lock debug logs go to the configured logger (usually syslog or file) with level INFO. The 🔍 [LOCK-DEBUG] prefix makes them easy to grep:

```bash
# Filter lock debug logs
journalctl -u dbbackup | grep 'LOCK-DEBUG'

# Or in log files
grep 'LOCK-DEBUG' /var/log/dbbackup.log
```

## Backward Compatibility

- ✅ No breaking changes
- ✅ Flag defaults to false (no output unless enabled)
- ✅ Existing scripts continue to work unchanged
- ✅ TUI users get new 'l' toggle automatically
- ✅ CLI users can add --debug-locks when needed

## Performance Impact

Negligible - the debug logging only adds:
- ~5 database queries (SHOW commands)
- ~10 conditional if statements checking cfg.DebugLocks
- ~50KB of additional log output when enabled
- No impact on restore performance itself

## Relationship to v3.42.82

This feature completes the lock protection system:

**v3.42.82 (Protection):**
- Fixed Guard to always force conservative mode if max_locks < 4096
- Fixed engine to abort restore if lock boost fails  
- Ensures no path allows 7-hour failures

**v3.42.83 (Visibility):**
- Shows why Guard chose conservative mode
- Displays lock config that was detected
- Tracks boost attempts and outcomes
- Explains why restore was aborted

Together: Bulletproof protection + complete transparency.

## Deployment

1. Update to v3.42.83:
   ```bash
   wget https://github.com/PlusOne/dbbackup/releases/download/v3.42.83/dbbackup_linux_amd64
   chmod +x dbbackup_linux_amd64
   sudo mv dbbackup_linux_amd64 /usr/local/bin/dbbackup
   ```

2. Test lock debugging:
   ```bash
   dbbackup restore cluster test_backup.tar.gz --debug-locks --dry-run
   ```

3. Enable for production if diagnosing issues:
   ```bash
   dbbackup restore cluster production_backup.tar.gz --debug-locks --confirm
   ```

## Support

For issues related to lock debugging:
- Check logs for 🔍 [LOCK-DEBUG] entries
- Verify PostgreSQL version supports ALTER SYSTEM (9.4+)
- Ensure user has SUPERUSER role for ALTER SYSTEM
- Check systemd/init scripts can restart PostgreSQL

Related documentation:
- verify_postgres_locks.sh - Script to check lock configuration
- v3.42.82 release notes - Lock exhaustion bug fixes
