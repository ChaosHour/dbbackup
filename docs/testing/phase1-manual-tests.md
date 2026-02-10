# Phase 1 TUI Bulletproofing — Manual Testing Guide

> This guide covers hands-on testing of all four Phase 1 features.
> Run each test against a real PostgreSQL instance.

---

## Prerequisites

```bash
# Build fresh binary
go build -o dbbackup .

# Verify version
./dbbackup version

# Ensure PostgreSQL is running
psql -c "SELECT version()"

# Create test database
createdb tui_manual_test
psql -d tui_manual_test -c "
  CREATE TABLE orders (id serial PRIMARY KEY, item text, qty int);
  INSERT INTO orders (item, qty) SELECT 'item_' || i, i FROM generate_series(1,1000) i;
"
```

---

## 1. Connection Health Indicator

### 1.1 Healthy Connection

```bash
./dbbackup interactive --host localhost --port 5432 --user postgres
```

**Expected:**
- Menu loads within 2 seconds
- Status bar shows `[OK] Connected to localhost:5432`
- Green color on connection indicator

**Pass criteria:** `[OK]` visible in menu header area.

### 1.2 Unreachable Host

```bash
./dbbackup interactive --host 192.0.2.1 --port 9999 --user testuser
```

**Expected:**
- Menu loads immediately (does not block)
- Status bar shows `[WAIT] Checking connection...` then after ~5s: `[FAIL] Disconnected`
- Red/yellow indicator
- Menu items remain navigable

**Pass criteria:** Menu is usable despite failed connection.

### 1.3 Auto-Retry (30s)

```bash
# Start with bad host
./dbbackup interactive --host 192.0.2.1 --port 9999 --tui-debug
```

**Expected:**
- Wait 30+ seconds
- Look for retry attempt in debug output or status change
- Should not accumulate goroutines or leak memory

**Pass criteria:** Connection check retries without crash.

### 1.4 Connection Lost Mid-Session

```bash
# Start normally
./dbbackup interactive --host localhost --port 5432 --user postgres

# In another terminal, stop PostgreSQL:
sudo systemctl stop postgresql

# Wait 30s for retry cycle
# Observe status change to [FAIL] Disconnected

# Restart PostgreSQL:
sudo systemctl start postgresql

# Wait 30s — should reconnect
```

**Pass criteria:** Status transitions: `[OK]` → `[FAIL]` → `[OK]`

---

## 2. Pre-Restore Validation Checklist

### 2.1 Create a Backup for Testing

```bash
./dbbackup backup single tui_manual_test \
  --backup-dir /tmp/manual_test_backups \
  --host localhost --port 5432 --user postgres
```

### 2.2 All Checks Pass (Happy Path)

```bash
# Use TUI restore path
./dbbackup interactive --host localhost --port 5432 --user postgres

# Navigate: Restore Single → select backup → select target
```

**Expected preflight sequence (7 checks):**

| # | Check | Expected |
|---|-------|----------|
| 1 | `archive_exists` | ✅ File found |
| 2 | `archive_integrity` | ✅ Valid gzip/tar |
| 3 | `disk_space` | ✅ Sufficient space |
| 4 | `required_tools` | ✅ pg_restore/psql found |
| 5 | `target_db_check` | ✅ Target DB state verified |
| 6 | `privileges_check` | ✅ CREATEDB or superuser |
| 7 | `locks_sufficient` | ✅ max_locks adequate |

**Pass criteria:** All 7 checks shown sequentially, each with ✅ status.

### 2.3 Missing Archive

```bash
# Delete the backup
rm /tmp/manual_test_backups/*.tar.gz

# Try to restore via TUI
./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Single → browse (no archives found)
```

**Pass criteria:** `archive_exists` check fails, user sees clear error.

### 2.4 Insufficient Disk Space

```bash
# Create a tmpfs with limited space
sudo mount -t tmpfs -o size=1M tmpfs /tmp/tiny_mount
# Try restoring a large backup to that mount
```

**Pass criteria:** `disk_space` check fails with human-readable message.

### 2.5 Corrupted Archive

```bash
# Create a corrupted file
head -c 1024 /dev/urandom > /tmp/manual_test_backups/corrupted.tar.gz

# Try to restore it
./dbbackup restore single /tmp/manual_test_backups/corrupted.tar.gz \
  --target tui_manual_test_restore \
  --host localhost --port 5432 --user postgres
```

**Pass criteria:** `archive_integrity` check fails before any restore attempts.

### 2.6 Missing Required Tools

```bash
# Temporarily hide pg_restore
sudo mv /usr/bin/pg_restore /usr/bin/pg_restore.bak

# Try to restore
./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Single → select archive

# Restore pg_restore afterwards!
sudo mv /usr/bin/pg_restore.bak /usr/bin/pg_restore
```

**Pass criteria:** `required_tools` check fails with message about missing pg_restore.

### 2.7 Privilege Check — Non-Superuser

```bash
# Create a limited user
psql -c "CREATE ROLE limited_user LOGIN PASSWORD 'test123';"
psql -c "GRANT CONNECT ON DATABASE tui_manual_test TO limited_user;"

# Run restore as limited user
./dbbackup interactive --host localhost --port 5432 --user limited_user

# Cleanup
psql -c "DROP ROLE limited_user;"
```

**Pass criteria:** `privileges_check` warns about missing CREATEDB privilege.

### 2.8 Lock Capacity Warning

```bash
# Check current setting
psql -c "SHOW max_locks_per_transaction;"
# Default is 64 — preflight should warn if jobs are high

# Test with many parallel jobs
./dbbackup interactive --host localhost --port 5432 --user postgres
# Set jobs to 16+ in profile, then try restore
```

**Pass criteria:** `locks_sufficient` warns if max_locks < (jobs × threshold).

---

## 3. Destructive Operation Warning

### 3.1 Restore to Existing Database

```bash
# Ensure target DB exists
createdb tui_manual_test_target
psql -d tui_manual_test_target -c "CREATE TABLE keep_me (id int);"

# Start restore via CLI
./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Single → select archive → target: tui_manual_test_target
```

**Expected:**
- Warning banner appears: "This will overwrite database 'tui_manual_test_target'"
- Prompt: "Type 'restore' to confirm:"
- Typing wrong text → rejected, stays at prompt
- Typing correct text → proceeds to preflight → restore

**Pass criteria:** 
- [ ] Warning banner visible with database name
- [ ] Incorrect confirmation rejected
- [ ] Correct confirmation accepted

### 3.2 Cluster Restore Warning

```bash
./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Cluster → select archive
```

**Expected:**
- Warning: "This will restore ALL databases in the cluster"
- Prompt: "Type 'cluster-restore' to confirm:"

**Pass criteria:** Must type exact phrase `cluster-restore`.

### 3.3 Auto-Confirm Bypass

```bash
./dbbackup interactive \
  --auto-select 4 --auto-confirm \
  --host localhost --port 5432 --user postgres
```

**Pass criteria:** No destructive warning prompt appears; proceeds directly.

### 3.4 Restore to New (Non-Existing) Database

```bash
dropdb tui_manual_test_new 2>/dev/null || true

./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Single → select archive → target: tui_manual_test_new
```

**Pass criteria:** No destructive warning (database doesn't exist yet).

---

## 4. Two-Stage Ctrl+C Abort

### 4.1 Backup Abort — First Ctrl+C

```bash
# Start a cluster backup (takes time)
./dbbackup interactive \
  --host localhost --port 5432 --user postgres
# Navigate: Cluster Backup → confirm

# During backup progress, press Ctrl+C once
```

**Expected:**
- Prompt: "Abort backup? (y/N)"
- Press `N` → backup continues
- Press `Y` → graceful shutdown, cleanup runs

**Pass criteria:**
- [ ] First Ctrl+C shows Y/N prompt (does NOT kill immediately)
- [ ] Pressing N resumes backup
- [ ] Pressing Y runs cleanup

### 4.2 Backup Abort — Second Ctrl+C (Force)

```bash
# Start cluster backup again
# Press Ctrl+C once (see Y/N prompt)
# Press Ctrl+C again immediately
```

**Expected:**
- Second Ctrl+C forces immediate abort
- Cleanup still runs (LIFO stack)
- No orphaned pg_dump processes

**Verification:**
```bash
# After abort, check for orphans
pgrep -f pg_dump
# Should return empty
```

**Pass criteria:** 
- [ ] Second Ctrl+C forces immediate exit
- [ ] No orphaned processes remain

### 4.3 Restore Abort

```bash
# Start restore of large backup
./dbbackup interactive --host localhost --port 5432 --user postgres
# Navigate: Restore Single → select backup → confirm → start

# Press Ctrl+C during restore
```

**Expected:**
- "Abort restore? (y/N)" prompt
- Y → cleanup runs, temp dirs removed

**Verification:**
```bash
# Check for leftover temp dirs
ls /tmp/.restore_*
# Should not exist
```

**Pass criteria:**
- [ ] Abort prompt appears
- [ ] Temp directories cleaned up
- [ ] No orphaned pg_restore processes

### 4.4 Abort During Preflight (Edge Case)

```bash
# Start restore → let preflight begin
# Press Ctrl+C during preflight checks
```

**Pass criteria:** Exits cleanly, no partial state left behind.

---

## 5. Regression Tests

### 5.1 Menu Navigation After Bad Connection

```bash
./dbbackup interactive --host 192.0.2.1 --port 9999
# Use arrow keys to navigate all menu items
# Verify no panic, no freeze
```

**Pass criteria:** All 19 menu items accessible, ↑↓ keys work.

### 5.2 Rapid Ctrl+C During Startup

```bash
./dbbackup interactive --host localhost --port 5432 --user postgres
# Immediately press Ctrl+C before menu fully renders
```

**Pass criteria:** Clean exit, no panic, no goroutine leak.

### 5.3 Multiple Restore Cycles

```bash
# Do 3 consecutive restore → cancel → restore cycles
# Verify memory doesn't grow, no leftover state
```

**Pass criteria:** Each cycle starts fresh.

---

## 6. Performance Benchmarks

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Menu load time | < 2s | `time ./dbbackup interactive --auto-select 18` |
| Connection check latency | < 5s | Observe `[WAIT]` → `[OK]` transition |
| Preflight (7 checks) | < 10s | Time from selecting restore to "Ready" |
| Abort cleanup | < 3s | Time from Y to process exit |
| Memory baseline | < 50MB | `ps aux | grep dbbackup` RSS column |

---

## 7. Cleanup

```bash
dropdb tui_manual_test 2>/dev/null
dropdb tui_manual_test_target 2>/dev/null
dropdb tui_manual_test_new 2>/dev/null
rm -rf /tmp/manual_test_backups
```
