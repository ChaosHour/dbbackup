# Expert Feedback Simulation - 1000+ DBAs & Linux Admins
**Version Reviewed:** 4.2.5  
**Date:** 2026-01-30  
**Participants:** 1000 experts (DBAs, Linux admins, SREs, Platform engineers)

---

## 🔴 CRITICAL ISSUES (Blocking Production Use)

### #1 - PostgreSQL Connection Pooler Incompatibility
**Reporter:** Senior DBA, Financial Services (10K+ databases)  
**Environment:** PgBouncer in transaction mode, 500 concurrent connections

```
PROBLEM: pg_restore hangs indefinitely when using connection pooler in transaction mode
- Works fine with direct PostgreSQL connection
- PgBouncer closes connection mid-transaction, pg_restore waits forever
- No timeout, no error message, just hangs

IMPACT: Cannot use dbbackup in our environment (mandatory PgBouncer for connection management)

EXPECTED: Detect connection pooler, warn user, or use session pooling mode
```

**Priority:** CRITICAL - affects all PgBouncer/pgpool users  
**Files Affected:** `internal/database/postgres.go` - connection setup

---

### #2 - Restore Fails with Non-Standard Schemas
**Reporter:** Platform Engineer, Healthcare SaaS (HIPAA compliance)  
**Environment:** PostgreSQL with 50+ custom schemas per database

```
PROBLEM: Cluster restore fails when database has non-standard search_path
- Our apps use schemas: app_v1, app_v2, patient_data, audit_log, etc.
- Restore completes but functions can't find tables
- Error: "relation 'users' does not exist" (exists in app_v1.users)

LOGS:
psql:globals.sql:45: ERROR: schema "app_v1" does not exist
pg_restore: [archiver] could not execute query: ERROR: relation "app_v1.users" does not exist

ROOT CAUSE: Schemas created AFTER data restore, not before

EXPECTED: Restore order should be: schemas → data → constraints
```

**Priority:** CRITICAL - breaks multi-schema databases  
**Workaround:** None - manual schema recreation required  
**Files Affected:** `internal/restore/engine.go` - restore phase ordering

---

### #3 - Silent Data Loss with Large Text Fields
**Reporter:** Lead DBA, E-commerce (250TB database)  
**Environment:** PostgreSQL 15, tables with TEXT columns > 1GB

```
PROBLEM: Restore silently truncates large text fields
- Product descriptions > 100MB get truncated to exactly 100MB
- No error, no warning, just silent data loss
- Discovered during data validation 3 days after restore

INVESTIGATION:
- pg_restore uses 100MB buffer by default
- Fields larger than buffer are truncated
- TOAST data not properly restored

IMPACT: DATA LOSS - unacceptable for production

EXPECTED: 
1. Detect TOAST data during backup
2. Increase buffer size automatically
3. FAIL LOUDLY if data truncation would occur
```

**Priority:** CRITICAL - SILENT DATA LOSS  
**Affected:** Large TEXT/BYTEA columns with TOAST  
**Files Affected:** `internal/backup/engine.go`, `internal/restore/engine.go`

---

### #4 - Backup Directory Permission Race Condition
**Reporter:** Linux SysAdmin, Government Agency  
**Environment:** RHEL 8, SELinux enforcing, 24/7 operations

```
PROBLEM: Parallel backups create race condition in directory creation
- Running 5 parallel cluster backups simultaneously
- Random failures: "mkdir: cannot create directory: File exists"
- 1 in 10 backups fails due to race condition

REPRODUCTION:
for i in {1..5}; do
  dbbackup backup cluster &
done
# Random failures on mkdir in temp directory creation

ROOT CAUSE: 
internal/backup/engine.go:426
if err := os.MkdirAll(tempDir, 0755); err != nil {
    return fmt.Errorf("failed to create temp directory: %w", err)
}

No check for EEXIST error - should be ignored

EXPECTED: Handle race condition gracefully (EEXIST is not an error)
```

**Priority:** HIGH - breaks parallel operations  
**Frequency:** 10% of parallel runs  
**Files Affected:** All `os.MkdirAll` calls need EEXIST handling

---

### #5 - Memory Leak in TUI During Long Operations
**Reporter:** SRE, Cloud Provider (manages 5000+ customer databases)  
**Environment:** Ubuntu 22.04, 8GB RAM, restoring 500GB cluster

```
PROBLEM: TUI memory usage grows unbounded during long operations
- Started: 45MB RSS
- After 2 hours: 3.2GB RSS
- After 4 hours: 7.8GB RSS
- OOM killed by kernel at 8GB

STRACE OUTPUT:
mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0x7f... [repeated 1M+ times]

ROOT CAUSE: Progress messages accumulating in memory
- m.details []string keeps growing
- No limit on array size
- Each progress update appends to slice

EXPECTED: 
1. Limit details slice to last 100 entries
2. Use ring buffer instead of append
3. Monitor memory usage and warn user
```

**Priority:** HIGH - prevents long-running operations  
**Affects:** All TUI operations > 2 hours  
**Files Affected:** `internal/tui/restore_exec.go`, `internal/tui/backup_exec.go`

---

## 🟠 HIGH PRIORITY BUGS

### #6 - Timezone Confusion in Backup Filenames
**Reporter:** 15 DBAs from different timezones

```
PROBLEM: Backup filename timestamps don't match server time
- Server time: 2026-01-30 14:30:00 EST
- Filename: cluster_20260130_193000.tar.gz (19:30 UTC)
- Cron script expects EST timestamps for rotation

CONFUSION:
- Monitoring scripts parse timestamps incorrectly
- Retention policies delete wrong backups
- Audit logs don't match backup times

EXPECTED:
1. Use LOCAL time by default (what DBA sees)
2. Add config option: timestamp_format: "local|utc|custom"
3. Include timezone in filename: cluster_20260130_143000_EST.tar.gz
```

**Priority:** HIGH - breaks automation  
**Workaround:** Manual timezone conversion in scripts  
**Files Affected:** All timestamp generation code

---

### #7 - Restore Hangs with Read-Only Filesystem
**Reporter:** Platform Engineer, Container Orchestration

```
PROBLEM: Restore hangs for 10 minutes when temp directory becomes read-only
- Kubernetes pod eviction remounts /tmp as read-only
- dbbackup continues trying to write, no error for 10 minutes
- Eventually times out with unclear error

EXPECTED:
1. Test write permissions before starting
2. Fail fast with clear error
3. Suggest alternative temp directory
```

**Priority:** HIGH - poor failure mode  
**Files Affected:** `internal/fs/`, temp directory handling

---

### #8 - PITR Recovery Stops at Wrong Time
**Reporter:** Senior DBA, Banking (PCI-DSS compliance)

```
PROBLEM: Point-in-time recovery overshoots target by several minutes
- Target: 2026-01-30 14:00:00
- Actual: 2026-01-30 14:03:47
- Replayed 227 extra transactions after target time

ROOT CAUSE: WAL replay doesn't check timestamp frequently enough
- Only checks at WAL segment boundaries (16MB)
- High-traffic database = 3-4 minutes per segment

IMPACT: Compliance violation - recovered data includes transactions after incident

EXPECTED: Check timestamp after EVERY transaction during recovery
```

**Priority:** HIGH - compliance issue  
**Files Affected:** `internal/pitr/`, `internal/wal/`

---

### #9 - Backup Catalog SQLite Corruption Under Load
**Reporter:** 8 SREs reporting same issue

```
PROBLEM: Catalog database corrupts during concurrent backups
Error: "database disk image is malformed"

FREQUENCY: 1-2 times per week under load
OPERATIONS: 50+ concurrent backups across different servers

ROOT CAUSE: SQLite WAL mode not enabled, no busy timeout
Multiple writers to catalog cause corruption

FIX NEEDED:
1. Enable WAL mode: PRAGMA journal_mode=WAL
2. Set busy timeout: PRAGMA busy_timeout=5000
3. Add retry logic with exponential backoff
4. Consider PostgreSQL for catalog (production-grade)
```

**Priority:** HIGH - data corruption  
**Files Affected:** `internal/catalog/`

---

### #10 - Cloud Upload Retry Logic Broken
**Reporter:** DevOps Engineer, Multi-cloud deployment

```
PROBLEM: S3 upload fails permanently on transient network errors
- Network hiccup during 100GB upload
- Tool returns: "upload failed: connection reset by peer"
- Starts over from 0 bytes (loses 3 hours of upload)

EXPECTED BEHAVIOR:
1. Use multipart upload with resume capability
2. Retry individual parts, not entire file
3. Persist upload ID for crash recovery
4. Show retry attempts: "Upload failed (attempt 3/5), retrying in 30s..."

CURRENT: No retry, no resume, fails completely
```

**Priority:** HIGH - wastes time and bandwidth  
**Files Affected:** `internal/cloud/s3.go`, `internal/cloud/azure.go`, `internal/cloud/gcs.go`

---

## 🟡 MEDIUM PRIORITY ISSUES

### #11 - Log Files Fill Disk During Large Restores
**Reporter:** 12 Linux Admins

```
PROBLEM: Log file grows to 50GB+ during cluster restore
- Verbose progress logging fills /var/log
- Disk fills up, system becomes unstable
- No log rotation, no size limit

EXPECTED:
1. Rotate logs during operation if size > 100MB
2. Add --log-level flag (error|warn|info|debug)
3. Use structured logging (JSON) for better parsing
4. Send bulk logs to syslog instead of file
```

**Impact:** Fills disk, crashes system  
**Workaround:** Manual log cleanup during restore

---

### #12 - Environment Variable Precedence Confusing
**Reporter:** 25 DevOps Engineers

```
PROBLEM: Config priority is unclear and inconsistent
- Set PGPASSWORD in environment
- Set password in config file
- Password still prompted?

EXPECTED PRECEDENCE (most to least specific):
1. Command-line flags
2. Environment variables
3. Config file
4. Defaults

CURRENT: Inconsistent between different settings
```

**Impact:** Confusion, failed automation  
**Documentation:** README doesn't explain precedence

---

### #13 - TUI Crashes on Terminal Resize
**Reporter:** 8 users

```
PROBLEM: Terminal resize during operation crashes TUI
SIGWINCH → panic: runtime error: index out of range

EXPECTED: Redraw UI with new dimensions
```

**Impact:** Lost operation state  
**Files Affected:** `internal/tui/` - all models

---

### #14 - Backup Verification Takes Too Long
**Reporter:** DevOps Manager, 200-node fleet

```
PROBLEM: --verify flag makes backup take 3x longer
- 1 hour backup + 2 hours verification = 3 hours total
- Verification is sequential, doesn't use parallelism
- Blocks next backup in schedule

SUGGESTION:
1. Verify in background after backup completes
2. Parallelize verification (verify N databases concurrently)
3. Quick verify by default (structure only), deep verify optional
```

**Impact:** Backup windows too long

---

### #15 - Inconsistent Exit Codes
**Reporter:** 30 Engineers automating scripts

```
PROBLEM: Exit codes don't follow conventions
- Backup fails: exit 1
- Restore fails: exit 1
- Config error: exit 1
- All errors return exit 1!

EXPECTED (standard convention):
0   = success
1   = general error
2   = command-line usage error
64  = input data error
65  = input file missing
69  = service unavailable
70  = internal error
75  = temp failure (retry)
77  = permission denied

AUTOMATION NEEDS SPECIFIC EXIT CODES TO HANDLE FAILURES
```

**Impact:** Cannot differentiate failures in automation

---

## 🟢 FEATURE REQUESTS (High Demand)

### #FR1 - Backup Compression Level Selection
**Requested by:** 45 users

```
FEATURE: Allow compression level selection at runtime
Current: Uses default compression (level 6)
Wanted: --compression-level 1-9 flag

USE CASES:
- Level 1: Fast backup, less CPU (production hot backups)
- Level 9: Max compression, archival (cold storage)
- Level 6: Balanced (default)

BENEFIT: 
- Level 1: 3x faster backup, 20% larger file
- Level 9: 2x slower backup, 15% smaller file
```

**Priority:** HIGH demand  
**Effort:** LOW (pgzip supports this already)

---

### #FR2 - Differential Backups (vs Incremental)
**Requested by:** 35 enterprise DBAs

```
FEATURE: Support differential backups (diff from last FULL, not last backup)

BACKUP STRATEGY NEEDED:
- Sunday: FULL backup (baseline)
- Monday: DIFF from Sunday
- Tuesday: DIFF from Sunday (not Monday!)
- Wednesday: DIFF from Sunday
...

CURRENT INCREMENTAL:
- Sunday: FULL
- Monday: INCR from Sunday
- Tuesday: INCR from Monday ← requires Monday to restore
- Wednesday: INCR from Tuesday ← requires Monday+Tuesday

BENEFIT: Faster restores (FULL + 1 DIFF vs FULL + 7 INCR)
```

**Priority:** HIGH for enterprise  
**Effort:** MEDIUM

---

### #FR3 - Pre/Post Backup Hooks
**Requested by:** 50+ users

```
FEATURE: Run custom scripts before/after backup
Config:
backup:
  pre_backup_script: /scripts/before_backup.sh
  post_backup_script: /scripts/after_backup.sh
  post_backup_success: /scripts/on_success.sh
  post_backup_failure: /scripts/on_failure.sh

USE CASES:
- Quiesce application before backup
- Snapshot filesystem
- Update monitoring dashboard
- Send custom notifications
- Sync to additional storage
```

**Priority:** HIGH  
**Effort:** LOW

---

### #FR4 - Database-Level Encryption Keys
**Requested by:** 20 security teams

```
FEATURE: Different encryption keys per database (multi-tenancy)

CURRENT: Single encryption key for all backups
NEEDED: Per-database encryption for customer isolation

Config:
encryption:
  default_key: /keys/default.key
  database_keys:
    customer_a_db: /keys/customer_a.key
    customer_b_db: /keys/customer_b.key

BENEFIT: Cryptographic tenant isolation
```

**Priority:** HIGH for SaaS providers  
**Effort:** MEDIUM

---

### #FR5 - Backup Streaming (No Local Disk)
**Requested by:** 30 cloud-native teams

```
FEATURE: Stream backup directly to cloud without local storage

PROBLEM: 
- Database: 500GB
- Local disk: 100GB
- Can't backup (insufficient space)

WANTED:
dbbackup backup single mydb --stream-to s3://bucket/backup.tar.gz

FLOW:
pg_dump → gzip → S3 multipart upload (streaming)
No local temp files, no disk space needed

BENEFIT: Backup databases larger than available disk
```

**Priority:** HIGH for cloud  
**Effort:** HIGH (requires streaming architecture)

---

## 🔵 OPERATIONAL CONCERNS

### #OP1 - No Health Check Endpoint
**Reporter:** 40 SREs

```
PROBLEM: Cannot monitor dbbackup health in container environments
Kubernetes needs: HTTP health endpoint

WANTED:
dbbackup server --health-port 8080

GET /health → 200 OK {"status": "healthy"}
GET /ready → 200 OK {"status": "ready", "last_backup": "..."}
GET /metrics → Prometheus format

USE CASE: Kubernetes liveness/readiness probes
```

**Priority:** MEDIUM  
**Effort:** LOW

---

### #OP2 - Structured Logging (JSON)
**Reporter:** 35 Platform Engineers

```
PROBLEM: Log parsing is painful
Current: Human-readable text logs
Needed: Machine-readable JSON logs

EXAMPLE:
{"timestamp":"2026-01-30T14:30:00Z","level":"info","msg":"backup started","database":"prod","size":1024000}

BENEFIT: 
- Easy parsing by log aggregators (ELK, Splunk)
- Structured queries
- Correlation with other systems
```

**Priority:** MEDIUM  
**Effort:** LOW (switch to zerolog or zap)

---

### #OP3 - Backup Age Alerting
**Reporter:** 20 Operations Teams

```
FEATURE: Alert if backup is too old
Config:
monitoring:
  max_backup_age: 24h
  alert_webhook: https://alerts.company.com/webhook

BEHAVIOR:
If last successful backup > 24h ago:
  → Send alert
  → Update Prometheus metric: dbbackup_backup_age_seconds
  → Exit with specific code for monitoring
```

**Priority:** MEDIUM  
**Effort:** LOW

---

## 🟣 PERFORMANCE OPTIMIZATION

### #PERF1 - Table-Level Parallel Restore
**Requested by:** 15 large-scale DBAs

```
FEATURE: Restore tables in parallel, not just databases

CURRENT: 
- Cluster restore: parallel by database ✓
- Single DB restore: sequential by table ✗

PROBLEM:
- Single 5TB database with 1000 tables
- Sequential restore takes 18 hours
- Only 1 CPU core used (12.5% of 8-core system)

WANTED:
dbbackup restore single mydb.tar.gz --parallel-tables 8

BENEFIT: 
- 8x faster restore (18h → 2.5h)
- Better resource utilization
```

**Priority:** HIGH for large databases  
**Effort:** HIGH (complex pg_restore orchestration)

---

### #PERF2 - Incremental Catalog Updates
**Reporter:** 10 high-volume users

```
PROBLEM: Catalog sync after each backup is slow
- 10,000 backups in catalog
- Each new backup → full table scan
- Sync takes 30 seconds

WANTED: Incremental updates only
- Track last_sync_timestamp
- Only scan backups created after last sync
```

**Priority:** MEDIUM  
**Effort:** LOW

---

### #PERF3 - Compression Algorithm Selection
**Requested by:** 25 users

```
FEATURE: Choose compression algorithm

CURRENT: gzip only
WANTED: 
- gzip: universal compatibility
- zstd: 2x faster, same ratio
- lz4: 3x faster, larger files
- xz: slower, better compression

Flag: --compression-algorithm zstd
Config: compression_algorithm: zstd

BENEFIT: 
- zstd: 50% faster backups
- lz4: 70% faster backups (for fast networks)
```

**Priority:** MEDIUM  
**Effort:** MEDIUM

---

## 🔒 SECURITY CONCERNS

### #SEC1 - Password Logged in Process List
**Reporter:** 15 Security Teams (CRITICAL!)

```
SECURITY ISSUE: Password visible in process list
ps aux shows:
dbbackup backup single mydb --password SuperSecret123

RISK: 
- Any user can see password
- Logged in audit trails
- Visible in monitoring tools

FIX NEEDED:
1. NEVER accept password as command-line arg
2. Use environment variable only
3. Prompt if not provided
4. Use .pgpass file
```

**Priority:** CRITICAL SECURITY ISSUE  
**Status:** MUST FIX IMMEDIATELY

---

### #SEC2 - Backup Files World-Readable
**Reporter:** 8 Compliance Officers

```
SECURITY ISSUE: Backup files created with 0644 permissions
Anyone on system can read database dumps!

EXPECTED: 0600 (owner read/write only)

IMPACT: 
- Compliance violation (PCI-DSS, HIPAA)
- Data breach risk
```

**Priority:** HIGH SECURITY ISSUE  
**Files Affected:** All backup creation code

---

### #SEC3 - No Backup Encryption by Default
**Reporter:** 30 Security Engineers

```
CONCERN: Encryption is optional, not enforced

SUGGESTION: 
1. Warn loudly if backup is unencrypted
2. Add config: require_encryption: true (fail if no key)
3. Make encryption default in v5.0

RISK: Unencrypted backups leaked (S3 bucket misconfiguration)
```

**Priority:** MEDIUM (policy issue)

---

## 📚 DOCUMENTATION GAPS

### #DOC1 - No Disaster Recovery Runbook
**Reporter:** 20 Junior DBAs

```
MISSING: Step-by-step DR procedure
Needed:
1. How to restore from complete datacenter loss
2. What order to restore databases
3. How to verify restore completeness
4. RTO/RPO expectations by database size
5. Troubleshooting common restore failures
```

---

### #DOC2 - No Capacity Planning Guide
**Reporter:** 15 Platform Engineers

```
MISSING: Resource requirements documentation
Questions:
- How much RAM needed for X GB database?
- How much disk space for restore?
- Network bandwidth requirements?
- CPU cores for optimal performance?
```

---

### #DOC3 - No Security Hardening Guide
**Reporter:** 12 Security Teams

```
MISSING: Security best practices
Needed:
- Secure key management
- File permissions
- Network isolation
- Audit logging
- Compliance checklist (PCI, HIPAA, SOC2)
```

---

## 📊 STATISTICS SUMMARY

### Issue Severity Distribution
- 🔴 CRITICAL: 5 issues (blocker, data loss, security)
- 🟠 HIGH: 10 issues (major bugs, affects operations)
- 🟡 MEDIUM: 15 issues (annoyances, workarounds exist)
- 🟢 ENHANCEMENT: 20+ feature requests

### Most Requested Features (by votes)
1. Pre/post backup hooks (50 votes)
2. Differential backups (35 votes)
3. Table-level parallel restore (30 votes)
4. Backup streaming to cloud (30 votes)
5. Compression level selection (25 votes)

### Top Pain Points (by frequency)
1. Partial cluster restore handling (45 reports)
2. Exit code inconsistency (30 reports)
3. Timezone confusion (15 reports)
4. TUI memory leak (12 reports)
5. Catalog corruption (8 reports)

### Environment Distribution
- PostgreSQL users: 65%
- MySQL/MariaDB users: 30%
- Mixed environments: 5%
- Cloud-native (containers): 40%
- Traditional VMs: 35%
- Bare metal: 25%

---

## 🎯 RECOMMENDED PRIORITY ORDER

### Sprint 1 (Critical Security & Data Loss)
1. #SEC1 - Password in process list → SECURITY
2. #3 - Silent data loss (TOAST) → DATA INTEGRITY
3. #SEC2 - World-readable backups → SECURITY
4. #2 - Schema restore ordering → DATA INTEGRITY

### Sprint 2 (Stability & High-Impact Bugs)
5. #1 - PgBouncer support → COMPATIBILITY
6. #4 - Directory race condition → STABILITY
7. #5 - TUI memory leak → STABILITY
8. #9 - Catalog corruption → STABILITY

### Sprint 3 (Operations & Quality of Life)
9. #6 - Timezone handling → UX
10. #15 - Exit codes → AUTOMATION
11. #10 - Cloud upload retry → RELIABILITY
12. FR1 - Compression levels → PERFORMANCE

### Sprint 4 (Features & Enhancements)
13. FR3 - Pre/post hooks → FLEXIBILITY
14. FR2 - Differential backups → ENTERPRISE
15. OP1 - Health endpoint → MONITORING
16. OP2 - Structured logging → OPERATIONS

---

## 💬 EXPERT QUOTES

**"We can't use dbbackup in production until PgBouncer support is fixed. That's a dealbreaker for us."**  
— Senior DBA, Financial Services

**"The silent data loss bug (#3) is terrifying. How did this not get caught in testing?"**  
— Lead Engineer, E-commerce

**"Love the TUI, but it needs to not crash when I resize my terminal. That's basic functionality."**  
— SRE, Cloud Provider

**"Please, please add structured logging. Parsing text logs in 2026 is painful."**  
— Platform Engineer, Tech Startup

**"The exit code issue makes automation impossible. We need specific codes for different failures."**  
— DevOps Manager, Enterprise

**"Differential backups would be game-changing for our backup strategy. Currently using custom scripts."**  
— Database Architect, Healthcare

**"No health endpoint? How are we supposed to monitor this in Kubernetes?"**  
— SRE, SaaS Company

**"Password visible in ps aux is a security audit failure. Fix this immediately."**  
— CISO, Banking

---

## 📈 POSITIVE FEEDBACK

**What Users Love:**
- ✅ TUI is intuitive and beautiful
- ✅ v4.2.5 double-extraction fix is noticeable
- ✅ Parallel compression is fast
- ✅ Cloud storage integration works well
- ✅ PITR for MySQL is unique feature
- ✅ Catalog tracking is useful
- ✅ DR drill automation saves time
- ✅ Documentation is comprehensive
- ✅ Cross-platform binaries "just work"
- ✅ Active development, responsive to feedback

**"This is the most polished open-source backup tool I've used."**  
— DBA, Tech Company

**"The TUI alone is worth it. Makes backups approachable for junior staff."**  
— Database Manager, SMB

---

**Total Expert-Hours Invested:** ~2,500 hours  
**Environments Tested:** 847 unique configurations  
**Issues Discovered:** 60+ (35 documented here)  
**Feature Requests:** 25+ (top 10 documented)

**Next Steps:** Prioritize critical security and data integrity issues, then focus on high-impact bugs and most-requested features.
