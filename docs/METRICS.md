# DBBackup Prometheus Metrics

This document describes all Prometheus metrics exposed by DBBackup for monitoring and alerting.

## Backup Status Metrics

### `dbbackup_rpo_seconds`
**Type:** Gauge  
**Labels:** `server`, `database`, `backup_type`  
**Description:** Time in seconds since the last successful backup (Recovery Point Objective).

**Recommended Thresholds:**
- Green: < 43200 (12 hours)
- Yellow: 43200-86400 (12-24 hours)  
- Red: > 86400 (24+ hours)

**Example Query:**
```promql
dbbackup_rpo_seconds{server="prod-db-01"} > 86400

# RPO by backup type
dbbackup_rpo_seconds{backup_type="full"}
dbbackup_rpo_seconds{backup_type="incremental"}
```

---

### `dbbackup_backup_total`
**Type:** Gauge  
**Labels:** `server`, `database`, `status`  
**Description:** Total count of backup attempts, labeled by status (`success` or `failure`).

**Example Query:**
```promql
# Total successful backups
dbbackup_backup_total{status="success"}
```

---

### `dbbackup_backup_by_type`
**Type:** Gauge  
**Labels:** `server`, `database`, `backup_type`  
**Description:** Total count of backups by backup type (`full`, `incremental`, `pitr_base`).

> **Note:** The `backup_type` label values are:
> - `full` - Created with `--backup-type full` (default)
> - `incremental` - Created with `--backup-type incremental`
> - `pitr_base` - Auto-assigned when using `dbbackup pitr base` command
>
> The CLI `--backup-type` flag only accepts `full` or `incremental`.

**Example Query:**
```promql
# Count of each backup type
dbbackup_backup_by_type{backup_type="full"}
dbbackup_backup_by_type{backup_type="incremental"}
dbbackup_backup_by_type{backup_type="pitr_base"}
```

---

### `dbbackup_backup_verified`
**Type:** Gauge  
**Labels:** `server`, `database`  
**Description:** Whether the most recent backup was verified successfully (1 = verified, 0 = not verified).

---

### `dbbackup_last_backup_size_bytes`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`, `backup_type`  
**Description:** Size of the last successful backup in bytes.

**Example Query:**
```promql
# Total backup storage across all databases
sum(dbbackup_last_backup_size_bytes)

# Size by backup type
dbbackup_last_backup_size_bytes{backup_type="full"}
```

---

### `dbbackup_last_backup_duration_seconds`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`, `backup_type`  
**Description:** Duration of the last backup operation in seconds.

---

### `dbbackup_last_success_timestamp`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`, `backup_type`  
**Description:** Unix timestamp of the last successful backup.

---

## PITR (Point-in-Time Recovery) Metrics

### `dbbackup_pitr_enabled`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Whether PITR is enabled for the database (1 = enabled, 0 = disabled).

**Example Query:**
```promql
# Check if PITR is enabled
dbbackup_pitr_enabled{database="production"} == 1
```

---

### `dbbackup_pitr_last_archived_timestamp`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Unix timestamp of the last archived WAL segment (PostgreSQL) or binlog file (MySQL).

---

### `dbbackup_pitr_archive_lag_seconds`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Seconds since the last WAL/binlog was archived. High values indicate archiving issues.

**Recommended Thresholds:**
- Green: < 300 (5 minutes)
- Yellow: 300-600 (5-10 minutes)
- Red: > 600 (10+ minutes)

**Example Query:**
```promql
# Alert on high archive lag
dbbackup_pitr_archive_lag_seconds > 600
```

---

### `dbbackup_pitr_archive_count`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Total number of archived WAL segments or binlog files.

---

### `dbbackup_pitr_archive_size_bytes`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Total size of archived logs in bytes.

---

### `dbbackup_pitr_chain_valid`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Whether the WAL/binlog chain is valid (1 = valid, 0 = gaps detected).

**Example Query:**
```promql
# Alert on broken chain
dbbackup_pitr_chain_valid == 0
```

---

### `dbbackup_pitr_gap_count`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Number of gaps detected in the WAL/binlog chain. Any value > 0 requires investigation.

---

### `dbbackup_pitr_recovery_window_minutes`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Estimated recovery window in minutes - the time span covered by archived logs.

---

## Deduplication Metrics

### `dbbackup_dedup_ratio`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Overall deduplication efficiency (0-1). A ratio of 0.5 means 50% space savings.

---

### `dbbackup_dedup_database_ratio`
**Type:** Gauge  
**Labels:** `server`, `database`  
**Description:** Per-database deduplication ratio.

---

### `dbbackup_dedup_space_saved_bytes`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Total bytes saved by deduplication across all backups.

---

### `dbbackup_dedup_disk_usage_bytes`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Actual disk usage of the chunk store after deduplication.

---

### `dbbackup_dedup_chunks_total`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Total number of unique content-addressed chunks in the dedup store.

---

### `dbbackup_dedup_compression_ratio`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Compression ratio achieved on chunk data (0-1). Higher = better compression.

---

### `dbbackup_dedup_oldest_chunk_timestamp`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Unix timestamp of the oldest chunk. Useful for monitoring retention policy.

---

### `dbbackup_dedup_newest_chunk_timestamp`
**Type:** Gauge  
**Labels:** `server`  
**Description:** Unix timestamp of the newest chunk. Confirms dedup is working on recent backups.

---

## Build Information Metrics

### `dbbackup_build_info`
**Type:** Gauge  
**Labels:** `server`, `version`, `commit`, `build_time`  
**Description:** Build information for the dbbackup exporter. Value is always 1.

This metric is useful for:
- Tracking which version is deployed across your fleet
- Alerting when versions drift between servers
- Correlating behavior changes with deployments

**Example Queries:**
```promql
# Show all deployed versions
group by (version) (dbbackup_build_info)

# Find servers not on latest version
dbbackup_build_info{version!="4.1.4"}

# Alert on version drift
count(count by (version) (dbbackup_build_info)) > 1

# PITR archive lag
dbbackup_pitr_archive_lag_seconds > 600

# Check PITR chain integrity
dbbackup_pitr_chain_valid == 1

# Estimate available PITR window (in minutes)
dbbackup_pitr_recovery_window_minutes

# PITR gaps detected
dbbackup_pitr_gap_count > 0
```

---

## Alerting Rules

See [alerting-rules.yaml](../grafana/alerting-rules.yaml) for pre-configured Prometheus alerting rules.

### Recommended Alerts

| Alert Name | Condition | Severity |
|------------|-----------|----------|
| BackupStale | `dbbackup_rpo_seconds > 86400` | Critical |
| BackupFailed | `increase(dbbackup_backup_total{status="failure"}[1h]) > 0` | Warning |
| BackupNotVerified | `dbbackup_backup_verified == 0` | Warning |
| DedupDegraded | `dbbackup_dedup_ratio < 0.1` | Info |
| PITRArchiveLag | `dbbackup_pitr_archive_lag_seconds > 600` | Warning |
| PITRChainBroken | `dbbackup_pitr_chain_valid == 0` | Critical |
| PITRDisabled | `dbbackup_pitr_enabled == 0` (unexpected) | Critical |
| NoIncrementalBackups | `dbbackup_backup_by_type{backup_type="incremental"} == 0` for 7d | Info |

---

## Dashboard

Import the [Grafana dashboard](../grafana/dbbackup-dashboard.json) for visualization of all metrics.

## Exporting Metrics

Metrics are exposed at `/metrics` when running with `--metrics` flag:

```bash
dbbackup backup cluster --metrics --metrics-port 9090
```

Or configure in `.dbbackup.conf`:
```ini
[metrics]
enabled = true
port = 9090
path = /metrics
```
