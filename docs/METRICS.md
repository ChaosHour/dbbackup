# DBBackup Prometheus Metrics

This document describes all Prometheus metrics exposed by DBBackup for monitoring and alerting.

## Backup Status Metrics

### `dbbackup_rpo_seconds`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Time in seconds since the last successful backup (Recovery Point Objective).

**Recommended Thresholds:**
- Green: < 43200 (12 hours)
- Yellow: 43200-86400 (12-24 hours)  
- Red: > 86400 (24+ hours)

**Example Query:**
```promql
dbbackup_rpo_seconds{server="prod-db-01"} > 86400
```

---

### `dbbackup_backup_total`
**Type:** Counter  
**Labels:** `server`, `database`, `engine`, `status`  
**Description:** Total count of backup attempts, labeled by status (`success` or `failure`).

**Example Query:**
```promql
# Failure rate over last hour
rate(dbbackup_backup_total{status="failure"}[1h])
```

---

### `dbbackup_backup_verified`
**Type:** Gauge  
**Labels:** `server`, `database`  
**Description:** Whether the most recent backup was verified successfully (1 = verified, 0 = not verified).

---

### `dbbackup_last_backup_size_bytes`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Size of the last successful backup in bytes.

**Example Query:**
```promql
# Total backup storage across all databases
sum(dbbackup_last_backup_size_bytes)
```

---

### `dbbackup_last_backup_duration_seconds`
**Type:** Gauge  
**Labels:** `server`, `database`, `engine`  
**Description:** Duration of the last backup operation in seconds.

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

## Alerting Rules

See [alerting-rules.yaml](../grafana/alerting-rules.yaml) for pre-configured Prometheus alerting rules.

### Recommended Alerts

| Alert Name | Condition | Severity |
|------------|-----------|----------|
| BackupStale | `dbbackup_rpo_seconds > 86400` | Critical |
| BackupFailed | `increase(dbbackup_backup_total{status="failure"}[1h]) > 0` | Warning |
| BackupNotVerified | `dbbackup_backup_verified == 0` | Warning |
| DedupDegraded | `dbbackup_dedup_ratio < 0.1` | Info |

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
