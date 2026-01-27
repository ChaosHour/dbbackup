# DBBackup Prometheus Exporter & Grafana Dashboard

This document provides complete reference for the DBBackup Prometheus exporter, including all exported metrics, setup instructions, and Grafana dashboard configuration.

## What's New (January 2026)

### New Features
- **Backup Type Tracking**: All backup metrics now include a `backup_type` label (`full`, `incremental`, `pitr_base`)
- **PITR Metrics**: Complete Point-in-Time Recovery monitoring for PostgreSQL WAL and MySQL binlog archiving
- **New Alerts**: PITR-specific alerts for archive lag, chain integrity, and gap detection

### New Metrics Added
| Metric | Description |
|--------|-------------|
| `dbbackup_backup_by_type` | Count backups by type (full/incremental/pitr_base) |
| `dbbackup_pitr_enabled` | Whether PITR is enabled (1/0) |
| `dbbackup_pitr_archive_lag_seconds` | Seconds since last WAL/binlog archived |
| `dbbackup_pitr_chain_valid` | WAL/binlog chain integrity (1=valid) |
| `dbbackup_pitr_gap_count` | Number of gaps in archive chain |
| `dbbackup_pitr_archive_count` | Total archived segments |
| `dbbackup_pitr_archive_size_bytes` | Total archive storage |
| `dbbackup_pitr_recovery_window_minutes` | Estimated PITR coverage |

### Label Changes
- `backup_type` label added to: `dbbackup_rpo_seconds`, `dbbackup_last_success_timestamp`, `dbbackup_last_backup_duration_seconds`, `dbbackup_last_backup_size_bytes`
- `dbbackup_backup_total` type changed from counter to gauge (more accurate for snapshot-based collection)

---

## Table of Contents

- [Quick Start](#quick-start)
- [Exporter Modes](#exporter-modes)
- [Complete Metrics Reference](#complete-metrics-reference)
- [Grafana Dashboard Setup](#grafana-dashboard-setup)
- [Alerting Rules](#alerting-rules)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Start the Metrics Server

```bash
# Start HTTP exporter on default port 9399 (auto-detects hostname for server label)
dbbackup metrics serve

# Custom port
dbbackup metrics serve --port 9100

# Specify server name for labels (overrides auto-detection)
dbbackup metrics serve --server production-db-01

# Specify custom catalog database location
dbbackup metrics serve --catalog-db /path/to/catalog.db
```

### Export to Textfile (for node_exporter)

```bash
# Export to default location
dbbackup metrics export

# Custom output path
dbbackup metrics export --output /var/lib/node_exporter/textfile_collector/dbbackup.prom

# Specify catalog database and server name
dbbackup metrics export --catalog-db /root/.dbbackup/catalog.db --server myhost
```

### Install as Systemd Service

```bash
# Install with metrics exporter
sudo dbbackup install --with-metrics

# Start the service
sudo systemctl start dbbackup-exporter
```

---

## Exporter Modes

### HTTP Server Mode (`metrics serve`)

Runs a standalone HTTP server exposing metrics for direct Prometheus scraping.

| Endpoint    | Description                      |
|-------------|----------------------------------|
| `/metrics`  | Prometheus metrics               |
| `/health`   | Health check (returns 200 OK)    |
| `/`         | Service info page                |

**Default Port:** 9399

**Server Label:** Auto-detected from hostname (use `--server` to override)

**Catalog Location:** `~/.dbbackup/catalog.db` (use `--catalog-db` to override)

**Configuration:**
```bash
dbbackup metrics serve [--server <instance-name>] [--port <port>] [--catalog-db <path>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | hostname | Server label for metrics (auto-detected if not set) |
| `--port` | 9399 | HTTP server port |
| `--catalog-db` | ~/.dbbackup/catalog.db | Path to catalog SQLite database |

### Textfile Mode (`metrics export`)

Writes metrics to a file for collection by node_exporter's textfile collector.

**Default Path:** `/var/lib/dbbackup/metrics/dbbackup.prom`

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | hostname | Server label for metrics (auto-detected if not set) |
| `--output` | /var/lib/dbbackup/metrics/dbbackup.prom | Output file path |
| `--catalog-db` | ~/.dbbackup/catalog.db | Path to catalog SQLite database |

**node_exporter Configuration:**
```bash
node_exporter --collector.textfile.directory=/var/lib/dbbackup/metrics/
```

---

## Complete Metrics Reference

All metrics use the `dbbackup_` prefix. Below is the **validated** list of metrics exported by DBBackup.

### Backup Status Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `dbbackup_last_success_timestamp` | gauge | `server`, `database`, `engine`, `backup_type` | Unix timestamp of last successful backup |
| `dbbackup_last_backup_duration_seconds` | gauge | `server`, `database`, `engine`, `backup_type` | Duration of last successful backup in seconds |
| `dbbackup_last_backup_size_bytes` | gauge | `server`, `database`, `engine`, `backup_type` | Size of last successful backup in bytes |
| `dbbackup_backup_total` | gauge | `server`, `database`, `status` | Total backup attempts (status: `success` or `failure`) |
| `dbbackup_backup_by_type` | gauge | `server`, `database`, `backup_type` | Backup count by type (`full`, `incremental`, `pitr_base`) |
| `dbbackup_rpo_seconds` | gauge | `server`, `database`, `backup_type` | Seconds since last successful backup (RPO) |
| `dbbackup_backup_verified` | gauge | `server`, `database` | Whether last backup was verified (1=yes, 0=no) |
| `dbbackup_scrape_timestamp` | gauge | `server` | Unix timestamp when metrics were collected |

### PITR (Point-in-Time Recovery) Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `dbbackup_pitr_enabled` | gauge | `server`, `database`, `engine` | Whether PITR is enabled (1=yes, 0=no) |
| `dbbackup_pitr_last_archived_timestamp` | gauge | `server`, `database`, `engine` | Unix timestamp of last archived WAL/binlog |
| `dbbackup_pitr_archive_lag_seconds` | gauge | `server`, `database`, `engine` | Seconds since last archive (lower is better) |
| `dbbackup_pitr_archive_count` | gauge | `server`, `database`, `engine` | Total archived WAL segments or binlog files |
| `dbbackup_pitr_archive_size_bytes` | gauge | `server`, `database`, `engine` | Total size of archived logs in bytes |
| `dbbackup_pitr_chain_valid` | gauge | `server`, `database`, `engine` | Whether archive chain is valid (1=yes, 0=gaps) |
| `dbbackup_pitr_gap_count` | gauge | `server`, `database`, `engine` | Number of gaps in archive chain |
| `dbbackup_pitr_recovery_window_minutes` | gauge | `server`, `database`, `engine` | Estimated PITR coverage window in minutes |
| `dbbackup_pitr_scrape_timestamp` | gauge | `server` | PITR metrics collection timestamp |

### Deduplication Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `dbbackup_dedup_chunks_total` | gauge | `server` | Total unique chunks stored |
| `dbbackup_dedup_manifests_total` | gauge | `server` | Total number of deduplicated backups |
| `dbbackup_dedup_backup_bytes_total` | gauge | `server` | Total logical size of all backups (bytes) |
| `dbbackup_dedup_stored_bytes_total` | gauge | `server` | Total unique data stored after dedup (bytes) |
| `dbbackup_dedup_space_saved_bytes` | gauge | `server` | Bytes saved by deduplication |
| `dbbackup_dedup_ratio` | gauge | `server` | Dedup efficiency (0-1, higher = better) |
| `dbbackup_dedup_disk_usage_bytes` | gauge | `server` | Actual disk usage of chunk store |
| `dbbackup_dedup_compression_ratio` | gauge | `server` | Compression ratio (0-1, higher = better) |
| `dbbackup_dedup_oldest_chunk_timestamp` | gauge | `server` | Unix timestamp of oldest chunk |
| `dbbackup_dedup_newest_chunk_timestamp` | gauge | `server` | Unix timestamp of newest chunk |
| `dbbackup_dedup_scrape_timestamp` | gauge | `server` | Dedup metrics collection timestamp |

### Per-Database Dedup Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `dbbackup_dedup_database_backup_count` | gauge | `server`, `database` | Deduplicated backups per database |
| `dbbackup_dedup_database_ratio` | gauge | `server`, `database` | Per-database dedup ratio |
| `dbbackup_dedup_database_last_backup_timestamp` | gauge | `server`, `database` | Last backup timestamp per database |
| `dbbackup_dedup_database_total_bytes` | gauge | `server`, `database` | Total logical size per database |
| `dbbackup_dedup_database_stored_bytes` | gauge | `server`, `database` | Stored bytes per database (after dedup) |
| `dbbackup_rpo_seconds` | gauge | `server`, `database` | Seconds since last backup (same as regular backups for unified alerting) |

> **Note:** The `dbbackup_rpo_seconds` metric is exported by both regular backups and dedup backups, enabling unified alerting without complex PromQL expressions.

---

## Example Metrics Output

```prometheus
# DBBackup Prometheus Metrics
# Generated at: 2026-01-27T10:30:00Z
# Server: production

# HELP dbbackup_last_success_timestamp Unix timestamp of last successful backup
# TYPE dbbackup_last_success_timestamp gauge
dbbackup_last_success_timestamp{server="production",database="myapp",engine="postgres",backup_type="full"} 1737884600

# HELP dbbackup_last_backup_duration_seconds Duration of last successful backup in seconds
# TYPE dbbackup_last_backup_duration_seconds gauge
dbbackup_last_backup_duration_seconds{server="production",database="myapp",engine="postgres",backup_type="full"} 125.50

# HELP dbbackup_last_backup_size_bytes Size of last successful backup in bytes
# TYPE dbbackup_last_backup_size_bytes gauge
dbbackup_last_backup_size_bytes{server="production",database="myapp",engine="postgres",backup_type="full"} 1073741824

# HELP dbbackup_backup_total Total number of backup attempts by type and status
# TYPE dbbackup_backup_total gauge
dbbackup_backup_total{server="production",database="myapp",status="success"} 42
dbbackup_backup_total{server="production",database="myapp",status="failure"} 2

# HELP dbbackup_backup_by_type Total number of backups by backup type
# TYPE dbbackup_backup_by_type gauge
dbbackup_backup_by_type{server="production",database="myapp",backup_type="full"} 30
dbbackup_backup_by_type{server="production",database="myapp",backup_type="incremental"} 12

# HELP dbbackup_rpo_seconds Recovery Point Objective - seconds since last successful backup
# TYPE dbbackup_rpo_seconds gauge
dbbackup_rpo_seconds{server="production",database="myapp",backup_type="full"} 3600

# HELP dbbackup_backup_verified Whether the last backup was verified (1=yes, 0=no)
# TYPE dbbackup_backup_verified gauge
dbbackup_backup_verified{server="production",database="myapp"} 1

# HELP dbbackup_pitr_enabled Whether PITR is enabled for database (1=enabled, 0=disabled)
# TYPE dbbackup_pitr_enabled gauge
dbbackup_pitr_enabled{server="production",database="myapp",engine="postgres"} 1

# HELP dbbackup_pitr_archive_lag_seconds Seconds since last WAL/binlog was archived
# TYPE dbbackup_pitr_archive_lag_seconds gauge
dbbackup_pitr_archive_lag_seconds{server="production",database="myapp",engine="postgres"} 45

# HELP dbbackup_pitr_chain_valid Whether the WAL/binlog chain is valid (1=valid, 0=gaps detected)
# TYPE dbbackup_pitr_chain_valid gauge
dbbackup_pitr_chain_valid{server="production",database="myapp",engine="postgres"} 1

# HELP dbbackup_pitr_recovery_window_minutes Estimated recovery window in minutes
# TYPE dbbackup_pitr_recovery_window_minutes gauge
dbbackup_pitr_recovery_window_minutes{server="production",database="myapp",engine="postgres"} 10080

# HELP dbbackup_dedup_ratio Deduplication ratio (0-1, higher is better)
# TYPE dbbackup_dedup_ratio gauge
dbbackup_dedup_ratio{server="production"} 0.6500

# HELP dbbackup_dedup_space_saved_bytes Bytes saved by deduplication
# TYPE dbbackup_dedup_space_saved_bytes gauge
dbbackup_dedup_space_saved_bytes{server="production"} 5368709120
```

---

## Prometheus Scrape Configuration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'dbbackup'
    scrape_interval: 60s
    scrape_timeout: 10s
    
    static_configs:
      - targets:
          - 'db-server-01:9399'
          - 'db-server-02:9399'
        labels:
          environment: 'production'
      
      - targets:
          - 'db-staging:9399'
        labels:
          environment: 'staging'
    
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):\d+'
        replacement: '$1'
```

### File-based Service Discovery

```yaml
  - job_name: 'dbbackup-sd'
    scrape_interval: 60s
    file_sd_configs:
      - files:
          - '/etc/prometheus/targets/dbbackup/*.yml'
        refresh_interval: 5m
```

---

## Grafana Dashboard Setup

### Import Dashboard

1. Open Grafana → **Dashboards** → **Import**
2. Upload `grafana/dbbackup-dashboard.json` or paste the JSON
3. Select your Prometheus data source
4. Click **Import**

### Dashboard Panels

The dashboard includes the following panels:

#### Backup Overview Row
| Panel | Metric Used | Description |
|-------|-------------|-------------|
| Last Backup Status | `dbbackup_rpo_seconds < bool 604800` | SUCCESS/FAILED indicator |
| Time Since Last Backup | `dbbackup_rpo_seconds` | Time elapsed since last backup |
| Verification Status | `dbbackup_backup_verified` | VERIFIED/NOT VERIFIED |
| Total Successful Backups | `dbbackup_backup_total{status="success"}` | Counter |
| Total Failed Backups | `dbbackup_backup_total{status="failure"}` | Counter |
| RPO Over Time | `dbbackup_rpo_seconds` | Time series graph |
| Backup Size | `dbbackup_last_backup_size_bytes` | Bar chart |
| Backup Duration | `dbbackup_last_backup_duration_seconds` | Time series |
| Backup Status Overview | Multiple metrics | Table with color-coded status |

#### Deduplication Statistics Row
| Panel | Metric Used | Description |
|-------|-------------|-------------|
| Dedup Ratio | `dbbackup_dedup_ratio` | Percentage efficiency |
| Space Saved | `dbbackup_dedup_space_saved_bytes` | Total bytes saved |
| Disk Usage | `dbbackup_dedup_disk_usage_bytes` | Actual storage used |
| Total Chunks | `dbbackup_dedup_chunks_total` | Chunk count |
| Compression Ratio | `dbbackup_dedup_compression_ratio` | Compression efficiency |
| Oldest Chunk | `dbbackup_dedup_oldest_chunk_timestamp` | Age of oldest data |
| Newest Chunk | `dbbackup_dedup_newest_chunk_timestamp` | Most recent chunk |
| Dedup Ratio by Database | `dbbackup_dedup_database_ratio` | Per-database efficiency |
| Dedup Storage Over Time | `dbbackup_dedup_space_saved_bytes`, `dbbackup_dedup_disk_usage_bytes` | Storage trends |

### Dashboard Variables

| Variable | Query | Description |
|----------|-------|-------------|
| `$server` | `label_values(dbbackup_rpo_seconds, server)` | Filter by server |
| `$DS_PROMETHEUS` | datasource | Prometheus data source |

### Dashboard Thresholds

#### RPO Thresholds
- **Green:** < 12 hours (43200 seconds)
- **Yellow:** 12-24 hours
- **Red:** > 24 hours (86400 seconds)

#### Backup Status Thresholds
- **1 (Green):** SUCCESS
- **0 (Red):** FAILED

---

## Alerting Rules

### Pre-configured Alerts

Import `deploy/prometheus/alerting-rules.yaml` into Prometheus/Alertmanager.

#### Backup Status Alerts
| Alert | Expression | Severity | Description |
|-------|------------|----------|-------------|
| `DBBackupRPOWarning` | `dbbackup_rpo_seconds > 43200` | warning | No backup for 12+ hours |
| `DBBackupRPOCritical` | `dbbackup_rpo_seconds > 86400` | critical | No backup for 24+ hours |
| `DBBackupFailed` | `increase(dbbackup_backup_total{status="failure"}[1h]) > 0` | critical | Backup failed |
| `DBBackupFailureRateHigh` | Failure rate > 10% in 24h | warning | High failure rate |
| `DBBackupSizeAnomaly` | Size changed > 50% vs 7-day avg | warning | Unusual backup size |
| `DBBackupSizeZero` | `dbbackup_last_backup_size_bytes == 0` | critical | Empty backup file |
| `DBBackupDurationHigh` | `dbbackup_last_backup_duration_seconds > 3600` | warning | Backup taking > 1 hour |
| `DBBackupNotVerified` | `dbbackup_backup_verified == 0` for 24h | warning | Backup not verified |
| `DBBackupNoRecentFull` | No full backup in 7+ days | warning | Need full backup for incremental chain |

#### PITR Alerts (New)
| Alert | Expression | Severity | Description |
|-------|------------|----------|-------------|
| `DBBackupPITRArchiveLag` | `dbbackup_pitr_archive_lag_seconds > 600` | warning | Archive 10+ min behind |
| `DBBackupPITRArchiveCritical` | `dbbackup_pitr_archive_lag_seconds > 1800` | critical | Archive 30+ min behind |
| `DBBackupPITRChainBroken` | `dbbackup_pitr_chain_valid == 0` | critical | Gaps in WAL/binlog chain |
| `DBBackupPITRGaps` | `dbbackup_pitr_gap_count > 0` | warning | Gaps detected in archive chain |
| `DBBackupPITRDisabled` | PITR unexpectedly disabled | critical | PITR was enabled but now off |

#### Infrastructure Alerts
| Alert | Expression | Severity | Description |
|-------|------------|----------|-------------|
| `DBBackupExporterDown` | `up{job="dbbackup"} == 0` | critical | Exporter unreachable |
| `DBBackupDedupRatioLow` | `dbbackup_dedup_ratio < 0.2` for 24h | info | Low dedup efficiency |
| `DBBackupStorageHigh` | `dbbackup_dedup_disk_usage_bytes > 1TB` | warning | High storage usage |

### Example Alert Configuration

```yaml
groups:
  - name: dbbackup
    rules:
      - alert: DBBackupRPOCritical
        expr: dbbackup_rpo_seconds > 86400
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "No backup for {{ $labels.database }} in 24+ hours"
          description: "RPO violation on {{ $labels.server }}. Last backup: {{ $value | humanizeDuration }} ago."
      
      - alert: DBBackupPITRChainBroken
        expr: dbbackup_pitr_chain_valid == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PITR chain broken for {{ $labels.database }}"
          description: "WAL/binlog chain has gaps. Point-in-time recovery is NOT possible. New base backup required."
```

---

## Troubleshooting

### Exporter Not Returning Metrics

1. **Check catalog access:**
   ```bash
   dbbackup catalog list
   ```

2. **Verify port is open:**
   ```bash
   curl -v http://localhost:9399/metrics
   ```

3. **Check logs:**
   ```bash
   journalctl -u dbbackup-exporter -f
   ```

### Missing Dedup Metrics

Dedup metrics are only exported when using deduplication:
```bash
# Ensure dedup is enabled
dbbackup dedup status
```

### Metrics Not Updating

The exporter caches metrics for 30 seconds. The `/health` endpoint can confirm the exporter is running.

### Stale or Empty Metrics (Catalog Location Mismatch)

If the exporter shows stale or no backup data, verify the catalog database location:

```bash
# Check where catalog sync writes
dbbackup catalog sync /path/to/backups
# Output shows: [STATS] Catalog database: /root/.dbbackup/catalog.db

# Ensure exporter reads from the same location
dbbackup metrics serve --catalog-db /root/.dbbackup/catalog.db
```

**Common Issue:** If backup scripts run as root but the exporter runs as a different user, they may use different catalog locations. Use `--catalog-db` to ensure consistency.

### Dashboard Shows "No Data"

1. Verify Prometheus is scraping successfully:
   ```bash
   curl http://prometheus:9090/api/v1/targets | grep dbbackup
   ```

2. Check metric names match (case-sensitive):
   ```promql
   {__name__=~"dbbackup_.*"}
   ```

3. Verify `server` label matches dashboard variable.

### Label Mismatch Issues

Ensure the `--server` flag matches across all instances:
```bash
# Consistent naming (or let it auto-detect from hostname)
dbbackup metrics serve --server prod-db-01
```

> **Note:** As of v3.x, the exporter auto-detects hostname if `--server` is not specified. This ensures unique server labels in multi-host deployments.

---

## Metrics Validation Checklist

Use this checklist to validate your exporter setup:

- [ ] `/metrics` endpoint returns HTTP 200
- [ ] `/health` endpoint returns `{"status":"ok"}`
- [ ] `dbbackup_rpo_seconds` shows correct RPO values
- [ ] `dbbackup_backup_total` increments after backups
- [ ] `dbbackup_backup_verified` reflects verification status
- [ ] `dbbackup_last_backup_size_bytes` matches actual backup sizes
- [ ] Prometheus scrape succeeds (check targets page)
- [ ] Grafana dashboard loads without errors
- [ ] Dashboard variables populate correctly
- [ ] All panels show data (no "No Data" messages)

---

## Files Reference

| File | Description |
|------|-------------|
| `grafana/dbbackup-dashboard.json` | Grafana dashboard JSON |
| `grafana/alerting-rules.yaml` | Grafana alerting rules |
| `deploy/prometheus/alerting-rules.yaml` | Prometheus alerting rules |
| `deploy/prometheus/scrape-config.yaml` | Prometheus scrape configuration |
| `docs/METRICS.md` | Metrics documentation |

---

## Version Compatibility

| DBBackup Version | Metrics Version | Dashboard UID |
|------------------|-----------------|---------------|
| 1.0.0+           | v1              | `dbbackup-overview` |

---

## Support

For issues with the exporter or dashboard:
1. Check the [troubleshooting section](#troubleshooting)
2. Review logs: `journalctl -u dbbackup-exporter`
3. Open an issue with metrics output and dashboard screenshots
