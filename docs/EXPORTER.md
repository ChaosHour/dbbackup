# DBBackup Prometheus Exporter & Grafana Dashboard

This document provides complete reference for the DBBackup Prometheus exporter, including all exported metrics, setup instructions, and Grafana dashboard configuration.

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
# Start HTTP exporter on default port 9399
dbbackup metrics serve

# Custom port
dbbackup metrics serve --port 9100

# Specify server name for labels
dbbackup metrics serve --server production-db-01
```

### Export to Textfile (for node_exporter)

```bash
# Export to default location
dbbackup metrics export

# Custom output path
dbbackup metrics export --output /var/lib/node_exporter/textfile_collector/dbbackup.prom
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

**Configuration:**
```bash
dbbackup metrics serve --server <instance-name> --port <port>
```

### Textfile Mode (`metrics export`)

Writes metrics to a file for collection by node_exporter's textfile collector.

**Default Path:** `/var/lib/dbbackup/metrics/dbbackup.prom`

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
| `dbbackup_last_success_timestamp` | gauge | `server`, `database`, `engine` | Unix timestamp of last successful backup |
| `dbbackup_last_backup_duration_seconds` | gauge | `server`, `database`, `engine` | Duration of last successful backup in seconds |
| `dbbackup_last_backup_size_bytes` | gauge | `server`, `database`, `engine` | Size of last successful backup in bytes |
| `dbbackup_backup_total` | counter | `server`, `database`, `status` | Total backup attempts (status: `success` or `failure`) |
| `dbbackup_rpo_seconds` | gauge | `server`, `database` | Seconds since last successful backup (RPO) |
| `dbbackup_backup_verified` | gauge | `server`, `database` | Whether last backup was verified (1=yes, 0=no) |
| `dbbackup_scrape_timestamp` | gauge | `server` | Unix timestamp when metrics were collected |

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

---

## Example Metrics Output

```prometheus
# DBBackup Prometheus Metrics
# Generated at: 2026-01-26T10:30:00Z
# Server: production

# HELP dbbackup_last_success_timestamp Unix timestamp of last successful backup
# TYPE dbbackup_last_success_timestamp gauge
dbbackup_last_success_timestamp{server="production",database="myapp",engine="postgres"} 1737884600

# HELP dbbackup_last_backup_duration_seconds Duration of last successful backup in seconds
# TYPE dbbackup_last_backup_duration_seconds gauge
dbbackup_last_backup_duration_seconds{server="production",database="myapp",engine="postgres"} 125.50

# HELP dbbackup_last_backup_size_bytes Size of last successful backup in bytes
# TYPE dbbackup_last_backup_size_bytes gauge
dbbackup_last_backup_size_bytes{server="production",database="myapp",engine="postgres"} 1073741824

# HELP dbbackup_backup_total Total number of backup attempts
# TYPE dbbackup_backup_total counter
dbbackup_backup_total{server="production",database="myapp",status="success"} 42
dbbackup_backup_total{server="production",database="myapp",status="failure"} 2

# HELP dbbackup_rpo_seconds Recovery Point Objective - seconds since last successful backup
# TYPE dbbackup_rpo_seconds gauge
dbbackup_rpo_seconds{server="production",database="myapp"} 3600

# HELP dbbackup_backup_verified Whether the last backup was verified (1=yes, 0=no)
# TYPE dbbackup_backup_verified gauge
dbbackup_backup_verified{server="production",database="myapp"} 1

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
# Consistent naming
dbbackup metrics serve --server prod-db-01
```

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
