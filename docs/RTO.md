# RTO/RPO Analysis

Complete reference for Recovery Time Objective (RTO) and Recovery Point Objective (RPO) analysis and calculation.

## Overview

RTO and RPO are critical metrics for disaster recovery planning:

- **RTO (Recovery Time Objective)** - Maximum acceptable time to restore systems
- **RPO (Recovery Point Objective)** - Maximum acceptable data loss (time)

dbbackup calculates these based on:
- Backup size and compression
- Database size and transaction rate
- Network bandwidth
- Hardware resources
- Retention policy

## Quick Start

```bash
# Show RTO/RPO analysis
dbbackup rto show

# Show recommendations
dbbackup rto recommendations

# Export for disaster recovery plan
dbbackup rto export --format pdf --output drp.pdf
```

## RTO Calculation

RTO depends on restore operations:

```
RTO = Time to: Extract + Restore + Validation

Extract Time = Backup Size / Extraction Speed (~500 MB/s typical)
Restore Time = Total Operations / Database Write Speed (~10-100K rows/sec)
Validation = Backup Verify (~10% of restore time)
```

### Example

```
Backup: myapp_production
- Size on disk: 2.5 GB
- Compressed: 850 MB

Extract Time = 850 MB / 500 MB/s = 1.7 minutes
Restore Time = 1.5M rows / 50K rows/sec = 30 minutes
Validation = 3 minutes

Total RTO = 34.7 minutes
```

## RPO Calculation

RPO depends on backup frequency and transaction rate:

```
RPO = Backup Interval + WAL Replay Time

Example with daily backups:
- Backup interval: 24 hours
- WAL available for PITR: +6 hours

RPO = 24-30 hours (worst case)
```

### Optimizing RPO

Reduce RPO by:

```bash
# More frequent backups (hourly vs daily)
dbbackup backup single myapp --schedule "0 * * * *"  # Every hour

# Enable PITR (Point-in-Time Recovery)
dbbackup pitr enable myapp /mnt/wal
dbbackup pitr base myapp /mnt/wal

# Continuous WAL archiving
dbbackup pitr status myapp /mnt/wal
```

With PITR enabled:
```
RPO = Time since last transaction (typically < 5 minutes)
```

## Analysis Command

### Show Current Metrics

```bash
dbbackup rto show
```

Output:
```
Database: production
Engine: PostgreSQL 15

Current Status:
  Last Backup:           2026-01-23 02:00:00 (22 hours ago)
  Backup Size:           2.5 GB (compressed: 850 MB)
  RTO Estimate:          35 minutes
  RPO Current:           22 hours
  PITR Enabled:          yes
  PITR Window:           6 hours
  
Recommendations:
  - RTO is acceptable (< 1 hour)
  - RPO could be improved with hourly backups (currently 22h)
  - PITR reduces RPO to 6 hours in case of full backup loss
  
Recovery Plans:
  Scenario 1: Full database loss
    RTO: 35 minutes (restore from latest backup)
    RPO: 22 hours (data since last backup lost)
    
  Scenario 2: Point-in-time recovery
    RTO: 45 minutes (restore backup + replay WAL)
    RPO: 5 minutes (last transaction available)
    
  Scenario 3: Table-level recovery (single table drop)
    RTO: 30 minutes (restore to temp DB, extract table)
    RPO: 22 hours
```

### Get Recommendations

```bash
dbbackup rto recommendations

# Output includes:
# - Suggested backup frequency
# - PITR recommendations
# - Parallelism recommendations
# - Resource utilization tips
# - Cost-benefit analysis
```

## Scenarios

### Scenario Analysis

Calculate RTO/RPO for different failure modes.

```bash
# Full database loss (use latest backup)
dbbackup rto scenario --type full-loss

# Point-in-time recovery (specific time before incident)
dbbackup rto scenario --type point-in-time --time "2026-01-23 14:30:00"

# Table-level recovery
dbbackup rto scenario --type table-level --table users

# Multiple databases
dbbackup rto scenario --type multi-db --databases myapp,mydb
```

### Custom Scenario

```bash
# Network bandwidth constraint
dbbackup rto scenario \
  --type full-loss \
  --bandwidth 10MB/s \
  --storage-type s3

# Limited resources (small restore server)
dbbackup rto scenario \
  --type full-loss \
  --cpu-cores 4 \
  --memory-gb 8

# High transaction rate database
dbbackup rto scenario \
  --type point-in-time \
  --tps 100000
```

## Monitoring

### Track RTO/RPO Trends

```bash
# Show trend over time
dbbackup rto history

# Export metrics for trending
dbbackup rto export --format csv

# Output:
# Date,Database,RTO_Minutes,RPO_Hours,Backup_Size_GB,Status
# 2026-01-15,production,35,22,2.5,ok
# 2026-01-16,production,35,22,2.5,ok
# 2026-01-17,production,38,24,2.6,warning
```

### Alert on RTO/RPO Violations

```bash
# Alert if RTO > 1 hour
dbbackup rto alert --type rto-violation --threshold 60

# Alert if RPO > 24 hours
dbbackup rto alert --type rpo-violation --threshold 24

# Email on violations
dbbackup rto alert \
  --type rpo-violation \
  --threshold 24 \
  --notify-email admin@example.com
```

## Detailed Calculations

### Backup Time Components

```bash
# Analyze last backup performance
dbbackup rto backup-analysis

# Output:
#   Database: production
#   Backup Date: 2026-01-23 02:00:00
#   Total Duration: 45 minutes
#   
#   Components:
#   - Data extraction:  25m 30s (56%)
#   - Compression:      12m 15s (27%)
#   - Encryption:       5m 45s  (13%)
#   - Upload to cloud:  1m 30s  (3%)
#   
#   Throughput: 95 MB/s
#   Compression Ratio: 65%
```

### Restore Time Components

```bash
# Analyze restore performance from a test drill
dbbackup rto restore-analysis myapp_2026-01-23.dump.gz

# Output:
#   Extract Time:       1m 45s
#   Restore Time:       28m 30s
#   Validation:         3m 15s
#   Total RTO:          33m 30s
#   
#   Restore Speed:      2.8M rows/minute
#   Objects Created:    4200
#   Indexes Built:      145
```

## Configuration

Configure RTO/RPO targets in `.dbbackup.conf`:

```ini
[rto_rpo]
# Target RTO (minutes)
target_rto_minutes = 60

# Target RPO (hours)
target_rpo_hours = 4

# Alert on threshold violation
alert_on_violation = true

# Minimum backups to maintain RTO
min_backups_for_rto = 5

# PITR window target (hours)
pitr_window_hours = 6
```

## SLAs and Compliance

### Define SLA

```bash
# Create SLA requirement
dbbackup rto sla \
  --name production \
  --target-rto-minutes 30 \
  --target-rpo-hours 4 \
  --databases myapp,payments

# Verify compliance
dbbackup rto sla --verify production

# Generate compliance report
dbbackup rto sla --report production
```

### Audit Trail

```bash
# Show RTO/RPO audit history
dbbackup rto audit

# Output shows:
# Date                  Metric  Value     Target    Status
# 2026-01-25 03:15:00  RTO     35m       60m       PASS
# 2026-01-25 03:15:00  RPO     22h       4h        FAIL
# 2026-01-24 03:00:00  RTO     35m       60m       PASS
# 2026-01-24 03:00:00  RPO     22h       4h        FAIL
```

## Reporting

### Generate Report

```bash
# Markdown report
dbbackup rto report --format markdown --output rto-report.md

# PDF for disaster recovery plan
dbbackup rto report --format pdf --output drp.pdf

# HTML for dashboard
dbbackup rto report --format html --output rto-metrics.html
```

## Best Practices

1. **Define SLA targets** - Start with business requirements
   - Critical systems: RTO < 1 hour
   - Important systems: RTO < 4 hours
   - Standard systems: RTO < 24 hours

2. **Test RTO regularly** - DR drills validate estimates
   ```bash
   dbbackup drill /mnt/backups --full-validation
   ```

3. **Monitor trends** - Increasing RTO may indicate issues

4. **Optimize backups** - Faster backups = smaller RTO
   - Increase parallelism
   - Use faster storage
   - Optimize compression level

5. **Plan for PITR** - Critical systems should have PITR enabled
   ```bash
   dbbackup pitr enable myapp /mnt/wal
   ```

6. **Document assumptions** - RTO/RPO calculations depend on:
   - Available bandwidth
   - Target hardware
   - Parallelism settings
   - Database size changes

7. **Regular audit** - Monthly SLA compliance review
   ```bash
   dbbackup rto sla --verify production
   ```
