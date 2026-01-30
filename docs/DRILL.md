# Disaster Recovery Drilling

Complete guide for automated disaster recovery testing with dbbackup.

## Overview

DR drills automate the process of validating backup integrity through actual restore testing. Instead of hoping backups work when needed, automated drills regularly restore backups in isolated containers to verify:

- Backup file integrity
- Database compatibility
- Restore time estimates (RTO)
- Schema validation
- Data consistency

## Quick Start

```bash
# Run single DR drill on latest backup
dbbackup drill /mnt/backups/databases

# Drill specific database
dbbackup drill /mnt/backups/databases --database myapp

# Drill multiple databases
dbbackup drill /mnt/backups/databases --database myapp,mydb

# Schedule daily drills
dbbackup drill /mnt/backups/databases --schedule daily
```

## How It Works

1. **Select backup** - Picks latest or specified backup
2. **Create container** - Starts isolated database container
3. **Extract backup** - Decompresses to temporary storage
4. **Restore** - Imports data to test database
5. **Validate** - Runs integrity checks
6. **Cleanup** - Removes test container
7. **Report** - Stores results in catalog

## Drill Configuration

### Select Specific Backup

```bash
# Latest backup for database
dbbackup drill /mnt/backups/databases --database myapp

# Backup from specific date
dbbackup drill /mnt/backups/databases --database myapp --date 2026-01-23

# Oldest backup (best test)
dbbackup drill /mnt/backups/databases --database myapp --oldest
```

### Drill Options

```bash
# Full validation (slower)
dbbackup drill /mnt/backups/databases --full-validation

# Quick validation (schema only, faster)
dbbackup drill /mnt/backups/databases --quick-validation

# Store results in catalog
dbbackup drill /mnt/backups/databases --catalog

# Send notification on failure
dbbackup drill /mnt/backups/databases --notify-on-failure

# Custom test database name
dbbackup drill /mnt/backups/databases --test-database dr_test_prod
```

## Scheduled Drills

Run drills automatically on a schedule.

### Configure Schedule

```bash
# Daily drill at 03:00
dbbackup drill /mnt/backups/databases --schedule "03:00"

# Weekly drill (Sunday 02:00)
dbbackup drill /mnt/backups/databases --schedule "sun 02:00"

# Monthly drill (1st of month)
dbbackup drill /mnt/backups/databases --schedule "monthly"

# Install as systemd timer
sudo dbbackup install drill \
  --backup-path /mnt/backups/databases \
  --schedule "03:00"
```

### Verify Schedule

```bash
# Show next 5 scheduled drills
dbbackup drill list --upcoming

# Check drill history
dbbackup drill list --history

# Show drill statistics
dbbackup drill stats
```

## Drill Results

### View Drill History

```bash
# All drill results
dbbackup drill list

# Recent 10 drills
dbbackup drill list --limit 10

# Drills from last week
dbbackup drill list --after "$(date -d '7 days ago' +%Y-%m-%d)"

# Failed drills only
dbbackup drill list --status failed

# Passed drills only
dbbackup drill list --status passed
```

### Detailed Drill Report

```bash
dbbackup drill report myapp_2026-01-23.dump.gz

# Output includes:
#   - Backup filename
#   - Database version
#   - Extract time
#   - Restore time
#   - Row counts (before/after)
#   - Table verification results
#   - Data integrity status
#   - Pass/Fail verdict
#   - Warnings/errors
```

## Validation Types

### Full Validation

Deep integrity checks on restored data.

```bash
dbbackup drill /mnt/backups/databases --full-validation

# Checks:
# - All tables restored
# - Row counts match original
# - Indexes present and valid
# - Constraints enforced
# - Foreign key references valid
# - Sequence values correct (PostgreSQL)
# - Triggers present (if not system-generated)
```

### Quick Validation

Schema-only validation (fast).

```bash
dbbackup drill /mnt/backups/databases --quick-validation

# Checks:
# - Database connects
# - All tables present
# - Column definitions correct
# - Indexes exist
```

### Custom Validation

Run custom SQL checks.

```bash
# Add custom validation query
dbbackup drill /mnt/backups/databases \
  --validation-query "SELECT COUNT(*) FROM users" \
  --validation-expected 15000

# Example for multiple tables
dbbackup drill /mnt/backups/databases \
  --validation-query "SELECT COUNT(*) FROM orders WHERE status='completed'" \
  --validation-expected 42000
```

## Reporting

### Generate Drill Report

```bash
# HTML report (email-friendly)
dbbackup drill report --format html --output drill-report.html

# JSON report (for CI/CD pipelines)
dbbackup drill report --format json --output drill-results.json

# Markdown report (GitHub integration)
dbbackup drill report --format markdown --output drill-results.md
```

### Example Report Format

```
Disaster Recovery Drill Results
================================

Backup: myapp_2026-01-23_14-30-00.dump.gz
Date: 2026-01-25 03:15:00
Duration: 5m 32s
Status: PASSED

Details:
  Extract Time:        1m 15s
  Restore Time:        3m 42s
  Validation Time:     34s
  
  Tables Restored:     42
  Rows Verified:       1,234,567
  Total Size:          2.5 GB
  
Validation:
  Schema Check:        OK
  Row Count Check:     OK (all tables)
  Index Check:         OK (all 28 indexes present)
  Constraint Check:    OK (all 5 foreign keys valid)
  
Warnings: None
Errors: None
```

## Integration with CI/CD

### GitHub Actions

```yaml
name: Daily DR Drill

on:
  schedule:
    - cron: '0 3 * * *'  # Daily at 03:00

jobs:
  dr-drill:
    runs-on: ubuntu-latest
    steps:
      - name: Run DR drill
        run: |
          dbbackup drill /backups/databases \
            --full-validation \
            --format json \
            --output results.json
      
      - name: Check results
        run: |
          if grep -q '"status":"failed"' results.json; then
            echo "DR drill failed!"
            exit 1
          fi
      
      - name: Upload report
        uses: actions/upload-artifact@v2
        with:
          name: drill-results
          path: results.json
```

### Jenkins Pipeline

```groovy
pipeline {
  triggers {
    cron('H 3 * * *')  // Daily at 03:00
  }
  
  stages {
    stage('DR Drill') {
      steps {
        sh 'dbbackup drill /backups/databases --full-validation --format json --output drill.json'
      }
    }
    
    stage('Validate Results') {
      steps {
        script {
          def results = readJSON file: 'drill.json'
          if (results.status != 'passed') {
            error("DR drill failed!")
          }
        }
      }
    }
  }
}
```

## Troubleshooting

### Drill Fails with "Out of Space"

```bash
# Check available disk space
df -h

# Clean up old test databases
docker system prune -a

# Use faster storage for test
dbbackup drill /mnt/backups/databases --temp-dir /ssd/drill-temp
```

### Drill Times Out

```bash
# Increase timeout (minutes)
dbbackup drill /mnt/backups/databases --timeout 30

# Skip certain validations to speed up
dbbackup drill /mnt/backups/databases --quick-validation
```

### Drill Shows Data Mismatch

Indicates a problem with the backup - investigate immediately:

```bash
# Get detailed diff report
dbbackup drill report --show-diffs myapp_2026-01-23.dump.gz

# Regenerate backup
dbbackup backup single myapp --force-full
```

## Best Practices

1. **Run weekly drills minimum** - Catch issues early

2. **Test oldest backups** - Verify full retention chain works
   ```bash
   dbbackup drill /mnt/backups/databases --oldest
   ```

3. **Test critical databases first** - Prioritize by impact

4. **Store results in catalog** - Track historical pass/fail rates

5. **Alert on failures** - Automatic notification via email/Slack

6. **Document RTO** - Use drill times to refine recovery objectives

7. **Test cross-major-versions** - Use test environment with different DB version
   ```bash
   # Test PostgreSQL 15 backup on PostgreSQL 16
   dbbackup drill /mnt/backups/databases --target-version 16
   ```
