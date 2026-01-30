# Backup Catalog

Complete reference for the dbbackup catalog system for tracking, managing, and analyzing backup inventory.

## Overview

The catalog is a SQLite database that tracks all backups, providing:
- Backup gap detection (missing scheduled backups)
- Retention policy compliance verification
- Backup integrity tracking
- Historical retention enforcement
- Full-text search over backup metadata

## Quick Start

```bash
# Initialize catalog (automatic on first use)
dbbackup catalog sync /mnt/backups/databases

# List all backups in catalog
dbbackup catalog list

# Show catalog statistics
dbbackup catalog stats

# View backup details
dbbackup catalog info mydb_2026-01-23.dump.gz

# Search for backups
dbbackup catalog search --database myapp --after 2026-01-01
```

## Catalog Sync

Syncs local backup directory with catalog database.

```bash
# Sync all backups in directory
dbbackup catalog sync /mnt/backups/databases

# Force rescan (useful if backups were added manually)
dbbackup catalog sync /mnt/backups/databases --force

# Sync specific database backups
dbbackup catalog sync /mnt/backups/databases --database myapp

# Dry-run to see what would be synced
dbbackup catalog sync /mnt/backups/databases --dry-run
```

Catalog entries include:
- Backup filename
- Database name
- Backup timestamp
- Size (bytes)
- Compression ratio
- Encryption status
- Backup type (full/incremental/pitr_base)
- Retention status
- Checksum/hash

## Listing Backups

### Show All Backups

```bash
dbbackup catalog list
```

Output format:
```
Database        Timestamp            Size        Compressed  Encrypted  Verified  Type
myapp           2026-01-23 14:30:00  2.5 GB      62%         yes        yes       full
myapp           2026-01-23 02:00:00  1.2 GB      58%         yes        yes       incremental
mydb            2026-01-23 22:15:00  856 MB      64%         no         no        full
```

### Filter by Database

```bash
dbbackup catalog list --database myapp
```

### Filter by Date Range

```bash
dbbackup catalog list --after 2026-01-01 --before 2026-01-31
```

### Sort Results

```bash
dbbackup catalog list --sort size --reverse     # Largest first
dbbackup catalog list --sort date              # Oldest first
dbbackup catalog list --sort verified           # Verified first
```

## Statistics and Gaps

### Show Catalog Statistics

```bash
dbbackup catalog stats
```

Output includes:
- Total backups
- Total size stored
- Unique databases
- Success/failure ratio
- Oldest/newest backup
- Average backup size

### Detect Backup Gaps

Gaps are missing expected backups based on schedule.

```bash
# Show gaps in mydb backups (assuming daily schedule)
dbbackup catalog gaps mydb --interval 24h

# 12-hour interval
dbbackup catalog gaps mydb --interval 12h

# Show as calendar grid
dbbackup catalog gaps mydb --interval 24h --calendar

# Define custom work hours (backup only weekdays 02:00)
dbbackup catalog gaps mydb --interval 24h --workdays-only
```

Output shows:
- Dates with missing backups
- Expected backup count
- Actual backup count
- Gap duration
- Reasons (if known)

## Searching

Full-text search across backup metadata.

```bash
# Search by database name
dbbackup catalog search --database myapp

# Search by date
dbbackup catalog search --after 2026-01-01 --before 2026-01-31

# Search by size range (GB)
dbbackup catalog search --min-size 0.5 --max-size 5.0

# Search by backup type
dbbackup catalog search --backup-type incremental

# Search by encryption status
dbbackup catalog search --encrypted

# Search by verification status
dbbackup catalog search --verified

# Combine filters
dbbackup catalog search --database myapp --encrypted --after 2026-01-01
```

## Backup Details

```bash
# Show full details for a specific backup
dbbackup catalog info mydb_2026-01-23.dump.gz

# Output includes:
#   - Filename and path
#   - Database name and version
#   - Backup timestamp
#   - Backup type (full/incremental/pitr_base)
#   - Size (compressed/uncompressed)
#   - Compression ratio
#   - Encryption (algorithm, key hash)
#   - Checksums (md5, sha256)
#   - Verification status and date
#   - Retention classification (daily/weekly/monthly)
#   - Comments/notes
```

## Retention Classification

The catalog classifies backups according to retention policies.

### GFS (Grandfather-Father-Son) Classification

```
Daily:   Last 7 backups
Weekly:  One backup per week for 4 weeks
Monthly: One backup per month for 12 months
```

Example:
```bash
dbbackup catalog list --show-retention

# Output shows:
# myapp_2026-01-23.dump.gz  daily    (retain 6 more days)
# myapp_2026-01-16.dump.gz  weekly   (retain 3 more weeks)
# myapp_2026-01-01.dump.gz  monthly  (retain 11 more months)
```

## Compliance Reports

Generate compliance reports based on catalog data.

```bash
# Backup compliance report
dbbackup catalog compliance-report

# Shows:
# - All backups compliant with retention policy
# - Gaps exceeding SLA
# - Failed backups
# - Unverified backups
# - Encryption status
```

## Configuration

Catalog settings in `.dbbackup.conf`:

```ini
[catalog]
# Enable catalog (default: true)
enabled = true

# Catalog database path (default: ~/.dbbackup/catalog.db)
db_path = /var/lib/dbbackup/catalog.db

# Retention days (default: 30)
retention_days = 30

# Minimum backups to keep (default: 5)
min_backups = 5

# Enable gap detection (default: true)
gap_detection = true

# Gap alert threshold (hours, default: 36)
gap_threshold_hours = 36

# Verify backups automatically (default: true)
auto_verify = true
```

## Maintenance

### Rebuild Catalog

Rebuild from scratch (useful if corrupted):

```bash
dbbackup catalog rebuild /mnt/backups/databases
```

### Export Catalog

Export to CSV for analysis in spreadsheet/BI tools:

```bash
dbbackup catalog export --format csv --output catalog.csv
```

Supported formats:
- csv (Excel compatible)
- json (structured data)
- html (browseable report)

### Cleanup Orphaned Entries

Remove catalog entries for deleted backups:

```bash
dbbackup catalog cleanup --orphaned

# Dry-run
dbbackup catalog cleanup --orphaned --dry-run
```

## Examples

### Find All Encrypted Backups from Last Week

```bash
dbbackup catalog search \
  --after "$(date -d '7 days ago' +%Y-%m-%d)" \
  --encrypted
```

### Generate Weekly Compliance Report

```bash
dbbackup catalog search \
  --after "$(date -d '7 days ago' +%Y-%m-%d)" \
  --show-retention \
  --verified
```

### Monitor Backup Size Growth

```bash
dbbackup catalog stats | grep "Average backup size"

# Track over time
for week in $(seq 1 4); do
  DATE=$(date -d "$((week*7)) days ago" +%Y-%m-%d)
  echo "Week of $DATE:"
  dbbackup catalog stats --after "$DATE" | grep "Average backup size"
done
```

## Troubleshooting

### Catalog Shows Wrong Count

Resync the catalog:
```bash
dbbackup catalog sync /mnt/backups/databases --force
```

### Gaps Detected But Backups Exist

Manual backups not in catalog - sync them:
```bash
dbbackup catalog sync /mnt/backups/databases
```

### Corruption Error

Rebuild catalog:
```bash
dbbackup catalog rebuild /mnt/backups/databases
```
