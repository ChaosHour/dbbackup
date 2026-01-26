#!/bin/bash
# Health Check Script for dbbackup
# Returns exit codes for monitoring systems:
#   0 = OK (backup within RPO)
#   1 = WARNING (backup older than warning threshold)
#   2 = CRITICAL (backup older than critical threshold or missing)
#
# Usage: health-check.sh [backup-dir] [warning-hours] [critical-hours]

set -euo pipefail

BACKUP_DIR="${1:-/var/backups/databases}"
WARNING_HOURS="${2:-24}"
CRITICAL_HOURS="${3:-48}"

# Convert to seconds
WARNING_SECONDS=$((WARNING_HOURS * 3600))
CRITICAL_SECONDS=$((CRITICAL_HOURS * 3600))

echo "dbbackup Health Check"
echo "====================="
echo "Backup directory: $BACKUP_DIR"
echo "Warning threshold: ${WARNING_HOURS}h"
echo "Critical threshold: ${CRITICAL_HOURS}h"
echo ""

# Check if backup directory exists
if [[ ! -d "$BACKUP_DIR" ]]; then
    echo "CRITICAL: Backup directory does not exist"
    exit 2
fi

# Find most recent backup file
LATEST_BACKUP=$(find "$BACKUP_DIR" -type f \( -name "*.dump" -o -name "*.dump.gz" -o -name "*.sql" -o -name "*.sql.gz" -o -name "*.tar.gz" \) -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1)

if [[ -z "$LATEST_BACKUP" ]]; then
    echo "CRITICAL: No backup files found in $BACKUP_DIR"
    exit 2
fi

# Extract timestamp and path
BACKUP_TIMESTAMP=$(echo "$LATEST_BACKUP" | cut -d' ' -f1 | cut -d'.' -f1)
BACKUP_PATH=$(echo "$LATEST_BACKUP" | cut -d' ' -f2-)
BACKUP_NAME=$(basename "$BACKUP_PATH")

# Calculate age
NOW=$(date +%s)
AGE_SECONDS=$((NOW - BACKUP_TIMESTAMP))
AGE_HOURS=$((AGE_SECONDS / 3600))
AGE_DAYS=$((AGE_HOURS / 24))

# Format age string
if [[ $AGE_DAYS -gt 0 ]]; then
    AGE_STR="${AGE_DAYS}d $((AGE_HOURS % 24))h"
else
    AGE_STR="${AGE_HOURS}h $((AGE_SECONDS % 3600 / 60))m"
fi

# Get backup size
BACKUP_SIZE=$(du -h "$BACKUP_PATH" 2>/dev/null | cut -f1)

echo "Latest backup:"
echo "  File: $BACKUP_NAME"
echo "  Size: $BACKUP_SIZE"
echo "  Age:  $AGE_STR"
echo ""

# Verify backup integrity if dbbackup is available
if command -v dbbackup &> /dev/null; then
    echo "Verifying backup integrity..."
    if dbbackup verify "$BACKUP_PATH" --quiet 2>/dev/null; then
        echo "  ✓ Backup integrity verified"
    else
        echo "  ✗ Backup verification failed"
        echo ""
        echo "CRITICAL: Latest backup is corrupted"
        exit 2
    fi
    echo ""
fi

# Check thresholds
if [[ $AGE_SECONDS -ge $CRITICAL_SECONDS ]]; then
    echo "CRITICAL: Last backup is ${AGE_STR} old (threshold: ${CRITICAL_HOURS}h)"
    exit 2
elif [[ $AGE_SECONDS -ge $WARNING_SECONDS ]]; then
    echo "WARNING: Last backup is ${AGE_STR} old (threshold: ${WARNING_HOURS}h)"
    exit 1
else
    echo "OK: Last backup is ${AGE_STR} old"
    exit 0
fi
