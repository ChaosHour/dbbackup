#!/bin/bash
# Backup Rotation Script for dbbackup
# Implements GFS (Grandfather-Father-Son) retention policy
#
# Usage: backup-rotation.sh /path/to/backups [--dry-run]

set -euo pipefail

BACKUP_DIR="${1:-/var/backups/databases}"
DRY_RUN="${2:-}"

# GFS Configuration
DAILY_KEEP=7
WEEKLY_KEEP=4
MONTHLY_KEEP=12
YEARLY_KEEP=3

# Minimum backups to always keep
MIN_BACKUPS=5

echo "═══════════════════════════════════════════════════════════════"
echo "  dbbackup GFS Rotation"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "  Backup Directory: $BACKUP_DIR"
echo "  Retention Policy:"
echo "    Daily:   $DAILY_KEEP backups"
echo "    Weekly:  $WEEKLY_KEEP backups"  
echo "    Monthly: $MONTHLY_KEEP backups"
echo "    Yearly:  $YEARLY_KEEP backups"
echo ""

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "  [DRY RUN MODE - No files will be deleted]"
    echo ""
fi

# Check if dbbackup is available
if ! command -v dbbackup &> /dev/null; then
    echo "ERROR: dbbackup command not found"
    exit 1
fi

# Build cleanup command
CLEANUP_CMD="dbbackup cleanup $BACKUP_DIR \
    --gfs \
    --gfs-daily $DAILY_KEEP \
    --gfs-weekly $WEEKLY_KEEP \
    --gfs-monthly $MONTHLY_KEEP \
    --gfs-yearly $YEARLY_KEEP \
    --min-backups $MIN_BACKUPS"

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    CLEANUP_CMD="$CLEANUP_CMD --dry-run"
fi

echo "Running: $CLEANUP_CMD"
echo ""

$CLEANUP_CMD

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Rotation complete"
echo "═══════════════════════════════════════════════════════════════"
