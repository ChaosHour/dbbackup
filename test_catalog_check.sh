#!/bin/bash
# Test catalog check on the dump directory
# Shows health of all backup archives and their meta files

set -e

DUMP_DIR="/mnt/HC_Volume_104577460/dump/"
BINARY="/root/dbbackup/bin/dbbackup_linux_amd64"

# Run catalog check with verbose output
echo "=== Catalog Check Test ==="
echo "Directory: $DUMP_DIR"
echo ""
"$BINARY" catalog check "$DUMP_DIR" --verbose
EXIT=$?
echo ""
echo "Exit code: $EXIT"
