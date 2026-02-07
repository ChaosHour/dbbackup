#!/bin/bash
# Test catalog generate on the 99GB cluster archive
# Should complete in SECONDS with the shell pipeline fix (tar tzf | head)

set -e

ARCHIVE="/mnt/HC_Volume_104577460/dump/cluster_20260205_201240.tar.gz"
BINARY="/root/dbbackup/bin/dbbackup_linux_amd64"
META="${ARCHIVE}.meta.json"

echo "=== Catalog Generate Test ==="
echo "Archive: $ARCHIVE"
echo "Size: $(du -h "$ARCHIVE" | cut -f1)"
echo ""

# Remove old meta
rm -f "$META"
echo "Cleared old meta file"
echo ""

# Run with timing
echo "Running catalog generate..."
echo "---"
time "$BINARY" catalog generate "$ARCHIVE" --verbose 2>&1
EXIT=$?
echo "---"
echo ""
echo "Exit code: $EXIT"

# Check result
if [ -f "$META" ]; then
    echo "Meta file created: $(ls -lh "$META" | awk '{print $5}')"
    echo ""
    echo "Contents:"
    cat "$META"
else
    echo "WARNING: No meta file created"
fi
