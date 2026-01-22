#!/bin/bash
#
# PostgreSQL Lock Configuration Check & Restore Guidance
#

echo "════════════════════════════════════════════════════════════"
echo "  PostgreSQL Lock Configuration & Restore Strategy"
echo "════════════════════════════════════════════════════════════"
echo

# Get values - extract ONLY digits, remove all non-numeric chars
LOCKS=$(sudo -u postgres psql --no-psqlrc -t -A -c "SHOW max_locks_per_transaction;" 2>/dev/null | tr -cd '0-9' | head -c 10)
CONNS=$(sudo -u postgres psql --no-psqlrc -t -A -c "SHOW max_connections;" 2>/dev/null | tr -cd '0-9' | head -c 10)
PREPARED=$(sudo -u postgres psql --no-psqlrc -t -A -c "SHOW max_prepared_transactions;" 2>/dev/null | tr -cd '0-9' | head -c 10)

if [ -z "$LOCKS" ]; then
    LOCKS=$(psql --no-psqlrc -t -A -c "SHOW max_locks_per_transaction;" 2>/dev/null | tr -cd '0-9' | head -c 10)
    CONNS=$(psql --no-psqlrc -t -A -c "SHOW max_connections;" 2>/dev/null | tr -cd '0-9' | head -c 10)
    PREPARED=$(psql --no-psqlrc -t -A -c "SHOW max_prepared_transactions;" 2>/dev/null | tr -cd '0-9' | head -c 10)
fi

if [ -z "$LOCKS" ] || [ -z "$CONNS" ]; then
    echo "❌ ERROR: Could not retrieve PostgreSQL settings"
    echo "   Ensure PostgreSQL is running and accessible"
    exit 1
fi

echo "📊 Current Configuration:"
echo "────────────────────────────────────────────────────────────"
echo "  max_locks_per_transaction: $LOCKS"
echo "  max_connections: $CONNS"
echo "  max_prepared_transactions: ${PREPARED:-0}"
echo

# Calculate capacity
PREPARED=${PREPARED:-0}
CAPACITY=$((LOCKS * (CONNS + PREPARED)))

echo "  Total Lock Capacity: $CAPACITY locks"
echo

# Determine status
if [ "$LOCKS" -lt 2048 ]; then
    STATUS="❌ CRITICAL"
    RECOMMENDATION="increase_locks"
elif [ "$LOCKS" -lt 4096 ]; then
    STATUS="⚠️  LOW"
    RECOMMENDATION="single_threaded"
else
    STATUS="✅ OK"
    RECOMMENDATION="single_threaded"
fi

echo "Status: $STATUS (locks=$LOCKS, capacity=$CAPACITY)"
echo
echo "════════════════════════════════════════════════════════════"
echo "  🎯 RECOMMENDED RESTORE COMMAND"
echo "════════════════════════════════════════════════════════════"
echo

if [ "$RECOMMENDATION" = "increase_locks" ]; then
    echo "CRITICAL: Locks too low. Increase first, THEN use single-threaded:"
    echo
    echo "1. Increase locks (requires PostgreSQL restart):"
    echo "   sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 4096;\""
    echo "   sudo systemctl restart postgresql"
    echo
    echo "2. Run restore with single-threaded mode:"
    echo "   dbbackup restore cluster <backup-file> \\"
    echo "     --profile conservative \\"
    echo "     --parallel-dbs 1 \\"
    echo "     --jobs 1 \\"
    echo "     --confirm"
else
    echo "✅ Use default CONSERVATIVE profile (single-threaded, prevents lock issues):"
    echo
    echo "   dbbackup restore cluster <backup-file> --confirm"
    echo
    echo "   (Default profile is now 'conservative' = single-threaded)"
    echo
    echo "   For faster restore (if locks are sufficient):"
    echo "   dbbackup restore cluster <backup-file> --profile balanced --confirm"
    echo "   dbbackup restore cluster <backup-file> --profile aggressive --confirm"
fi

echo
echo "════════════════════════════════════════════════════════════"
echo "  ℹ️  WHY SINGLE-THREADED?"
echo "════════════════════════════════════════════════════════════"
echo
echo "  Parallel restore with large databases (especially with BLOBs)"
echo "  can exhaust locks EVEN with high max_locks_per_transaction."
echo
echo "  --jobs 1        = Single-threaded pg_restore (minimal locks)"
echo "  --parallel-dbs 1 = Restore one database at a time"
echo
echo "  Trade-off: Slower restore, but GUARANTEED completion."
echo
echo "════════════════════════════════════════════════════════════"
