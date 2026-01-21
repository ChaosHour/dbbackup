#!/bin/bash
#
# Verify PostgreSQL Lock Configuration
#

echo "════════════════════════════════════════════════════════════"
echo "  PostgreSQL Lock Configuration Verification"
echo "════════════════════════════════════════════════════════════"
echo

echo "📊 Current Running Configuration:"
echo "────────────────────────────────────────────────────────────"

# Get values without timing info - strip everything after first space
LOCKS=$(sudo -u postgres psql -t -A -q -c "SHOW max_locks_per_transaction;" 2>/dev/null | awk '{print $1}' | xargs)
CONNS=$(sudo -u postgres psql -t -A -q -c "SHOW max_connections;" 2>/dev/null | awk '{print $1}' | xargs)
PREPARED=$(sudo -u postgres psql -t -A -q -c "SHOW max_prepared_transactions;" 2>/dev/null | awk '{print $1}' | xargs)

if [ -z "$LOCKS" ]; then
    LOCKS=$(psql -t -A -q -c "SHOW max_locks_per_transaction;" 2>/dev/null | awk '{print $1}' | xargs)
    CONNS=$(psql -t -A -q -c "SHOW max_connections;" 2>/dev/null | awk '{print $1}' | xargs)
    PREPARED=$(psql -t -A -q -c "SHOW max_prepared_transactions;" 2>/dev/null | awk '{print $1}' | xargs)
fi

echo "  max_locks_per_transaction: $LOCKS"
echo "  max_connections: $CONNS"
echo "  max_prepared_transactions: $PREPARED"

echo
echo "📊 Calculated Lock Capacity:"
echo "────────────────────────────────────────────────────────────"

# Calculate capacity (protect against empty values)
if [ -n "$LOCKS" ] && [ -n "$CONNS" ] && [ -n "$PREPARED" ]; then
    CAPACITY=$((LOCKS * (CONNS + PREPARED)))
else
    echo "❌ ERROR: Could not retrieve PostgreSQL settings"
    exit 1
fi

echo "  Total Lock Capacity: $CAPACITY locks"
echo "  Formula: $LOCKS × ($CONNS + $PREPARED) = $CAPACITY"
echo

if [ "$LOCKS" -lt 4096 ]; then
    echo "❌ PROBLEM: max_locks_per_transaction is only $LOCKS (should be 4096+)"
    echo
    echo "FIX:"
    echo "  sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 8192;\""
    echo "  sudo systemctl restart postgresql"
elif [ "$CAPACITY" -lt 400000 ]; then
    echo "⚠️  WARNING: Total capacity is only $CAPACITY locks"
    echo "   For very large databases with BLOBs, you may need MORE"
    echo
    echo "   Try increasing to 8192:"
    echo "   sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 8192;\""
    echo "   sudo systemctl restart postgresql"
else
    echo "✅ Lock capacity looks good ($CAPACITY locks)"
fi

echo
echo "════════════════════════════════════════════════════════════"
