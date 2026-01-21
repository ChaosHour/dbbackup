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
sudo -u postgres psql -c "SHOW max_locks_per_transaction;" 2>/dev/null || psql -c "SHOW max_locks_per_transaction;"
sudo -u postgres psql -c "SHOW max_connections;" 2>/dev/null || psql -c "SHOW max_connections;"
sudo -u postgres psql -c "SHOW max_prepared_transactions;" 2>/dev/null || psql -c "SHOW max_prepared_transactions;"

echo
echo "📊 Calculated Lock Capacity:"
echo "────────────────────────────────────────────────────────────"

LOCKS=$(sudo -u postgres psql -t -c "SHOW max_locks_per_transaction;" 2>/dev/null | xargs)
CONNS=$(sudo -u postgres psql -t -c "SHOW max_connections;" 2>/dev/null | xargs)
PREPARED=$(sudo -u postgres psql -t -c "SHOW max_prepared_transactions;" 2>/dev/null | xargs)

if [ -z "$LOCKS" ]; then
    LOCKS=$(psql -t -c "SHOW max_locks_per_transaction;" | xargs)
    CONNS=$(psql -t -c "SHOW max_connections;" | xargs)
    PREPARED=$(psql -t -c "SHOW max_prepared_transactions;" | xargs)
fi

CAPACITY=$((LOCKS * (CONNS + PREPARED)))

echo "  max_locks_per_transaction: $LOCKS"
echo "  max_connections: $CONNS"
echo "  max_prepared_transactions: $PREPARED"
echo
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
