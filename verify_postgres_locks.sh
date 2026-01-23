#!/bin/bash
#
# PostgreSQL Lock Configuration Check & Restore Guidance
#

echo "════════════════════════════════════════════════════════════"
echo "  PostgreSQL Lock Configuration & Restore Strategy"
echo "════════════════════════════════════════════════════════════"
echo

set -euo pipefail
IFS=$'\n\t'

# Get values - robustly probe psql (try sudo then local), return only digits (max 10 chars)
get_pg_setting() {
    local sql="$1" out
    # try sudo postgres first, then without sudo; swallow errors
    out=$(sudo -u postgres psql --no-psqlrc -t -A -c "$sql" 2>/dev/null || \
          psql --no-psqlrc -t -A -c "$sql" 2>/dev/null || true)
    # keep digits only, limit length (protect against weird output)
    out=${out//[^0-9]/}
    printf '%s' "${out:0:10}"
}

LOCKS=$(get_pg_setting "SHOW max_locks_per_transaction;")
CONNS=$(get_pg_setting "SHOW max_connections;")
PREPARED=$(get_pg_setting "SHOW max_prepared_transactions;")

# Basic validation
PREPARED=${PREPARED:-0}
if ! [[ $LOCKS =~ ^[0-9]+$ ]] || ! [[ $CONNS =~ ^[0-9]+$ ]]; then
    echo "❌ ERROR: Could not retrieve PostgreSQL settings"
    echo "   Ensure PostgreSQL is running and accessible and that \`psql\` succeeds"
    exit 1
fi

# warn if prepared transactions looks suspiciously large
if [[ $PREPARED =~ ^[0-9]+$ ]] && (( PREPARED > CONNS )); then
    echo "⚠️  NOTE: max_prepared_transactions ($PREPARED) is greater than max_connections ($CONNS) — this is unusual."
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

# Determine status - MASSIVE databases need extreme settings
if [ "$LOCKS" -lt 2048 ]; then
    STATUS="❌ CRITICAL - TOO LOW"
    RECOMMENDATION="increase_to_65536"
elif [ "$LOCKS" -lt 8192 ]; then
    STATUS="⚠️  INSUFFICIENT for large DBs"
    RECOMMENDATION="increase_to_65536"
elif [ "$LOCKS" -lt 65536 ]; then
    STATUS="⚠️  MAY BE INSUFFICIENT for massive DBs with BLOBs"
    RECOMMENDATION="single_threaded_or_increase"
else
    STATUS="✅ SUFFICIENT (but use single-threaded for safety)"
    RECOMMENDATION="single_threaded"
fi

echo "Status: $STATUS (locks=$LOCKS, capacity=$CAPACITY)"
echo
echo "════════════════════════════════════════════════════════════"
echo "  🎯 BULLETPROOF RESTORE STRATEGY"
echo "════════════════════════════════════════════════════════════"
echo

if [ "$RECOMMENDATION" = "increase_to_65536" ]; then
    echo "🔴 CRITICAL: Your lock setting is TOO LOW for large databases!"
    echo
    echo "MANDATORY STEPS:"
    echo "────────────────────────────────────────────────────────────"
    echo "1. Increase locks to 65536 (or higher for massive DBs):"
    echo
    echo "   sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 65536;\""
    echo "   sudo systemctl restart postgresql"
    echo
    echo "2. Verify the change took effect:"
    echo "   sudo -u postgres psql -c 'SHOW max_locks_per_transaction;'"
    echo
    echo "3. Run restore in SINGLE-THREADED mode (MANDATORY):"
    echo
    echo "   dbbackup restore cluster <backup-file> \\"
    echo "     --jobs 1 \\"
    echo "     --parallel-dbs 1 \\"
    echo "     --confirm"
    echo
    echo "⚠️  WARNING: Even with 65536 locks, parallel restore WILL FAIL."
    echo "   You MUST use --jobs 1 --parallel-dbs 1 for large databases."
    echo
elif [ "$RECOMMENDATION" = "single_threaded_or_increase" ]; then
    echo "⚠️  Your database is MASSIVE. Choose ONE option:"
    echo
    echo "════════════════════════════════════════════════════════════"
    echo "OPTION A: BULLETPROOF (slower but guaranteed) - RECOMMENDED"
    echo "════════════════════════════════════════════════════════════"
    echo
    echo "   Use current locks ($LOCKS) with STRICT single-threaded mode:"
    echo
    echo "   dbbackup restore cluster <backup-file> \\"
    echo "     --jobs 1 \\"
    echo "     --parallel-dbs 1 \\"
    echo "     --confirm"
    echo
    echo "   This WILL work but takes longer (4-8+ hours typical)."
    echo
    echo "════════════════════════════════════════════════════════════"
    echo "OPTION B: Increase locks + single-threaded (still safer)"
    echo "════════════════════════════════════════════════════════════"
    echo
    echo "   1. Increase to maximum safe value:"
    echo "      sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 65536;\""
    echo "      sudo systemctl restart postgresql"
    echo
    echo "   2. Still use single-threaded (parallel will exhaust locks):"
    echo "      dbbackup restore cluster <backup-file> \\"
    echo "        --jobs 1 \\"
    echo "        --parallel-dbs 1 \\"
    echo "        --confirm"
    echo
else
    echo "✅ RECOMMENDED: Always use single-threaded for large databases"
    echo
    echo "   dbbackup restore cluster <backup-file> \\"
    echo "     --jobs 1 \\"
    echo "     --parallel-dbs 1 \\"
    echo "     --confirm"
    echo
    echo "   Even with high lock settings, single-threaded is safest."
fi

echo
echo "════════════════════════════════════════════════════════════"
echo "  ⚠️  CRITICAL: WHY YOU KEEP FAILING"
echo "════════════════════════════════════════════════════════════"
echo
echo "  Your database has BLOBs or massive tables that require"
echo "  65536+ locks. Parallel restore is MATHEMATICALLY IMPOSSIBLE."
echo
echo "  Each pg_restore worker can lock THOUSANDS of objects."
echo "  With --jobs 4, you need 4× the locks."
echo "  With --parallel-dbs 2, you need 2× MORE locks."
echo
echo "  SOLUTION: Force single-threaded mode"
echo "  ────────────────────────────────────────────────────────────"
echo "  --jobs 1         = ONE pg_restore worker (minimal locks)"
echo "  --parallel-dbs 1 = ONE database at a time (no lock stacking)"
echo
echo "  Trade-off: 4-8 hours restore time vs GUARANTEED success."
echo
echo "  YOUR FAILURES = trying to use parallel with insufficient locks."
echo
echo "════════════════════════════════════════════════════════════"
echo "  📋 EXACT COMMAND TO RUN NOW"
echo "════════════════════════════════════════════════════════════"
echo
echo "  sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 65536;\""
echo "  sudo systemctl restart postgresql"
echo
echo "  dbbackup restore cluster cluster_20260113_091134.tar.gz \\"
echo "    --jobs 1 \\"
echo "    --parallel-dbs 1 \\"
echo "    --confirm"
echo
echo "════════════════════════════════════════════════════════════"
