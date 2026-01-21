#!/bin/bash
#
# Fix PostgreSQL Lock Table Exhaustion
# Increases max_locks_per_transaction to handle large database restores
#

set -e

echo "════════════════════════════════════════════════════════════"
echo "  PostgreSQL Lock Configuration Fix"
echo "════════════════════════════════════════════════════════════"
echo

# Check if running as postgres user or with sudo
if [ "$EUID" -ne 0 ] && [ "$(whoami)" != "postgres" ]; then
    echo "⚠️  This script should be run as:"
    echo "   sudo $0"
    echo "   or as the postgres user"
    echo
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Detect PostgreSQL version and config
PSQL=$(command -v psql || echo "")
if [ -z "$PSQL" ]; then
    echo "❌ psql not found in PATH"
    exit 1
fi

echo "📊 Current PostgreSQL Configuration:"
echo "────────────────────────────────────────────────────────────"
sudo -u postgres psql -c "SHOW max_locks_per_transaction;" 2>/dev/null || psql -c "SHOW max_locks_per_transaction;" || echo "Unable to query current value"
sudo -u postgres psql -c "SHOW max_connections;" 2>/dev/null || psql -c "SHOW max_connections;" || echo "Unable to query current value"
sudo -u postgres psql -c "SHOW work_mem;" 2>/dev/null || psql -c "SHOW work_mem;" || echo "Unable to query current value"
sudo -u postgres psql -c "SHOW maintenance_work_mem;" 2>/dev/null || psql -c "SHOW maintenance_work_mem;" || echo "Unable to query current value"
echo

# Recommended values
RECOMMENDED_LOCKS=4096
RECOMMENDED_WORK_MEM="256MB"
RECOMMENDED_MAINTENANCE_WORK_MEM="4GB"

echo "🔧 Applying Fixes:"
echo "────────────────────────────────────────────────────────────"
echo "1. Setting max_locks_per_transaction = $RECOMMENDED_LOCKS"
echo "2. Setting work_mem = $RECOMMENDED_WORK_MEM (improves query performance)"
echo "3. Setting maintenance_work_mem = $RECOMMENDED_MAINTENANCE_WORK_MEM (speeds up restore/vacuum)"
echo

# Apply the settings
SUCCESS=0

# Fix 1: max_locks_per_transaction
if sudo -u postgres psql -c "ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;" 2>/dev/null; then
    echo "✅ max_locks_per_transaction updated successfully"
    SUCCESS=$((SUCCESS + 1))
elif psql -c "ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;" 2>/dev/null; then
    echo "✅ max_locks_per_transaction updated successfully"
    SUCCESS=$((SUCCESS + 1))
else
    echo "❌ Failed to update max_locks_per_transaction"
fi

# Fix 2: work_mem
if sudo -u postgres psql -c "ALTER SYSTEM SET work_mem = '$RECOMMENDED_WORK_MEM';" 2>/dev/null; then
    echo "✅ work_mem updated successfully"
    SUCCESS=$((SUCCESS + 1))
elif psql -c "ALTER SYSTEM SET work_mem = '$RECOMMENDED_WORK_MEM';" 2>/dev/null; then
    echo "✅ work_mem updated successfully"
    SUCCESS=$((SUCCESS + 1))
else
    echo "❌ Failed to update work_mem"
fi

# Fix 3: maintenance_work_mem
if sudo -u postgres psql -c "ALTER SYSTEM SET maintenance_work_mem = '$RECOMMENDED_MAINTENANCE_WORK_MEM';" 2>/dev/null; then
    echo "✅ maintenance_work_mem updated successfully"
    SUCCESS=$((SUCCESS + 1))
elif psql -c "ALTER SYSTEM SET maintenance_work_mem = '$RECOMMENDED_MAINTENANCE_WORK_MEM';" 2>/dev/null; then
    echo "✅ maintenance_work_mem updated successfully"
    SUCCESS=$((SUCCESS + 1))
else
    echo "❌ Failed to update maintenance_work_mem"
fi

if [ $SUCCESS -eq 0 ]; then
    echo
    echo "❌ All configuration updates failed"
    echo
    echo "Manual steps:"
    echo "1. Connect to PostgreSQL as superuser:"
    echo "   sudo -u postgres psql"
    echo
    echo "2. Run these commands:"
    echo "   ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;"
    echo "   ALTER SYSTEM SET work_mem = '$RECOMMENDED_WORK_MEM';"
    echo "   ALTER SYSTEM SET maintenance_work_mem = '$RECOMMENDED_MAINTENANCE_WORK_MEM';"
    echo
    exit 1
fi

echo
echo "✅ Applied $SUCCESS out of 3 configuration changes"

echo
echo "⚠️  IMPORTANT: PostgreSQL restart required!"
echo "────────────────────────────────────────────────────────────"
echo
echo "Restart PostgreSQL using one of these commands:"
echo
echo "  • systemd:     sudo systemctl restart postgresql"
echo "  • pg_ctl:      sudo -u postgres pg_ctl restart -D /var/lib/postgresql/data"
echo "  • service:     sudo service postgresql restart"
echo
echo "📊 Expected capacity after restart:"
echo "────────────────────────────────────────────────────────────"
echo "  Lock capacity: max_locks_per_transaction × (max_connections + max_prepared)"
echo "                 = $RECOMMENDED_LOCKS × (connections + prepared)"
echo
echo "  Work memory:   $RECOMMENDED_WORK_MEM per query operation"
echo "  Maintenance:   $RECOMMENDED_MAINTENANCE_WORK_MEM for restore/vacuum/index"
echo
echo "After restarting, verify with:"
echo "  psql -c 'SHOW max_locks_per_transaction;'"
echo "  psql -c 'SHOW work_mem;'"
echo "  psql -c 'SHOW maintenance_work_mem;'"
echo
echo "💡 Benefits:"
echo "  ✓ Prevents 'out of shared memory' errors during restore"
echo "  ✓ Reduces temp file usage (better performance)"
echo "  ✓ Faster restore, vacuum, and index operations"
echo
echo "🔍 For comprehensive diagnostics, run:"
echo "  ./diagnose_postgres_memory.sh"
echo
echo "════════════════════════════════════════════════════════════"
