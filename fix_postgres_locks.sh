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
echo

# Recommended value
RECOMMENDED_LOCKS=4096

echo "🔧 Applying Fix:"
echo "────────────────────────────────────────────────────────────"
echo "Setting max_locks_per_transaction = $RECOMMENDED_LOCKS"
echo

# Apply the setting
if sudo -u postgres psql -c "ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;" 2>/dev/null; then
    echo "✅ Configuration updated successfully"
elif psql -c "ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;" 2>/dev/null; then
    echo "✅ Configuration updated successfully"
else
    echo "❌ Failed to update configuration"
    echo
    echo "Manual steps:"
    echo "1. Connect to PostgreSQL as superuser:"
    echo "   sudo -u postgres psql"
    echo
    echo "2. Run this command:"
    echo "   ALTER SYSTEM SET max_locks_per_transaction = $RECOMMENDED_LOCKS;"
    echo
    exit 1
fi

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
echo "Lock capacity after restart will be:"
echo "  max_locks_per_transaction × (max_connections + max_prepared_transactions)"
echo "  = $RECOMMENDED_LOCKS × (connections + prepared)"
echo
echo "After restarting, verify with:"
echo "  psql -c 'SHOW max_locks_per_transaction;'"
echo
echo "🔍 For comprehensive diagnostics, run:"
echo "  ./diagnose_postgres_memory.sh"
echo
echo "════════════════════════════════════════════════════════════"
