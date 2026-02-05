#!/bin/bash
# Test script to verify context cancellation works in TUI restore
# Run this BEFORE deploying to enterprise machine

set -e

echo "🧪 Testing TUI Cancellation Behavior"
echo "====================================="

# Create a test SQL file that simulates a large dump
TEST_SQL="/tmp/test_large_dump.sql"
echo "Creating test SQL file with 50000 statements..."

cat > "$TEST_SQL" << 'EOF'
-- Test pg_dumpall format
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
EOF

# Add 50000 simple statements to simulate a large dump
for i in $(seq 1 50000); do
    echo "SELECT $i; -- padding line to simulate large file" >> "$TEST_SQL"
done

echo "-- End of test dump" >> "$TEST_SQL"

echo "✅ Created $TEST_SQL ($(wc -l < "$TEST_SQL") lines)"

# Test 1: Verify parsing can be cancelled
echo ""
echo "Test 1: Parsing Cancellation (5 second timeout)"
echo "------------------------------------------------"
timeout 5 ./bin/dbbackup_linux_amd64 restore single "$TEST_SQL" --target testdb_noexist 2>&1 || {
    if [ $? -eq 124 ]; then
        echo "⚠️  TIMEOUT - parsing took longer than 5 seconds (potential hang)"
        echo "   This may indicate the fix is not working"
    else
        echo "✅ Command completed or failed normally (no hang)"
    fi
}

# Test 2: TUI interrupt test (requires manual verification)
echo ""
echo "Test 2: TUI Interrupt Test (MANUAL)"
echo "------------------------------------"
echo "Run this command and press Ctrl+C after 2-3 seconds:"
echo ""
echo "  ./bin/dbbackup_linux_amd64 restore single $TEST_SQL --target testdb"
echo ""
echo "Expected: Should exit cleanly within 1-2 seconds of Ctrl+C"
echo "Problem:  If it hangs for 30+ seconds, the fix didn't work"

# Cleanup
echo ""
echo "Cleanup: rm $TEST_SQL"
echo ""
echo "🏁 Test complete!"
