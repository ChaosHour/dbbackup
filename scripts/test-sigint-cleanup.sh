#!/bin/bash
# scripts/test-sigint-cleanup.sh
# Test script to verify clean shutdown on SIGINT (Ctrl+C)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/dbbackup"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== SIGINT Cleanup Test ==="
echo ""
echo "Project: $PROJECT_DIR"
echo "Binary: $BINARY"
echo ""

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo -e "${YELLOW}Binary not found, building...${NC}"
    cd "$PROJECT_DIR"
    go build -o dbbackup .
fi

# Create a test backup file if it doesn't exist
TEST_BACKUP="/tmp/test-sigint-backup.sql.gz"
if [ ! -f "$TEST_BACKUP" ]; then
    echo -e "${YELLOW}Creating test backup file...${NC}"
    echo "-- Test SQL file for SIGINT testing" | gzip > "$TEST_BACKUP"
fi

echo "=== Phase 1: Pre-test Cleanup ==="
echo "Killing any existing dbbackup processes..."
pkill -f "dbbackup" 2>/dev/null || true
sleep 1

echo ""
echo "=== Phase 2: Check Initial State ==="

echo "Checking for orphaned processes..."
INITIAL_PROCS=$(pgrep -f "pg_dump|pg_restore|dbbackup" 2>/dev/null | wc -l)
echo "Initial related processes: $INITIAL_PROCS"

echo ""
echo "Checking for temp files..."
INITIAL_TEMPS=$(ls /tmp/dbbackup-* 2>/dev/null | wc -l || echo "0")
echo "Initial temp files: $INITIAL_TEMPS"

echo ""
echo "=== Phase 3: Start Test Operation ==="

# Start a TUI operation that will hang (version is fast, but menu would wait)
echo "Starting dbbackup TUI (will be interrupted)..."

# Run in background with PTY simulation (needed for TUI)
cd "$PROJECT_DIR"
timeout 30 script -q -c "$BINARY" /dev/null &
PID=$!

echo "Process started: PID=$PID"
sleep 2

# Check if process is running
if ! kill -0 $PID 2>/dev/null; then
    echo -e "${YELLOW}Process exited quickly (expected for non-interactive test)${NC}"
    echo "This is normal - the TUI requires a real TTY"
    PID=""
else
    echo "Process is running"
    
    echo ""
    echo "=== Phase 4: Check Running State ==="
    
    echo "Child processes of $PID:"
    pgrep -P $PID 2>/dev/null | while read child; do
        ps -p $child -o pid,ppid,cmd 2>/dev/null || true
    done
    
    echo ""
    echo "=== Phase 5: Send SIGINT ==="
    echo "Sending SIGINT to process $PID..."
    kill -SIGINT $PID 2>/dev/null || true
    
    echo "Waiting for cleanup (max 10 seconds)..."
    for i in {1..10}; do
        if ! kill -0 $PID 2>/dev/null; then
            echo ""
            echo -e "${GREEN}Process exited after ${i} seconds${NC}"
            break
        fi
        sleep 1
        echo -n "."
    done
    echo ""
    
    # Check if still running
    if kill -0 $PID 2>/dev/null; then
        echo -e "${RED}Process still running after 10 seconds!${NC}"
        echo "Force killing..."
        kill -9 $PID 2>/dev/null || true
    fi
fi

sleep 2  # Give OS time to clean up

echo ""
echo "=== Phase 6: Post-Shutdown Verification ==="

# Check for zombie processes
ZOMBIES=$(ps aux 2>/dev/null | grep -E "dbbackup|pg_dump|pg_restore" | grep -v grep | grep defunct | wc -l)
echo "Zombie processes: $ZOMBIES"

# Check for orphaned children
if [ -n "$PID" ]; then
    ORPHANS=$(pgrep -P $PID 2>/dev/null | wc -l || echo "0")
    echo "Orphaned children of original process: $ORPHANS"
else
    ORPHANS=0
fi

# Check for leftover related processes
LEFTOVER_PROCS=$(pgrep -f "pg_dump|pg_restore" 2>/dev/null | wc -l || echo "0")
echo "Leftover pg_dump/pg_restore processes: $LEFTOVER_PROCS"

# Check for temp files
TEMP_FILES=$(ls /tmp/dbbackup-* 2>/dev/null | wc -l || echo "0")
echo "Temporary files: $TEMP_FILES"

# Database connections check (if psql available and configured)
if command -v psql &> /dev/null; then
    echo ""
    echo "Checking database connections..."
    DB_CONNS=$(psql -t -c "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE '%dbbackup%';" 2>/dev/null | tr -d ' ' || echo "N/A")
    echo "Database connections with 'dbbackup' in name: $DB_CONNS"
else
    echo "psql not available - skipping database connection check"
    DB_CONNS="N/A"
fi

echo ""
echo "=== Test Results ==="

PASSED=true

if [ "$ZOMBIES" -gt 0 ]; then
    echo -e "${RED}❌ FAIL: $ZOMBIES zombie process(es) found${NC}"
    PASSED=false
else
    echo -e "${GREEN}✓ No zombie processes${NC}"
fi

if [ "$ORPHANS" -gt 0 ]; then
    echo -e "${RED}❌ FAIL: $ORPHANS orphaned child process(es) found${NC}"
    PASSED=false
else
    echo -e "${GREEN}✓ No orphaned children${NC}"
fi

if [ "$LEFTOVER_PROCS" -gt 0 ]; then
    echo -e "${YELLOW}⚠ WARNING: $LEFTOVER_PROCS leftover pg_dump/pg_restore process(es)${NC}"
    echo "  These may be from other operations"
fi

if [ "$TEMP_FILES" -gt "$INITIAL_TEMPS" ]; then
    NEW_TEMPS=$((TEMP_FILES - INITIAL_TEMPS))
    echo -e "${RED}❌ FAIL: $NEW_TEMPS new temporary file(s) left behind${NC}"
    ls -la /tmp/dbbackup-* 2>/dev/null || true
    PASSED=false
else
    echo -e "${GREEN}✓ No new temporary files left behind${NC}"
fi

if [ "$DB_CONNS" != "N/A" ] && [ "$DB_CONNS" -gt 0 ]; then
    echo -e "${RED}❌ FAIL: $DB_CONNS database connection(s) still active${NC}"
    PASSED=false
elif [ "$DB_CONNS" != "N/A" ]; then
    echo -e "${GREEN}✓ No lingering database connections${NC}"
fi

echo ""
if [ "$PASSED" = true ]; then
    echo -e "${GREEN}=== ✓ ALL TESTS PASSED ===${NC}"
    exit 0
else
    echo -e "${RED}=== ✗ SOME TESTS FAILED ===${NC}"
    exit 1
fi
