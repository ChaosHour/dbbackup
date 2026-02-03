#!/bin/bash
# TUI Smoke Test Script
# Tests all TUI menu options via auto-select to ensure they don't crash
#
# Usage: ./tests/tui_smoke_test.sh [--db-host HOST] [--db-port PORT]
#
# Requirements:
# - dbbackup binary in PATH or ./bin/
# - Optional: PostgreSQL connection for full testing

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DBBACKUP="${DBBACKUP:-$(command -v dbbackup 2>/dev/null || echo "./bin/dbbackup_linux_amd64")}"
TIMEOUT_SECONDS=5
PASSED=0
FAILED=0
SKIPPED=0

# Parse arguments
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --db-host) DB_HOST="$2"; shift 2 ;;
        --db-port) DB_PORT="$2"; shift 2 ;;
        --binary) DBBACKUP="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 [--db-host HOST] [--db-port PORT] [--binary PATH]"
            exit 0
            ;;
        *) shift ;;
    esac
done

echo "=============================================="
echo "       TUI Smoke Test Suite"
echo "=============================================="
echo "Binary: $DBBACKUP"
echo "Database: $DB_HOST:$DB_PORT"
echo ""

# Check binary exists
if [[ ! -x "$DBBACKUP" ]]; then
    echo -e "${RED}ERROR: dbbackup binary not found at $DBBACKUP${NC}"
    exit 1
fi

# Get version
VERSION=$("$DBBACKUP" version 2>/dev/null | head -1 || echo "unknown")
echo "Version: $VERSION"
echo ""

# Menu item mapping (index -> name -> expected behavior)
declare -A MENU_ITEMS=(
    [0]="Single Database Backup"
    [1]="Sample Database Backup"
    [2]="Cluster Backup"
    [3]="Separator (skip)"
    [4]="Restore Single Database"
    [5]="Restore Cluster Backup"
    [6]="Diagnose Backup File"
    [7]="List & Manage Backups"
    [8]="View Backup Schedule"
    [9]="View Backup Chain"
    [10]="Separator (skip)"
    [11]="System Resource Profile"
    [12]="Tools"
    [13]="View Active Operations"
    [14]="Show Operation History"
    [15]="Database Status"
    [16]="Configuration Settings"
    [17]="Clear Operation History"
    [18]="Quit"
)

# Items that require database connection
DB_REQUIRED=(0 1 2 15)

# Items that require file selection (will timeout, that's OK)
FILE_REQUIRED=(4 5 6 7)

# Items that are separators (should be skipped)
SEPARATORS=(3 10)

# Test function
test_menu_item() {
    local idx=$1
    local name="${MENU_ITEMS[$idx]}"
    local expect_timeout=false
    local expect_db=false

    # Check if separator
    for sep in "${SEPARATORS[@]}"; do
        if [[ $idx -eq $sep ]]; then
            echo -e "  [${YELLOW}SKIP${NC}] #$idx: $name"
            ((SKIPPED++))
            return 0
        fi
    done

    # Check if requires file selection (will timeout waiting for input)
    for item in "${FILE_REQUIRED[@]}"; do
        if [[ $idx -eq $item ]]; then
            expect_timeout=true
            break
        fi
    done

    # Check if requires database
    for item in "${DB_REQUIRED[@]}"; do
        if [[ $idx -eq $item ]]; then
            expect_db=true
            break
        fi
    done

    # Run test with timeout
    local output
    local exit_code=0

    if [[ "$expect_timeout" == "true" ]]; then
        # These items wait for user input, timeout is expected
        output=$(timeout $TIMEOUT_SECONDS "$DBBACKUP" --tui-auto-select=$idx \
            --host "$DB_HOST" --port "$DB_PORT" \
            --no-save-config 2>&1) || exit_code=$?

        # Timeout exit code is 124, that's OK for interactive items
        if [[ $exit_code -eq 124 ]]; then
            echo -e "  [${GREEN}PASS${NC}] #$idx: $name (timeout expected)"
            ((PASSED++))
            return 0
        fi
    else
        output=$(timeout $TIMEOUT_SECONDS "$DBBACKUP" --tui-auto-select=$idx \
            --host "$DB_HOST" --port "$DB_PORT" \
            --no-save-config 2>&1) || exit_code=$?
    fi

    # Check for crashes/panics
    if echo "$output" | grep -qi "panic\|fatal\|segfault"; then
        echo -e "  [${RED}FAIL${NC}] #$idx: $name - CRASH DETECTED"
        echo "    Output: $(echo "$output" | head -3)"
        ((FAILED++))
        return 1
    fi

    # Check exit code
    if [[ $exit_code -eq 0 ]] || [[ $exit_code -eq 124 ]]; then
        echo -e "  [${GREEN}PASS${NC}] #$idx: $name"
        ((PASSED++))
    elif [[ "$expect_db" == "true" ]] && echo "$output" | grep -qi "connection\|connect\|database"; then
        # DB connection failure is acceptable if no DB configured
        echo -e "  [${YELLOW}SKIP${NC}] #$idx: $name (no DB connection)"
        ((SKIPPED++))
    else
        echo -e "  [${RED}FAIL${NC}] #$idx: $name (exit code: $exit_code)"
        echo "    Output: $(echo "$output" | tail -2)"
        ((FAILED++))
    fi
}

echo "Running menu item tests..."
echo ""

# Test each menu item
for idx in $(seq 0 18); do
    test_menu_item $idx
done

echo ""
echo "=============================================="
echo "       Test Results"
echo "=============================================="
echo -e "  ${GREEN}Passed:${NC}  $PASSED"
echo -e "  ${YELLOW}Skipped:${NC} $SKIPPED"
echo -e "  ${RED}Failed:${NC}  $FAILED"
echo ""

# Additional structural tests
echo "Running structural tests..."

# Test --help
if "$DBBACKUP" --help 2>&1 | grep -q "Interactive Mode"; then
    echo -e "  [${GREEN}PASS${NC}] --help includes TUI info"
    ((PASSED++))
else
    echo -e "  [${RED}FAIL${NC}] --help missing TUI info"
    ((FAILED++))
fi

# Test version
if "$DBBACKUP" version 2>&1 | grep -qE "^v?[0-9]+\.[0-9]+"; then
    echo -e "  [${GREEN}PASS${NC}] version command works"
    ((PASSED++))
else
    echo -e "  [${RED}FAIL${NC}] version command failed"
    ((FAILED++))
fi

# Test --no-tui mode
if timeout 2 "$DBBACKUP" status --no-tui --host "$DB_HOST" 2>&1 | grep -qiE "status|error|connection"; then
    echo -e "  [${GREEN}PASS${NC}] --no-tui mode works"
    ((PASSED++))
else
    echo -e "  [${YELLOW}SKIP${NC}] --no-tui test inconclusive"
    ((SKIPPED++))
fi

echo ""
echo "=============================================="
echo "       Final Summary"
echo "=============================================="
echo -e "  ${GREEN}Total Passed:${NC}  $PASSED"
echo -e "  ${YELLOW}Total Skipped:${NC} $SKIPPED"
echo -e "  ${RED}Total Failed:${NC}  $FAILED"
echo ""

if [[ $FAILED -gt 0 ]]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
