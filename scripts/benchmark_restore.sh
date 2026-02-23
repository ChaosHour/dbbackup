#!/bin/bash
# =============================================================================
# dbbackup Restore Performance Benchmark Script
# =============================================================================
# This script helps identify restore performance bottlenecks by comparing:
# 1. dbbackup restore with TUI
# 2. dbbackup restore without TUI (--no-tui --quiet)
# 3. Native pg_restore -j8 baseline
#
# Usage:
#   ./benchmark_restore.sh backup_file.dump.gz [target_database]
#
# Requirements:
#   - dbbackup binary in PATH or current directory
#   - PostgreSQL tools (pg_restore, psql)
#   - A backup file to test with
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
BACKUP_FILE="${1:-}"
TARGET_DB="${2:-benchmark_restore_test}"

if [ -z "$BACKUP_FILE" ]; then
    echo -e "${RED}Error: Backup file required${NC}"
    echo "Usage: $0 backup_file.dump.gz [target_database]"
    exit 1
fi

if [ ! -f "$BACKUP_FILE" ]; then
    echo -e "${RED}Error: Backup file not found: $BACKUP_FILE${NC}"
    exit 1
fi

# Find dbbackup binary
DBBACKUP=""
if command -v dbbackup &> /dev/null; then
    DBBACKUP="dbbackup"
elif [ -f "./dbbackup" ]; then
    DBBACKUP="./dbbackup"
elif [ -f "./bin/dbbackup_linux_amd64" ]; then
    DBBACKUP="./bin/dbbackup_linux_amd64"
else
    echo -e "${RED}Error: dbbackup binary not found${NC}"
    exit 1
fi

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}  dbbackup Restore Performance Benchmark${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""
echo -e "Backup file: ${GREEN}$BACKUP_FILE${NC}"
echo -e "Target database: ${GREEN}$TARGET_DB${NC}"
echo -e "dbbackup binary: ${GREEN}$DBBACKUP${NC}"
echo ""

# Get backup file size
BACKUP_SIZE=$(stat -c%s "$BACKUP_FILE" 2>/dev/null || stat -f%z "$BACKUP_FILE" 2>/dev/null)
BACKUP_SIZE_MB=$((BACKUP_SIZE / 1024 / 1024))
echo -e "Backup size: ${GREEN}${BACKUP_SIZE_MB} MB${NC}"
echo ""

# Function to drop test database
drop_test_db() {
    echo -e "${YELLOW}Dropping test database...${NC}"
    psql -c "DROP DATABASE IF EXISTS $TARGET_DB;" postgres 2>/dev/null || true
}

# Function to create test database
create_test_db() {
    echo -e "${YELLOW}Creating test database...${NC}"
    psql -c "CREATE DATABASE $TARGET_DB;" postgres 2>/dev/null || true
}

# Function to get PostgreSQL settings
get_pg_settings() {
    echo -e "\n${BLUE}=== PostgreSQL Configuration ===${NC}"
    psql -c "
        SELECT name, setting, unit 
        FROM pg_settings 
        WHERE name IN (
            'max_connections',
            'shared_buffers',
            'work_mem',
            'maintenance_work_mem',
            'max_wal_size',
            'max_locks_per_transaction',
            'synchronous_commit',
            'wal_level'
        )
        ORDER BY name;
    " postgres 2>/dev/null || echo "(Could not query settings)"
}

# Function to run benchmark test
run_benchmark() {
    local name="$1"
    local cmd="$2"
    
    echo -e "\n${BLUE}=== Test: $name ===${NC}"
    echo -e "Command: ${YELLOW}$cmd${NC}"
    
    drop_test_db
    create_test_db
    
    # Run the restore and capture time
    local start_time=$(date +%s.%N)
    eval "$cmd" 2>&1 | tail -20
    local exit_code=$?
    local end_time=$(date +%s.%N)
    
    local duration=$(echo "$end_time - $start_time" | bc)
    local throughput=$(echo "scale=2; $BACKUP_SIZE_MB / $duration" | bc)
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ Success${NC}"
        echo -e "Duration: ${GREEN}${duration}s${NC}"
        echo -e "Throughput: ${GREEN}${throughput} MB/s${NC}"
    else
        echo -e "${RED}✗ Failed (exit code: $exit_code)${NC}"
    fi
    
    echo "$name,$duration,$throughput,$exit_code" >> benchmark_results.csv
}

# Initialize results file
echo "test_name,duration_seconds,throughput_mbps,exit_code" > benchmark_results.csv

# Get system info
echo -e "\n${BLUE}=== System Information ===${NC}"
echo -e "CPU cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'unknown')"
echo -e "Memory: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo 'unknown')"
echo -e "Disk: $(df -h . | tail -1 | awk '{print $4}' || echo 'unknown') available"

get_pg_settings

echo -e "\n${BLUE}=== Starting Benchmarks ===${NC}"
echo -e "${YELLOW}This may take a while depending on backup size...${NC}"

# Test 1: dbbackup with TUI (default)
run_benchmark "dbbackup_with_tui" \
    "$DBBACKUP restore single '$BACKUP_FILE' --target '$TARGET_DB' --confirm --profile turbo"

# Test 2: dbbackup without TUI
run_benchmark "dbbackup_no_tui" \
    "$DBBACKUP restore single '$BACKUP_FILE' --target '$TARGET_DB' --confirm --no-tui --quiet --profile turbo"

# Test 3: dbbackup max performance
run_benchmark "dbbackup_max_perf" \
    "$DBBACKUP restore single '$BACKUP_FILE' --target '$TARGET_DB' --confirm --no-tui --quiet --profile max-performance --jobs 8"

# Test 4: Native pg_restore baseline (if custom format)
if [[ "$BACKUP_FILE" == *.dump* ]]; then
    RESTORE_FILE="$BACKUP_FILE"
    if [[ "$BACKUP_FILE" == *.gz ]]; then
        echo -e "\n${YELLOW}Decompressing for pg_restore baseline...${NC}"
        RESTORE_FILE="/tmp/benchmark_restore_temp.dump"
        gunzip -c "$BACKUP_FILE" > "$RESTORE_FILE"
    fi
    
    run_benchmark "pg_restore_j8" \
        "pg_restore -j8 --no-owner --no-privileges -d '$TARGET_DB' '$RESTORE_FILE'"
    
    # Cleanup temp file
    [ "$RESTORE_FILE" != "$BACKUP_FILE" ] && rm -f "$RESTORE_FILE"
fi

# Cleanup
drop_test_db

# Print summary
echo -e "\n${BLUE}======================================================${NC}"
echo -e "${BLUE}  Benchmark Results Summary${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""
column -t -s',' benchmark_results.csv 2>/dev/null || cat benchmark_results.csv
echo ""

# Calculate speedup
if [ -f benchmark_results.csv ]; then
    TUI_TIME=$(grep "dbbackup_with_tui" benchmark_results.csv | cut -d',' -f2)
    NO_TUI_TIME=$(grep "dbbackup_no_tui" benchmark_results.csv | cut -d',' -f2)
    
    if [ -n "$TUI_TIME" ] && [ -n "$NO_TUI_TIME" ]; then
        SPEEDUP=$(echo "scale=2; $TUI_TIME / $NO_TUI_TIME" | bc)
        echo -e "TUI overhead: ${YELLOW}${SPEEDUP}x${NC} (TUI time / no-TUI time)"
        
        if (( $(echo "$SPEEDUP > 2.0" | bc -l) )); then
            echo -e "${RED}⚠ TUI is causing significant slowdown!${NC}"
            echo -e "   Consider using --no-tui --quiet for production restores"
        elif (( $(echo "$SPEEDUP > 1.2" | bc -l) )); then
            echo -e "${YELLOW}⚠ TUI adds some overhead${NC}"
        else
            echo -e "${GREEN}✓ TUI overhead is minimal${NC}"
        fi
    fi
fi

echo ""
echo -e "${BLUE}Results saved to: ${GREEN}benchmark_results.csv${NC}"
echo ""

# Performance recommendations
echo -e "${BLUE}=== Performance Recommendations ===${NC}"
echo ""
echo "For fastest restores:"
echo "  1. Use --profile turbo or --profile max-performance"
echo "  2. Use --jobs 8 (or higher for more cores)"
echo "  3. Use --no-tui --quiet for batch/scripted restores"
echo "  4. Ensure PostgreSQL has:"
echo "     - maintenance_work_mem = 1GB+"
echo "     - max_wal_size = 10GB+"
echo "     - synchronous_commit = off (for restores only)"
echo ""
echo "Example optimal command:"
echo -e "  ${GREEN}$DBBACKUP restore single backup.dump.gz --confirm --profile max-performance --jobs 8 --no-tui --quiet${NC}"
echo ""
