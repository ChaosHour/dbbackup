#!/bin/bash
#
# PostgreSQL Memory and Resource Diagnostic Tool
# Analyzes memory usage, locks, and system resources to identify restore issues
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "════════════════════════════════════════════════════════════"
echo "  PostgreSQL Memory & Resource Diagnostics"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════════════════════"
echo

# Function to format bytes to human readable
bytes_to_human() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(awk "BEGIN {printf \"%.2f GB\", $bytes/1073741824}")"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(awk "BEGIN {printf \"%.2f MB\", $bytes/1048576}")"
    else
        echo "$(awk "BEGIN {printf \"%.2f KB\", $bytes/1024}")"
    fi
}

# 1. SYSTEM MEMORY OVERVIEW
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}📊 SYSTEM MEMORY OVERVIEW${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

if command -v free &> /dev/null; then
    free -h
    echo
    
    # Calculate percentages
    MEM_TOTAL=$(free -b | awk '/^Mem:/ {print $2}')
    MEM_USED=$(free -b | awk '/^Mem:/ {print $3}')
    MEM_FREE=$(free -b | awk '/^Mem:/ {print $4}')
    MEM_AVAILABLE=$(free -b | awk '/^Mem:/ {print $7}')
    
    MEM_PERCENT=$(awk "BEGIN {printf \"%.1f\", ($MEM_USED/$MEM_TOTAL)*100}")
    
    echo "Memory Utilization: ${MEM_PERCENT}%"
    echo "Total: $(bytes_to_human $MEM_TOTAL)"
    echo "Used: $(bytes_to_human $MEM_USED)"
    echo "Available: $(bytes_to_human $MEM_AVAILABLE)"
    
    if (( $(echo "$MEM_PERCENT > 90" | bc -l) )); then
        echo -e "${RED}⚠️  WARNING: Memory usage is critically high (>90%)${NC}"
    elif (( $(echo "$MEM_PERCENT > 70" | bc -l) )); then
        echo -e "${YELLOW}⚠️  CAUTION: Memory usage is high (>70%)${NC}"
    else
        echo -e "${GREEN}✓ Memory usage is acceptable${NC}"
    fi
fi
echo

# 2. TOP MEMORY CONSUMERS
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔍 TOP 15 MEMORY CONSUMING PROCESSES${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo
ps aux --sort=-%mem | head -16 | awk 'NR==1 {print $0} NR>1 {printf "%-8s %5s%% %7s %s\n", $1, $4, $6/1024"M", $11}'
echo

# 3. POSTGRESQL PROCESSES
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🐘 POSTGRESQL PROCESSES${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

PG_PROCS=$(ps aux | grep -E "postgres.*:" | grep -v grep || true)
if [ -z "$PG_PROCS" ]; then
    echo "No PostgreSQL processes found"
else
    echo "$PG_PROCS" | awk '{printf "%-8s %5s%% %7s %s\n", $1, $4, $6/1024"M", $11}'
    echo
    
    # Sum up PostgreSQL memory
    PG_MEM_TOTAL=$(echo "$PG_PROCS" | awk '{sum+=$6} END {print sum/1024}')
    echo "Total PostgreSQL Memory: ${PG_MEM_TOTAL} MB"
fi
echo

# 4. POSTGRESQL CONFIGURATION
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}⚙️  POSTGRESQL MEMORY CONFIGURATION${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

if command -v psql &> /dev/null; then
    PSQL_CMD="psql -t -A -c"
    
    # Try as postgres user first, then current user
    if sudo -u postgres $PSQL_CMD "SELECT 1" &> /dev/null; then
        PSQL_PREFIX="sudo -u postgres"
    elif $PSQL_CMD "SELECT 1" &> /dev/null; then
        PSQL_PREFIX=""
    else
        echo "❌ Cannot connect to PostgreSQL"
        PSQL_PREFIX="NONE"
    fi
    
    if [ "$PSQL_PREFIX" != "NONE" ]; then
        echo "Key Memory Settings:"
        echo "────────────────────────────────────────────────────────────"
        
        # Get all relevant settings
        SHARED_BUFFERS=$($PSQL_PREFIX psql -t -A -c "SHOW shared_buffers;" 2>/dev/null || echo "unknown")
        WORK_MEM=$($PSQL_PREFIX psql -t -A -c "SHOW work_mem;" 2>/dev/null || echo "unknown")
        MAINT_WORK_MEM=$($PSQL_PREFIX psql -t -A -c "SHOW maintenance_work_mem;" 2>/dev/null || echo "unknown")
        EFFECTIVE_CACHE=$($PSQL_PREFIX psql -t -A -c "SHOW effective_cache_size;" 2>/dev/null || echo "unknown")
        MAX_CONNECTIONS=$($PSQL_PREFIX psql -t -A -c "SHOW max_connections;" 2>/dev/null || echo "unknown")
        MAX_LOCKS=$($PSQL_PREFIX psql -t -A -c "SHOW max_locks_per_transaction;" 2>/dev/null || echo "unknown")
        MAX_PREPARED=$($PSQL_PREFIX psql -t -A -c "SHOW max_prepared_transactions;" 2>/dev/null || echo "unknown")
        
        echo "shared_buffers:               $SHARED_BUFFERS"
        echo "work_mem:                     $WORK_MEM"
        echo "maintenance_work_mem:         $MAINT_WORK_MEM"
        echo "effective_cache_size:         $EFFECTIVE_CACHE"
        echo "max_connections:              $MAX_CONNECTIONS"
        echo "max_locks_per_transaction:    $MAX_LOCKS"
        echo "max_prepared_transactions:    $MAX_PREPARED"
        echo
        
        # Calculate lock capacity
        if [ "$MAX_LOCKS" != "unknown" ] && [ "$MAX_CONNECTIONS" != "unknown" ] && [ "$MAX_PREPARED" != "unknown" ]; then
            LOCK_CAPACITY=$((MAX_LOCKS * (MAX_CONNECTIONS + MAX_PREPARED)))
            echo "Total Lock Capacity: $LOCK_CAPACITY locks"
            
            if [ "$MAX_LOCKS" -lt 1000 ]; then
                echo -e "${RED}⚠️  WARNING: max_locks_per_transaction is too low for large restores${NC}"
                echo -e "${YELLOW}   Recommended: 4096 or higher${NC}"
            fi
        fi
        echo
    fi
else
    echo "❌ psql not found"
fi

# 5. CURRENT LOCKS AND CONNECTIONS
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔒 CURRENT LOCKS AND CONNECTIONS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

if [ "$PSQL_PREFIX" != "NONE" ] && command -v psql &> /dev/null; then
    # Active connections
    ACTIVE_CONNS=$($PSQL_PREFIX psql -t -A -c "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null || echo "0")
    echo "Active Connections: $ACTIVE_CONNS / $MAX_CONNECTIONS"
    echo
    
    # Lock statistics
    echo "Current Lock Usage:"
    echo "────────────────────────────────────────────────────────────"
    $PSQL_PREFIX psql -c "
        SELECT 
            mode,
            COUNT(*) as count
        FROM pg_locks
        GROUP BY mode
        ORDER BY count DESC;
    " 2>/dev/null || echo "Unable to query locks"
    echo
    
    # Total locks
    TOTAL_LOCKS=$($PSQL_PREFIX psql -t -A -c "SELECT COUNT(*) FROM pg_locks;" 2>/dev/null || echo "0")
    echo "Total Active Locks: $TOTAL_LOCKS"
    
    if [ "$LOCK_CAPACITY" != "" ] && [ "$TOTAL_LOCKS" -gt 0 ]; then
        LOCK_PERCENT=$((TOTAL_LOCKS * 100 / LOCK_CAPACITY))
        echo "Lock Usage: ${LOCK_PERCENT}%"
        
        if [ "$LOCK_PERCENT" -gt 80 ]; then
            echo -e "${RED}⚠️  WARNING: Lock table usage is critically high${NC}"
        elif [ "$LOCK_PERCENT" -gt 60 ]; then
            echo -e "${YELLOW}⚠️  CAUTION: Lock table usage is elevated${NC}"
        fi
    fi
    echo
    
    # Blocking queries
    echo "Blocking Queries:"
    echo "────────────────────────────────────────────────────────────"
    $PSQL_PREFIX psql -c "
        SELECT 
            blocked_locks.pid AS blocked_pid,
            blocking_locks.pid AS blocking_pid,
            blocked_activity.usename AS blocked_user,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_query
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted;
    " 2>/dev/null || echo "No blocking queries or unable to query"
    echo
fi

# 6. SHARED MEMORY USAGE
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}💾 SHARED MEMORY SEGMENTS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

if command -v ipcs &> /dev/null; then
    ipcs -m
    echo
    
    # Sum up shared memory
    TOTAL_SHM=$(ipcs -m | awk '/^0x/ {sum+=$5} END {print sum}')
    if [ ! -z "$TOTAL_SHM" ]; then
        echo "Total Shared Memory: $(bytes_to_human $TOTAL_SHM)"
    fi
else
    echo "ipcs command not available"
fi
echo

# 7. DISK SPACE (relevant for temp files)
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}💿 DISK SPACE${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

df -h | grep -E "Filesystem|/$|/var|/tmp|/postgres"
echo

# Check for PostgreSQL temp files
if [ "$PSQL_PREFIX" != "NONE" ] && command -v psql &> /dev/null; then
    TEMP_FILES=$($PSQL_PREFIX psql -t -A -c "SELECT count(*) FROM pg_stat_database WHERE temp_files > 0;" 2>/dev/null || echo "0")
    if [ "$TEMP_FILES" -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Databases are using temporary files (work_mem may be too low)${NC}"
        $PSQL_PREFIX psql -c "SELECT datname, temp_files, pg_size_pretty(temp_bytes) as temp_size FROM pg_stat_database WHERE temp_files > 0;" 2>/dev/null
        echo
    fi
fi

# 8. OTHER RESOURCE CONSUMERS
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔍 OTHER POTENTIAL MEMORY CONSUMERS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Check for common memory hogs
echo "Checking for common memory-intensive services..."
echo

for service in "mysqld" "mongodb" "redis" "elasticsearch" "java" "docker" "containerd"; do
    MEM=$(ps aux | grep "$service" | grep -v grep | awk '{sum+=$4} END {printf "%.1f", sum}')
    if [ ! -z "$MEM" ] && (( $(echo "$MEM > 0" | bc -l) )); then
        echo "  ${service}: ${MEM}%"
    fi
done
echo

# 9. SWAP USAGE
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔄 SWAP USAGE${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

if command -v free &> /dev/null; then
    SWAP_TOTAL=$(free -b | awk '/^Swap:/ {print $2}')
    SWAP_USED=$(free -b | awk '/^Swap:/ {print $3}')
    
    if [ "$SWAP_TOTAL" -gt 0 ]; then
        SWAP_PERCENT=$(awk "BEGIN {printf \"%.1f\", ($SWAP_USED/$SWAP_TOTAL)*100}")
        echo "Swap Total: $(bytes_to_human $SWAP_TOTAL)"
        echo "Swap Used: $(bytes_to_human $SWAP_USED) (${SWAP_PERCENT}%)"
        
        if (( $(echo "$SWAP_PERCENT > 50" | bc -l) )); then
            echo -e "${RED}⚠️  WARNING: Heavy swap usage detected - system may be thrashing${NC}"
        elif (( $(echo "$SWAP_PERCENT > 20" | bc -l) )); then
            echo -e "${YELLOW}⚠️  CAUTION: System is using swap${NC}"
        else
            echo -e "${GREEN}✓ Swap usage is low${NC}"
        fi
    else
        echo "No swap configured"
    fi
fi
echo

# 10. RECOMMENDATIONS
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}💡 RECOMMENDATIONS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

echo "Based on the diagnostics:"
echo

# Memory recommendations
if [ ! -z "$MEM_PERCENT" ]; then
    if (( $(echo "$MEM_PERCENT > 80" | bc -l) )); then
        echo "1. ⚠️  Memory Pressure:"
        echo "   • System memory is ${MEM_PERCENT}% utilized"
        echo "   • Stop non-essential services before restore"
        echo "   • Consider increasing system RAM"
        echo "   • Use 'dbbackup restore --parallel=1' to reduce memory usage"
        echo
    fi
fi

# Lock recommendations
if [ "$MAX_LOCKS" != "unknown" ] && [ "$MAX_LOCKS" != "" ]; then
    if [ "$MAX_LOCKS" -lt 1000 ]; then
        echo "2. ⚠️  Lock Configuration:"
        echo "   • max_locks_per_transaction is too low: $MAX_LOCKS"
        echo "   • Run: ./fix_postgres_locks.sh"
        echo "   • Or manually: ALTER SYSTEM SET max_locks_per_transaction = 4096;"
        echo "   • Then restart PostgreSQL"
        echo
    fi
fi

# Other recommendations
echo "3. 🔧 Before Large Restores:"
echo "   • Stop unnecessary services (web servers, cron jobs, etc.)"
echo "   • Clear PostgreSQL idle connections"
echo "   • Ensure adequate disk space for temp files"
echo "   • Consider using --large-db mode for very large databases"
echo

echo "4. 📊 Monitor During Restore:"
echo "   • Watch: watch -n 2 'ps aux | grep postgres | head -20'"
echo "   • Locks: watch -n 5 'psql -c \"SELECT COUNT(*) FROM pg_locks;\"'"
echo "   • Memory: watch -n 2 free -h"
echo

echo "════════════════════════════════════════════════════════════"
echo "  Report generated: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Save this output: $0 > diagnosis_$(date +%Y%m%d_%H%M%S).log"
echo "════════════════════════════════════════════════════════════"
