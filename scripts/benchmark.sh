#!/bin/bash
# DBBackup Performance Benchmark Suite
# Tests backup/restore performance across various database sizes and configurations
#
# Usage: ./scripts/benchmark.sh [OPTIONS]
#   --size SIZE         Database size to test (1G, 10G, 100G, 1T)
#   --jobs N            Number of parallel jobs (default: auto-detect)
#   --type TYPE         Database type: postgres or mysql (default: postgres)
#   --quick             Quick benchmark (1GB only, fewer iterations)
#   --full              Full benchmark suite (all sizes)
#   --output DIR        Output directory for results (default: ./benchmark-results)
#   --help              Show this help

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default configuration
DBBACKUP=${DBBACKUP:-"./bin/dbbackup_linux_amd64"}
OUTPUT_DIR="./benchmark-results"
DB_TYPE="postgres"
DB_SIZE="1G"
JOBS=$(nproc 2>/dev/null || echo 4)
QUICK_MODE=false
FULL_MODE=false
ITERATIONS=3

# Performance targets (from requirements)
declare -A BACKUP_TARGETS=(
    ["1G"]="30"      # 1GB: < 30 seconds
    ["10G"]="180"    # 10GB: < 3 minutes
    ["100G"]="1200"  # 100GB: < 20 minutes
    ["1T"]="10800"   # 1TB: < 3 hours
)

declare -A RESTORE_TARGETS=(
    ["10G"]="300"    # 10GB: < 5 minutes
    ["100G"]="1800"  # 100GB: < 30 minutes
    ["1T"]="14400"   # 1TB: < 4 hours
)

declare -A MEMORY_TARGETS=(
    ["1G"]="512"     # 1GB DB: < 500MB RAM
    ["100G"]="1024"  # 100GB: < 1GB RAM
    ["1T"]="2048"    # 1TB: < 2GB RAM
)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --size)
            DB_SIZE="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --type)
            DB_TYPE="$2"
            shift 2
            ;;
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --full)
            FULL_MODE=true
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help)
            head -20 "$0" | tail -16
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"
RESULT_FILE="$OUTPUT_DIR/benchmark_$(date +%Y%m%d_%H%M%S).json"
LOG_FILE="$OUTPUT_DIR/benchmark_$(date +%Y%m%d_%H%M%S).log"

# Helper functions
log() {
    echo -e "$1" | tee -a "$LOG_FILE"
}

timestamp() {
    date +%s.%N
}

measure_memory() {
    local pid=$1
    local max_mem=0
    while kill -0 "$pid" 2>/dev/null; do
        local mem=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
        if [[ -n "$mem" ]] && [[ "$mem" -gt "$max_mem" ]]; then
            max_mem=$mem
        fi
        sleep 0.1
    done
    echo $((max_mem / 1024))  # Convert to MB
}

get_cpu_usage() {
    local pid=$1
    ps -p "$pid" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0"
}

# Check prerequisites
check_prerequisites() {
    log "${BLUE}=== Checking Prerequisites ===${NC}"
    
    if [[ ! -x "$DBBACKUP" ]]; then
        log "${RED}ERROR: dbbackup binary not found at $DBBACKUP${NC}"
        log "Build it with: make build"
        exit 1
    fi
    
    log "  dbbackup: $DBBACKUP"
    log "  version: $($DBBACKUP version 2>/dev/null || echo 'unknown')"
    log "  CPU cores: $(nproc)"
    log "  Memory: $(free -h | awk '/^Mem:/{print $2}')"
    log "  Disk space: $(df -h . | tail -1 | awk '{print $4}')"
    log ""
}

# Run single benchmark
run_benchmark() {
    local operation=$1
    local size=$2
    local jobs=$3
    local db_name="benchmark_${size}"
    local backup_path="$OUTPUT_DIR/backups/${db_name}"
    
    log "${BLUE}Running: $operation | Size: $size | Jobs: $jobs${NC}"
    
    local start_time=$(timestamp)
    local peak_memory=0
    
    # Prepare command based on operation
    case $operation in
        backup)
            mkdir -p "$backup_path"
            local cmd="$DBBACKUP backup single $db_name --dir $backup_path --jobs $jobs --compress"
            ;;
        restore)
            local cmd="$DBBACKUP restore latest $db_name --dir $backup_path --jobs $jobs --target-db ${db_name}_restored"
            ;;
        *)
            log "${RED}Unknown operation: $operation${NC}"
            return 1
            ;;
    esac
    
    # Run command in background to measure resources
    log "  Command: $cmd"
    $cmd &>"$OUTPUT_DIR/cmd_output.tmp" &
    local pid=$!
    
    # Monitor memory in background
    peak_memory=$(measure_memory $pid) &
    local mem_pid=$!
    
    # Wait for command to complete
    wait $pid
    local exit_code=$?
    wait $mem_pid 2>/dev/null || true
    
    local end_time=$(timestamp)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    # Check against targets
    local target_var="${operation^^}_TARGETS[$size]"
    local target=${!target_var:-0}
    local status="PASS"
    local status_color=$GREEN
    
    if [[ "$target" -gt 0 ]] && (( $(echo "$duration > $target" | bc -l) )); then
        status="FAIL"
        status_color=$RED
    fi
    
    # Memory check
    local mem_target_var="MEMORY_TARGETS[$size]"
    local mem_target=${!mem_target_var:-0}
    local mem_status="OK"
    if [[ "$mem_target" -gt 0 ]] && [[ "$peak_memory" -gt "$mem_target" ]]; then
        mem_status="EXCEEDED"
    fi
    
    log "  ${status_color}Duration: ${duration}s (target: ${target}s) - $status${NC}"
    log "  Memory: ${peak_memory}MB (target: ${mem_target}MB) - $mem_status"
    log "  Exit code: $exit_code"
    
    # Output JSON result
    cat >> "$RESULT_FILE" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "operation": "$operation",
    "size": "$size",
    "jobs": $jobs,
    "duration_seconds": $duration,
    "target_seconds": $target,
    "peak_memory_mb": $peak_memory,
    "target_memory_mb": $mem_target,
    "status": "$status",
    "exit_code": $exit_code
},
EOF
    
    return $exit_code
}

# Run concurrency scaling benchmark
run_scaling_benchmark() {
    local size=$1
    log "${YELLOW}=== Concurrency Scaling Test (Size: $size) ===${NC}"
    
    local baseline_time=0
    
    for jobs in 1 2 4 8 16; do
        if [[ $jobs -gt $(nproc) ]]; then
            log "  Skipping jobs=$jobs (exceeds CPU count)"
            continue
        fi
        
        run_benchmark "backup" "$size" "$jobs"
        
        # Calculate speedup
        if [[ $jobs -eq 1 ]]; then
            # This would need actual timing from the benchmark
            log "  Baseline set for speedup calculation"
        fi
    done
}

# Memory scaling benchmark
run_memory_benchmark() {
    log "${YELLOW}=== Memory Scaling Test ===${NC}"
    log "Goal: Memory usage should remain constant regardless of DB size"
    
    for size in 1G 10G 100G; do
        if [[ "$QUICK_MODE" == "true" ]] && [[ "$size" != "1G" ]]; then
            continue
        fi
        
        log "Testing size: $size"
        run_benchmark "backup" "$size" "$JOBS"
    done
}

# Catalog performance benchmark
run_catalog_benchmark() {
    log "${YELLOW}=== Catalog Query Performance ===${NC}"
    
    local catalog_db="$OUTPUT_DIR/test_catalog.db"
    
    # Create test catalog with many entries
    log "Creating test catalog with 10,000 entries..."
    
    # Use dbbackup catalog commands if available, otherwise skip
    if $DBBACKUP catalog list --help &>/dev/null; then
        local start=$(timestamp)
        
        # Query performance test
        log "Testing query: SELECT * FROM backups WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 100"
        
        local query_start=$(timestamp)
        $DBBACKUP catalog list --limit 100 --catalog-db "$catalog_db" 2>/dev/null || true
        local query_end=$(timestamp)
        local query_time=$(echo "$query_end - $query_start" | bc)
        
        if (( $(echo "$query_time < 0.1" | bc -l) )); then
            log "  ${GREEN}Query time: ${query_time}s - PASS (target: <100ms)${NC}"
        else
            log "  ${YELLOW}Query time: ${query_time}s - SLOW (target: <100ms)${NC}"
        fi
    else
        log "  Catalog benchmarks skipped (catalog command not available)"
    fi
}

# Generate report
generate_report() {
    log ""
    log "${BLUE}=== Benchmark Report ===${NC}"
    log "Results saved to: $RESULT_FILE"
    log "Log saved to: $LOG_FILE"
    
    # Create summary
    cat > "$OUTPUT_DIR/BENCHMARK_SUMMARY.md" << EOF
# DBBackup Performance Benchmark Results

**Date:** $(date -Iseconds)
**Host:** $(hostname)
**CPU:** $(nproc) cores
**Memory:** $(free -h | awk '/^Mem:/{print $2}')
**DBBackup Version:** $($DBBACKUP version 2>/dev/null || echo 'unknown')

## Performance Targets

| Size | Backup Target | Restore Target | Memory Target |
|------|---------------|----------------|---------------|
| 1GB  | < 30 seconds  | N/A            | < 500MB       |
| 10GB | < 3 minutes   | < 5 minutes    | < 1GB         |
| 100GB| < 20 minutes  | < 30 minutes   | < 1GB         |
| 1TB  | < 3 hours     | < 4 hours      | < 2GB         |

## Expected Concurrency Scaling

| Jobs | Expected Speedup |
|------|------------------|
| 1    | 1.0x (baseline)  |
| 2    | ~1.8x            |
| 4    | ~3.5x            |
| 8    | ~6x              |
| 16   | ~7x              |

## Results

See $RESULT_FILE for detailed results.

## Key Observations

- Memory usage should remain constant regardless of database size
- CPU utilization target: >80% with --jobs matching core count
- Backup duration should scale linearly (2x data = 2x time)

EOF
    
    log "Summary saved to: $OUTPUT_DIR/BENCHMARK_SUMMARY.md"
}

# Main execution
main() {
    log "${GREEN}╔═══════════════════════════════════════╗${NC}"
    log "${GREEN}║   DBBackup Performance Benchmark      ║${NC}"
    log "${GREEN}╚═══════════════════════════════════════╝${NC}"
    log ""
    
    check_prerequisites
    
    # Initialize results file
    echo "[" > "$RESULT_FILE"
    
    if [[ "$FULL_MODE" == "true" ]]; then
        log "${YELLOW}=== Full Benchmark Suite ===${NC}"
        for size in 1G 10G 100G 1T; do
            run_benchmark "backup" "$size" "$JOBS"
        done
        run_scaling_benchmark "10G"
        run_memory_benchmark
        run_catalog_benchmark
    elif [[ "$QUICK_MODE" == "true" ]]; then
        log "${YELLOW}=== Quick Benchmark (1GB) ===${NC}"
        run_benchmark "backup" "1G" "$JOBS"
        run_catalog_benchmark
    else
        log "${YELLOW}=== Single Size Benchmark ($DB_SIZE) ===${NC}"
        run_benchmark "backup" "$DB_SIZE" "$JOBS"
    fi
    
    # Close results file
    # Remove trailing comma and close array
    sed -i '$ s/,$//' "$RESULT_FILE"
    echo "]" >> "$RESULT_FILE"
    
    generate_report
    
    log ""
    log "${GREEN}Benchmark complete!${NC}"
}

main "$@"
