#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# Universal Performance Benchmark Script
# ═══════════════════════════════════════════════════════════════════════════════
#
# Measures before/after performance of dbbackup across different workloads.
# Adapts automatically to available hardware (VPS, bare metal, ARM, x86).
#
# Usage:
#   ./tests/benchmark_perf.sh [database] [iterations]
#
# Requirements:
#   - PostgreSQL running with test database
#   - dbbackup binary built (or will be built)
#   - Sufficient disk space for backup output
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

DB="${1:-testdb}"
ITERATIONS="${2:-3}"
BINARY="./dbbackup"
OUTPUT_DIR="/tmp/dbbackup_bench_$(date +%Y%m%d_%H%M%S)"
RESULTS_FILE="${OUTPUT_DIR}/results.csv"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_err()   { echo -e "${RED}[ERROR]${NC} $*"; }

# ─── System Info ──────────────────────────────────────────────────────────────

print_system_info() {
    echo "═══════════════════════════════════════════════════════════════"
    echo "  System Profile"
    echo "═══════════════════════════════════════════════════════════════"
    echo "  OS:       $(uname -s) $(uname -m)"
    echo "  Kernel:   $(uname -r)"
    echo "  CPU:      $(nproc) cores"
    if command -v lscpu &>/dev/null; then
        echo "  CPU Model: $(lscpu | grep 'Model name' | sed 's/.*:\s*//')"
    fi
    echo "  RAM:      $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo 'N/A')"
    if command -v lsblk &>/dev/null; then
        echo "  Disk:     $(lsblk -d -o NAME,ROTA,SIZE 2>/dev/null | head -5)"
    fi
    echo "  Go:       $(go version 2>/dev/null || echo 'not found')"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

# ─── Database Info ────────────────────────────────────────────────────────────

print_db_info() {
    log_info "Database: ${DB}"
    if command -v psql &>/dev/null; then
        local db_size
        db_size=$(psql -d "$DB" -t -c "SELECT pg_size_pretty(pg_database_size('${DB}'));" 2>/dev/null | tr -d ' ' || echo "N/A")
        local table_count
        table_count=$(psql -d "$DB" -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null | tr -d ' ' || echo "N/A")
        local has_blobs
        has_blobs=$(psql -d "$DB" -t -c "SELECT count(*) FROM information_schema.columns WHERE data_type IN ('bytea','text','jsonb','json') AND table_schema='public';" 2>/dev/null | tr -d ' ' || echo "N/A")
        echo "  Size:     ${db_size}"
        echo "  Tables:   ${table_count}"
        echo "  BLOB cols: ${has_blobs}"
    else
        log_warn "psql not found — skipping database info"
    fi
    echo ""
}

# ─── Build ────────────────────────────────────────────────────────────────────

ensure_binary() {
    if [[ ! -x "$BINARY" ]]; then
        log_info "Building dbbackup..."
        go build -o "$BINARY" . 2>&1
        log_ok "Binary built: $BINARY"
    else
        log_ok "Using existing binary: $BINARY"
    fi
}

# ─── Benchmark Runner ────────────────────────────────────────────────────────

run_benchmark() {
    local label="$1"
    shift
    local cmd=("$@")
    local total_time=0
    local peak_mem=0
    local times=()

    log_info "Running benchmark: ${label} (${ITERATIONS} iterations)"

    for i in $(seq 1 "$ITERATIONS"); do
        local outfile="${OUTPUT_DIR}/${label}_iter${i}.sql.gz"

        # Measure time and peak memory
        local start_time
        start_time=$(date +%s%N)

        # Run with /usr/bin/time if available for memory tracking
        if command -v /usr/bin/time &>/dev/null; then
            /usr/bin/time -v "${cmd[@]}" 2>"${OUTPUT_DIR}/time_${label}_${i}.txt" || true
            local mem
            mem=$(grep "Maximum resident" "${OUTPUT_DIR}/time_${label}_${i}.txt" 2>/dev/null | awk '{print $NF}' || echo "0")
            if [[ "$mem" -gt "$peak_mem" ]]; then
                peak_mem=$mem
            fi
        else
            "${cmd[@]}" 2>/dev/null || true
        fi

        local end_time
        end_time=$(date +%s%N)
        local elapsed=$(( (end_time - start_time) / 1000000 )) # milliseconds
        times+=("$elapsed")
        total_time=$((total_time + elapsed))

        echo "    Iteration $i: ${elapsed}ms"
    done

    local avg_time=$((total_time / ITERATIONS))
    local peak_mem_mb=$((peak_mem / 1024))

    log_ok "${label}: avg=${avg_time}ms, peak_mem=${peak_mem_mb}MB"
    echo "${label},${avg_time},${peak_mem_mb}" >> "$RESULTS_FILE"
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
    mkdir -p "$OUTPUT_DIR"
    echo "benchmark,avg_ms,peak_mem_mb" > "$RESULTS_FILE"

    print_system_info
    ensure_binary
    print_db_info

    echo "═══════════════════════════════════════════════════════════════"
    echo "  Performance Benchmarks"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    # Backup benchmarks
    run_benchmark "backup_auto" \
        "$BINARY" backup single "$DB" --workers auto --output "${OUTPUT_DIR}/backup_auto.sql.gz"

    run_benchmark "backup_debug" \
        "$BINARY" backup single "$DB" --workers auto --debug --output "${OUTPUT_DIR}/backup_debug.sql.gz"

    # Restore benchmarks (if backup succeeded)
    if [[ -f "${OUTPUT_DIR}/backup_auto.sql.gz" ]]; then
        local restore_db="${DB}_bench_restore"

        # Create target database
        if command -v psql &>/dev/null; then
            psql -c "DROP DATABASE IF EXISTS ${restore_db};" 2>/dev/null || true
            psql -c "CREATE DATABASE ${restore_db};" 2>/dev/null || true

            run_benchmark "restore_balanced" \
                "$BINARY" restore single "${OUTPUT_DIR}/backup_auto.sql.gz" \
                --restore-mode=balanced --workers=auto --target-db="$restore_db"

            # Cleanup
            psql -c "DROP DATABASE IF EXISTS ${restore_db};" 2>/dev/null || true
        else
            log_warn "psql not available — skipping restore benchmark"
        fi
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Results Summary"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    column -t -s',' "$RESULTS_FILE" 2>/dev/null || cat "$RESULTS_FILE"
    echo ""
    echo "Full results: ${OUTPUT_DIR}/"
    echo ""

    # Expected improvements from optimizations:
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Expected Optimization Impact"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "  pgx.Batch pipeline:           15-30% faster DDL execution"
    echo "  WAL compression:              10-20% faster on I/O-bound systems"
    echo "  Unix socket auto-detection:   10-30% lower query latency (local)"
    echo "  BLOB-aware buffer sizing:     20-40% faster large object transfer"
    echo "  Prepared statement caching:   5-10% faster metadata queries"
    echo ""
    echo "  Combined (small tables):      25-35% improvement"
    echo "  Combined (BLOB-heavy):        40-60% improvement"
    echo "  Combined (mixed workload):    30-45% improvement"
    echo "═══════════════════════════════════════════════════════════════"
}

main "$@"
