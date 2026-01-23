#!/bin/bash
#
# COMPREHENSIVE RESTORE PREPARATION SCRIPT
# =========================================
# Prepares system for large database restores by:
# - Checking and creating swap if needed
# - Tuning PostgreSQL memory settings
# - Protecting from OOM killer
# - Boosting lock limits
# - Running system diagnostics
#
# Usage:
#   ./prepare_restore.sh                # Run diagnostics only
#   ./prepare_restore.sh --fix          # Apply all fixes
#   ./prepare_restore.sh --swap 32G     # Create specific swap size
#   ./prepare_restore.sh --tune-pg      # Tune PostgreSQL only
#   ./prepare_restore.sh --oom-protect  # Enable OOM protection only
#
# PostgreSQL connection (via environment variables):
#   PGHOST=localhost PGUSER=postgres PGPASSWORD=secret ./prepare_restore.sh --tune-pg
#

set -euo pipefail

#==============================================================================
# CONFIGURATION
#==============================================================================
VERSION="1.1.0"
SWAP_FILE="/swapfile_dbbackup"
DEFAULT_SWAP_SIZE="16G"
MIN_SWAP_GB=8
MIN_AVAILABLE_RAM_GB=4

# PostgreSQL connection defaults (can be overridden via environment)
export PGHOST="${PGHOST:-localhost}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGPORT="${PGPORT:-5432}"

# PostgreSQL tuning for large restores (low memory mode)
PG_WORK_MEM="64MB"
PG_MAINTENANCE_WORK_MEM="256MB"
PG_MAX_LOCKS="65536"
PG_MAX_PARALLEL="0"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

#==============================================================================
# HELPER FUNCTIONS
#==============================================================================
log_info()  { echo -e "${BLUE}ℹ${NC} $1"; }
log_ok()    { echo -e "${GREEN}✓${NC} $1"; }
log_warn()  { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }
log_section() { 
    echo
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

bytes_to_gb() {
    echo "scale=1; $1 / 1073741824" | bc
}

gb_to_bytes() {
    local size="$1"
    local num="${size//[!0-9]/}"
    local unit="${size//[0-9]/}"
    case "$unit" in
        G|g) echo $((num * 1073741824)) ;;
        M|m) echo $((num * 1048576)) ;;
        *) echo "$num" ;;
    esac
}

get_pg_setting() {
    local sql="$1"
    # Try multiple connection methods
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${PGHOST:-localhost}" -U "${PGUSER:-postgres}" -d postgres --no-psqlrc -t -A -c "$sql" 2>/dev/null || \
    sudo -u postgres psql --no-psqlrc -t -A -c "$sql" 2>/dev/null || \
    psql --no-psqlrc -t -A -c "$sql" 2>/dev/null || \
    echo "N/A"
}

set_pg_setting() {
    local setting="$1"
    local value="$2"
    
    # Try with env vars first (most common for remote/docker)
    if PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${PGHOST:-localhost}" -U "${PGUSER:-postgres}" -d postgres -c "ALTER SYSTEM SET $setting = '$value';" 2>/dev/null; then
        return 0
    # Try sudo to postgres user (local unix socket)
    elif sudo -u postgres psql -c "ALTER SYSTEM SET $setting = '$value';" 2>/dev/null; then
        return 0
    # Try direct psql (if already postgres user)
    elif psql -c "ALTER SYSTEM SET $setting = '$value';" 2>/dev/null; then
        return 0
    # Try with explicit socket
    elif psql -h /var/run/postgresql -U postgres -c "ALTER SYSTEM SET $setting = '$value';" 2>/dev/null; then
        return 0
    elif psql -h /tmp -U postgres -c "ALTER SYSTEM SET $setting = '$value';" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

reload_pg() {
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${PGHOST:-localhost}" -U "${PGUSER:-postgres}" -d postgres -c "SELECT pg_reload_conf();" 2>/dev/null || \
    sudo -u postgres psql -c "SELECT pg_reload_conf();" 2>/dev/null || \
    psql -c "SELECT pg_reload_conf();" 2>/dev/null || \
    sudo systemctl reload postgresql 2>/dev/null || \
    sudo service postgresql reload 2>/dev/null || \
    true
}

#==============================================================================
# DIAGNOSIS FUNCTIONS
#==============================================================================
diagnose_memory() {
    log_section "MEMORY DIAGNOSIS"
    
    if ! command -v free &>/dev/null; then
        log_error "free command not available"
        return 1
    fi
    
    echo
    free -h
    echo
    
    local mem_total mem_available swap_total swap_free
    mem_total=$(free -b | awk '/^Mem:/ {print $2}')
    mem_available=$(free -b | awk '/^Mem:/ {print $7}')
    swap_total=$(free -b | awk '/^Swap:/ {print $2}')
    swap_free=$(free -b | awk '/^Swap:/ {print $4}')
    
    local mem_total_gb mem_available_gb swap_total_gb
    mem_total_gb=$(bytes_to_gb "$mem_total")
    mem_available_gb=$(bytes_to_gb "$mem_available")
    swap_total_gb=$(bytes_to_gb "$swap_total")
    
    echo "  Total RAM:      ${mem_total_gb}GB"
    echo "  Available RAM:  ${mem_available_gb}GB"
    echo "  Total Swap:     ${swap_total_gb}GB"
    echo
    
    # Evaluate memory status
    local status="OK"
    local issues=()
    
    if (( $(echo "$mem_available_gb < $MIN_AVAILABLE_RAM_GB" | bc -l) )); then
        status="CRITICAL"
        issues+=("Available RAM too low (${mem_available_gb}GB < ${MIN_AVAILABLE_RAM_GB}GB)")
    fi
    
    if (( $(echo "$swap_total_gb < $MIN_SWAP_GB" | bc -l) )); then
        status="WARNING"
        issues+=("Swap too low (${swap_total_gb}GB < ${MIN_SWAP_GB}GB) - recommend creating swap")
    fi
    
    if [ "$status" = "OK" ]; then
        log_ok "Memory status: OK"
    elif [ "$status" = "WARNING" ]; then
        log_warn "Memory status: WARNING"
        for issue in "${issues[@]}"; do
            echo "     → $issue"
        done
    else
        log_error "Memory status: CRITICAL"
        for issue in "${issues[@]}"; do
            echo "     → $issue"
        done
    fi
    
    # Store for later use
    DIAG_MEM_AVAILABLE_GB="$mem_available_gb"
    DIAG_SWAP_TOTAL_GB="$swap_total_gb"
}

diagnose_postgresql() {
    log_section "POSTGRESQL CONFIGURATION"
    
    echo
    echo "  Current Settings:"
    echo "  ────────────────────────────────────────"
    
    local locks conns work_mem maint_work_mem parallel
    locks=$(get_pg_setting "SHOW max_locks_per_transaction;")
    conns=$(get_pg_setting "SHOW max_connections;")
    work_mem=$(get_pg_setting "SHOW work_mem;")
    maint_work_mem=$(get_pg_setting "SHOW maintenance_work_mem;")
    parallel=$(get_pg_setting "SHOW max_parallel_workers;")
    
    printf "  %-35s %s\n" "max_locks_per_transaction:" "$locks"
    printf "  %-35s %s\n" "max_connections:" "$conns"
    printf "  %-35s %s\n" "work_mem:" "$work_mem"
    printf "  %-35s %s\n" "maintenance_work_mem:" "$maint_work_mem"
    printf "  %-35s %s\n" "max_parallel_workers:" "$parallel"
    echo
    
    # Evaluate lock capacity
    if [ "$locks" = "N/A" ]; then
        log_error "Cannot connect to PostgreSQL"
        return 1
    fi
    
    local locks_num="${locks//[!0-9]/}"
    local capacity=$((locks_num * ${conns//[!0-9]/}))
    
    echo "  Lock Capacity: $capacity total locks"
    echo
    
    if [ "$locks_num" -lt 2048 ]; then
        log_error "Lock limit CRITICAL: $locks (need 2048+ for large restores)"
        DIAG_LOCKS_OK=0
    elif [ "$locks_num" -lt 8192 ]; then
        log_warn "Lock limit low: $locks (recommend 65536 for very large DBs)"
        DIAG_LOCKS_OK=0
    else
        log_ok "Lock limit adequate: $locks"
        DIAG_LOCKS_OK=1
    fi
    
    # Check parallel workers (should be 0 for low-memory restores)
    local parallel_num="${parallel//[!0-9]/}"
    if [ "$parallel_num" -gt 0 ]; then
        log_warn "Parallel workers enabled ($parallel) - disable for low-memory restores"
    fi
    
    DIAG_LOCKS="$locks_num"
}

diagnose_disk() {
    log_section "DISK SPACE"
    
    echo
    df -h / /var/lib/pgsql /tmp 2>/dev/null | head -5 || df -h /
    echo
    
    local root_avail
    root_avail=$(df -BG / | tail -1 | awk '{print $4}' | tr -d 'G')
    
    if [ "$root_avail" -lt 50 ]; then
        log_warn "Root filesystem has only ${root_avail}GB free"
    else
        log_ok "Disk space adequate: ${root_avail}GB free on /"
    fi
}

diagnose_oom() {
    log_section "OOM KILLER STATUS"
    
    echo
    echo "  Recent OOM events (last 20):"
    echo "  ────────────────────────────────────────"
    
    local oom_count
    oom_count=$(dmesg 2>/dev/null | grep -ci "out of memory\|oom\|killed process" || echo "0")
    
    if [ "$oom_count" -gt 0 ]; then
        dmesg 2>/dev/null | grep -i "out of memory\|oom\|killed process" | tail -10 | while read -r line; do
            echo "    $line"
        done
        echo
        log_error "Found $oom_count OOM events in kernel log"
    else
        echo "    (none found)"
        echo
        log_ok "No recent OOM events"
    fi
    
    # Check PostgreSQL OOM score
    local pg_pid pg_oom_score
    pg_pid=$(pgrep -x postgres 2>/dev/null | head -1 || echo "")
    
    if [ -n "$pg_pid" ] && [ -f "/proc/$pg_pid/oom_score_adj" ]; then
        pg_oom_score=$(cat "/proc/$pg_pid/oom_score_adj" 2>/dev/null || echo "unknown")
        echo "  PostgreSQL OOM score adjustment: $pg_oom_score"
        
        if [ "$pg_oom_score" = "-1000" ]; then
            log_ok "PostgreSQL protected from OOM killer"
        else
            log_warn "PostgreSQL not protected (oom_score_adj = $pg_oom_score)"
        fi
    else
        log_warn "Cannot check PostgreSQL OOM protection"
    fi
}

run_full_diagnosis() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         RESTORE PREPARATION - SYSTEM DIAGNOSIS                   ║"
    echo "║         $(date '+%Y-%m-%d %H:%M:%S')                                        ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    
    diagnose_memory
    diagnose_postgresql
    diagnose_disk
    diagnose_oom
    
    log_section "SUMMARY & RECOMMENDATIONS"
    echo
    
    local needs_fix=0
    
    # Check swap
    if (( $(echo "$DIAG_SWAP_TOTAL_GB < $MIN_SWAP_GB" | bc -l) )); then
        log_warn "CREATE SWAP: Run with --swap auto (or --swap 2G for limited disk space)"
        needs_fix=1
    fi
    
    # Check locks
    if [ "${DIAG_LOCKS_OK:-0}" -eq 0 ]; then
        log_warn "TUNE POSTGRESQL: Run with --tune-pg"
        needs_fix=1
    fi
    
    if [ "$needs_fix" -eq 0 ]; then
        echo
        log_ok "System ready for large database restores!"
        echo
        echo "  Recommended restore command:"
        echo "    dbbackup restore cluster <backup.tar.gz> --confirm --jobs 1 --parallel-dbs 1"
    else
        echo
        echo "  To apply all fixes automatically:"
        echo "    sudo $0 --fix"
    fi
    echo
}

#==============================================================================
# FIX FUNCTIONS
#==============================================================================
create_swap() {
    local size="${1:-auto}"
    
    log_section "CREATING SWAP"
    
    # Get available disk space
    local avail_gb
    avail_gb=$(df -BG / | tail -1 | awk '{print $4}' | tr -d 'G')
    
    # Auto-detect optimal swap size based on available space
    # Use 80% of available space, leave 1GB minimum for system
    if [ "$size" = "auto" ]; then
        if [ "$avail_gb" -ge 40 ]; then
            size="32G"
        elif [ "$avail_gb" -ge 20 ]; then
            size="16G"
        elif [ "$avail_gb" -ge 12 ]; then
            size="8G"
        elif [ "$avail_gb" -ge 6 ]; then
            size="4G"
        elif [ "$avail_gb" -ge 4 ]; then
            size="3G"  # 4GB available → 3GB swap
        elif [ "$avail_gb" -ge 3 ]; then
            size="2G"  # 3GB available → 2GB swap
        elif [ "$avail_gb" -ge 2 ]; then
            size="1G"
        else
            log_error "Not enough disk space for swap (only ${avail_gb}GB available, need at least 2GB)"
            return 1
        fi
        log_info "Auto-detected swap size: $size (based on ${avail_gb}GB available)"
    fi
    
    echo "  Requested: $size"
    echo "  Available: ${avail_gb}GB"
    echo
    
    if [ -f "$SWAP_FILE" ]; then
        log_warn "Swap file already exists: $SWAP_FILE"
        swapon --show
        
        read -p "Remove and recreate? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return 0
        fi
        
        swapoff "$SWAP_FILE" 2>/dev/null || true
        rm -f "$SWAP_FILE"
    fi
    
    # Check disk space - need swap + 2GB buffer
    local size_gb="${size//[!0-9]/}"
    
    if [ "$avail_gb" -lt $((size_gb + 2)) ]; then
        # Try smaller size
        local new_size=$((avail_gb - 2))
        if [ "$new_size" -lt 1 ]; then
            log_error "Not enough disk space for swap (only ${avail_gb}GB available)"
            return 1
        fi
        log_warn "Reducing swap to ${new_size}G (not enough space for ${size})"
        size="${new_size}G"
        size_gb="$new_size"
    fi
    
    echo "  Creating ${size} swap file..."
    
    # Use fallocate if available (faster), otherwise dd
    if command -v fallocate &>/dev/null; then
        fallocate -l "$size" "$SWAP_FILE"
    else
        local size_mb=$((size_gb * 1024))
        dd if=/dev/zero of="$SWAP_FILE" bs=1M count="$size_mb" status=progress
    fi
    
    chmod 600 "$SWAP_FILE"
    mkswap "$SWAP_FILE"
    swapon "$SWAP_FILE"
    
    log_ok "Swap created and activated: $size"
    
    # Add to fstab for persistence
    if ! grep -q "$SWAP_FILE" /etc/fstab 2>/dev/null; then
        echo "$SWAP_FILE none swap sw 0 0" >> /etc/fstab
        log_ok "Added to /etc/fstab for persistence"
    fi
    
    swapon --show
}

tune_postgresql() {
    log_section "TUNING POSTGRESQL FOR LARGE RESTORES"
    
    echo
    echo "  Applying low-memory settings for restore:"
    echo "  ────────────────────────────────────────"
    
    local success=0
    local total=6
    
    # Work mem - keep LOW to prevent OOM
    if set_pg_setting "work_mem" "$PG_WORK_MEM"; then
        log_ok "work_mem = $PG_WORK_MEM"
        ((success++))
    else
        log_error "Failed to set work_mem"
    fi
    
    # Maintenance work mem - moderate for index creation
    if set_pg_setting "maintenance_work_mem" "$PG_MAINTENANCE_WORK_MEM"; then
        log_ok "maintenance_work_mem = $PG_MAINTENANCE_WORK_MEM"
        ((success++))
    else
        log_error "Failed to set maintenance_work_mem"
    fi
    
    # Max locks - HIGH for large object restores
    if set_pg_setting "max_locks_per_transaction" "$PG_MAX_LOCKS"; then
        log_ok "max_locks_per_transaction = $PG_MAX_LOCKS"
        ((success++))
    else
        log_error "Failed to set max_locks_per_transaction"
    fi
    
    # Disable parallel workers - prevents memory spikes
    if set_pg_setting "max_parallel_workers" "$PG_MAX_PARALLEL"; then
        log_ok "max_parallel_workers = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed to set max_parallel_workers"
    fi
    
    if set_pg_setting "max_parallel_workers_per_gather" "$PG_MAX_PARALLEL"; then
        log_ok "max_parallel_workers_per_gather = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed to set max_parallel_workers_per_gather"
    fi
    
    if set_pg_setting "max_parallel_maintenance_workers" "$PG_MAX_PARALLEL"; then
        log_ok "max_parallel_maintenance_workers = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed to set max_parallel_maintenance_workers"
    fi
    
    echo
    
    if [ "$success" -eq "$total" ]; then
        log_ok "All settings applied successfully"
    else
        log_warn "Some settings failed ($success/$total)"
    fi
    
    echo
    echo "  Reloading PostgreSQL configuration..."
    reload_pg
    log_ok "Configuration reloaded"
    
    echo
    log_warn "NOTE: max_locks_per_transaction requires PostgreSQL RESTART to take effect"
    echo "       Run: sudo systemctl restart postgresql"
}

enable_oom_protection() {
    log_section "ENABLING OOM PROTECTION"
    
    # Find PostgreSQL processes
    local pg_pids
    pg_pids=$(pgrep -x postgres 2>/dev/null || echo "")
    
    if [ -z "$pg_pids" ]; then
        log_error "No PostgreSQL processes found"
        return 1
    fi
    
    echo
    echo "  Protecting PostgreSQL processes from OOM killer:"
    
    for pid in $pg_pids; do
        if [ -f "/proc/$pid/oom_score_adj" ]; then
            echo -1000 > "/proc/$pid/oom_score_adj" 2>/dev/null || true
            log_ok "Protected PID $pid (oom_score_adj = -1000)"
        fi
    done
    
    # Kernel tuning for overcommit
    echo
    echo "  Tuning kernel memory overcommit:"
    
    if sysctl -w vm.overcommit_memory=2 2>/dev/null; then
        log_ok "vm.overcommit_memory = 2 (strict)"
    fi
    
    if sysctl -w vm.overcommit_ratio=90 2>/dev/null; then
        log_ok "vm.overcommit_ratio = 90"
    fi
    
    # Make persistent
    if ! grep -q "vm.overcommit_memory" /etc/sysctl.conf 2>/dev/null; then
        echo "vm.overcommit_memory = 2" >> /etc/sysctl.conf
        echo "vm.overcommit_ratio = 90" >> /etc/sysctl.conf
        log_ok "Settings persisted to /etc/sysctl.conf"
    fi
}

apply_all_fixes() {
    log_section "APPLYING ALL FIXES"
    
    # Check root
    if [ "$EUID" -ne 0 ]; then
        log_error "Must run as root to apply fixes"
        echo "  Run: sudo $0 --fix"
        exit 1
    fi
    
    create_swap "auto"  # Auto-detect based on available space
    tune_postgresql
    enable_oom_protection
    
    log_section "ALL FIXES APPLIED"
    echo
    log_ok "System is now prepared for large database restores"
    echo
    echo "  Next steps:"
    echo "    1. Restart PostgreSQL: sudo systemctl restart postgresql"
    echo "    2. Run restore: dbbackup restore cluster <backup.tar.gz> --confirm --jobs 1 --parallel-dbs 1"
    echo
}

reset_postgresql() {
    log_section "RESETTING POSTGRESQL TO DEFAULT SETTINGS"
    
    echo
    echo "  Reverting to default PostgreSQL settings..."
    
    sudo -u postgres psql -c "ALTER SYSTEM RESET work_mem;" 2>/dev/null || true
    sudo -u postgres psql -c "ALTER SYSTEM RESET maintenance_work_mem;" 2>/dev/null || true
    sudo -u postgres psql -c "ALTER SYSTEM RESET max_parallel_workers;" 2>/dev/null || true
    sudo -u postgres psql -c "ALTER SYSTEM RESET max_parallel_workers_per_gather;" 2>/dev/null || true
    sudo -u postgres psql -c "ALTER SYSTEM RESET max_parallel_maintenance_workers;" 2>/dev/null || true
    
    reload_pg
    
    log_ok "PostgreSQL settings reset to defaults"
    log_warn "NOTE: max_locks_per_transaction still at $PG_MAX_LOCKS (requires restart to change)"
}

#==============================================================================
# MAIN
#==============================================================================
show_help() {
    echo "RESTORE PREPARATION TOOL v$VERSION"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  (no options)      Run diagnostics only"
    echo "  --fix             Apply all recommended fixes (auto-detects swap size)"
    echo "  --swap SIZE       Create swap file (e.g., --swap 2G, --swap auto)"
    echo "  --tune-pg         Tune PostgreSQL for low-memory restore"
    echo "  --oom-protect     Enable OOM killer protection"
    echo "  --reset-pg        Reset PostgreSQL to default settings"
    echo "  --help            Show this help"
    echo
    echo "PostgreSQL Connection (via environment variables):"
    echo "  PGHOST            PostgreSQL host (default: localhost)"
    echo "  PGPORT            PostgreSQL port (default: 5432)"
    echo "  PGUSER            PostgreSQL user (default: postgres)"
    echo "  PGPASSWORD        PostgreSQL password (default: postgres)"
    echo
    echo "Examples:"
    echo "  $0                    # Diagnose system"
    echo "  sudo $0 --fix         # Apply all fixes (auto swap size)"
    echo "  sudo $0 --swap auto   # Auto-detect swap size based on disk"
    echo "  sudo $0 --swap 2G     # Create 2GB swap (for low disk space)"
    echo
    echo "  # With PostgreSQL connection:"
    echo "  PGHOST=db.example.com PGPASSWORD=secret sudo $0 --tune-pg"
    echo
}

main() {
    case "${1:-}" in
        --help|-h)
            show_help
            ;;
        --fix)
            apply_all_fixes
            ;;
        --swap)
            if [ -z "${2:-}" ]; then
                # Default to auto if no size given
                create_swap "auto"
            else
                create_swap "$2"
            fi
            ;;
        --tune-pg)
            tune_postgresql
            ;;
        --oom-protect)
            enable_oom_protection
            ;;
        --reset-pg)
            reset_postgresql
            ;;
        "")
            run_full_diagnosis
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
