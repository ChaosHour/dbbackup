#!/bin/bash
#
# POSTGRESQL TUNING FOR LARGE DATABASE RESTORES
# ==============================================
# Run as: postgres user
#
# This script tunes PostgreSQL for large restores:
# - Low memory settings (work_mem, maintenance_work_mem)
# - High lock limits (max_locks_per_transaction)
# - Disable parallel workers
#
# Usage:
#   su - postgres -c './prepare_postgres.sh'        # Run diagnostics
#   su - postgres -c './prepare_postgres.sh --fix'  # Apply tuning
#

set -euo pipefail

VERSION="1.0.0"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}ℹ${NC} $1"; }
log_ok()    { echo -e "${GREEN}✓${NC} $1"; }
log_warn()  { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }

# Tuning values for low-memory large restores
PG_WORK_MEM="64MB"
PG_MAINTENANCE_WORK_MEM="256MB"
PG_MAX_LOCKS="65536"
PG_MAX_PARALLEL="0"

#==============================================================================
# CHECK POSTGRES USER
#==============================================================================
check_postgres() {
    if [ "$(whoami)" != "postgres" ]; then
        log_error "This script must be run as postgres user"
        echo "  Run: su - postgres -c '$0'"
        exit 1
    fi
}

#==============================================================================
# GET SETTING
#==============================================================================
get_setting() {
    psql -t -A -c "SHOW $1;" 2>/dev/null || echo "N/A"
}

#==============================================================================
# DIAGNOSE
#==============================================================================
diagnose() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         POSTGRESQL CONFIGURATION                                 ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
    
    echo -e "${CYAN}━━━ CURRENT SETTINGS ━━━${NC}"
    printf "  %-35s %s\n" "work_mem:" "$(get_setting work_mem)"
    printf "  %-35s %s\n" "maintenance_work_mem:" "$(get_setting maintenance_work_mem)"
    printf "  %-35s %s\n" "max_locks_per_transaction:" "$(get_setting max_locks_per_transaction)"
    printf "  %-35s %s\n" "max_connections:" "$(get_setting max_connections)"
    printf "  %-35s %s\n" "max_parallel_workers:" "$(get_setting max_parallel_workers)"
    printf "  %-35s %s\n" "max_parallel_workers_per_gather:" "$(get_setting max_parallel_workers_per_gather)"
    printf "  %-35s %s\n" "max_parallel_maintenance_workers:" "$(get_setting max_parallel_maintenance_workers)"
    printf "  %-35s %s\n" "shared_buffers:" "$(get_setting shared_buffers)"
    echo
    
    # Lock capacity
    local locks=$(get_setting max_locks_per_transaction | tr -d ' ')
    local conns=$(get_setting max_connections | tr -d ' ')
    
    if [[ "$locks" =~ ^[0-9]+$ ]] && [[ "$conns" =~ ^[0-9]+$ ]]; then
        local capacity=$((locks * conns))
        echo "  Lock capacity: $capacity total locks"
        echo
        
        if [ "$locks" -lt 2048 ]; then
            log_error "CRITICAL: max_locks_per_transaction too low ($locks)"
        elif [ "$locks" -lt 8192 ]; then
            log_warn "max_locks_per_transaction may be insufficient ($locks)"
        else
            log_ok "max_locks_per_transaction adequate ($locks)"
        fi
    fi
    
    echo
    echo -e "${CYAN}━━━ RECOMMENDED FOR LARGE RESTORES ━━━${NC}"
    printf "  %-35s %s\n" "work_mem:" "$PG_WORK_MEM (low to prevent OOM)"
    printf "  %-35s %s\n" "maintenance_work_mem:" "$PG_MAINTENANCE_WORK_MEM"
    printf "  %-35s %s\n" "max_locks_per_transaction:" "$PG_MAX_LOCKS (high for BLOBs)"
    printf "  %-35s %s\n" "max_parallel_workers:" "$PG_MAX_PARALLEL (disabled)"
    echo
    
    echo "To apply: $0 --fix"
    echo
}

#==============================================================================
# APPLY TUNING
#==============================================================================
apply_tuning() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         APPLYING POSTGRESQL TUNING                               ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
    
    local success=0
    local total=6
    
    # Work mem - LOW to prevent OOM
    if psql -c "ALTER SYSTEM SET work_mem = '$PG_WORK_MEM';" 2>/dev/null; then
        log_ok "work_mem = $PG_WORK_MEM"
        ((success++))
    else
        log_error "Failed: work_mem"
    fi
    
    # Maintenance work mem
    if psql -c "ALTER SYSTEM SET maintenance_work_mem = '$PG_MAINTENANCE_WORK_MEM';" 2>/dev/null; then
        log_ok "maintenance_work_mem = $PG_MAINTENANCE_WORK_MEM"
        ((success++))
    else
        log_error "Failed: maintenance_work_mem"
    fi
    
    # Max locks - HIGH for BLOB restores
    if psql -c "ALTER SYSTEM SET max_locks_per_transaction = $PG_MAX_LOCKS;" 2>/dev/null; then
        log_ok "max_locks_per_transaction = $PG_MAX_LOCKS"
        ((success++))
    else
        log_error "Failed: max_locks_per_transaction"
    fi
    
    # Disable parallel workers - prevents memory spikes
    if psql -c "ALTER SYSTEM SET max_parallel_workers = $PG_MAX_PARALLEL;" 2>/dev/null; then
        log_ok "max_parallel_workers = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed: max_parallel_workers"
    fi
    
    if psql -c "ALTER SYSTEM SET max_parallel_workers_per_gather = $PG_MAX_PARALLEL;" 2>/dev/null; then
        log_ok "max_parallel_workers_per_gather = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed: max_parallel_workers_per_gather"
    fi
    
    if psql -c "ALTER SYSTEM SET max_parallel_maintenance_workers = $PG_MAX_PARALLEL;" 2>/dev/null; then
        log_ok "max_parallel_maintenance_workers = $PG_MAX_PARALLEL"
        ((success++))
    else
        log_error "Failed: max_parallel_maintenance_workers"
    fi
    
    echo
    
    if [ "$success" -eq "$total" ]; then
        log_ok "All settings applied ($success/$total)"
    else
        log_warn "Some settings failed ($success/$total)"
    fi
    
    # Reload
    echo
    echo "Reloading configuration..."
    psql -c "SELECT pg_reload_conf();" 2>/dev/null && log_ok "Configuration reloaded"
    
    echo
    log_warn "NOTE: max_locks_per_transaction requires PostgreSQL RESTART"
    echo "      Ask admin to run: systemctl restart postgresql"
    echo
    
    # Show new values
    echo -e "${CYAN}━━━ NEW SETTINGS ━━━${NC}"
    printf "  %-35s %s\n" "work_mem:" "$(get_setting work_mem)"
    printf "  %-35s %s\n" "maintenance_work_mem:" "$(get_setting maintenance_work_mem)"
    printf "  %-35s %s\n" "max_locks_per_transaction:" "$(get_setting max_locks_per_transaction) (needs restart)"
    printf "  %-35s %s\n" "max_parallel_workers:" "$(get_setting max_parallel_workers)"
    echo
}

#==============================================================================
# RESET TO DEFAULTS
#==============================================================================
reset_defaults() {
    echo
    echo "Resetting to PostgreSQL defaults..."
    
    psql -c "ALTER SYSTEM RESET work_mem;" 2>/dev/null
    psql -c "ALTER SYSTEM RESET maintenance_work_mem;" 2>/dev/null
    psql -c "ALTER SYSTEM RESET max_parallel_workers;" 2>/dev/null
    psql -c "ALTER SYSTEM RESET max_parallel_workers_per_gather;" 2>/dev/null
    psql -c "ALTER SYSTEM RESET max_parallel_maintenance_workers;" 2>/dev/null
    
    psql -c "SELECT pg_reload_conf();" 2>/dev/null
    
    log_ok "Settings reset to defaults"
    log_warn "NOTE: max_locks_per_transaction still at $PG_MAX_LOCKS (requires restart)"
    echo
}

#==============================================================================
# HELP
#==============================================================================
show_help() {
    echo "POSTGRESQL TUNING v$VERSION"
    echo
    echo "Usage: $0 [OPTION]"
    echo
    echo "Run as postgres user:"
    echo "  su - postgres -c '$0 [OPTION]'"
    echo
    echo "Options:"
    echo "  (none)    Show current settings"
    echo "  --fix     Apply tuning for large restores"
    echo "  --reset   Reset to PostgreSQL defaults"
    echo "  --help    Show this help"
    echo
}

#==============================================================================
# MAIN
#==============================================================================
main() {
    check_postgres
    
    case "${1:-}" in
        --help|-h) show_help ;;
        --fix) apply_tuning ;;
        --reset) reset_defaults ;;
        "") diagnose ;;
        *) log_error "Unknown option: $1"; show_help; exit 1 ;;
    esac
}

main "$@"
