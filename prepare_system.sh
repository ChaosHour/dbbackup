#!/bin/bash
#
# SYSTEM PREPARATION FOR LARGE DATABASE RESTORES
# ===============================================
# Run as: root
#
# This script handles system-level preparation:
# - Swap creation
# - OOM killer protection
# - Kernel tuning
#
# Usage:
#   sudo ./prepare_system.sh           # Run diagnostics
#   sudo ./prepare_system.sh --fix     # Apply all fixes
#   sudo ./prepare_system.sh --swap    # Create swap only
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

#==============================================================================
# CHECK ROOT
#==============================================================================
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "This script must be run as root"
        echo "  Run: sudo $0"
        exit 1
    fi
}

#==============================================================================
# DIAGNOSE
#==============================================================================
diagnose() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         SYSTEM DIAGNOSIS FOR LARGE RESTORES                      ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
    
    # Memory
    echo -e "${CYAN}━━━ MEMORY ━━━${NC}"
    free -h
    echo
    
    # Swap
    echo -e "${CYAN}━━━ SWAP ━━━${NC}"
    swapon --show 2>/dev/null || echo "  No swap configured!"
    echo
    
    # Disk
    echo -e "${CYAN}━━━ DISK SPACE ━━━${NC}"
    df -h / /var/lib/pgsql 2>/dev/null || df -h /
    echo
    
    # OOM
    echo -e "${CYAN}━━━ RECENT OOM KILLS ━━━${NC}"
    dmesg 2>/dev/null | grep -i "out of memory\|oom\|killed process" | tail -5 || echo "  None found"
    echo
    
    # PostgreSQL OOM protection
    echo -e "${CYAN}━━━ POSTGRESQL OOM PROTECTION ━━━${NC}"
    local pg_pid
    pg_pid=$(pgrep -x postgres 2>/dev/null | head -1 || echo "")
    if [ -n "$pg_pid" ] && [ -f "/proc/$pg_pid/oom_score_adj" ]; then
        local score=$(cat "/proc/$pg_pid/oom_score_adj")
        if [ "$score" = "-1000" ]; then
            log_ok "PostgreSQL protected (oom_score_adj = -1000)"
        else
            log_warn "PostgreSQL NOT protected (oom_score_adj = $score)"
        fi
    else
        log_warn "Cannot check PostgreSQL OOM status"
    fi
    echo
    
    # Summary
    echo -e "${CYAN}━━━ RECOMMENDATIONS ━━━${NC}"
    local swap_gb=$(free -g | awk '/^Swap:/ {print $2}')
    local avail_gb=$(df -BG / | tail -1 | awk '{print $4}' | tr -d 'G')
    
    if [ "${swap_gb:-0}" -lt 4 ]; then
        log_warn "Create swap: sudo $0 --swap"
    fi
    
    if [ -n "$pg_pid" ]; then
        local score=$(cat "/proc/$pg_pid/oom_score_adj" 2>/dev/null || echo "0")
        if [ "$score" != "-1000" ]; then
            log_warn "Enable OOM protection: sudo $0 --oom-protect"
        fi
    fi
    
    echo
    echo "To apply all fixes: sudo $0 --fix"
    echo
}

#==============================================================================
# CREATE SWAP
#==============================================================================
create_swap() {
    local SWAP_FILE="/swapfile_dbbackup"
    
    echo -e "${CYAN}━━━ CREATING SWAP ━━━${NC}"
    
    # Get available disk space
    local avail_gb=$(df -BG / | tail -1 | awk '{print $4}' | tr -d 'G')
    
    # Auto-detect size
    local size
    if [ "$avail_gb" -ge 40 ]; then
        size="32G"
    elif [ "$avail_gb" -ge 20 ]; then
        size="16G"
    elif [ "$avail_gb" -ge 12 ]; then
        size="8G"
    elif [ "$avail_gb" -ge 6 ]; then
        size="4G"
    elif [ "$avail_gb" -ge 4 ]; then
        size="3G"
    elif [ "$avail_gb" -ge 3 ]; then
        size="2G"
    elif [ "$avail_gb" -ge 2 ]; then
        size="1G"
    else
        log_error "Not enough disk space (only ${avail_gb}GB available)"
        return 1
    fi
    
    log_info "Auto-detected swap size: $size (${avail_gb}GB disk available)"
    
    if [ -f "$SWAP_FILE" ]; then
        log_warn "Swap file already exists"
        swapon --show
        return 0
    fi
    
    echo "  Creating ${size} swap file..."
    
    if command -v fallocate &>/dev/null; then
        fallocate -l "$size" "$SWAP_FILE"
    else
        local size_mb=$((${size//[!0-9]/} * 1024))
        dd if=/dev/zero of="$SWAP_FILE" bs=1M count="$size_mb" status=progress
    fi
    
    chmod 600 "$SWAP_FILE"
    mkswap "$SWAP_FILE"
    swapon "$SWAP_FILE"
    
    # Persist
    if ! grep -q "$SWAP_FILE" /etc/fstab 2>/dev/null; then
        echo "$SWAP_FILE none swap sw 0 0" >> /etc/fstab
        log_ok "Added to /etc/fstab"
    fi
    
    log_ok "Swap created: $size"
    swapon --show
}

#==============================================================================
# OOM PROTECTION
#==============================================================================
enable_oom_protection() {
    echo -e "${CYAN}━━━ ENABLING OOM PROTECTION ━━━${NC}"
    
    # Protect PostgreSQL
    local pg_pids=$(pgrep -x postgres 2>/dev/null || echo "")
    
    if [ -z "$pg_pids" ]; then
        log_warn "PostgreSQL not running"
    else
        for pid in $pg_pids; do
            if [ -f "/proc/$pid/oom_score_adj" ]; then
                echo -1000 > "/proc/$pid/oom_score_adj" 2>/dev/null || true
            fi
        done
        log_ok "PostgreSQL processes protected"
    fi
    
    # Kernel tuning
    sysctl -w vm.overcommit_memory=2 2>/dev/null && log_ok "vm.overcommit_memory = 2"
    sysctl -w vm.overcommit_ratio=90 2>/dev/null && log_ok "vm.overcommit_ratio = 90"
    
    # Persist
    if ! grep -q "vm.overcommit_memory" /etc/sysctl.conf 2>/dev/null; then
        echo "vm.overcommit_memory = 2" >> /etc/sysctl.conf
        echo "vm.overcommit_ratio = 90" >> /etc/sysctl.conf
        log_ok "Settings persisted to /etc/sysctl.conf"
    fi
}

#==============================================================================
# APPLY ALL FIXES
#==============================================================================
apply_all() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         APPLYING SYSTEM FIXES                                    ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
    
    create_swap
    echo
    enable_oom_protection
    
    echo
    log_ok "System preparation complete!"
    echo
    echo "  Next: Run PostgreSQL tuning as postgres user:"
    echo "    su - postgres -c './prepare_postgres.sh --fix'"
    echo
}

#==============================================================================
# HELP
#==============================================================================
show_help() {
    echo "SYSTEM PREPARATION v$VERSION"
    echo
    echo "Usage: sudo $0 [OPTION]"
    echo
    echo "Options:"
    echo "  (none)        Run diagnostics"
    echo "  --fix         Apply all fixes"
    echo "  --swap        Create swap file only"
    echo "  --oom-protect Enable OOM protection only"
    echo "  --help        Show this help"
    echo
}

#==============================================================================
# MAIN
#==============================================================================
main() {
    check_root
    
    case "${1:-}" in
        --help|-h) show_help ;;
        --fix) apply_all ;;
        --swap) create_swap ;;
        --oom-protect) enable_oom_protection ;;
        "") diagnose ;;
        *) log_error "Unknown option: $1"; show_help; exit 1 ;;
    esac
}

main "$@"
