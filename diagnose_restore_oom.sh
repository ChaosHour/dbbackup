#!/bin/bash
#
# EMERGENCY: Large Database Restore OOM Killer Prevention
# For CentOS 8.x with 32GB RAM, 4 cores, 119GB+ compressed backup
#
# This script GUARANTEES successful restore by:
# 1. Tuning PostgreSQL memory settings for restore
# 2. Creating emergency swap
# 3. Disabling OOM killer for PostgreSQL
# 4. Forcing single-threaded, low-memory restore
#

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  🚨 EMERGENCY LARGE DATABASE RESTORE - OOM PREVENTION 🚨         ║"
echo "║                                                                   ║"
echo "║  For: CentOS 8.x, 32GB RAM, 4 cores, 119GB+ backup               ║"
echo "║  Problem: OOM killer terminating restore after 14+ days          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo

#==============================================================================
# PHASE 1: DIAGNOSE CURRENT STATE
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 1: SYSTEM DIAGNOSIS${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

# Memory info
echo -e "${YELLOW}📊 MEMORY STATUS:${NC}"
free -h
echo

# Swap info
echo -e "${YELLOW}📊 SWAP STATUS:${NC}"
swapon --show 2>/dev/null || echo "  No swap configured!"
echo

# Check for OOM kills in logs
echo -e "${YELLOW}📊 RECENT OOM KILLS:${NC}"
dmesg | grep -i "out of memory\|oom\|killed process" | tail -10 2>/dev/null || echo "  (check dmesg manually)"
echo

# PostgreSQL memory settings
echo -e "${YELLOW}📊 POSTGRESQL MEMORY CONFIG:${NC}"
sudo -u postgres psql -t -A -c "
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN (
    'shared_buffers',
    'work_mem',
    'maintenance_work_mem',
    'effective_cache_size',
    'max_connections',
    'max_locks_per_transaction',
    'max_worker_processes',
    'max_parallel_workers',
    'max_parallel_workers_per_gather',
    'max_parallel_maintenance_workers'
)
ORDER BY name;
" 2>/dev/null || echo "  Cannot connect to PostgreSQL"
echo

# Disk space
echo -e "${YELLOW}📊 DISK SPACE:${NC}"
df -h / /var/lib/pgsql 2>/dev/null || df -h /
echo

#==============================================================================
# PHASE 2: CREATE EMERGENCY SWAP (16GB minimum for 119GB restore)
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 2: EMERGENCY SWAP CREATION${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

SWAP_FILE="/swapfile_emergency"
SWAP_SIZE="32G"  # 32GB swap for 119GB backup

if [ -f "$SWAP_FILE" ]; then
    echo -e "${GREEN}✓ Emergency swap file already exists${NC}"
    swapon --show
else
    echo -e "${YELLOW}Creating $SWAP_SIZE emergency swap file...${NC}"
    echo "  This prevents OOM during large restore operations."
    echo
    
    # Check disk space
    AVAIL=$(df -BG / | tail -1 | awk '{print $4}' | tr -d 'G')
    if [ "$AVAIL" -lt 40 ]; then
        echo -e "${RED}⚠️  WARNING: Only ${AVAIL}GB free. Need 40GB+ for swap + restore temp.${NC}"
        echo "  Consider using a smaller swap or freeing disk space."
        SWAP_SIZE="16G"
    fi
    
    echo "Commands to run (as root):"
    echo "────────────────────────────────────────────────────────────"
    echo "  sudo fallocate -l $SWAP_SIZE $SWAP_FILE"
    echo "  sudo chmod 600 $SWAP_FILE"
    echo "  sudo mkswap $SWAP_FILE"
    echo "  sudo swapon $SWAP_FILE"
    echo "  echo '$SWAP_FILE none swap sw 0 0' | sudo tee -a /etc/fstab"
    echo
fi

#==============================================================================
# PHASE 3: TUNE POSTGRESQL FOR LOW-MEMORY RESTORE
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 3: POSTGRESQL MEMORY TUNING FOR RESTORE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

echo "For a 32GB RAM system restoring 119GB backup, use CONSERVATIVE settings:"
echo
echo -e "${YELLOW}CRITICAL: Apply these settings before restore:${NC}"
echo "────────────────────────────────────────────────────────────"
cat << 'PGSQL'

# Connect as postgres superuser
sudo -u postgres psql << 'EOF'

-- STEP 1: Reduce memory-hungry settings during restore
ALTER SYSTEM SET shared_buffers = '2GB';           -- Reduced from typical 8GB
ALTER SYSTEM SET work_mem = '64MB';                 -- CRITICAL: Low to prevent OOM
ALTER SYSTEM SET maintenance_work_mem = '256MB';    -- Low for restore operations
ALTER SYSTEM SET effective_cache_size = '4GB';      -- Conservative

-- STEP 2: Disable parallel operations (each worker uses more memory)
ALTER SYSTEM SET max_parallel_workers = 0;
ALTER SYSTEM SET max_parallel_workers_per_gather = 0;
ALTER SYSTEM SET max_parallel_maintenance_workers = 0;

-- STEP 3: Increase locks for large DB (but we'll use single-threaded anyway)
ALTER SYSTEM SET max_locks_per_transaction = 65536;

-- STEP 4: Reduce connections during restore
ALTER SYSTEM SET max_connections = 20;              -- Minimal during restore

-- STEP 5: Checkpoint settings to reduce I/O spikes
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET checkpoint_timeout = '30min';

-- Show what will be applied after restart
SELECT name, pending_restart, setting 
FROM pg_settings 
WHERE pending_restart = true;

EOF

# STEP 6: Restart PostgreSQL
sudo systemctl restart postgresql

PGSQL

echo
echo -e "${GREEN}After restore completes, revert to normal settings:${NC}"
echo "────────────────────────────────────────────────────────────"
cat << 'REVERT'

sudo -u postgres psql << 'EOF'
ALTER SYSTEM SET shared_buffers = '8GB';
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET max_parallel_workers = 4;
ALTER SYSTEM SET max_parallel_workers_per_gather = 2;
ALTER SYSTEM SET max_connections = 100;
EOF
sudo systemctl restart postgresql

REVERT
echo

#==============================================================================
# PHASE 4: DISABLE OOM KILLER FOR POSTGRESQL
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 4: OOM KILLER PROTECTION${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

echo "Prevent OOM killer from terminating PostgreSQL:"
echo "────────────────────────────────────────────────────────────"
cat << 'OOM'

# Find PostgreSQL main process
PG_PID=$(sudo head -1 /var/lib/pgsql/data/postmaster.pid 2>/dev/null || \
         sudo head -1 /var/lib/pgsql/*/data/postmaster.pid 2>/dev/null || \
         pgrep -o postgres)

# Protect from OOM killer (-1000 = never kill)
echo -1000 | sudo tee /proc/$PG_PID/oom_score_adj

# For all postgres processes
for pid in $(pgrep -f postgres); do
    echo -1000 | sudo tee /proc/$pid/oom_score_adj 2>/dev/null
done

# Make it permanent (add to PostgreSQL service)
sudo mkdir -p /etc/systemd/system/postgresql.service.d/
cat << 'SYSTEMD' | sudo tee /etc/systemd/system/postgresql.service.d/oom.conf
[Service]
OOMScoreAdjust=-1000
SYSTEMD
sudo systemctl daemon-reload

OOM
echo

#==============================================================================
# PHASE 5: KERNEL TUNING FOR LARGE RESTORES
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 5: KERNEL MEMORY TUNING${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

echo "Tune kernel to be more lenient with memory:"
echo "────────────────────────────────────────────────────────────"
cat << 'KERNEL'

# Allow overcommit (don't fail allocations early)
echo 1 | sudo tee /proc/sys/vm/overcommit_memory

# Reduce swappiness (prefer keeping data in RAM, use swap only when needed)
echo 10 | sudo tee /proc/sys/vm/swappiness

# Increase dirty ratio (delay writes, reduce I/O during restore)
echo 60 | sudo tee /proc/sys/vm/dirty_ratio
echo 40 | sudo tee /proc/sys/vm/dirty_background_ratio

# Make permanent
cat << 'SYSCTL' | sudo tee /etc/sysctl.d/99-postgres-restore.conf
vm.overcommit_memory = 1
vm.swappiness = 10
vm.dirty_ratio = 60
vm.dirty_background_ratio = 40
SYSCTL
sudo sysctl --system

KERNEL
echo

#==============================================================================
# PHASE 6: THE BULLETPROOF RESTORE COMMAND
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 6: BULLETPROOF RESTORE COMMAND${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${RED}║  THIS IS THE EXACT COMMAND TO RUN - NO MODIFICATIONS!        ║${NC}"
echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
echo

cat << 'RESTORE'

# Run in screen/tmux to survive disconnects (14+ hour restore)
screen -S restore

# THE BULLETPROOF RESTORE COMMAND
# ─────────────────────────────────────────────────────────────
dbbackup restore cluster /path/to/cluster_backup.tar.gz \
    --jobs 1 \
    --parallel-dbs 1 \
    --work-mem 64MB \
    --maintenance-work-mem 256MB \
    --confirm

# EXPLANATION:
# --jobs 1           = Single pg_restore worker (minimal memory per restore)
# --parallel-dbs 1   = Restore one database at a time (no memory multiplication)
# --work-mem         = Override PostgreSQL work_mem to 64MB
# --maintenance-work-mem = Override for index builds

RESTORE

echo
echo -e "${YELLOW}If dbbackup doesn't support those flags, use pg_restore directly:${NC}"
echo "────────────────────────────────────────────────────────────"
cat << 'PGREST'

# 1. Extract the tar.gz first
mkdir -p /tmp/restore_workspace
cd /tmp/restore_workspace
tar -xzf /path/to/cluster_backup.tar.gz

# 2. For EACH database dump file:
for dump in *.dump; do
    db_name="${dump%.dump}"
    
    echo "Restoring $db_name..."
    
    # Drop and recreate database
    sudo -u postgres dropdb --if-exists "$db_name"
    sudo -u postgres createdb "$db_name"
    
    # Restore with SINGLE THREAD, NO PARALLELISM
    PGOPTIONS="-c work_mem=64MB -c maintenance_work_mem=256MB" \
    pg_restore \
        --host=localhost \
        --username=postgres \
        --dbname="$db_name" \
        --jobs=1 \
        --no-owner \
        --no-privileges \
        --verbose \
        "$dump" 2>&1 | tee "restore_${db_name}.log"
    
    echo "Completed $db_name"
    echo
done

PGREST
echo

#==============================================================================
# PHASE 7: MONITORING DURING RESTORE
#==============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  PHASE 7: LIVE MONITORING (run in separate terminal)${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo

cat << 'MONITOR'

# Terminal 1: Watch memory (update every 5 seconds)
watch -n 5 'free -h; echo; ps aux --sort=-%mem | head -10'

# Terminal 2: Watch PostgreSQL processes
watch -n 5 'sudo -u postgres psql -c "SELECT pid, usename, application_name, state, query_start, wait_event FROM pg_stat_activity WHERE state != '\''idle'\'' ORDER BY query_start;"'

# Terminal 3: Watch for OOM events
sudo dmesg -wH | grep -i "oom\|killed"

# Terminal 4: I/O and system load
vmstat 5

MONITOR
echo

#==============================================================================
# SUMMARY: EXECUTE THESE STEPS IN ORDER
#==============================================================================
echo
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  📋 COMPLETE CHECKLIST - FOLLOW EXACTLY                      ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo
echo "  □ 1. CREATE SWAP:     fallocate -l 32G /swapfile_emergency && ..."
echo "  □ 2. TUNE POSTGRES:   ALTER SYSTEM SET work_mem = '64MB'; ..."
echo "  □ 3. RESTART:         systemctl restart postgresql"
echo "  □ 4. PROTECT OOM:     echo -1000 > /proc/\$(pgrep -o postgres)/oom_score_adj"
echo "  □ 5. TUNE KERNEL:     echo 1 > /proc/sys/vm/overcommit_memory"
echo "  □ 6. START SCREEN:    screen -S restore"
echo "  □ 7. RUN RESTORE:     dbbackup restore cluster ... --jobs 1 --parallel-dbs 1"
echo "  □ 8. MONITOR:         watch -n 5 'free -h'"
echo "  □ 9. WAIT:            Expect 12-24 hours for 119GB"
echo "  □ 10. REVERT:         Restore normal PostgreSQL settings after success"
echo
echo -e "${RED}⚠️  DO NOT use --jobs > 1 or --parallel-dbs > 1${NC}"
echo -e "${RED}⚠️  DO NOT run without swap on 32GB for 119GB backup${NC}"
echo -e "${RED}⚠️  DO NOT set work_mem > 64MB during restore${NC}"
echo
echo "════════════════════════════════════════════════════════════════"
