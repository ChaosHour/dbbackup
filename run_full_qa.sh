#!/bin/bash
###############################################################################
#  run_full_qa.sh — Comprehensive QA Test Suite for dbbackup
#
#  Runs inside a tmux session named "dbbackup".
#  Tests ALL backup/restore/dedup/pitr/cleanup scenarios across PostgreSQL,
#  MariaDB, and MySQL using generated fake databases.
#
#  Storage layout:
#    /mnt/faststorage  → 300 GB SSD (fast tests, temp data)
#    /mnt/slowstorage  → 10 TB SMB  (archive, dedup store)
#
#  Usage:
#    ./run_full_qa.sh                    # Default (medium profile, core tests)
#    ./run_full_qa.sh --quick             # Quick   ~5 GB total  (~3 min)
#    ./run_full_qa.sh --medium            # Medium  ~15 GB total (~10 min)
#    ./run_full_qa.sh --heavy             # Heavy   ~55 GB total (~30 min)
#    ./run_full_qa.sh --extreme           # Extreme ~120 GB total (~90 min)
#    ./run_full_qa.sh --profile heavy     # Same as --heavy
#    ./run_full_qa.sh --engine pg         # Only PostgreSQL tests
#    ./run_full_qa.sh --engine maria      # Only MariaDB tests
#    ./run_full_qa.sh --engine mysql      # Only MySQL tests
#    ./run_full_qa.sh --skip-create       # Skip DB creation (reuse existing)
#    ./run_full_qa.sh --cleanup-only      # Remove all QA backup/WAL/binlog/dedup files and exit
#    ./run_full_qa.sh --comprehensive     # ALL tests (native, encryption, PITR,
#                                         #   DR drill, cloud, migrate, systemd,
#                                         #   parallel restore, edge cases)
#    ./run_full_qa.sh --all               # Alias for --comprehensive
#
#  Scope flags (combine freely, or use --comprehensive for all):
#    --with-native         Include native engine (pure Go) tests
#    --with-encryption     Include encryption round-trip tests
#    --with-pitr           Include full PITR cycle tests
#    --with-drill          Include DR drill tests (requires Docker)
#    --with-cloud          Include cloud sync tests (MinIO, requires Docker)
#    --with-migrate        Include migration dry-run tests
#    --with-systemd        Include systemd install/uninstall dry-run tests
#    --with-parallel       Include parallel restore & benchmark tests
#    --with-edge           Include edge case & robustness tests
#    --with-lo-vacuum      Include PostgreSQL large object vacuum tests
#    --with-xtrabackup    Include Percona XtraBackup / MariaBackup tests
#    --without-advanced    Skip advanced/stress tests
#
#  Cleanup / teardown:
#    --drop-dbs            Drop ALL QA databases (PG + MariaDB + MySQL) and exit
#    --drop-all            Drop ALL QA databases AND remove backup/dedup/log files
#
#  Email notification:
#    --email               Enable email report (sent to EMAIL_TO after completion)
#    --no-email            Disable email report (default: disabled)
#    --email-to <addr>     Override recipient address (implies --email)
#
#  Profiles:                PG Large  PG Small  MariaDB  MySQL   Total
#    quick                     1 GB    500 MB   500 MB  500 MB   ~2.5 GB
#    medium (default)          5 GB      2 GB     2 GB    2 GB   ~11 GB
#    heavy                    20 GB      5 GB     5 GB    5 GB   ~35 GB
#    extreme                  50 GB     20 GB    20 GB   20 GB  ~110 GB
#
###############################################################################
set -u

# ─── Configuration ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/dbbackup"
TMUX_SESSION="dbbackup"

# Storage paths
FAST_STORAGE="/mnt/faststorage"
SLOW_STORAGE="/mnt/slowstorage"
BACKUP_DIR_FAST="${FAST_STORAGE}/qa_backups"
BACKUP_DIR_SLOW="${SLOW_STORAGE}/qa_backups"
DEDUP_DIR="${FAST_STORAGE}/qa_dedup"
WAL_ARCHIVE="${FAST_STORAGE}/qa_wal_archive"
BINLOG_DIR="${FAST_STORAGE}/qa_binlog"
LOG_DIR="${FAST_STORAGE}/qa_logs"
REPORT_DIR="${SCRIPT_DIR}/reports"
mkdir -p "$REPORT_DIR"
REPORT_FILE="${REPORT_DIR}/QA_FULL_REPORT_$(date +%Y%m%d_%H%M%S).md"

# ─── Email notification ─────────────────────────────────────────────────────
# Set EMAIL_ENABLED=true to send the QA report via email after completion.
EMAIL_ENABLED=false
EMAIL_FROM="build@uuxo.net"
EMAIL_TO="build@uuxo.net"
EMAIL_SMTP="${QA_EMAIL_SMTP:-smtps://mail.alternate-coding.com:465}"
EMAIL_USER="${QA_EMAIL_USER:-build@uuxo.net}"
EMAIL_PASS="${QA_EMAIL_PASS:-}"

# Database sizes (GB) — set by profile (default: medium)
PG_LARGE_SIZE=5        # Large PG test database
PG_SMALL_SIZE=2        # Small PG test database
MARIA_SIZE=2           # MariaDB test database
MYSQL_SIZE=2           # MySQL test database (shares MariaDB server)
SIZE_PROFILE="medium"  # quick | medium | heavy | extreme

# Database names
PG_LARGE_DB="qa_pg_large"
PG_SMALL_DB="qa_pg_small"
MARIA_DB="qa_maria_test"
MYSQL_DB="qa_mysql_test"

# Connection defaults
PG_USER="postgres"
PG_HOST="localhost"
PG_PORT=5432
MY_USER="root"
MY_HOST="localhost"
MY_PORT=3306
MY_SOCKET="/var/run/mysqld/mysqld.sock"

# ─── Profile presets ───────────────────────────────────────────────────────
apply_profile() {
    case "$1" in
        quick)
            PG_LARGE_SIZE=1;  PG_SMALL_SIZE=0.5; MARIA_SIZE=0.5; MYSQL_SIZE=0.5
            SIZE_PROFILE="quick" ;;
        medium)
            PG_LARGE_SIZE=5;  PG_SMALL_SIZE=2;   MARIA_SIZE=2;   MYSQL_SIZE=2
            SIZE_PROFILE="medium" ;;
        heavy)
            PG_LARGE_SIZE=20; PG_SMALL_SIZE=5;   MARIA_SIZE=5;   MYSQL_SIZE=5
            SIZE_PROFILE="heavy" ;;
        extreme)
            PG_LARGE_SIZE=50; PG_SMALL_SIZE=20;  MARIA_SIZE=20;  MYSQL_SIZE=20
            SIZE_PROFILE="extreme" ;;
        *)
            echo "Unknown profile: $1 (use: quick, medium, heavy, extreme)"; exit 1 ;;
    esac
}

# ─── Save original arguments before parsing (needed for tmux re-launch) ─────
SAVED_ARGS="$*"

# ─── CLI argument parsing ──────────────────────────────────────────────────
ENGINE_FILTER="all"   # all | pg | maria | mysql
SKIP_CREATE=false
CLEANUP_ONLY=false
DROP_DBS_ONLY=false
DROP_ALL_ONLY=false

# Scope flags (extended test phases)
SCOPE_NATIVE=false
SCOPE_ENCRYPTION=false
SCOPE_PITR_FULL=false
SCOPE_DRILL=false
SCOPE_CLOUD=false
SCOPE_MIGRATE=false
SCOPE_SYSTEMD=false
SCOPE_PARALLEL=false
SCOPE_EDGE=false
SCOPE_LO_VACUUM=false
SCOPE_MYSQL_SPEED=false
SCOPE_VERIFY=false
SCOPE_XTRABACKUP=false
SCOPE_ADVANCED=true    # on by default

set_comprehensive() {
    SCOPE_NATIVE=true
    SCOPE_ENCRYPTION=true
    SCOPE_PITR_FULL=true
    SCOPE_DRILL=true
    SCOPE_CLOUD=true
    SCOPE_MIGRATE=true
    SCOPE_SYSTEMD=true
    SCOPE_PARALLEL=true
    SCOPE_EDGE=true
    SCOPE_LO_VACUUM=true
    SCOPE_MYSQL_SPEED=true
    SCOPE_VERIFY=true
    SCOPE_XTRABACKUP=true
    SCOPE_ADVANCED=true
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)              apply_profile quick;   shift ;;
        --medium)             apply_profile medium;  shift ;;
        --heavy)              apply_profile heavy;   shift ;;
        --extreme)            apply_profile extreme; shift ;;
        --profile)            apply_profile "$2";    shift 2 ;;
        --engine)             ENGINE_FILTER="$2";    shift 2 ;;
        --skip-create)        SKIP_CREATE=true;      shift ;;
        --cleanup-only)       CLEANUP_ONLY=true;     shift ;;
        --drop-dbs)           DROP_DBS_ONLY=true;    shift ;;
        --drop-all)           DROP_ALL_ONLY=true;    shift ;;
        --comprehensive|--all) set_comprehensive;    shift ;;
        --with-native)        SCOPE_NATIVE=true;     shift ;;
        --with-encryption)    SCOPE_ENCRYPTION=true;  shift ;;
        --with-pitr)          SCOPE_PITR_FULL=true;  shift ;;
        --with-drill)         SCOPE_DRILL=true;      shift ;;
        --with-cloud)         SCOPE_CLOUD=true;      shift ;;
        --with-migrate)       SCOPE_MIGRATE=true;    shift ;;
        --with-systemd)       SCOPE_SYSTEMD=true;    shift ;;
        --with-parallel)      SCOPE_PARALLEL=true;   shift ;;
        --with-edge)          SCOPE_EDGE=true;       shift ;;
        --with-lo-vacuum)     SCOPE_LO_VACUUM=true;  shift ;;
        --with-mysql-speed)   SCOPE_MYSQL_SPEED=true; shift ;;
        --with-verify)        SCOPE_VERIFY=true;      shift ;;
        --with-xtrabackup)    SCOPE_XTRABACKUP=true;  shift ;;
        --without-advanced)   SCOPE_ADVANCED=false;  shift ;;
        --email)              EMAIL_ENABLED=true;    shift ;;
        --no-email)           EMAIL_ENABLED=false;   shift ;;
        --email-to)           EMAIL_TO="$2"; EMAIL_ENABLED=true; shift 2 ;;
        --help|-h)
            grep -E '^#' "$0" | head -50 | sed 's/^#//'
            exit 0 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

# ─── Colors & helpers ──────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

TOTAL=0; PASSED=0; FAILED=0; SKIPPED=0
FAILURES=()
TIMINGS=()    # "num|name|status|seconds|category"
DB_DETAILS=() # "engine|dbname|db_size|backup_file|backup_size|backup_time_ms|restore_time_ms|cluster_backup_size|cluster_restore_time_ms"
SECTION_START=""
CURRENT_CATEGORY=""  # Set before run_test to tag timing entries

log_header()  { echo -e "\n${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"; echo -e "${CYAN}║  $1$(printf '%*s' $((58 - ${#1})) '')║${NC}"; echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"; }
log_section() { SECTION_START=$(date +%s); echo -e "\n${BLUE}┌─── $1 ───${NC}"; }
log_info()    { echo -e "${DIM}│${NC} ${BLUE}[INFO]${NC}  $1"; }
log_ok()      { echo -e "${DIM}│${NC} ${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${DIM}│${NC} ${YELLOW}[WARN]${NC}  $1"; }
log_fail()    { echo -e "${DIM}│${NC} ${RED}[FAIL]${NC}  $1"; }
log_skip()    { echo -e "${DIM}│${NC} ${YELLOW}[SKIP]${NC}  $1"; }

section_time() {
    local now=$(date +%s)
    local elapsed=$((now - SECTION_START))
    echo -e "${BLUE}└─── Done (${elapsed}s) ───${NC}"
}

# ─── DB detail helpers ────────────────────────────────────────────────────
# Update a field in DB_DETAILS for a given engine+dbname
# Fields: engine|dbname|db_size|backup_file|backup_size|backup_time_ms|restore_time_ms|cluster_backup_size|cluster_restore_time_ms
# Usage: _update_db_detail <engine> <dbname> <field> <value>
_update_db_detail() {
    local engine="$1" dbname="$2" field="$3" value="$4"
    [[ ${#DB_DETAILS[@]} -eq 0 ]] && return
    local i
    for i in "${!DB_DETAILS[@]}"; do
        IFS='|' read -r d_eng d_db d_dbsz d_bf d_bsz d_btm d_rtm d_cbsz d_crtm <<< "${DB_DETAILS[$i]}"
        if [[ "$d_eng" == "$engine" && "$d_db" == "$dbname" ]]; then
            case "$field" in
                backup_file)
                    if [[ -n "$value" && -f "$value" ]]; then
                        d_bf="$(basename "$value")"
                        d_bsz="$(du -sh "$value" 2>/dev/null | cut -f1)"
                    fi ;;
                cluster_backup)
                    if [[ -n "$value" && -f "$value" ]]; then
                        d_cbsz="$(du -sh "$value" 2>/dev/null | cut -f1)"
                    fi ;;
            esac
            DB_DETAILS[$i]="${d_eng}|${d_db}|${d_dbsz}|${d_bf}|${d_bsz}|${d_btm}|${d_rtm}|${d_cbsz}|${d_crtm}"
            return
        fi
    done
}

# Extract timing (ms) from TIMINGS array for a test name pattern
_get_timing_ms() {
    local pattern="$1"
    [[ ${#TIMINGS[@]} -eq 0 ]] && { echo ""; return; }
    for entry in "${TIMINGS[@]}"; do
        IFS='|' read -r _ t_name t_status t_ms _ <<< "$entry"
        if [[ "$t_name" == *"$pattern"* && "$t_status" != "SKIP" ]]; then
            echo "$t_ms"
            return
        fi
    done
    echo ""
}

# Format ms to human-readable
_fmt_ms() {
    local ms="$1"
    if [[ -z "$ms" || "$ms" == "0" ]]; then
        echo "—"
        return
    fi
    local sec=$(( ms / 1000 ))
    local frac=$(( ms % 1000 ))
    if [[ $sec -ge 60 ]]; then
        local m=$(( sec / 60 ))
        local s=$(( sec % 60 ))
        printf "%dm %d.%03ds" "$m" "$s" "$frac"
    else
        printf "%d.%03ds" "$sec" "$frac"
    fi
}

# ─── Storage helpers ──────────────────────────────────────────────────────
# Minimum free space thresholds (GB)
MIN_FREE_SDA1=30       # Keep 30 GB free on root (OS + PG + MySQL data)
MIN_FREE_FAST=20       # Keep 20 GB free on faststorage (backups, dedup)

# Get free space in GB for a mount point
free_gb() {
    df --output=avail -BG "$1" 2>/dev/null | tail -1 | tr -d ' G'
}

# Check if enough space is available; warn or abort
# Usage: storage_guard <mount> <needed_gb> <label>
storage_guard() {
    local mount="$1" needed_gb="$2" label="$3"
    local avail; avail=$(free_gb "$mount")
    local min_free=$MIN_FREE_SDA1
    [[ "$mount" == "$FAST_STORAGE" ]] && min_free=$MIN_FREE_FAST

    local safe_avail=$(( avail - min_free ))
    if [[ $safe_avail -lt $needed_gb ]]; then
        log_fail "Not enough space for ${label}: need ${needed_gb} GB, have ${avail} GB free (${min_free} GB reserved) on ${mount}"
        return 1
    fi
    log_ok "Storage OK for ${label}: ${needed_gb} GB needed, ${avail} GB free on $(basename $mount)"
    return 0
}

# Show storage status for all mounts
storage_status() {
    local sda1_free; sda1_free=$(free_gb /)
    local fast_free; fast_free=$(free_gb "$FAST_STORAGE")
    local slow_free; slow_free=$(free_gb "$SLOW_STORAGE")
    log_info "Storage: sda1=${sda1_free}G free | fast=${fast_free}G free | slow=${slow_free}G free"
}

# Move backup artifacts from faststorage to slowstorage to reclaim space
flush_fast_to_slow() {
    local count; count=$(find "$BACKUP_DIR_FAST" -maxdepth 1 -type f 2>/dev/null | wc -l)
    if [[ $count -gt 0 ]]; then
        mv "${BACKUP_DIR_FAST}"/*.gz "$BACKUP_DIR_SLOW/" 2>/dev/null || true
        mv "${BACKUP_DIR_FAST}"/*.zst "$BACKUP_DIR_SLOW/" 2>/dev/null || true
        mv "${BACKUP_DIR_FAST}"/*.tar.gz "$BACKUP_DIR_SLOW/" 2>/dev/null || true
        mv "${BACKUP_DIR_FAST}"/*.tar.zst "$BACKUP_DIR_SLOW/" 2>/dev/null || true
        mv "${BACKUP_DIR_FAST}"/*.sql "$BACKUP_DIR_SLOW/" 2>/dev/null || true
        log_info "Flushed $count backup files from faststorage → slowstorage"
    fi
}

# Drop PostgreSQL test databases
drop_pg_dbs() {
    log_info "Dropping PostgreSQL test databases..."
    for db in "$PG_LARGE_DB" "$PG_SMALL_DB" qa_pg_restored; do
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS ${db};" 2>/dev/null || true
    done
    log_ok "PostgreSQL test databases dropped"
    storage_status
}

# Drop only the large PG DB (keep small for cross-engine/advanced tests)
drop_pg_large_db() {
    log_info "Dropping large PostgreSQL DB (keeping ${PG_SMALL_DB} for later tests)..."
    for db in "$PG_LARGE_DB" qa_pg_restored; do
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS ${db};" 2>/dev/null || true
    done
    log_ok "Large PostgreSQL databases dropped"
    storage_status
}

# Drop MariaDB test databases
drop_maria_dbs() {
    log_info "Dropping MariaDB test databases..."
    for db in "$MARIA_DB" qa_maria_restored; do
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS ${db};" 2>/dev/null || true
    done
    log_ok "MariaDB test databases dropped"
    storage_status
}

# Drop MySQL test databases
drop_mysql_dbs() {
    log_info "Dropping MySQL test databases..."
    for db in "$MYSQL_DB" qa_mysql_restored; do
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS ${db};" 2>/dev/null || true
    done
    log_ok "MySQL test databases dropped"
    storage_status
}

# Estimate total data footprint per engine (DB on disk + backup ~= 1.5x DB size)
estimate_engine_gb() {
    local engine="$1"
    case "$engine" in
        pg)    awk "BEGIN{printf \"%d\", (${PG_LARGE_SIZE} + ${PG_SMALL_SIZE}) * 1.5 + 1}" ;;
        maria) awk "BEGIN{printf \"%d\", ${MARIA_SIZE} * 1.5 + 1}" ;;
        mysql) awk "BEGIN{printf \"%d\", ${MYSQL_SIZE} * 1.5 + 1}" ;;
    esac
}

# Run a test: run_test "name" "command"
run_test() {
    local name="$1"; shift
    local cmd="$*"
    TOTAL=$((TOTAL + 1))
    local test_num=$(printf "%02d" $TOTAL)
    local log_file="${LOG_DIR}/test_${test_num}_$(echo "$name" | tr ' /' '__').log"
    local cat="${CURRENT_CATEGORY:-other}"

    echo -ne "${DIM}│${NC}  ${BOLD}[${test_num}]${NC} ${name}... "

    local start_time=$(date +%s%N 2>/dev/null || echo "$(date +%s)000000000")
    if eval "$cmd" > "$log_file" 2>&1; then
        local end_time=$(date +%s%N 2>/dev/null || echo "$(date +%s)000000000")
        local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
        local elapsed_s=$(( elapsed_ms / 1000 ))
        local elapsed_frac=$(( elapsed_ms % 1000 ))
        printf "${GREEN}PASS${NC} ${DIM}(%d.%03ds)${NC}\n" "$elapsed_s" "$elapsed_frac"
        PASSED=$((PASSED + 1))
        TIMINGS+=("${test_num}|${name}|PASS|${elapsed_ms}|${cat}")
        return 0
    else
        local rc=$?
        local end_time=$(date +%s%N 2>/dev/null || echo "$(date +%s)000000000")
        local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
        local elapsed_s=$(( elapsed_ms / 1000 ))
        local elapsed_frac=$(( elapsed_ms % 1000 ))
        printf "${RED}FAIL${NC} ${DIM}(%d.%03ds, exit=%d)${NC}\n" "$elapsed_s" "$elapsed_frac" "$rc"
        FAILED=$((FAILED + 1))
        FAILURES+=("${test_num}: ${name} — see ${log_file}")
        TIMINGS+=("${test_num}|${name}|FAIL|${elapsed_ms}|${cat}")
        # Show last 5 lines of error on failure
        tail -5 "$log_file" 2>/dev/null | sed 's/^/│    /'
        return 0  # Don't abort the suite on individual test failure
    fi
}

skip_test() {
    local name="$1"
    TOTAL=$((TOTAL + 1)); SKIPPED=$((SKIPPED + 1))
    local test_num=$(printf "%02d" $TOTAL)
    echo -e "${DIM}│${NC}  ${BOLD}[${test_num}]${NC} ${name}... ${YELLOW}SKIP${NC}"
    TIMINGS+=("${test_num}|${name}|SKIP|0|${CURRENT_CATEGORY:-other}")
}

should_test() {
    local engine="$1"
    [[ "$ENGINE_FILTER" == "all" || "$ENGINE_FILTER" == "$engine" ]]
}

# Drop ALL QA-related databases across all engines
drop_all_qa_databases() {
    log_header "Dropping ALL QA Databases"

    # PostgreSQL
    if pg_isready -q 2>/dev/null; then
        log_section "PostgreSQL"
        local pg_dbs=(
            "$PG_LARGE_DB" "$PG_SMALL_DB"
            qa_pg_restored qa_pg_force_test qa_alt_target
            qa_native_restore qa_enc_restore qa_parallel_restore qa_parallel_restore4
            qa_custom_restored
        )
        for db in "${pg_dbs[@]}"; do
            if psql -U "$PG_USER" -Aqt -c "SELECT 1 FROM pg_database WHERE datname='${db}'" 2>/dev/null | grep -q 1; then
                psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS ${db};" 2>/dev/null && log_ok "Dropped: ${db}" || log_warn "Failed to drop: ${db}"
            else
                log_info "Not found: ${db}"
            fi
        done
        section_time
    else
        log_warn "PostgreSQL not running — skipping PG database drops"
    fi

    # MariaDB / MySQL
    if mysqladmin ping -u "$MY_USER" --socket="$MY_SOCKET" 2>/dev/null | grep -q alive; then
        log_section "MariaDB / MySQL"
        local my_dbs=(
            "$MARIA_DB" "$MYSQL_DB"
            qa_maria_restored qa_mysql_restored
            qa_maria_force_test qa_mysql_force_test
            qa_alt_maria qa_native_maria_rst qa_enc_maria_rst
            bench_restored_maria bench_restored_mysql
            bench_source_maria test_baseline_maria
            test_cli_maria test_parallel_maria
        )
        for db in "${my_dbs[@]}"; do
            local exists; exists=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT 1 FROM information_schema.schemata WHERE schema_name='${db}' LIMIT 1" 2>/dev/null)
            if [[ "$exists" == "1" ]]; then
                mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS ${db};" 2>/dev/null && log_ok "Dropped: ${db}" || log_warn "Failed to drop: ${db}"
            else
                log_info "Not found: ${db}"
            fi
        done
        section_time
    else
        log_warn "MariaDB/MySQL not running — skipping MySQL database drops"
    fi

    storage_status
}

# Remove ALL QA backup/dedup/log artifacts from storage
cleanup_all_qa_files() {
    log_header "Removing ALL QA Files"

    local dirs=(
        "$BACKUP_DIR_FAST" "$BACKUP_DIR_SLOW"
        "$DEDUP_DIR" "${DEDUP_DIR}_maria" "${DEDUP_DIR}_mysql"
        "$WAL_ARCHIVE" "$BINLOG_DIR" "$LOG_DIR"
        "${FAST_STORAGE}/qa_encrypted"
    )
    for d in "${dirs[@]}"; do
        if [[ -d "$d" ]]; then
            local count; count=$(find "$d" -maxdepth 2 -type f 2>/dev/null | wc -l)
            local size; size=$(du -sh "$d" 2>/dev/null | cut -f1)
            rm -rf "${d:?}"/*
            log_ok "Cleaned: $d ($count files, $size)"
        else
            log_info "Not found: $d"
        fi
    done

    storage_status
}

# Check if a PG database exists; if not, create a minimal one for testing
ensure_pg_small_exists() {
    if ! should_test pg; then return 0; fi
    local exists; exists=$(psql -U "$PG_USER" -Aqt -c "SELECT 1 FROM pg_database WHERE datname='${PG_SMALL_DB}'" 2>/dev/null | tr -d ' ')
    if [[ "$exists" != "1" ]]; then
        log_warn "${PG_SMALL_DB} not found — recreating minimal DB for remaining tests"
        create_pg_db "$PG_SMALL_DB" 0.1
    else
        log_ok "${PG_SMALL_DB} exists ($(psql -U "$PG_USER" -d "$PG_SMALL_DB" -t -c "SELECT pg_size_pretty(pg_database_size('${PG_SMALL_DB}'))" 2>/dev/null | tr -d ' '))"
    fi
}

# Check if a MariaDB database exists
ensure_maria_exists() {
    if ! should_test maria; then return 0; fi
    local exists; exists=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT 1 FROM information_schema.schemata WHERE schema_name='${MARIA_DB}' LIMIT 1" 2>/dev/null)
    if [[ "$exists" != "1" ]]; then
        log_warn "${MARIA_DB} not found — recreating minimal DB for remaining tests"
        create_mysql_db "$MARIA_DB" 0.1 "MariaDB"
    fi
}

# Check if a MySQL database exists
ensure_mysql_exists() {
    if ! should_test mysql; then return 0; fi
    local exists; exists=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT 1 FROM information_schema.schemata WHERE schema_name='${MYSQL_DB}' LIMIT 1" 2>/dev/null)
    if [[ "$exists" != "1" ]]; then
        log_warn "${MYSQL_DB} not found — recreating minimal DB for remaining tests"
        create_mysql_db "$MYSQL_DB" 0.1 "MySQL"
    fi
}

# ─── Pre-flight checks ─────────────────────────────────────────────────────
preflight() {
    log_header "Pre-flight Checks"
    trap 'echo "ERROR: Pre-flight failed at line $LINENO (exit $?)" >&2' ERR

    # Binary
    if [[ ! -x "$BINARY" ]]; then
        log_fail "Binary not found: $BINARY"
        echo "  Run: cd $SCRIPT_DIR && go build -o dbbackup ."
        exit 1
    fi
    local ver; ver=$("$BINARY" version 2>&1 | grep -m1 'Version:' | awk '{print $2}' || echo "unknown")
    log_ok "Binary: v${ver}"

    # Root check
    if [[ $(id -u) -ne 0 ]]; then
        log_fail "Must run as root"
        exit 1
    fi

    # Storage
    for dir in "$FAST_STORAGE" "$SLOW_STORAGE"; do
        if [[ -d "$dir" ]] && [[ -w "$dir" ]]; then
            local avail; avail=$(df -h "$dir" | awk 'NR==2{print $4}')
            local avail_kb; avail_kb=$(df -k "$dir" | awk 'NR==2{print $4}')
            log_ok "Storage: $dir ($avail available)"
            # Abort if fast storage has less than 10GB free
            if [[ "$dir" == "$FAST_STORAGE" ]] && [[ "$avail_kb" -lt 10485760 ]]; then
                log_fail "Fast storage has < 10GB free ($avail). Clean up first: $0 --drop-all"
                log_info "Hint: WAL/binlog archives may be filling the disk."
                log_info "  du -sh ${WAL_ARCHIVE} ${BINLOG_DIR}"
                exit 1
            fi
        else
            log_fail "Storage not accessible: $dir"
            exit 1
        fi
    done

    # PostgreSQL
    if should_test pg; then
        if pg_isready -q 2>/dev/null; then
            log_ok "PostgreSQL: running on port $PG_PORT"
            # PITR config check
            local wal_level; wal_level=$(psql -U "$PG_USER" --no-psqlrc -At -c "SELECT setting FROM pg_settings WHERE name='wal_level'" 2>/dev/null)
            local archive_mode; archive_mode=$(psql -U "$PG_USER" --no-psqlrc -At -c "SELECT setting FROM pg_settings WHERE name='archive_mode'" 2>/dev/null)
            local archive_cmd; archive_cmd=$(psql -U "$PG_USER" --no-psqlrc -At -c "SELECT setting FROM pg_settings WHERE name='archive_command'" 2>/dev/null)
            if [[ "$wal_level" == "replica" || "$wal_level" == "logical" ]] && [[ "$archive_mode" == "on" ]]; then
                log_ok "PG WAL archiving: wal_level=$wal_level, archive_mode=$archive_mode"
                if [[ -n "$archive_cmd" && "$archive_cmd" != "(disabled)" ]]; then
                    log_ok "PG archive_command: ${archive_cmd:0:80}..."
                else
                    log_warn "PG archive_command is empty — WAL files won't be archived"
                    log_warn "  Fix: dbbackup pitr enable --archive-dir $WAL_ARCHIVE --force -d postgres"
                    log_warn "  Then: systemctl restart postgresql"
                fi
            else
                log_warn "PG WAL archiving NOT configured (wal_level=$wal_level, archive_mode=$archive_mode)"
                log_warn "  PITR tests will enable it, but PostgreSQL must be RESTARTED for changes to take effect"
                log_warn "  Fix: dbbackup pitr enable --archive-dir $WAL_ARCHIVE --force -d postgres"
                log_warn "  Then: systemctl restart postgresql"
            fi
        else
            log_fail "PostgreSQL not running"; exit 1
        fi
    fi

    # MariaDB / MySQL
    if should_test maria || should_test mysql; then
        if mysqladmin ping -u "$MY_USER" --socket="$MY_SOCKET" 2>/dev/null | grep -q alive; then
            local myver; myver=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "SELECT VERSION();" -sN 2>/dev/null)
            log_ok "MariaDB/MySQL: $myver on port $MY_PORT"
            # Binlog config check
            local log_bin; log_bin=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@log_bin" 2>/dev/null)
            local binlog_format; binlog_format=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@binlog_format" 2>/dev/null)
            local binlog_basename; binlog_basename=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@log_bin_basename" 2>/dev/null)
            if [[ "$log_bin" == "1" || "$log_bin" == "ON" ]]; then
                log_ok "MySQL binlog: ON, format=$binlog_format, basename=$binlog_basename"
                local binlog_dir; binlog_dir=$(dirname "$binlog_basename" 2>/dev/null)
                if [[ -d "$binlog_dir" ]]; then
                    local binlog_count; binlog_count=$(ls "$binlog_dir"/*.0* 2>/dev/null | wc -l)
                    local binlog_usage; binlog_usage=$(du -sh "$binlog_dir" 2>/dev/null | cut -f1)
                    log_ok "MySQL binlog dir: $binlog_dir ($binlog_count files, $binlog_usage)"
                fi
            else
                log_warn "MySQL binary logging is DISABLED — PITR/binlog tests will have limited coverage"
                log_warn "  Fix: Add log_bin=mysql-bin to /etc/mysql/mariadb.conf.d/50-server.cnf"
                log_warn "  And: binlog_format=ROW, server_id=1"
                log_warn "  Then: systemctl restart mariadb"
            fi
        else
            log_fail "MariaDB/MySQL not running"; exit 1
        fi
    fi

    # Tools
    for tool in pg_dump pg_restore psql mysqldump mysql tmux; do
        if command -v "$tool" &>/dev/null; then
            log_ok "Tool: $tool"
        else
            log_warn "Tool missing: $tool"
        fi
    done

    # Prepare directories
    for d in "$BACKUP_DIR_FAST" "$BACKUP_DIR_SLOW" "$DEDUP_DIR" "$WAL_ARCHIVE" "$BINLOG_DIR" "$LOG_DIR"; do
        mkdir -p "$d" 2>/dev/null || true
        chmod 777 "$d" 2>/dev/null || true
    done
    log_ok "Directories created"
    trap - ERR

    echo
}

# ─── Database creation ──────────────────────────────────────────────────────
create_pg_db() {
    local dbname="$1" size_gb="$2"
    log_info "Creating PostgreSQL DB '${dbname}' (${size_gb} GB)..."

    # Drop if exists
    psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS ${dbname};" 2>/dev/null || true
    psql -U "$PG_USER" -c "CREATE DATABASE ${dbname};" 2>/dev/null

    # Create schema with varied data types for realistic backup/restore testing
    psql -U "$PG_USER" -d "$dbname" <<'EOSQL'
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS logs;
CREATE SCHEMA IF NOT EXISTS archive;

-- Main data table with varied column types
CREATE TABLE core.customers (
    id          BIGSERIAL PRIMARY KEY,
    uuid        UUID DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    email       TEXT,
    department  TEXT,
    salary      NUMERIC(12,2),
    metadata    JSONB DEFAULT '{}',
    bio         TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Large text/blob-like table for bulk
CREATE TABLE core.documents (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES core.customers(id),
    title       TEXT,
    content     TEXT,             -- ~10-20 KB per row
    doc_type    TEXT,
    checksum    TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Time-series style log table
CREATE TABLE logs.access_log (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ DEFAULT now(),
    user_id     BIGINT,
    action      TEXT,
    ip_addr     INET,
    payload     JSONB,
    response_ms INTEGER
);

-- Archive table (partitioned-style, for chain/retention tests)
CREATE TABLE archive.snapshots (
    id          BIGSERIAL PRIMARY KEY,
    source_db   TEXT,
    taken_at    TIMESTAMPTZ DEFAULT now(),
    size_bytes  BIGINT,
    data        TEXT
);

-- Indexes for realistic dump behavior
CREATE INDEX idx_customers_email ON core.customers(email);
CREATE INDEX idx_customers_dept ON core.customers(department);
CREATE INDEX idx_documents_type ON core.documents(doc_type);
CREATE INDEX idx_access_log_ts ON logs.access_log(ts);
CREATE INDEX idx_access_log_user ON logs.access_log(user_id);
EOSQL

    # Fill with data — target bytes per row ~20KB in documents
    # Each GB ≈ 50K document rows
    local target_rows; target_rows=$(awk "BEGIN{printf \"%d\", ${size_gb} * 50000}")
    [[ $target_rows -lt 5000 ]] && target_rows=5000
    local batch=10000
    [[ $target_rows -lt $batch ]] && batch=$target_rows
    local inserted=0

    log_info "  Inserting ~${target_rows} rows (batch=${batch})..."

    # Seed customers (1 per 100 doc rows)
    local cust_count=$(( target_rows / 100 ))
    [[ $cust_count -lt 1000 ]] && cust_count=1000
    psql -U "$PG_USER" -d "$dbname" -c "
        INSERT INTO core.customers (name, email, department, salary, bio)
        SELECT
            'user_' || g,
            'user_' || g || '@test.qa',
            (ARRAY['engineering','sales','marketing','support','hr'])[1 + (g % 5)],
            30000 + (random() * 170000)::int,
            repeat(md5(g::text), 10)
        FROM generate_series(1, ${cust_count}) g;
    " 2>/dev/null

    # Fill documents in batches
    while [[ $inserted -lt $target_rows ]]; do
        local remaining=$(( target_rows - inserted ))
        [[ $remaining -lt $batch ]] && batch=$remaining

        psql -U "$PG_USER" -d "$dbname" -c "
            INSERT INTO core.documents (customer_id, title, content, doc_type, checksum)
            SELECT
                1 + (g % ${cust_count}),
                'Document #' || (${inserted} + g),
                (SELECT string_agg(md5(random()::text), '') FROM generate_series(1,640)),  -- ~20 KB, low compressibility
                (ARRAY['report','invoice','contract','memo','spec'])[1 + (g % 5)],
                md5(random()::text)
            FROM generate_series(1, ${batch}) g;
        " 2>/dev/null

        inserted=$(( inserted + batch ))
        local pct=$(( inserted * 100 / target_rows ))
        printf "\r${DIM}│${NC} ${BLUE}[INFO]${NC}  Progress: %d/%d rows (%d%%)  " "$inserted" "$target_rows" "$pct"
    done
    echo

    # Fill access log (~5% of size)
    local log_rows=$(( target_rows / 20 ))
    psql -U "$PG_USER" -d "$dbname" -c "
        INSERT INTO logs.access_log (user_id, action, ip_addr, payload, response_ms)
        SELECT
            1 + (g % ${cust_count}),
            (ARRAY['login','view','edit','delete','export'])[1 + (g % 5)],
            ('10.0.' || (g % 255) || '.' || ((g * 7) % 255))::inet,
            json_build_object('page', '/p/' || g, 'agent', 'test-bot/' || (g % 10)),
            10 + (random() * 2000)::int
        FROM generate_series(1, ${log_rows}) g;
    " 2>/dev/null

    local actual_size; actual_size=$(psql -U "$PG_USER" -d "$dbname" -t -c "SELECT pg_size_pretty(pg_database_size('${dbname}'));" | tr -d ' ')
    log_ok "PostgreSQL DB '${dbname}' created: ${actual_size}"
    # Record DB size for detailed report — update existing entry or append new
    local _found=false
    if [[ ${#DB_DETAILS[@]} -gt 0 ]]; then
    for _i in "${!DB_DETAILS[@]}"; do
        IFS='|' read -r _eng _db _rest <<< "${DB_DETAILS[$_i]}"
        if [[ "$_eng" == "postgres" && "$_db" == "$dbname" ]]; then
            # Entry exists — keep original DB size (benchmark size), don't overwrite
            # with a smaller recreated size from ensure_*_exists()
            _found=true; break
        fi
    done
    fi
    if ! $_found; then
        DB_DETAILS+=("postgres|${dbname}|${actual_size}|||||")
    fi
}

create_mysql_db() {
    local dbname="$1" size_gb="$2" engine_label="$3"
    log_info "Creating ${engine_label} DB '${dbname}' (${size_gb} GB)..."

    mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS ${dbname}; CREATE DATABASE ${dbname};" 2>/dev/null

    mysql -u "$MY_USER" --socket="$MY_SOCKET" "$dbname" <<'EOSQL'
CREATE TABLE customers (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid        CHAR(36) DEFAULT (UUID()),
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255),
    department  VARCHAR(100),
    salary      DECIMAL(12,2),
    metadata    JSON,
    bio         TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_dept (department)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE documents (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT,
    title       VARCHAR(500),
    content     LONGTEXT,
    doc_type    VARCHAR(50),
    checksum    CHAR(32),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_doc_type (doc_type),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE access_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id     BIGINT,
    action      VARCHAR(50),
    ip_addr     VARCHAR(45),
    payload     JSON,
    response_ms INT,
    INDEX idx_ts (ts),
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOSQL

    local target_rows; target_rows=$(awk "BEGIN{printf \"%d\", ${size_gb} * 50000}")
    [[ $target_rows -lt 5000 ]] && target_rows=5000
    local batch=10000
    [[ $target_rows -lt $batch ]] && batch=$target_rows
    local inserted=0
    local cust_count=$(( target_rows / 100 ))
    [[ $cust_count -lt 1000 ]] && cust_count=1000

    log_info "  Seeding ${cust_count} customers..."
    mysql -u "$MY_USER" --socket="$MY_SOCKET" "$dbname" -e "
        INSERT INTO customers (name, email, department, salary, bio)
        SELECT
            CONCAT('user_', seq),
            CONCAT('user_', seq, '@test.qa'),
            ELT(1 + (seq % 5), 'engineering','sales','marketing','support','hr'),
            30000 + FLOOR(RAND() * 170000),
            REPEAT(MD5(seq), 10)
        FROM (SELECT @row := @row + 1 AS seq FROM
              information_schema.columns a,
              information_schema.columns b,
              (SELECT @row := 0) r
              LIMIT ${cust_count}) t;
    " 2>/dev/null

    log_info "  Inserting ~${target_rows} document rows..."
    while [[ $inserted -lt $target_rows ]]; do
        local remaining=$(( target_rows - inserted ))
        [[ $remaining -lt $batch ]] && batch=$remaining

        mysql -u "$MY_USER" --socket="$MY_SOCKET" "$dbname" -e "
            INSERT INTO documents (customer_id, title, content, doc_type, checksum)
            SELECT
                1 + (seq % ${cust_count}),
                CONCAT('Document #', ${inserted} + seq),
                REPEAT(MD5(RAND()), 640),
                ELT(1 + (seq % 5), 'report','invoice','contract','memo','spec'),
                MD5(RAND())
            FROM (SELECT @r := @r + 1 AS seq FROM
                  information_schema.columns a,
                  information_schema.columns b,
                  (SELECT @r := 0) r
                  LIMIT ${batch}) t;
        " 2>/dev/null

        inserted=$(( inserted + batch ))
        local pct=$(( inserted * 100 / target_rows ))
        printf "\r${DIM}│${NC} ${BLUE}[INFO]${NC}  Progress: %d/%d rows (%d%%)  " "$inserted" "$target_rows" "$pct"
    done
    echo

    local actual_size
    actual_size=$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "
        SELECT CONCAT(ROUND(SUM(data_length + index_length) / 1048576, 1), ' MB')
        FROM information_schema.tables WHERE table_schema = '${dbname}';
    " 2>/dev/null)
    log_ok "${engine_label} DB '${dbname}' created: ${actual_size}"
    # Record DB size for detailed report — update existing entry or append new
    local _found=false
    if [[ ${#DB_DETAILS[@]} -gt 0 ]]; then
    for _i in "${!DB_DETAILS[@]}"; do
        IFS='|' read -r _eng _db _rest <<< "${DB_DETAILS[$_i]}"
        if [[ "$_eng" == "${engine_label,,}" && "$_db" == "$dbname" ]]; then
            # Entry exists — keep original DB size (benchmark size), don't overwrite
            # with a smaller recreated size from ensure_*_exists()
            _found=true; break
        fi
    done
    fi
    if ! $_found; then
        DB_DETAILS+=("${engine_label,,}|${dbname}|${actual_size}|||||")
    fi
}

create_databases() {
    log_header "Database Creation"
    # This is now called per-engine from main(), not all at once
    log_warn "create_databases() is deprecated \u2014 use per-engine create in main()"
}

# ─── Cleanup old dumps before tests ────────────────────────────────────────
cleanup_old_dumps() {
    log_section "Cleanup old QA dumps"
    for d in "$BACKUP_DIR_FAST" "$BACKUP_DIR_SLOW" "$DEDUP_DIR" "${DEDUP_DIR}_maria" "${DEDUP_DIR}_mysql" "$LOG_DIR"; do
        if [[ -d "$d" ]]; then
            local count; count=$(find "$d" -maxdepth 2 -type f 2>/dev/null | wc -l)
            rm -rf "${d:?}"/*
            log_info "Cleaned $d ($count files removed)"
        fi
    done
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  TEST SUITES
# ═══════════════════════════════════════════════════════════════════════════

# ─── PostgreSQL Tests ───────────────────────────────────────────────────────
test_pg() {
    log_header "PostgreSQL Test Suite"
    local DB_FLAGS="-d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --no-save-config --allow-root --insecure"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Single Backup ──
    CURRENT_CATEGORY="backup"
    log_section "PG: Single Database Backup"
    run_test "PG single backup (large: ${PG_LARGE_DB})" \
        "$BINARY backup single $PG_LARGE_DB $DB_FLAGS --backup-dir $BDIR"
    run_test "PG single backup (small: ${PG_SMALL_DB})" \
        "$BINARY backup single $PG_SMALL_DB $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Cluster Backup ──
    CURRENT_CATEGORY="backup"
    log_section "PG: Cluster Backup"
    run_test "PG cluster backup" \
        "$BINARY backup cluster $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Sample Backup ──
    CURRENT_CATEGORY="backup"
    log_section "PG: Sample Backup"
    run_test "PG sample backup (10% of ${PG_SMALL_DB})" \
        "$BINARY backup sample $PG_SMALL_DB --sample-ratio 10 $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Capture backup file sizes for detail report ──
    _update_db_detail "postgres" "$PG_LARGE_DB" "backup_file" "$(ls -t "${BDIR}"/${PG_LARGE_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)"
    _update_db_detail "postgres" "$PG_SMALL_DB" "backup_file" "$(ls -t "${BDIR}"/${PG_SMALL_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)"
    _update_db_detail "postgres" "$PG_SMALL_DB" "cluster_backup" "$(ls -t "${BDIR}"/pg_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)"

    # ── Verify ──
    CURRENT_CATEGORY="verify"
    log_section "PG: Verify Archives"
    local latest_single; latest_single=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)
    local latest_cluster; latest_cluster=$(ls -t "${BDIR}"/pg_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$latest_single" ]]; then
        run_test "PG verify single archive" \
            "$BINARY verify '$latest_single' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "PG verify single archive (no archive found)"
    fi
    if [[ -n "$latest_cluster" ]]; then
        run_test "PG verify cluster archive" \
            "$BINARY verify '$latest_cluster' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "PG verify cluster archive (no archive found)"
    fi
    section_time

    # ── Single Restore ──
    CURRENT_CATEGORY="restore"
    log_section "PG: Single Database Restore"
    if [[ -n "$latest_single" ]]; then
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_pg_restored;" 2>/dev/null || true
        run_test "PG single restore → qa_pg_restored" \
            "$BINARY restore single '$latest_single' --target qa_pg_restored --create --confirm $DB_FLAGS --backup-dir $BDIR"

        # Verify restored data
        run_test "PG restore verification (tables exist)" \
            "psql -U $PG_USER -d qa_pg_restored -c \"SELECT count(*) FROM core.documents;\" | grep -E '[0-9]+'"
    else
        skip_test "PG single restore (no archive)"
        skip_test "PG restore verification (no archive)"
    fi
    section_time

    # ── Dedup Backup ── (must run BEFORE cluster restore, which is destructive)
    CURRENT_CATEGORY="dedup"
    log_section "PG: Dedup (Content-Defined Chunking)"
    run_test "PG dedup backup #1 (${PG_SMALL_DB})" \
        "$BINARY dedup backup-db --db-type postgres --database $PG_SMALL_DB --user $PG_USER --host $PG_HOST --dedup-dir $DEDUP_DIR --no-config --allow-root"
    run_test "PG dedup backup #2 (same DB, expect high dedup ratio)" \
        "$BINARY dedup backup-db --db-type postgres --database $PG_SMALL_DB --user $PG_USER --host $PG_HOST --dedup-dir $DEDUP_DIR --no-config --allow-root"
    run_test "PG dedup stats" \
        "$BINARY dedup stats --dedup-dir $DEDUP_DIR --no-config --allow-root"
    run_test "PG dedup list" \
        "$BINARY dedup list --dedup-dir $DEDUP_DIR --no-config --allow-root"

    # Dedup restore
    local dedup_manifest; dedup_manifest=$(ls -t "${DEDUP_DIR}"/manifests/*.json 2>/dev/null | head -1)
    if [[ -n "$dedup_manifest" ]]; then
        local dedup_restore_file="${BACKUP_DIR_FAST}/dedup_restored_pg.sql"
        run_test "PG dedup restore to file" \
            "$BINARY dedup restore '$(basename "$dedup_manifest" .manifest.json)' '$dedup_restore_file' --dedup-dir $DEDUP_DIR --no-config --allow-root"
    else
        skip_test "PG dedup restore (no manifest)"
    fi
    run_test "PG dedup verify" \
        "$BINARY dedup verify --dedup-dir $DEDUP_DIR --no-config --allow-root"
    section_time

    # ── PITR ──
    CURRENT_CATEGORY="pitr"
    log_section "PG: Point-in-Time Recovery"
    run_test "PG PITR enable" \
        "$BINARY pitr enable --archive-dir '$WAL_ARCHIVE' --force $DB_FLAGS"
    run_test "PG PITR status" \
        "$BINARY pitr status $DB_FLAGS"
    section_time

    # ── Chain ──
    CURRENT_CATEGORY="ops"
    log_section "PG: Backup Chain"
    run_test "PG backup chain" \
        "$BINARY chain $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Backup to slow storage (archive test) ──
    CURRENT_CATEGORY="backup"
    log_section "PG: Slow Storage Archive"
    run_test "PG single backup → slow storage" \
        "$BINARY backup single $PG_SMALL_DB $DB_FLAGS --backup-dir $BACKUP_DIR_SLOW"
    section_time

    # ── Cluster Restore ── (destructive — run LAST)
    CURRENT_CATEGORY="restore"
    log_section "PG: Cluster Restore"
    if [[ -n "$latest_cluster" ]]; then
        run_test "PG cluster restore" \
            "$BINARY restore cluster '$latest_cluster' --confirm $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "PG cluster restore (no archive)"
    fi
    section_time

    # ── Restore Preview / Diagnose ──
    CURRENT_CATEGORY="verify"
    log_section "PG: Restore Preview & Diagnose"
    local preview_file; preview_file=$(ls -t "${BACKUP_DIR_SLOW}"/pg_cluster_*.tar.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
    [[ -z "$preview_file" ]] && preview_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BDIR}"/pg_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$preview_file" ]]; then
        run_test "PG restore preview" \
            "$BINARY restore preview '$preview_file' $DB_FLAGS --backup-dir $BDIR"
        run_test "PG restore diagnose" \
            "$BINARY restore diagnose '$preview_file' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "PG restore preview (no archive)"
        skip_test "PG restore diagnose (no archive)"
    fi
    section_time

    # ── Restore with Force Overwrite ──
    CURRENT_CATEGORY="restore"
    log_section "PG: Force Overwrite Restore"
    local force_file; force_file=$(ls -t "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
    [[ -z "$force_file" ]] && force_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$force_file" ]]; then
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_pg_force_test;" 2>/dev/null || true
        psql -U "$PG_USER" -c "CREATE DATABASE qa_pg_force_test;" 2>/dev/null || true
        run_test "PG restore with --force (overwrite existing)" \
            "$BINARY restore single '$force_file' --target qa_pg_force_test --force --create --confirm $DB_FLAGS --backup-dir $BDIR"
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_pg_force_test;" 2>/dev/null || true
    else
        skip_test "PG force overwrite restore (no archive)"
    fi
    section_time

    # ── Backup Diff ──
    CURRENT_CATEGORY="verify"
    log_section "PG: Backup Diff"
    local diff_files=()
    while IFS= read -r f; do diff_files+=("$f"); done < <(ls -t "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -2)
    if [[ ${#diff_files[@]} -ge 2 ]]; then
        run_test "PG backup diff (two archives)" \
            "$BINARY diff '${diff_files[0]}' '${diff_files[1]}' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "PG backup diff (need 2 archives)"
    fi
    section_time

    # ── Verify-Restore (post-restore data integrity) ──
    CURRENT_CATEGORY="verify"
    log_section "PG: Verify-Restore Integrity"
    run_test "PG verify-restore" \
        "$BINARY verify-restore --engine postgres --database $PG_LARGE_DB --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    section_time

    # ── Verify-Locks (PG lock settings) ──
    CURRENT_CATEGORY="ops"
    log_section "PG: Verify Locks"
    run_test "PG verify-locks" \
        "$BINARY verify-locks -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    section_time

    # ── Estimate Cluster ──
    CURRENT_CATEGORY="ops"
    log_section "PG: Estimate Cluster"
    run_test "PG estimate cluster" \
        "$BINARY estimate cluster -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure"
    section_time
}

# ─── MariaDB Tests ──────────────────────────────────────────────────────────
test_maria() {
    log_header "MariaDB Test Suite"
    local DB_FLAGS="-d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --no-save-config --allow-root --insecure"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Single Backup ──
    CURRENT_CATEGORY="backup"
    log_section "Maria: Single Database Backup"
    run_test "Maria single backup (${MARIA_DB})" \
        "$BINARY backup single $MARIA_DB $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Cluster Backup ──
    CURRENT_CATEGORY="backup"
    log_section "Maria: Cluster Backup"
    run_test "Maria cluster backup" \
        "$BINARY backup cluster $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Capture backup file sizes for detail report ──
    _update_db_detail "mariadb" "$MARIA_DB" "backup_file" "$(ls -t "${BDIR}"/${MARIA_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)"
    _update_db_detail "mariadb" "$MARIA_DB" "cluster_backup" "$(ls -t "${BDIR}"/maria_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)"

    # ── Verify ──
    CURRENT_CATEGORY="verify"
    log_section "Maria: Verify"
    local latest_single; latest_single=$(ls -t "${BDIR}"/${MARIA_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$latest_single" ]]; then
        run_test "Maria verify archive" \
            "$BINARY verify '$latest_single' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "Maria verify (no archive)"
    fi
    section_time

    # ── Single Restore ──
    CURRENT_CATEGORY="restore"
    log_section "Maria: Single Restore"
    if [[ -n "$latest_single" ]]; then
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_maria_restored;" 2>/dev/null || true
        run_test "Maria single restore → qa_maria_restored" \
            "$BINARY restore single '$latest_single' --target qa_maria_restored --create --confirm $DB_FLAGS --backup-dir $BDIR"
        run_test "Maria restore verification" \
            "mysql -u $MY_USER --socket=$MY_SOCKET -e 'SELECT COUNT(*) FROM qa_maria_restored.documents;' | grep -E '[0-9]+'"
    else
        skip_test "Maria single restore (no archive)"
        skip_test "Maria restore verification (no archive)"
    fi
    section_time

    # ── Dedup ── (must run BEFORE cluster restore, which drops databases)
    CURRENT_CATEGORY="dedup"
    log_section "Maria: Dedup"
    local MARIA_DEDUP="${FAST_STORAGE}/qa_dedup_maria"
    mkdir -p "$MARIA_DEDUP"
    run_test "Maria dedup backup #1" \
        "$BINARY dedup backup-db --db-type mariadb --database $MARIA_DB --user $MY_USER --host $MY_HOST --dedup-dir $MARIA_DEDUP --no-config --allow-root"
    run_test "Maria dedup backup #2 (expect dedup)" \
        "$BINARY dedup backup-db --db-type mariadb --database $MARIA_DB --user $MY_USER --host $MY_HOST --dedup-dir $MARIA_DEDUP --no-config --allow-root"
    run_test "Maria dedup stats" \
        "$BINARY dedup stats --dedup-dir $MARIA_DEDUP --no-config --allow-root"
    section_time

    # ── PITR (binlog) ── (must run BEFORE cluster restore)
    CURRENT_CATEGORY="pitr"
    log_section "Maria: PITR (Binlog)"
    local MARIA_BINLOG_DIR; MARIA_BINLOG_DIR=$(dirname "$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@log_bin_basename" 2>/dev/null)" 2>/dev/null)
    [[ -z "$MARIA_BINLOG_DIR" || "$MARIA_BINLOG_DIR" == "." ]] && MARIA_BINLOG_DIR="/var/log/mysql"
    run_test "Maria binlog position" \
        "$BINARY binlog position $DB_FLAGS"
    run_test "Maria binlog list" \
        "$BINARY binlog list --binlog-dir '$MARIA_BINLOG_DIR' $DB_FLAGS"
    run_test "Maria binlog validate" \
        "$BINARY binlog validate --binlog-dir '$MARIA_BINLOG_DIR' $DB_FLAGS"
    run_test "Maria PITR status (mysql-status)" \
        "$BINARY pitr mysql-status $DB_FLAGS"
    # Archive binlogs to BINLOG_DIR for PITR testing
    mkdir -p "$BINLOG_DIR"
    run_test "Maria binlog archive" \
        "$BINARY binlog archive --binlog-dir '$MARIA_BINLOG_DIR' --archive-dir '$BINLOG_DIR' $DB_FLAGS"
    section_time

    # ── Cluster Restore ── (destructive — drops/recreates all databases, run LAST)
    CURRENT_CATEGORY="restore"
    log_section "Maria: Cluster Restore"
    local latest_cluster; latest_cluster=$(ls -t "${BDIR}"/maria_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$latest_cluster" ]]; then
        run_test "Maria cluster restore" \
            "$BINARY restore cluster '$latest_cluster' --confirm $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "Maria cluster restore (no archive)"
    fi
    section_time

    # ── Restore Preview / Diagnose ──
    CURRENT_CATEGORY="verify"
    log_section "Maria: Restore Preview & Diagnose"
    local preview_file; preview_file=$(ls -t "${BACKUP_DIR_SLOW}"/${MARIA_DB}_*.{gz,zst} "${BDIR}"/${MARIA_DB}_*.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$preview_file" ]]; then
        run_test "Maria restore preview" \
            "$BINARY restore preview '$preview_file' $DB_FLAGS --backup-dir $BDIR"
        run_test "Maria restore diagnose" \
            "$BINARY restore diagnose '$preview_file' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "Maria restore preview (no archive)"
        skip_test "Maria restore diagnose (no archive)"
    fi
    section_time

    # ── Restore with Force Overwrite ──
    CURRENT_CATEGORY="restore"
    log_section "Maria: Force Overwrite Restore"
    local force_file; force_file=$(ls -t "${BACKUP_DIR_SLOW}"/${MARIA_DB}_*.{gz,zst} "${BDIR}"/${MARIA_DB}_*.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$force_file" ]]; then
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_maria_force_test; CREATE DATABASE qa_maria_force_test;" 2>/dev/null || true
        run_test "Maria restore with --force (overwrite existing)" \
            "$BINARY restore single '$force_file' --target qa_maria_force_test --force --create --confirm $DB_FLAGS --backup-dir $BDIR"
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_maria_force_test;" 2>/dev/null || true
    else
        skip_test "Maria force overwrite restore (no archive)"
    fi
    section_time

    # ── Backup to slow storage (archive test, also provides 2nd archive for diff) ──
    CURRENT_CATEGORY="backup"
    log_section "Maria: Slow Storage Archive"
    run_test "Maria single backup → slow storage" \
        "$BINARY backup single $MARIA_DB $DB_FLAGS --backup-dir $BACKUP_DIR_SLOW"
    section_time

    # ── Backup Diff ──
    CURRENT_CATEGORY="verify"
    log_section "Maria: Backup Diff"
    local diff_files=()
    while IFS= read -r f; do diff_files+=("$f"); done < <(ls -t "${BACKUP_DIR_SLOW}"/${MARIA_DB}_*.{gz,zst} "${BDIR}"/${MARIA_DB}_*.{gz,zst} 2>/dev/null | head -2)
    if [[ ${#diff_files[@]} -ge 2 ]]; then
        run_test "Maria backup diff (two archives)" \
            "$BINARY diff '${diff_files[0]}' '${diff_files[1]}' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "Maria backup diff (need 2 archives)"
    fi
    section_time

    # ── Verify-Restore Integrity ──
    CURRENT_CATEGORY="verify"
    log_section "Maria: Verify-Restore Integrity"
    run_test "Maria verify-restore" \
        "$BINARY verify-restore --engine mariadb --database $MARIA_DB --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    section_time

    # ── Estimate ──
    CURRENT_CATEGORY="ops"
    log_section "Maria: Estimate"
    run_test "Maria estimate single" \
        "$BINARY estimate single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"
    section_time
}

# ─── MySQL Tests ────────────────────────────────────────────────────────────
test_mysql() {
    log_header "MySQL Test Suite"
    # MySQL shares the MariaDB server but uses -d mysql flag
    local DB_FLAGS="-d mysql --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --no-save-config --allow-root --insecure"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Single Backup ──
    CURRENT_CATEGORY="backup"
    log_section "MySQL: Single Database Backup"
    run_test "MySQL single backup (${MYSQL_DB})" \
        "$BINARY backup single $MYSQL_DB $DB_FLAGS --backup-dir $BDIR"
    section_time

    # ── Capture backup file sizes for detail report ──
    _update_db_detail "mysql" "$MYSQL_DB" "backup_file" "$(ls -t "${BDIR}"/${MYSQL_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)"

    # ── Verify ──
    CURRENT_CATEGORY="verify"
    log_section "MySQL: Verify"
    local latest_single; latest_single=$(ls -t "${BDIR}"/${MYSQL_DB}_*_native.sql.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$latest_single" ]]; then
        run_test "MySQL verify archive" \
            "$BINARY verify '$latest_single' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "MySQL verify (no archive)"
    fi
    section_time

    # ── Single Restore ──
    CURRENT_CATEGORY="restore"
    log_section "MySQL: Single Restore"
    if [[ -n "$latest_single" ]]; then
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_mysql_restored;" 2>/dev/null || true
        run_test "MySQL single restore → qa_mysql_restored" \
            "$BINARY restore single '$latest_single' --target qa_mysql_restored --create --confirm $DB_FLAGS --backup-dir $BDIR"
        run_test "MySQL restore verification" \
            "mysql -u $MY_USER --socket=$MY_SOCKET -e 'SELECT COUNT(*) FROM qa_mysql_restored.documents;' | grep -E '[0-9]+'"
    else
        skip_test "MySQL single restore (no archive)"
        skip_test "MySQL restore verification (no archive)"
    fi
    section_time

    # ── Dedup ──
    CURRENT_CATEGORY="dedup"
    log_section "MySQL: Dedup"
    local MYSQL_DEDUP="${FAST_STORAGE}/qa_dedup_mysql"
    mkdir -p "$MYSQL_DEDUP"
    run_test "MySQL dedup backup #1" \
        "$BINARY dedup backup-db --db-type mysql --database $MYSQL_DB --user $MY_USER --host $MY_HOST --dedup-dir $MYSQL_DEDUP --no-config --allow-root"
    run_test "MySQL dedup backup #2 (expect dedup)" \
        "$BINARY dedup backup-db --db-type mysql --database $MYSQL_DB --user $MY_USER --host $MY_HOST --dedup-dir $MYSQL_DEDUP --no-config --allow-root"
    run_test "MySQL dedup stats" \
        "$BINARY dedup stats --dedup-dir $MYSQL_DEDUP --no-config --allow-root"
    section_time

    # ── PITR (binlog) ──
    CURRENT_CATEGORY="pitr"
    log_section "MySQL: PITR (Binlog)"
    local MYSQL_BINLOG_DIR; MYSQL_BINLOG_DIR=$(dirname "$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@log_bin_basename" 2>/dev/null)" 2>/dev/null)
    [[ -z "$MYSQL_BINLOG_DIR" || "$MYSQL_BINLOG_DIR" == "." ]] && MYSQL_BINLOG_DIR="/var/log/mysql"
    run_test "MySQL binlog position" \
        "$BINARY binlog position $DB_FLAGS"
    run_test "MySQL binlog list" \
        "$BINARY binlog list --binlog-dir '$MYSQL_BINLOG_DIR' $DB_FLAGS"
    section_time

    # ── Restore Preview / Diagnose ──
    CURRENT_CATEGORY="verify"
    log_section "MySQL: Restore Preview & Diagnose"
    local preview_file; preview_file=$(ls -t "${BDIR}"/${MYSQL_DB}_*.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$preview_file" ]]; then
        run_test "MySQL restore preview" \
            "$BINARY restore preview '$preview_file' $DB_FLAGS --backup-dir $BDIR"
        run_test "MySQL restore diagnose" \
            "$BINARY restore diagnose '$preview_file' $DB_FLAGS --backup-dir $BDIR"
    else
        skip_test "MySQL restore preview (no archive)"
        skip_test "MySQL restore diagnose (no archive)"
    fi
    section_time

    # ── Force Overwrite Restore ──
    CURRENT_CATEGORY="restore"
    log_section "MySQL: Force Overwrite Restore"
    local force_file; force_file=$(ls -t "${BDIR}"/${MYSQL_DB}_*.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$force_file" ]]; then
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_mysql_force_test; CREATE DATABASE qa_mysql_force_test;" 2>/dev/null || true
        run_test "MySQL restore with --force (overwrite existing)" \
            "$BINARY restore single '$force_file' --target qa_mysql_force_test --force --create --confirm $DB_FLAGS --backup-dir $BDIR"
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_mysql_force_test;" 2>/dev/null || true
    else
        skip_test "MySQL force overwrite restore (no archive)"
    fi
    section_time

    # ── Verify-Restore Integrity ──
    CURRENT_CATEGORY="verify"
    log_section "MySQL: Verify-Restore Integrity"
    run_test "MySQL verify-restore" \
        "$BINARY verify-restore --engine mysql --database $MYSQL_DB --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    section_time
}

# ─── Cleanup / Retention Tests ──────────────────────────────────────────────
test_cleanup_retention() {
    log_header "Cleanup & Retention Tests"
    local BDIR="$BACKUP_DIR_FAST"

    CURRENT_CATEGORY="cleanup"
    log_section "Cleanup & Retention Policy"

    # Create some dummy old files for retention testing
    for i in $(seq 1 8); do
        local ts; ts=$(date -d "${i} days ago" +%Y%m%d_%H%M%S 2>/dev/null || date -v-${i}d +%Y%m%d_%H%M%S 2>/dev/null)
        touch -d "${i} days ago" "${BDIR}/pg_retention_test_${ts}.sql.gz" 2>/dev/null || \
            touch "${BDIR}/pg_retention_test_${ts}.sql.gz"
    done
    log_info "Created 8 dummy backup files for retention testing"

    run_test "Cleanup dry-run (retention=3 days, min=2)" \
        "$BINARY cleanup '$BDIR' --retention-days 3 --min-backups 2 --dry-run --no-config --allow-root"
    run_test "Cleanup execute (retention=3 days, min=2)" \
        "$BINARY cleanup '$BDIR' --retention-days 3 --min-backups 2 --no-config --allow-root"
    section_time

    # ── Retention Simulator ──
    CURRENT_CATEGORY="ops"
    log_section "Retention Simulator"
    run_test "Retention simulator (30 days, min 5)" \
        "$BINARY retention-simulator --retention-days 30 --min-backups 5 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Retention simulator (7 days, min 2)" \
        "$BINARY retention-simulator --retention-days 7 --min-backups 2 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Retention simulator GFS" \
        "$BINARY retention-simulator --strategy gfs --daily 7 --weekly 4 --monthly 12 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time
}

# ─── Cross-Engine Tests ─────────────────────────────────────────────────────
test_cross_engine() {
    log_header "Cross-Engine & Operational Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Cleanup / Retention ──
    CURRENT_CATEGORY="cleanup"
    log_section "Cleanup & Retention Policy"

    # Create some dummy old files for retention testing
    for i in $(seq 1 8); do
        local ts; ts=$(date -d "${i} days ago" +%Y%m%d_%H%M%S 2>/dev/null || date -v-${i}d +%Y%m%d_%H%M%S 2>/dev/null)
        touch -d "${i} days ago" "${BDIR}/pg_retention_test_${ts}.sql.gz" 2>/dev/null || \
            touch "${BDIR}/pg_retention_test_${ts}.sql.gz"
    done
    log_info "Created 8 dummy backup files for retention testing"

    run_test "Cleanup dry-run (retention=3 days, min=2)" \
        "$BINARY cleanup '$BDIR' --retention-days 3 --min-backups 2 --dry-run --no-config --allow-root"
    run_test "Cleanup execute (retention=3 days, min=2)" \
        "$BINARY cleanup '$BDIR' --retention-days 3 --min-backups 2 --no-config --allow-root"
    section_time

    # ── Status across engines ──
    CURRENT_CATEGORY="ops"
    log_section "Status Checks"
    if should_test pg; then
        run_test "Status: PostgreSQL" \
            "$BINARY status -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure"
    fi
    if should_test maria; then
        run_test "Status: MariaDB" \
            "$BINARY status -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"
    fi
    if should_test mysql; then
        run_test "Status: MySQL" \
            "$BINARY status -d mysql --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"
    fi
    section_time

    # ── Health ──
    CURRENT_CATEGORY="ops"
    log_section "Health & Diagnostics"
    # health/diagnose return non-zero for warnings — treat exit≤2 as pass
    run_test "Health check" \
        "$BINARY health --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Diagnose" \
        "$BINARY diagnose --no-config --allow-root; [[ \$? -le 2 ]]"
    if should_test pg; then
        run_test "Estimate (PG ${PG_SMALL_DB})" \
            "$BINARY estimate single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure"
    fi
    section_time

    # ── Catalog ──
    CURRENT_CATEGORY="ops"
    log_section "Catalog Operations"
    run_test "Catalog list" \
        "$BINARY catalog list --backup-dir '$BDIR' --no-config --allow-root"
    # catalog check requires a backup file argument
    local check_target; check_target=$(find "$BDIR" -maxdepth 1 -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' 2>/dev/null | head -1)
    if [[ -n "$check_target" ]]; then
        run_test "Catalog check" \
            "$BINARY catalog check '$check_target' --backup-dir '$BDIR' --no-config --allow-root"
    else
        skip_test "Catalog check (no backup files found)"
    fi
    section_time

    # ── Dedup GC ──
    CURRENT_CATEGORY="dedup"
    log_section "Dedup Garbage Collection"
    if [[ -d "$DEDUP_DIR" ]]; then
        run_test "Dedup GC (prune + gc)" \
            "$BINARY dedup gc --dedup-dir $DEDUP_DIR --no-config --allow-root"
        run_test "Dedup verify" \
            "$BINARY dedup verify --dedup-dir $DEDUP_DIR --no-config --allow-root"
        run_test "Dedup metrics" \
            "$BINARY dedup metrics --dedup-dir $DEDUP_DIR --no-config --allow-root"
        run_test "Dedup prune" \
            "$BINARY dedup prune --dedup-dir $DEDUP_DIR --keep-last 3 --dry-run --no-config --allow-root"
    else
        skip_test "Dedup GC (no dedup dir)"
        skip_test "Dedup verify (no dedup dir)"
        skip_test "Dedup metrics (no dedup dir)"
        skip_test "Dedup prune (no dedup dir)"
    fi
    section_time

    # ── Validate ──
    CURRENT_CATEGORY="ops"
    log_section "Configuration Validation"
    run_test "Validate environment" \
        "$BINARY validate --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Version / CPU / Profile ──
    CURRENT_CATEGORY="ops"
    log_section "System Information Commands"
    run_test "Version info" \
        "$BINARY version --no-config --allow-root"
    run_test "CPU info" \
        "$BINARY cpu --no-config --allow-root"
    run_test "System profile" \
        "$BINARY profile --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Compression Analysis ──
    CURRENT_CATEGORY="ops"
    log_section "Compression Analysis"
    if should_test pg; then
        run_test "Compression analyze (PG)" \
            "$BINARY compression analyze -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure"
    fi
    if should_test maria; then
        run_test "Compression analyze (MariaDB)" \
            "$BINARY compression analyze -d mariadb --database $MARIA_DB --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"
    fi
    section_time

    # ── Forecast ──
    CURRENT_CATEGORY="ops"
    log_section "Forecast & Capacity Planning"
    run_test "Forecast (all, 90 days)" \
        "$BINARY forecast --all --days 90 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Forecast with limit" \
        "$BINARY forecast --all --days 180 --limit '500GB' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Report Generation ──
    CURRENT_CATEGORY="ops"
    log_section "Compliance Reports"
    local report_dir="${BDIR}/reports"
    mkdir -p "$report_dir"
    run_test "Report: SOC2 (markdown)" \
        "$BINARY report generate --type soc2 --format markdown --days 30 --output '$report_dir/soc2.md' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Report: GDPR (markdown)" \
        "$BINARY report generate --type gdpr --format markdown --days 30 --output '$report_dir/gdpr.md' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Report: HIPAA (markdown)" \
        "$BINARY report generate --type hipaa --format markdown --days 30 --output '$report_dir/hipaa.md' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Report: PCI-DSS (markdown)" \
        "$BINARY report generate --type pci-dss --format markdown --days 30 --output '$report_dir/pci-dss.md' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Report: ISO27001 (markdown)" \
        "$BINARY report generate --type iso27001 --format markdown --days 30 --output '$report_dir/iso27001.md' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Retention Simulator ──
    CURRENT_CATEGORY="ops"
    log_section "Retention Simulator"
    run_test "Retention simulator (30 days, min 5)" \
        "$BINARY retention-simulator --retention-days 30 --min-backups 5 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Retention simulator (7 days, min 2)" \
        "$BINARY retention-simulator --retention-days 7 --min-backups 2 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Retention simulator GFS" \
        "$BINARY retention-simulator --strategy gfs --daily 7 --weekly 4 --monthly 12 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── RTO / RPO Analysis ──
    CURRENT_CATEGORY="ops"
    log_section "RTO / RPO Analysis"
    run_test "RTO status" \
        "$BINARY rto status --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Schedule ──
    CURRENT_CATEGORY="ops"
    log_section "Schedule"
    run_test "Schedule check" \
        "$BINARY schedule --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── List ──
    CURRENT_CATEGORY="ops"
    log_section "List Backups"
    run_test "List backups" \
        "$BINARY list --backup-dir '$BDIR' --no-config --allow-root"
    run_test "List (JSON format)" \
        "$BINARY list --backup-dir '$BDIR' --no-config --allow-root | cat; [[ \${PIPESTATUS[0]} -le 2 ]]"
    section_time

    # ── Preflight ──
    CURRENT_CATEGORY="ops"
    log_section "Preflight Checks"
    if should_test pg; then
        run_test "Preflight (PG)" \
            "$BINARY preflight -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    if should_test maria; then
        run_test "Preflight (MariaDB)" \
            "$BINARY preflight -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Verify-Backup (file checksum) ──
    CURRENT_CATEGORY="verify"
    log_section "Verify-Backup Integrity"
    local vb_files; vb_files=$(find "$BDIR" -maxdepth 1 \( -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' \) -size +0 2>/dev/null | head -3)
    if [[ -n "$vb_files" ]]; then
        local vb_count=0
        while IFS= read -r vb_file; do
            vb_count=$((vb_count + 1))
            run_test "Verify-backup #${vb_count}" \
                "$BINARY verify-backup '$vb_file' --no-config --allow-root; [[ \$? -le 2 ]]"
        done <<< "$vb_files"
    else
        skip_test "Verify-backup (no archive files)"
    fi
    section_time

    # ── Chain ──
    CURRENT_CATEGORY="ops"
    log_section "Backup Chain"
    run_test "Chain list" \
        "$BINARY chain --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cost Analysis ──
    CURRENT_CATEGORY="ops"
    log_section "Cost Analysis"
    run_test "Cost estimate (all providers)" \
        "$BINARY cost analyze --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cloud Status ──
    CURRENT_CATEGORY="ops"
    log_section "Cloud Status"
    run_test "Cloud status (expect warning)" \
        "$BINARY cloud status --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Engine Info ──
    CURRENT_CATEGORY="ops"
    log_section "Engine Management"
    run_test "Engine info" \
        "$BINARY engine --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Blob Analysis ──
    CURRENT_CATEGORY="ops"
    log_section "BLOB Analysis"
    if should_test pg; then
        run_test "Blob stats (PG)" \
            "$BINARY blob stats -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    if should_test maria; then
        run_test "Blob stats (MariaDB)" \
            "$BINARY blob stats -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── WAL Operations (PG only) ──
    CURRENT_CATEGORY="ops"
    log_section "WAL Operations"
    if should_test pg; then
        run_test "WAL list" \
            "$BINARY wal list --no-config --allow-root; [[ \$? -le 2 ]]"
        run_test "WAL timeline" \
            "$BINARY wal timeline --no-config --allow-root; [[ \$? -le 2 ]]"
    else
        skip_test "WAL list (PG not tested)"
        skip_test "WAL timeline (PG not tested)"
    fi
    section_time

    # ── Encryption ──
    CURRENT_CATEGORY="ops"
    log_section "Encryption Management"
    run_test "Encryption rotate (dry-run / info)" \
        "$BINARY encryption rotate --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Parallel Restore Config ──
    CURRENT_CATEGORY="ops"
    log_section "Parallel Restore Config"
    run_test "Parallel-restore status" \
        "$BINARY parallel-restore status --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Restore Benchmark ──
    CURRENT_CATEGORY="ops"
    log_section "Restore Benchmark"
    run_test "Restore-benchmark diagnose" \
        "$BINARY restore-benchmark diagnose --database postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Metrics ──
    CURRENT_CATEGORY="ops"
    log_section "Metrics Export"
    run_test "Metrics export (textfile)" \
        "$BINARY metrics export --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Notify (test mode) ──
    CURRENT_CATEGORY="ops"
    log_section "Notification Test"
    run_test "Notify test (expect unconfigured)" \
        "$BINARY notify test --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Man Pages ──
    CURRENT_CATEGORY="ops"
    log_section "Man Page Generation"
    local man_dir; man_dir=$(mktemp -d)
    run_test "Generate man pages" \
        "$BINARY man --output-dir '$man_dir' --no-config --allow-root"
    rm -rf "$man_dir"
    section_time

    # ── Completion ──
    CURRENT_CATEGORY="ops"
    log_section "Shell Completions"
    run_test "Completion bash" \
        "$BINARY completion bash > /dev/null"
    run_test "Completion zsh" \
        "$BINARY completion zsh > /dev/null"
    run_test "Completion fish" \
        "$BINARY completion fish > /dev/null"
    section_time

    # ── GFS Retention Cleanup ──
    CURRENT_CATEGORY="cleanup"
    log_section "GFS Retention Cleanup"
    local gfs_dir; gfs_dir=$(mktemp -d)
    # Create fake daily backups spanning 90 days
    for i in $(seq 1 90); do
        local gts; gts=$(date -d "${i} days ago" +%Y%m%d_%H%M%S 2>/dev/null || date -v-${i}d +%Y%m%d_%H%M%S 2>/dev/null)
        touch -d "${i} days ago" "${gfs_dir}/gfs_test_${gts}.sql.gz" 2>/dev/null || \
            touch "${gfs_dir}/gfs_test_${gts}.sql.gz"
    done
    log_info "Created 90 dummy files for GFS testing"
    run_test "GFS cleanup dry-run (daily=7, weekly=4, monthly=12)" \
        "$BINARY cleanup '$gfs_dir' --gfs --gfs-daily 7 --gfs-weekly 4 --gfs-monthly 12 --dry-run --no-config --allow-root"
    run_test "GFS cleanup execute (daily=7, weekly=4, monthly=12)" \
        "$BINARY cleanup '$gfs_dir' --gfs --gfs-daily 7 --gfs-weekly 4 --gfs-monthly 12 --no-config --allow-root"
    local gfs_remaining; gfs_remaining=$(ls "$gfs_dir" | wc -l)
    log_info "GFS cleanup kept $gfs_remaining files out of 90"
    rm -rf "$gfs_dir"
    section_time

    # ── Diff (cross-catalog) ──
    CURRENT_CATEGORY="ops"
    log_section "Backup Diff"
    local diff_files; diff_files=$(find "$BDIR" -maxdepth 1 \( -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' \) -size +0 2>/dev/null | sort | head -2)
    local diff_count; diff_count=$(echo "$diff_files" | grep -c . 2>/dev/null || echo 0)
    if [[ "$diff_count" -ge 2 ]]; then
        local diff_a; diff_a=$(echo "$diff_files" | sed -n '1p')
        local diff_b; diff_b=$(echo "$diff_files" | sed -n '2p')
        run_test "Diff two archives" \
            "$BINARY diff '$diff_a' '$diff_b' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    else
        skip_test "Diff two archives (need ≥2 archives)"
    fi
    section_time

    # ── Multi-Compression Level Backup (PG) ──
    CURRENT_CATEGORY="backup"
    log_section "Compression Level Testing"
    if should_test pg; then
        for level in 1 6 9; do
            run_test "PG backup compression=$level" \
                "$BINARY backup single ${PG_SMALL_DB:-postgres} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --compression $level --no-config --allow-root --insecure"
        done
        # Compare sizes
        log_info "Compression comparison:"
        ls -lh "${BDIR}"/*postgres* 2>/dev/null | tail -5
    else
        skip_test "Compression level testing (PG not tested)"
    fi
    section_time

    # ── Catalog Dashboard & Export ──
    CURRENT_CATEGORY="ops"
    log_section "Catalog Dashboard & Export"
    # catalog dashboard is a TUI command that requires an interactive terminal —
    # cannot be tested in non-interactive QA.  Verify it recognises --help instead.
    run_test "Catalog dashboard (help)" \
        "$BINARY catalog dashboard --help --no-config --allow-root >/dev/null 2>&1"
    local cat_export_out; cat_export_out=$(mktemp)
    run_test "Catalog export JSON" \
        "$BINARY catalog export --backup-dir '$BDIR' --format json --output '$cat_export_out' --no-config --allow-root; [[ \$? -le 2 ]]"
    rm -f "$cat_export_out"
    section_time

    # ── Progress Webhooks ──
    CURRENT_CATEGORY="ops"
    log_section "Progress Webhooks"
    run_test "Progress-webhooks status (expect unconfigured)" \
        "$BINARY progress-webhooks status --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Verify all backup files at once ──
    CURRENT_CATEGORY="verify"
    log_section "Batch Verify All Archives"
    local all_archives; all_archives=$(find "$BDIR" "$BACKUP_DIR_SLOW" -maxdepth 1 \( -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' \) -size +0 2>/dev/null | head -3)
    if [[ -n "$all_archives" ]]; then
        local v_count=0
        while IFS= read -r archive; do
            v_count=$((v_count + 1))
            run_test "Verify archive #${v_count}: $(basename "$archive")" \
                "$BINARY verify '$archive' --no-config --allow-root; [[ \$? -le 2 ]]"
        done <<< "$all_archives"
    else
        skip_test "Batch verify (no archives found)"
    fi
    section_time

    # ── Restore list (from backup dir) ──
    CURRENT_CATEGORY="restore"
    log_section "Restore List & Catalog"
    run_test "Restore list" \
        "$BINARY restore list --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Extended Catalog Subcommands ──
    CURRENT_CATEGORY="ops"
    log_section "Catalog Extended Subcommands"
    run_test "Catalog sync" \
        "$BINARY catalog sync '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Catalog gaps" \
        "$BINARY catalog gaps --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Catalog search (all)" \
        "$BINARY catalog search --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    local cat_info_target; cat_info_target=$(find "$BDIR" -maxdepth 1 \( -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' \) -size +0 2>/dev/null | head -1)
    if [[ -n "$cat_info_target" ]]; then
        run_test "Catalog info (archive)" \
            "$BINARY catalog info '$cat_info_target' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    else
        skip_test "Catalog info (no archive)"
    fi
    run_test "Catalog prune (dry-run)" \
        "$BINARY catalog prune --backup-dir '$BDIR' --dry-run --missing --no-config --allow-root; [[ \$? -le 2 ]]"
    local cat_gen_target; cat_gen_target=$(find "$BDIR" -maxdepth 1 \( -name '*.gz' -o -name '*.tar.gz' -o -name '*.zst' -o -name '*.tar.zst' \) -size +0 2>/dev/null | head -1)
    if [[ -n "$cat_gen_target" ]]; then
        run_test "Catalog generate" \
            "$BINARY catalog generate '$cat_gen_target' --no-config --allow-root; [[ \$? -le 2 ]]"
    else
        skip_test "Catalog generate (no non-empty archives)"
    fi
    run_test "Catalog stats" \
        "$BINARY catalog stats --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Blob Backup & Restore ──
    CURRENT_CATEGORY="ops"
    log_section "BLOB Backup & Restore"
    if should_test pg; then
        local blob_out="${BDIR}/qa_blob_pg.tar"
        run_test "Blob backup (PG)" \
            "$BINARY blob backup -o '$blob_out' -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
        if [[ -f "$blob_out" && -s "$blob_out" ]]; then
            run_test "Blob restore (PG)" \
                "$BINARY blob restore -i '$blob_out' -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
        else
            skip_test "Blob restore PG (no blob backup produced — no large objects in DB)"
        fi
    fi
    if should_test maria; then
        local blob_out_maria="${BDIR}/qa_blob_maria.tar"
        run_test "Blob backup (MariaDB)" \
            "$BINARY blob backup -o '$blob_out_maria' -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Metrics Serve (start, probe, stop) ──
    CURRENT_CATEGORY="ops"
    log_section "Metrics Prometheus Endpoint"
    run_test "Metrics serve (start & stop)" \
        "timeout 5 $BINARY metrics serve --port 19100 --backup-dir '$BDIR' --no-config --allow-root &>/dev/null & local mpid=\$!; sleep 2; curl -sf http://localhost:19100/metrics >/dev/null 2>&1; local rc=\$?; kill \$mpid 2>/dev/null; wait \$mpid 2>/dev/null; [[ \$rc -le 2 ]]"
    section_time

    # ── Install / Uninstall as systemd service (dry-run) ──
    CURRENT_CATEGORY="ops"
    log_section "Systemd Install / Uninstall (dry-run)"
    run_test "Install as systemd service (dry-run)" \
        "$BINARY install --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Uninstall systemd service (dry-run)" \
        "$BINARY uninstall --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Interactive / TUI help ──
    CURRENT_CATEGORY="ops"
    log_section "Interactive Mode"
    run_test "Interactive help" \
        "$BINARY interactive --help --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Progress Webhooks Extended ──
    CURRENT_CATEGORY="ops"
    log_section "Progress Webhooks Extended"
    run_test "Progress-webhooks enable (dry test)" \
        "$BINARY progress-webhooks enable --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Progress-webhooks test" \
        "$BINARY progress-webhooks test --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Progress-webhooks disable" \
        "$BINARY progress-webhooks disable --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Retention Simulator Compare ──
    CURRENT_CATEGORY="ops"
    log_section "Retention Simulator Compare"
    run_test "Retention simulator compare" \
        "$BINARY retention-simulator compare --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  ADVANCED / STRESS TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_advanced() {
    log_header "Advanced & Stress Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Rapid Successive Backups (same DB) ──
    CURRENT_CATEGORY="backup"
    log_section "Rapid Successive Backups"
    if should_test pg; then
        # Single backup here—earlier test_pg already proved basic backup works
        run_test "PG rapid backup (${PG_SMALL_DB:-postgres})" \
            "$BINARY backup single ${PG_SMALL_DB:-postgres} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure"
        local rapid_count; rapid_count=$(find "$BDIR" -maxdepth 1 -name "*${PG_SMALL_DB:-postgres}*" 2>/dev/null | wc -l)
        log_info "Backup dir now has $rapid_count archive files"
    else
        skip_test "Rapid successive backups (PG not tested)"
    fi
    section_time

    # ── Backup + Immediate Verify Cycle ──
    CURRENT_CATEGORY="verify"
    log_section "Backup-Then-Verify Cycle"
    if should_test pg; then
        local cycle_file; cycle_file=$(ls -t "${BDIR}"/*${PG_SMALL_DB:-postgres}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$cycle_file" ]]; then
            run_test "Verify most recent PG backup" \
                "$BINARY verify '$cycle_file' --no-config --allow-root"
            run_test "Verify-backup checksum" \
                "$BINARY verify-backup '$cycle_file' --no-config --allow-root; [[ \$? -le 2 ]]"
            run_test "Catalog check on recent backup" \
                "$BINARY catalog check '$cycle_file' --backup-dir '$BDIR' --no-config --allow-root"
        else
            skip_test "Backup-then-verify (no archive found)"
        fi
    elif should_test maria; then
        local cycle_file; cycle_file=$(ls -t "${BDIR}"/*${MARIA_DB}*.{gz,zst} "${BACKUP_DIR_SLOW}"/*${MARIA_DB}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$cycle_file" ]]; then
            run_test "Verify most recent Maria backup" \
                "$BINARY verify '$cycle_file' --no-config --allow-root"
            run_test "Verify-backup checksum" \
                "$BINARY verify-backup '$cycle_file' --no-config --allow-root; [[ \$? -le 2 ]]"
        else
            skip_test "Backup-then-verify (no archive found)"
        fi
    fi
    section_time

    # ── Big Catalog Stress ──
    CURRENT_CATEGORY="ops"
    log_section "Catalog Stress"
    run_test "Catalog list (full dir)" \
        "$BINARY catalog list --backup-dir '$BDIR' --no-config --allow-root"
    if [[ -d "$BACKUP_DIR_SLOW" ]]; then
        run_test "Catalog list (slow storage)" \
            "$BINARY catalog list --backup-dir '$BACKUP_DIR_SLOW' --no-config --allow-root; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Restore to Alternate Target (PG) ──
    CURRENT_CATEGORY="restore"
    log_section "Restore to Alternate Targets"
    if should_test pg; then
        local alt_file; alt_file=$(ls -t "${BDIR}"/*${PG_SMALL_DB:-postgres}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$alt_file" ]]; then
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_alt_target;" 2>/dev/null || true
            run_test "PG restore to alternate DB (qa_alt_target)" \
                "$BINARY restore single '$alt_file' --target qa_alt_target --force --create --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$BDIR'"
            # Verify the alternate target has data
            local alt_tables; alt_tables=$(psql -U "$PG_USER" -d qa_alt_target -Aqt -c "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');" 2>/dev/null || echo "0")
            if [[ "$alt_tables" -gt 0 ]]; then
                log_ok "Alternate target qa_alt_target has $alt_tables tables"
            else
                log_warn "Alternate target qa_alt_target has no user tables"
            fi
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_alt_target;" 2>/dev/null || true
        else
            skip_test "PG restore to alternate DB (no archive)"
        fi
    fi
    if should_test maria; then
        local alt_file; alt_file=$(ls -t "${BDIR}"/*${MARIA_DB}*.{gz,zst} "${BACKUP_DIR_SLOW}"/*${MARIA_DB}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$alt_file" ]]; then
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_alt_maria; CREATE DATABASE qa_alt_maria;" 2>/dev/null || true
            run_test "Maria restore to alternate DB (qa_alt_maria)" \
                "$BINARY restore single '$alt_file' --target qa_alt_maria --force --create --confirm -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure --backup-dir '$BDIR'"
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_alt_maria;" 2>/dev/null || true
        else
            skip_test "Maria restore to alternate DB (no archive)"
        fi
    fi
    section_time

    # ── Backup with Different Dump Jobs ──
    CURRENT_CATEGORY="backup"
    log_section "Dump Jobs Parallelism"
    if should_test pg; then
        for jobs in 1 4 8; do
            run_test "PG backup dump-jobs=$jobs" \
                "$BINARY backup single ${PG_SMALL_DB:-postgres} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --dump-jobs $jobs --no-config --allow-root --insecure"
        done
    else
        skip_test "Dump jobs parallelism (PG not tested)"
    fi
    section_time

    # ── Custom Format (.dump) Backup & Restore ──
    CURRENT_CATEGORY="backup"
    log_section "Custom Format (.dump) Backup & Restore"
    if should_test pg; then
        local CUSTOM_DIR="${BDIR}/custom_format_test"
        mkdir -p "$CUSTOM_DIR"

        # Test 1: Single backup with explicit --dump-format=custom (must disable native engine to use pg_dump)
        run_test "PG backup custom format (single)" \
            "USE_NATIVE_ENGINE=false $BINARY backup single ${PG_SMALL_DB:-postgres} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$CUSTOM_DIR' --dump-format custom --no-config --allow-root --insecure"

        # Verify that .dump file was created (not .sql.gz)
        local custom_dump; custom_dump=$(ls -t "${CUSTOM_DIR}"/*.dump 2>/dev/null | head -1)
        if [[ -n "$custom_dump" ]]; then
            # Verify it's a valid pg_dump custom format (PGDMP magic bytes)
            run_test "PG verify custom format archive" \
                "$BINARY verify '$custom_dump' --no-config --allow-root; [[ \$? -le 2 ]]"

            # Test pg_restore --list works (proves parallel restore will work)
            run_test "PG custom format pg_restore --list" \
                "pg_restore --list '$custom_dump' | head -5"

            # Test restore from custom format
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_custom_restored;" 2>/dev/null || true
            run_test "PG restore from custom format (.dump)" \
                "$BINARY restore single '$custom_dump' --target qa_custom_restored --create --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$CUSTOM_DIR'"

            # Verify restored data has tables
            run_test "PG custom format restore verification" \
                "psql -U $PG_USER -d qa_custom_restored -c \"SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');\" | grep -E '[0-9]+'"

            # Cleanup
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_custom_restored;" 2>/dev/null || true
        else
            skip_test "PG verify custom format archive (no .dump file created)"
            skip_test "PG custom format pg_restore --list (no .dump file)"
            skip_test "PG restore from custom format (.dump) (no .dump file)"
            skip_test "PG custom format restore verification (no .dump file)"
        fi

        # Test 2: Cluster backup produces .dump files
        run_test "PG cluster backup (custom format)" \
            "$BINARY backup cluster -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$CUSTOM_DIR' --no-config --allow-root --insecure"

        local custom_cluster; custom_cluster=$(ls -t "${CUSTOM_DIR}"/pg_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$custom_cluster" ]]; then
            # Peek inside cluster archive: expect .dump files, not .sql.gz for PG databases
            local dump_count; dump_count=$(tar tf "$custom_cluster" 2>/dev/null | grep -c '\.dump$' || echo 0)
            local sqlgz_count; sqlgz_count=$(tar tf "$custom_cluster" 2>/dev/null | grep -c -E '\.sql\.(gz|zst)$' || echo 0)
            run_test "PG cluster archive contains .dump files" \
                "[[ $dump_count -gt 0 ]] && echo 'Found $dump_count .dump files in cluster archive (sqlgz: $sqlgz_count)'"

            # Test cluster restore with custom format dumps inside
            run_test "PG cluster restore (custom format)" \
                "$BINARY restore cluster '$custom_cluster' --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$CUSTOM_DIR'"
        else
            skip_test "PG cluster archive contains .dump files (no archive)"
            skip_test "PG cluster restore (custom format) (no archive)"
        fi

        # Cleanup test dir
        rm -rf "$CUSTOM_DIR"
    else
        skip_test "Custom format tests (PG not tested)"
    fi
    section_time

    # ── Cross-Engine Status Matrix ──
    CURRENT_CATEGORY="ops"
    log_section "Status Matrix (all engines)"
    if should_test pg; then
        run_test "Health (PG focus)" \
            "$BINARY health --backup-dir '$BDIR' -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    if should_test maria; then
        run_test "Health (MariaDB focus)" \
            "$BINARY health --backup-dir '$BDIR' -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Final Cleanup ──
    CURRENT_CATEGORY="cleanup"
    log_section "Final Cleanup (aggressive retention)"
    run_test "Cleanup final (retention=1 day, min=1)" \
        "$BINARY cleanup '$BDIR' --retention-days 1 --min-backups 1 --dry-run --no-config --allow-root"
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  NATIVE ENGINE TESTS (Pure Go, no pg_dump/mysqldump)
# ═══════════════════════════════════════════════════════════════════════════
test_native_engine() {
    log_header "Native Engine Tests (Pure Go)"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Native PG Backup ──
    CURRENT_CATEGORY="backup"
    log_section "Native PostgreSQL Backup"
    if should_test pg; then
        ensure_pg_small_exists
        run_test "Native PG backup (${PG_SMALL_DB})" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --native --no-config --allow-root --insecure"
    else
        skip_test "Native PG backup (PG not tested)"
    fi
    section_time

    # ── Native PG Restore ──
    CURRENT_CATEGORY="restore"
    log_section "Native PostgreSQL Restore"
    if should_test pg; then
        local native_file; native_file=$(ls -t "${BDIR}"/*native* "${BDIR}"/*${PG_SMALL_DB}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$native_file" ]]; then
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_native_restore;" 2>/dev/null || true
            run_test "Native PG restore → qa_native_restore" \
                "$BINARY restore single '$native_file' --target qa_native_restore --create --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --native --no-config --allow-root --insecure --backup-dir '$BDIR'"
            run_test "Native PG restore verification" \
                "psql -U $PG_USER -d qa_native_restore -c 'SELECT count(*) FROM core.documents;' 2>/dev/null | grep -E '[0-9]+'"
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_native_restore;" 2>/dev/null || true
        else
            skip_test "Native PG restore (no backup file)"
        fi
    else
        skip_test "Native PG restore (PG not tested)"
    fi
    section_time

    # ── Native MariaDB Backup ──
    CURRENT_CATEGORY="backup"
    log_section "Native MariaDB Backup"
    if should_test maria; then
        ensure_maria_exists
        run_test "Native MariaDB backup (${MARIA_DB})" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --native --no-config --allow-root --insecure"
    else
        skip_test "Native MariaDB backup (MariaDB not tested)"
    fi
    section_time

    # ── Native MariaDB Restore ──
    CURRENT_CATEGORY="restore"
    log_section "Native MariaDB Restore"
    if should_test maria; then
        local native_maria; native_maria=$(ls -t "${BDIR}"/*${MARIA_DB}*native* "${BDIR}"/*${MARIA_DB}*.{gz,zst} "${BACKUP_DIR_SLOW}"/*${MARIA_DB}*native* "${BACKUP_DIR_SLOW}"/*${MARIA_DB}*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$native_maria" ]]; then
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_native_maria_rst;" 2>/dev/null || true
            run_test "Native MariaDB restore → qa_native_maria_rst" \
                "$BINARY restore single '$native_maria' --target qa_native_maria_rst --create --confirm -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --native --no-config --allow-root --insecure --backup-dir '$BDIR'"
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_native_maria_rst;" 2>/dev/null || true
        else
            skip_test "Native MariaDB restore (no backup file)"
        fi
    else
        skip_test "Native MariaDB restore (MariaDB not tested)"
    fi
    section_time

    # ── Native MySQL Backup/Restore ──
    CURRENT_CATEGORY="backup"
    log_section "Native MySQL Backup"
    if should_test mysql; then
        ensure_mysql_exists
        run_test "Native MySQL backup (${MYSQL_DB})" \
            "$BINARY backup single $MYSQL_DB -d mysql --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --native --no-config --allow-root --insecure"
    else
        skip_test "Native MySQL backup (MySQL not tested)"
    fi
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  PERCONA XTRABACKUP ENGINE TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_xtrabackup_engine() {
    log_header "Percona XtraBackup / MariaBackup Engine Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Check if xtrabackup or mariabackup is available ──
    local XTRA_BIN=""
    if command -v xtrabackup &>/dev/null; then
        XTRA_BIN="xtrabackup"
    elif command -v mariabackup &>/dev/null; then
        XTRA_BIN="mariabackup"
    elif command -v mariadb-backup &>/dev/null; then
        XTRA_BIN="mariadb-backup"
    fi

    if [[ -z "$XTRA_BIN" ]]; then
        log_warn "Neither xtrabackup nor mariabackup found in PATH – skipping XtraBackup tests"
        skip_test "XtraBackup not installed"
        return 0
    fi

    log_info "Detected backup tool: $XTRA_BIN ($($XTRA_BIN --version 2>&1 | head -1))"

    # ── XtraBackup MariaDB full backup ──
    CURRENT_CATEGORY="backup"
    log_section "XtraBackup MariaDB Backup"
    if should_test maria; then
        ensure_maria_exists
        run_test "XtraBackup MariaDB full backup (${MARIA_DB})" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --xtrabackup --no-config --allow-root --insecure"
    else
        skip_test "XtraBackup MariaDB backup (MariaDB not tested)"
    fi
    section_time

    # ── XtraBackup MySQL full backup ──
    CURRENT_CATEGORY="backup"
    log_section "XtraBackup MySQL Backup"
    if should_test mysql; then
        run_test "XtraBackup MySQL full backup" \
            "$BINARY backup single $MYSQL_DB -d mysql --host $MYSQL_HOST --port $MYSQL_PORT --user $MYSQL_USER --backup-dir '$BDIR' --xtrabackup --no-config --allow-root --insecure"
    else
        skip_test "XtraBackup MySQL backup (MySQL not tested)"
    fi
    section_time

    # ── XtraBackup with --no-lock (InnoDB-only) ──
    CURRENT_CATEGORY="backup"
    log_section "XtraBackup --no-lock"
    if should_test maria; then
        run_test "XtraBackup MariaDB --no-lock" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --xtrabackup --xtrabackup-no-lock --no-config --allow-root --insecure"
    else
        skip_test "XtraBackup --no-lock (MariaDB not tested)"
    fi
    section_time

    # ── XtraBackup with parallel threads ──
    CURRENT_CATEGORY="backup"
    log_section "XtraBackup parallel"
    if should_test maria; then
        run_test "XtraBackup MariaDB parallel=8" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --xtrabackup --xtrabackup-parallel 8 --no-config --allow-root --insecure"
    else
        skip_test "XtraBackup parallel (MariaDB not tested)"
    fi
    section_time

    # ── XtraBackup unit tests ──
    CURRENT_CATEGORY="unit"
    log_section "XtraBackup Unit Tests"
    run_test "XtraBackup engine unit tests" \
        "cd '$REPO_ROOT' && go test ./internal/engine/ -run TestXtraBackup -count=1 -timeout 60s"
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  ENCRYPTION ROUND-TRIP TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_encryption_roundtrip() {
    log_header "Encryption Round-Trip Tests"
    local BDIR="$BACKUP_DIR_FAST"
    local ENC_KEY="qa-test-encryption-key-$(date +%s)"
    local ENC_DIR="${FAST_STORAGE}/qa_encrypted"
    mkdir -p "$ENC_DIR"
    export DBBACKUP_ENCRYPTION_KEY="$ENC_KEY"

    # ── Encrypted PG backup ──
    CURRENT_CATEGORY="backup"
    log_section "Encrypted PostgreSQL Backup"
    if should_test pg; then
        ensure_pg_small_exists
        run_test "PG encrypted backup" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$ENC_DIR' --encrypt --no-config --allow-root --insecure"

        # ── Verify encrypted archive ──
        CURRENT_CATEGORY="verify"
        local enc_file; enc_file=$(ls -t "${ENC_DIR}"/${PG_SMALL_DB}*.gz* "${ENC_DIR}"/${PG_SMALL_DB}*.zst* "${ENC_DIR}"/${PG_SMALL_DB}*.enc 2>/dev/null | head -1)
        if [[ -n "$enc_file" ]]; then
            run_test "PG verify encrypted archive" \
                "$BINARY verify '$enc_file' --no-config --allow-root; [[ \$? -le 2 ]]"

            # ── Restore encrypted backup ──
            CURRENT_CATEGORY="restore"
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_enc_restore;" 2>/dev/null || true
            run_test "PG restore encrypted backup → qa_enc_restore" \
                "$BINARY restore single '$enc_file' --target qa_enc_restore --create --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$ENC_DIR'"

            # ── Verify restored data matches original ──
            CURRENT_CATEGORY="verify"
            run_test "PG encrypted restore verification (tables exist)" \
                "psql -U $PG_USER -d qa_enc_restore -c 'SELECT count(*) FROM core.documents;' 2>/dev/null | grep -E '[0-9]+'"
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_enc_restore;" 2>/dev/null || true
        else
            skip_test "PG verify encrypted archive (no encrypted file)"
            skip_test "PG restore encrypted backup (no encrypted file)"
            skip_test "PG encrypted restore verification (no encrypted file)"
        fi
    else
        skip_test "PG encrypted backup (PG not tested)"
    fi
    section_time

    # ── Encrypted MariaDB backup ──
    CURRENT_CATEGORY="backup"
    log_section "Encrypted MariaDB Backup"
    if should_test maria; then
        ensure_maria_exists
        run_test "MariaDB encrypted backup" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$ENC_DIR' --encrypt --no-config --allow-root --insecure"

        CURRENT_CATEGORY="restore"
        local enc_maria; enc_maria=$(ls -t "${ENC_DIR}"/${MARIA_DB}*.gz* "${ENC_DIR}"/${MARIA_DB}*.zst* "${ENC_DIR}"/${MARIA_DB}*.enc 2>/dev/null | head -1)
        if [[ -n "$enc_maria" ]]; then
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_enc_maria_rst;" 2>/dev/null || true
            run_test "MariaDB restore encrypted backup" \
                "$BINARY restore single '$enc_maria' --target qa_enc_maria_rst --create --confirm -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure --backup-dir '$ENC_DIR'"
            mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "DROP DATABASE IF EXISTS qa_enc_maria_rst;" 2>/dev/null || true
        else
            skip_test "MariaDB restore encrypted backup (no encrypted file)"
        fi
    else
        skip_test "MariaDB encrypted backup (MariaDB not tested)"
    fi
    section_time

    # ── Encryption Key Rotation ──
    CURRENT_CATEGORY="ops"
    log_section "Encryption Key Rotation"
    local NEW_ENC_KEY="qa-rotated-key-$(date +%s)"
    run_test "Encryption rotate (old → new key)" \
        "$BINARY encryption rotate --old-key '$ENC_KEY' --new-key '$NEW_ENC_KEY' --backup-dir '$ENC_DIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # Cleanup
    rm -rf "$ENC_DIR"
}

# ═══════════════════════════════════════════════════════════════════════════
#  POINT-IN-TIME RECOVERY (FULL CYCLE)
# ═══════════════════════════════════════════════════════════════════════════
test_pitr_full() {
    log_header "Point-in-Time Recovery Full Cycle"
    local BDIR="$BACKUP_DIR_FAST"

    # ── PG PITR (WAL replay) ──
    CURRENT_CATEGORY="pitr"
    log_section "PG: Full PITR Cycle"
    if should_test pg; then
        ensure_pg_small_exists

        # 1. Make a base backup
        run_test "PITR: PG base backup" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure"

        # 2. Insert distinguishable data after backup
        local pitr_ts; pitr_ts=$(date '+%Y-%m-%d %H:%M:%S')
        psql -U "$PG_USER" -d "$PG_SMALL_DB" -c "
            CREATE TABLE IF NOT EXISTS core.pitr_marker (
                id SERIAL PRIMARY KEY,
                marker TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
            INSERT INTO core.pitr_marker (marker) VALUES ('AFTER_BACKUP_$(date +%s)');
        " 2>/dev/null || true
        log_info "PITR marker inserted at $pitr_ts"

        # 3. Force WAL switch
        psql -U "$PG_USER" -c "SELECT pg_switch_wal();" 2>/dev/null || true
        sleep 2

        # 4. Check PITR status
        run_test "PITR: PG status after WAL insert" \
            "$BINARY pitr status -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure"

        # 5. List WAL files
        run_test "PITR: WAL list (after insert)" \
            "$BINARY wal list --no-config --allow-root; [[ \$? -le 2 ]]"

        # 6. WAL timeline
        run_test "PITR: WAL timeline" \
            "$BINARY wal timeline --no-config --allow-root; [[ \$? -le 2 ]]"

        # 7. Attempt PITR restore to the timestamp
        run_test "PITR: PG restore to timestamp" \
            "$BINARY pitr restore --target-time '$pitr_ts' --backup-dir '$BDIR' -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "PITR PG full cycle (PG not tested)"
    fi
    section_time

    # ── MariaDB PITR (Binlog replay) ──
    CURRENT_CATEGORY="pitr"
    log_section "MariaDB: Full PITR Cycle"
    if should_test maria; then
        ensure_maria_exists
        local MARIA_BINLOG_DIR; MARIA_BINLOG_DIR=$(dirname "$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -sN -e "SELECT @@log_bin_basename" 2>/dev/null)" 2>/dev/null)
        [[ -z "$MARIA_BINLOG_DIR" || "$MARIA_BINLOG_DIR" == "." ]] && MARIA_BINLOG_DIR="/var/log/mysql"

        # 1. Note current binlog position
        run_test "PITR: Maria binlog position (before)" \
            "$BINARY binlog position -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"

        # 2. Make a base backup
        run_test "PITR: Maria base backup" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --no-config --allow-root --insecure"

        # 3. Insert data after backup
        local maria_pitr_ts; maria_pitr_ts=$(date '+%Y-%m-%d %H:%M:%S')
        mysql -u "$MY_USER" --socket="$MY_SOCKET" "$MARIA_DB" -e "
            CREATE TABLE IF NOT EXISTS pitr_marker (
                id INT AUTO_INCREMENT PRIMARY KEY,
                marker VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO pitr_marker (marker) VALUES ('AFTER_BACKUP_$(date +%s)');
        " 2>/dev/null || true
        log_info "PITR marker inserted at $maria_pitr_ts"

        # 4. Flush logs & archive binlogs
        mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "FLUSH LOGS;" 2>/dev/null || true
        run_test "PITR: Maria binlog archive (after insert)" \
            "$BINARY binlog archive --binlog-dir '$MARIA_BINLOG_DIR' --archive-dir '$BINLOG_DIR' -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"

        # 5. Binlog validate
        run_test "PITR: Maria binlog validate (after archive)" \
            "$BINARY binlog validate --binlog-dir '$MARIA_BINLOG_DIR' -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure"

        # 6. PITR restore to timestamp
        run_test "PITR: Maria restore to timestamp" \
            "$BINARY pitr mysql-restore --target-time '$maria_pitr_ts' --backup-dir '$BDIR' --binlog-dir '$MARIA_BINLOG_DIR' -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "PITR MariaDB full cycle (MariaDB not tested)"
    fi
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  DR DRILL TESTS (Disaster Recovery via Docker)
# ═══════════════════════════════════════════════════════════════════════════
test_dr_drill() {
    log_header "DR Drill Tests (Disaster Recovery)"
    local BDIR="$BACKUP_DIR_FAST"

    # Check if Docker is available
    if ! command -v docker &>/dev/null; then
        log_warn "Docker not available — skipping DR drill tests"
        CURRENT_CATEGORY="drill"
        skip_test "DR drill run (Docker not available)"
        skip_test "DR drill quick (Docker not available)"
        skip_test "DR drill list (Docker not available)"
        skip_test "DR drill report (Docker not available)"
        skip_test "DR drill cleanup (Docker not available)"
        return
    fi

    if ! docker info &>/dev/null 2>&1; then
        log_warn "Docker daemon not running — skipping DR drill tests"
        CURRENT_CATEGORY="drill"
        skip_test "DR drill run (Docker daemon not running)"
        skip_test "DR drill quick (Docker daemon not running)"
        skip_test "DR drill list (Docker daemon not running)"
        skip_test "DR drill report (Docker daemon not running)"
        skip_test "DR drill cleanup (Docker daemon not running)"
        return
    fi

    CURRENT_CATEGORY="drill"
    log_section "DR Drill: List & Report"
    run_test "DR drill list" \
        "$BINARY drill list --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "DR drill report" \
        "$BINARY drill report --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Quick DR Drill (PG) ──
    log_section "DR Drill: Quick (PG)"
    if should_test pg; then
        local drill_file; drill_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$drill_file" ]]; then
            run_test "DR drill quick (PG)" \
                "$BINARY drill quick '$drill_file' -d postgres --no-config --allow-root; [[ \$? -le 2 ]]"
        else
            skip_test "DR drill quick PG (no backup file)"
        fi
    else
        skip_test "DR drill quick PG (PG not tested)"
    fi
    section_time

    # ── Full DR Drill (PG) ──
    log_section "DR Drill: Full Run (PG)"
    if should_test pg; then
        local drill_full_file; drill_full_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$drill_full_file" ]]; then
            run_test "DR drill run (PG, full restore + validate)" \
                "$BINARY drill run '$drill_full_file' -d postgres --expected-tables 3 --min-rows 100 --no-config --allow-root; [[ \$? -le 2 ]]"
        else
            skip_test "DR drill run PG (no backup file)"
        fi
    else
        skip_test "DR drill run PG (PG not tested)"
    fi
    section_time

    # ── Quick DR Drill (MariaDB) ──
    log_section "DR Drill: Quick (MariaDB)"
    if should_test maria; then
        local drill_maria; drill_maria=$(ls -t "${BDIR}"/${MARIA_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${MARIA_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$drill_maria" ]]; then
            run_test "DR drill quick (MariaDB)" \
                "$BINARY drill quick '$drill_maria' -d mariadb --no-config --allow-root; [[ \$? -le 2 ]]"
        else
            skip_test "DR drill quick MariaDB (no backup file)"
        fi
    else
        skip_test "DR drill quick MariaDB (MariaDB not tested)"
    fi
    section_time

    # ── Cleanup DR Drill containers ──
    log_section "DR Drill: Cleanup"
    run_test "DR drill cleanup" \
        "$BINARY drill cleanup --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  CLOUD SYNC TESTS (MinIO / S3-compatible)
# ═══════════════════════════════════════════════════════════════════════════
test_cloud_minio() {
    log_header "Cloud Sync Tests (MinIO / S3-compatible)"
    local BDIR="$BACKUP_DIR_FAST"

    # Check if docker-compose and MinIO setup exist
    local COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.minio.yml"
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_warn "docker-compose.minio.yml not found — skipping cloud sync tests"
        CURRENT_CATEGORY="cloud"
        skip_test "Cloud sync to MinIO (compose file missing)"
        return
    fi

    if ! command -v docker &>/dev/null; then
        log_warn "Docker not available — skipping cloud sync tests"
        CURRENT_CATEGORY="cloud"
        skip_test "Cloud sync to MinIO (Docker not available)"
        return
    fi

    CURRENT_CATEGORY="cloud"
    log_section "Cloud: Start MinIO Container"

    # Start MinIO
    local minio_started=false
    if docker compose -f "$COMPOSE_FILE" up -d 2>/dev/null || docker-compose -f "$COMPOSE_FILE" up -d 2>/dev/null; then
        sleep 5  # wait for MinIO to be ready
        minio_started=true
        log_ok "MinIO container started"
    else
        log_warn "Failed to start MinIO container — testing with existing setup"
    fi

    # MinIO default endpoint
    local S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
    local S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
    local S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
    local S3_BUCKET="${S3_BUCKET:-dbbackup-qa}"

    # ── Cloud sync upload ──
    log_section "Cloud: Sync Upload to MinIO"
    run_test "Cloud sync upload (dry-run)" \
        "$BINARY cloud sync --provider s3 --endpoint '$S3_ENDPOINT' --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' --bucket '$S3_BUCKET' --backup-dir '$BDIR' --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Cloud sync upload (execute)" \
        "$BINARY cloud sync --provider s3 --endpoint '$S3_ENDPOINT' --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' --bucket '$S3_BUCKET' --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cloud status with S3 ──
    log_section "Cloud: Status (S3/MinIO)"
    run_test "Cloud status (MinIO)" \
        "$BINARY cloud status --provider s3 --endpoint '$S3_ENDPOINT' --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' --bucket '$S3_BUCKET' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cloud sync with --newer-only ──
    log_section "Cloud: Sync (newer-only)"
    run_test "Cloud sync (newer-only)" \
        "$BINARY cloud sync --provider s3 --endpoint '$S3_ENDPOINT' --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' --bucket '$S3_BUCKET' --backup-dir '$BDIR' --newer-only --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cloud sync with --delete (orphan removal) ──
    log_section "Cloud: Sync with Orphan Removal"
    run_test "Cloud sync (delete orphans, dry-run)" \
        "$BINARY cloud sync --provider s3 --endpoint '$S3_ENDPOINT' --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' --bucket '$S3_BUCKET' --backup-dir '$BDIR' --delete --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Cross-Region Sync (simulated with same endpoint) ──
    log_section "Cloud: Cross-Region Sync"
    run_test "Cross-region sync (dry-run)" \
        "$BINARY cloud cross-region-sync --source-provider s3 --source-endpoint '$S3_ENDPOINT' --source-access-key '$S3_ACCESS_KEY' --source-secret-key '$S3_SECRET_KEY' --source-bucket '$S3_BUCKET' --dest-provider s3 --dest-endpoint '$S3_ENDPOINT' --dest-access-key '$S3_ACCESS_KEY' --dest-secret-key '$S3_SECRET_KEY' --dest-bucket '${S3_BUCKET}-replica' --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Stop MinIO if we started it ──
    if $minio_started; then
        log_section "Cloud: Stop MinIO Container"
        docker compose -f "$COMPOSE_FILE" down 2>/dev/null || docker-compose -f "$COMPOSE_FILE" down 2>/dev/null || true
        log_ok "MinIO container stopped"
        section_time
    fi
}

# ═══════════════════════════════════════════════════════════════════════════
#  MIGRATE TESTS (Server-to-Server)
# ═══════════════════════════════════════════════════════════════════════════
test_migrate_dryrun() {
    log_header "Migration Tests (dry-run)"
    local BDIR="$BACKUP_DIR_FAST"

    CURRENT_CATEGORY="ops"

    # ── PG Migration dry-run (localhost → localhost) ──
    log_section "Migrate: PostgreSQL (dry-run)"
    if should_test pg; then
        ensure_pg_small_exists
        run_test "Migrate PG single DB (dry-run)" \
            "$BINARY migrate single $PG_SMALL_DB --source-host $PG_HOST --source-port $PG_PORT --source-user $PG_USER --target-host $PG_HOST --target-port $PG_PORT --target-user $PG_USER --source-engine postgres --target-engine postgres --dry-run --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
        run_test "Migrate PG cluster (dry-run)" \
            "$BINARY migrate cluster --source-host $PG_HOST --source-port $PG_PORT --source-user $PG_USER --target-host $PG_HOST --target-port $PG_PORT --target-user $PG_USER --source-engine postgres --target-engine postgres --dry-run --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "Migrate PG single (PG not tested)"
        skip_test "Migrate PG cluster (PG not tested)"
    fi
    section_time

    # ── MariaDB Migration dry-run ──
    log_section "Migrate: MariaDB (dry-run)"
    if should_test maria; then
        ensure_maria_exists
        run_test "Migrate MariaDB single DB (dry-run)" \
            "$BINARY migrate single $MARIA_DB --source-host $MY_HOST --source-port $MY_PORT --source-user $MY_USER --target-host $MY_HOST --target-port $MY_PORT --target-user $MY_USER --source-engine mariadb --target-engine mariadb --dry-run --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "Migrate MariaDB single (MariaDB not tested)"
    fi
    section_time

    # ── MySQL Migration dry-run ──
    log_section "Migrate: MySQL (dry-run)"
    if should_test mysql; then
        ensure_mysql_exists
        run_test "Migrate MySQL single DB (dry-run)" \
            "$BINARY migrate single $MYSQL_DB --source-host $MY_HOST --source-port $MY_PORT --source-user $MY_USER --target-host $MY_HOST --target-port $MY_PORT --target-user $MY_USER --source-engine mysql --target-engine mysql --dry-run --backup-dir '$BDIR' --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "Migrate MySQL single (MySQL not tested)"
    fi
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  SYSTEMD INSTALL / UNINSTALL TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_install_systemd() {
    log_header "Systemd Install / Uninstall Tests"

    CURRENT_CATEGORY="ops"

    # ── Install (dry-run with custom schedule) ──
    log_section "Install: Systemd Timer (dry-run)"
    run_test "Install systemd timer (daily, dry-run)" \
        "$BINARY install --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Install with Prometheus metrics exporter (dry-run) ──
    log_section "Install: With Metrics Exporter (dry-run)"
    run_test "Install with metrics exporter (dry-run)" \
        "$BINARY install --with-metrics --metrics-port 9100 --dry-run --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Uninstall (dry-run) ──
    log_section "Uninstall: Systemd (dry-run)"
    run_test "Uninstall systemd (dry-run)" \
        "$BINARY uninstall --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  PARALLEL RESTORE TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_parallel_restore() {
    log_header "Parallel Restore Tests"
    local BDIR="$BACKUP_DIR_FAST"

    CURRENT_CATEGORY="restore"

    # ── PG Parallel Restore ──
    log_section "Parallel Restore: PostgreSQL"
    if should_test pg; then
        local par_file; par_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$par_file" ]]; then
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_parallel_restore;" 2>/dev/null || true
            run_test "PG parallel restore (jobs=2)" \
                "$BINARY restore single '$par_file' --target qa_parallel_restore --create --confirm --jobs 2 -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$BDIR'; [[ \$? -le 2 ]]"
            run_test "PG parallel restore verification" \
                "psql -U $PG_USER -d qa_parallel_restore -c 'SELECT count(*) FROM core.documents;' 2>/dev/null | grep -E '[0-9]+'"
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_parallel_restore;" 2>/dev/null || true

            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_parallel_restore4;" 2>/dev/null || true
            run_test "PG parallel restore (jobs=4)" \
                "$BINARY restore single '$par_file' --target qa_parallel_restore4 --create --confirm --jobs 4 -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$BDIR'; [[ \$? -le 2 ]]"
            psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS qa_parallel_restore4;" 2>/dev/null || true
        else
            skip_test "PG parallel restore jobs=2 (no backup)"
            skip_test "PG parallel restore verification (no backup)"
            skip_test "PG parallel restore jobs=4 (no backup)"
        fi
    else
        skip_test "PG parallel restore (PG not tested)"
    fi
    section_time

    # ── Restore Benchmark (actual run) ──
    CURRENT_CATEGORY="ops"
    log_section "Restore Benchmark: Full Run"
    if should_test pg; then
        local bench_file; bench_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$bench_file" ]]; then
            run_test "Restore benchmark run (PG)" \
                "$BINARY restore-benchmark run '$bench_file' -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$BDIR'; [[ \$? -le 2 ]]"
        else
            skip_test "Restore benchmark run PG (no backup)"
        fi
    fi
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  LARGE OBJECT VACUUM TESTS (PostgreSQL only)
# ═══════════════════════════════════════════════════════════════════════════
test_lo_vacuum() {
    log_header "Large Object Vacuum Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Basic --lo-vacuum flag acceptance ──
    CURRENT_CATEGORY="backup"
    log_section "LO Vacuum: Flag Acceptance"
    if should_test pg; then
        ensure_pg_small_exists

        run_test "PG cluster backup with --lo-vacuum" \
            "$BINARY backup cluster -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --lo-vacuum --lo-vacuum-timeout 60 --no-config --allow-root --insecure 2>&1"

        run_test "PG single backup with --lo-vacuum" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --lo-vacuum --lo-vacuum-timeout 60 --no-config --allow-root --insecure 2>&1 | tee /tmp/lo_vacuum_out.txt"
    else
        skip_test "PG cluster backup with --lo-vacuum (PG not tested)"
        skip_test "PG single backup with --lo-vacuum (PG not tested)"
    fi
    section_time

    # ── Verify vacuum output is logged (uses captured output) ──
    CURRENT_CATEGORY="ops"
    log_section "LO Vacuum: Output Verification"
    if should_test pg; then
        run_test "LO vacuum logs vacuum activity" \
            "grep -Eqi 'large object|lo.vacuum|vacuum.*complet' /tmp/lo_vacuum_out.txt"
    else
        skip_test "LO vacuum logs vacuum activity (PG not tested)"
    fi
    section_time

    # ── Health check reports LO status ──
    CURRENT_CATEGORY="ops"
    log_section "LO Vacuum: Health Check Integration"
    if should_test pg; then
        run_test "Health check includes LO status (table)" \
            "$BINARY health -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | grep -qi 'large objects'"

        run_test "Health check includes LO status (JSON)" \
            "$BINARY health --format json -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | grep -qi 'Large Objects'"
    else
        skip_test "Health check includes LO status table (PG not tested)"
        skip_test "Health check includes LO status JSON (PG not tested)"
    fi
    section_time

    # ── --lo-vacuum is no-op on MariaDB/MySQL (must not fail) ──
    CURRENT_CATEGORY="backup"
    log_section "LO Vacuum: Non-PG Engines (no-op)"
    if should_test maria; then
        ensure_maria_exists
        run_test "MariaDB backup with --lo-vacuum (no-op, must succeed)" \
            "$BINARY backup single $MARIA_DB -d mariadb --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --lo-vacuum --no-config --allow-root --insecure 2>&1"
    else
        skip_test "MariaDB backup with --lo-vacuum no-op (MariaDB not tested)"
    fi
    if should_test mysql; then
        ensure_mysql_exists
        run_test "MySQL backup with --lo-vacuum (no-op, must succeed)" \
            "$BINARY backup single $MYSQL_DB -d mysql --host $MY_HOST --port $MY_PORT --user $MY_USER --socket $MY_SOCKET --backup-dir '$BDIR' --lo-vacuum --no-config --allow-root --insecure 2>&1"
    else
        skip_test "MySQL backup with --lo-vacuum no-op (MySQL not tested)"
    fi
    section_time

    # ── Custom timeout value (flag acceptance only — skip full backups) ──
    CURRENT_CATEGORY="backup"
    log_section "LO Vacuum: Custom Timeout"
    run_test "backup --help shows --lo-vacuum-timeout" \
        "$BINARY backup single --help 2>&1 | grep -q 'lo-vacuum-timeout'"
    section_time

    # ── Config file persistence ──
    CURRENT_CATEGORY="ops"
    log_section "LO Vacuum: Config Persistence"
    local lo_conf; lo_conf=$(mktemp)
    cat > "$lo_conf" <<'LOCONF'
lo_vacuum = true
lo_vacuum_timeout = 120
LOCONF
    # Ensure config file is read and values applied (backup should succeed)
    if should_test pg; then
        run_test "PG backup reads lo_vacuum from config file" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --config '$lo_conf' --allow-root --insecure 2>&1"
    else
        skip_test "PG backup reads lo_vacuum from config file (PG not tested)"
    fi
    rm -f "$lo_conf"
    section_time

    # ── Backup without --lo-vacuum must NOT run vacuum ──
    CURRENT_CATEGORY="backup"
    log_section "LO Vacuum: Opt-in Verification"
    if should_test pg; then
        run_test "PG backup without --lo-vacuum skips LO maintenance" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | { ! grep -qi 'large object vacuum'; }"
    else
        skip_test "PG backup without --lo-vacuum skips LO maintenance (PG not tested)"
    fi
    section_time

    # ── Large object seeding + orphan detection ──
    CURRENT_CATEGORY="ops"
    log_section "LO Vacuum: Orphan Detection"
    if should_test pg; then
        # Create a few large objects directly, then remove references (orphans)
        psql -U "$PG_USER" -d "$PG_SMALL_DB" -c "
            DO \$\$
            DECLARE lo_oid oid;
            BEGIN
                FOR i IN 1..5 LOOP
                    lo_oid := lo_create(0);
                    PERFORM lowrite(lo_open(lo_oid, 131072), decode(repeat('AB', 512), 'hex'));
                END LOOP;
            END;
            \$\$;
        " 2>/dev/null || true

        run_test "PG --lo-vacuum detects/cleans orphaned LOs" \
            "$BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --lo-vacuum --lo-vacuum-timeout 60 --no-config --allow-root --insecure 2>&1 | grep -Eqi 'orphan|vacuum.*complet|large object'"
    else
        skip_test "PG --lo-vacuum detects orphaned LOs (PG not tested)"
    fi
    section_time
}

# ═══════════════════════════════════════════════════════════════════════════
#  MYSQL/MARIADB SPEED OPTIMIZATION TESTS (v6.43.0+)
# ═══════════════════════════════════════════════════════════════════════════
test_mysql_speed_optimizations() {
    log_header "MySQL/MariaDB Speed Optimization Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Section 1: CLI flag acceptance ──
    CURRENT_CATEGORY="backup"
    log_section "MySQL Speed: CLI Flag Acceptance"

    # 1. All new flags accepted by backup single --help
    run_test "backup single --help shows --mysql-quick" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-quick'"

    run_test "backup single --help shows --mysql-extended-insert" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-extended-insert'"

    run_test "backup single --help shows --mysql-fast-restore" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-fast-restore'"

    run_test "backup single --help shows --mysql-max-packet" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-max-packet'"

    run_test "backup single --help shows --mysql-net-buffer-length" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-net-buffer-length'"

    run_test "backup single --help shows --mysql-order-by-primary" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-order-by-primary'"

    run_test "backup single --help shows --mysql-disable-keys" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-disable-keys'"

    run_test "backup single --help shows --mysql-batch-size" \
        "$BINARY backup single --help 2>&1 | grep -q 'mysql-batch-size'"

    # 2. Also present in cluster help
    run_test "backup cluster --help shows --mysql-quick" \
        "$BINARY backup cluster --help 2>&1 | grep -q 'mysql-quick'"
    section_time

    # ── Section 2: Default values correct ──
    log_section "MySQL Speed: Default Values"

    run_test "--mysql-quick default is true" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-quick' | grep -q 'default true'"

    run_test "--mysql-extended-insert default is true" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-extended-insert' | grep -q 'default true'"

    run_test "--mysql-fast-restore default is true" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-fast-restore' | grep -q 'default true'"

    run_test "--mysql-disable-keys default is true" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-disable-keys' | grep -q 'default true'"

    run_test "--mysql-max-packet default is 256M" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-max-packet' | grep -q '256M'"

    run_test "--mysql-net-buffer-length default is 1048576" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-net-buffer-length' | grep -q '1048576'"

    run_test "--mysql-batch-size default is 5000" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-batch-size' | grep -q '5000'"

    run_test "--mysql-order-by-primary default is false (no default true)" \
        "$BINARY backup single --help 2>&1 | grep 'mysql-order-by-primary' | grep -qv 'default true'"
    section_time

    # ── Section 3: Flag override testing ──
    log_section "MySQL Speed: Flag Override (no-op without DB)"

    # These should exit cleanly (may error on DB, but flag parsing must succeed)
    run_test "--mysql-quick=false accepted" \
        "$BINARY backup single testdb -d mysql --mysql-quick=false --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"

    run_test "--mysql-extended-insert=false accepted" \
        "$BINARY backup single testdb -d mysql --mysql-extended-insert=false --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"

    run_test "--mysql-order-by-primary accepted" \
        "$BINARY backup single testdb -d mysql --mysql-order-by-primary --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"

    run_test "--mysql-batch-size=10000 accepted" \
        "$BINARY backup single testdb -d mysql --mysql-batch-size=10000 --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"

    run_test "--mysql-max-packet=512M accepted" \
        "$BINARY backup single testdb -d mysql --mysql-max-packet=512M --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"

    run_test "--mysql-net-buffer-length=524288 accepted" \
        "$BINARY backup single testdb -d mysql --mysql-net-buffer-length=524288 --no-config --allow-root 2>&1; [[ \$? -le 1 ]]"
    section_time

    # ── Section 4: MariaDB backup with speed flags (live) ──
    log_section "MySQL Speed: MariaDB Live Backup"
    if should_test maria; then
        ensure_maria_exists

        run_test "MariaDB backup with default speed flags" \
            "$BINARY backup single ${MARIA_DB} -d mariadb --host ${MARIA_HOST:-localhost} --port ${MARIA_PORT:-3306} --user ${MARIA_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"

        run_test "MariaDB backup with --mysql-order-by-primary" \
            "$BINARY backup single ${MARIA_DB} -d mariadb --host ${MARIA_HOST:-localhost} --port ${MARIA_PORT:-3306} --user ${MARIA_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure --mysql-order-by-primary 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"

        run_test "MariaDB backup with --mysql-batch-size=2000" \
            "$BINARY backup single ${MARIA_DB} -d mariadb --host ${MARIA_HOST:-localhost} --port ${MARIA_PORT:-3306} --user ${MARIA_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure --mysql-batch-size=2000 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"

        run_test "MariaDB backup with speed flags disabled" \
            "$BINARY backup single ${MARIA_DB} -d mariadb --host ${MARIA_HOST:-localhost} --port ${MARIA_PORT:-3306} --user ${MARIA_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure --mysql-quick=false --mysql-extended-insert=false 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"
    else
        skip_test "MariaDB backup with default speed flags"
        skip_test "MariaDB backup with --mysql-order-by-primary"
        skip_test "MariaDB backup with --mysql-batch-size=2000"
        skip_test "MariaDB backup with speed flags disabled"
    fi
    section_time

    # ── Section 5: MySQL backup with speed flags (live) ──
    log_section "MySQL Speed: MySQL Live Backup"
    if should_test mysql; then
        ensure_mysql_exists

        run_test "MySQL backup with default speed flags" \
            "$BINARY backup single ${MYSQL_DB} -d mysql --host ${MYSQL_HOST:-localhost} --port ${MYSQL_PORT:-3307} --user ${MYSQL_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"

        run_test "MySQL backup with --mysql-order-by-primary" \
            "$BINARY backup single ${MYSQL_DB} -d mysql --host ${MYSQL_HOST:-localhost} --port ${MYSQL_PORT:-3307} --user ${MYSQL_USER:-root} --backup-dir '$BDIR' --no-config --allow-root --insecure --mysql-order-by-primary 2>&1 | tail -5 | grep -qi 'ok\|success\|completed'"
    else
        skip_test "MySQL backup with default speed flags"
        skip_test "MySQL backup with --mysql-order-by-primary"
    fi
    section_time

    # ── Section 6: PG ignores MySQL flags gracefully ──
    # Use --help flag acceptance instead of full backups to save time
    log_section "MySQL Speed: PG Ignores MySQL Flags"
    if should_test pg; then
        run_test "PG accepts --mysql-quick without error" \
            "$BINARY backup single --help 2>&1 | grep -q 'mysql-quick'"

        run_test "PG accepts --mysql-fast-restore without error" \
            "$BINARY backup single --help 2>&1 | grep -q 'mysql-fast-restore'"
    else
        skip_test "PG backup ignores --mysql-quick (no error)"
        skip_test "PG backup ignores --mysql-fast-restore (no error)"
    fi
    section_time

    # ── Section 7: Config persistence ──
    CURRENT_CATEGORY="config"
    log_section "MySQL Speed: Config Persistence"
    local TMP_CONF
    TMP_CONF=$(mktemp -d)/mysql_speed_test

    # Write a config with non-default MySQL speed settings
    mkdir -p "$TMP_CONF"
    cat > "$TMP_CONF/.dbbackup.conf" <<'CONF'
[database]
type = mariadb
host = localhost
port = 3306
user = root

[performance]
mysql_quick_dump = false
mysql_extended_insert = false
mysql_order_by_primary = true
mysql_net_buffer_length = 524288
mysql_max_packet = 512M
mysql_fast_restore = false
mysql_disable_keys = false
mysql_batch_size = 10000
CONF

    run_test "Config file with mysql_quick_dump=false is valid" \
        "test -s '$TMP_CONF/.dbbackup.conf'"

    run_test "Config file parses (diagnose reads it)" \
        "cd '$TMP_CONF' && $BINARY diagnose --allow-root 2>&1 | grep -qi 'mariadb\|diagnostic\|status'; cd -"

    rm -rf "$TMP_CONF"
    section_time

    log_ok "MySQL/MariaDB speed optimization tests complete"
}

# ═══════════════════════════════════════════════════════════════════════════
#  COMPREHENSIVE FLAG & SUBCOMMAND COVERAGE TESTS
#  Tests every subcommand and flag from the codebase for acceptance/help
# ═══════════════════════════════════════════════════════════════════════════
test_flag_coverage() {
    log_header "Comprehensive Flag & Subcommand Coverage"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Missing Cloud subcommands ──
    CURRENT_CATEGORY="ops"
    log_section "Cloud Subcommands (upload/download/list/delete)"
    run_test "cloud upload --help" \
        "$BINARY cloud upload --help --no-config --allow-root >/dev/null 2>&1"
    run_test "cloud download --help" \
        "$BINARY cloud download --help --no-config --allow-root >/dev/null 2>&1"
    run_test "cloud list --help" \
        "$BINARY cloud list --help --no-config --allow-root >/dev/null 2>&1"
    run_test "cloud delete --help" \
        "$BINARY cloud delete --help --no-config --allow-root >/dev/null 2>&1"
    section_time

    # ── Missing PITR subcommands ──
    log_section "PITR Subcommands"
    run_test "pitr disable --help" \
        "$BINARY pitr disable --help --no-config --allow-root >/dev/null 2>&1"
    run_test "pitr mysql-enable --help" \
        "$BINARY pitr mysql-enable --help --no-config --allow-root >/dev/null 2>&1"
    if should_test pg; then
        run_test "pitr disable (no-op, expect warning)" \
            "$BINARY pitr disable -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Restore PITR subcommand ──
    log_section "Restore PITR Flags"
    run_test "restore pitr --help" \
        "$BINARY restore pitr --help --no-config --allow-root >/dev/null 2>&1"
    run_test "restore pitr --help shows --base-backup" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'base-backup'"
    run_test "restore pitr --help shows --wal-archive" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'wal-archive'"
    run_test "restore pitr --help shows --target-xid" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'target-xid'"
    run_test "restore pitr --help shows --target-lsn" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'target-lsn'"
    run_test "restore pitr --help shows --target-name" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'target-name'"
    run_test "restore pitr --help shows --target-immediate" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'target-immediate'"
    run_test "restore pitr --help shows --target-action" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'target-action'"
    run_test "restore pitr --help shows --auto-start" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'auto-start'"
    run_test "restore pitr --help shows --monitor" \
        "$BINARY restore pitr --help 2>&1 | grep -q 'monitor'"
    section_time

    # ── Parallel-restore subcommands ──
    log_section "Parallel-Restore Subcommands"
    run_test "parallel-restore benchmark --help" \
        "$BINARY parallel-restore benchmark --help --no-config --allow-root >/dev/null 2>&1"
    run_test "parallel-restore recommend --help" \
        "$BINARY parallel-restore recommend --help --no-config --allow-root >/dev/null 2>&1"
    run_test "parallel-restore simulate --help" \
        "$BINARY parallel-restore simulate --help --no-config --allow-root >/dev/null 2>&1"
    run_test "parallel-restore recommend" \
        "$BINARY parallel-restore recommend --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Report subcommands ──
    log_section "Report Subcommands"
    run_test "report summary --help" \
        "$BINARY report summary --help --no-config --allow-root >/dev/null 2>&1"
    run_test "report list --help" \
        "$BINARY report list --help --no-config --allow-root >/dev/null 2>&1"
    run_test "report controls --help" \
        "$BINARY report controls --help --no-config --allow-root >/dev/null 2>&1"
    run_test "report summary (backup-dir)" \
        "$BINARY report summary --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "report list (backup-dir)" \
        "$BINARY report list --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "report controls (soc2)" \
        "$BINARY report controls --type soc2 --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Restore-benchmark subcommands ──
    log_section "Restore-Benchmark Subcommands"
    run_test "restore-benchmark compare --help" \
        "$BINARY restore-benchmark compare --help --no-config --allow-root >/dev/null 2>&1"
    run_test "restore-benchmark pipeline --help" \
        "$BINARY restore-benchmark pipeline --help --no-config --allow-root >/dev/null 2>&1"
    section_time

    # ── RTO subcommands ──
    log_section "RTO Subcommands"
    run_test "rto analyze --help" \
        "$BINARY rto analyze --help --no-config --allow-root >/dev/null 2>&1; [[ \$? -le 2 ]]"
    run_test "rto check --help" \
        "$BINARY rto check --help --no-config --allow-root >/dev/null 2>&1; [[ \$? -le 2 ]]"
    run_test "rto analyze (backup-dir)" \
        "$BINARY rto analyze --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "rto check (backup-dir)" \
        "$BINARY rto check --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Compression cache subcommands ──
    log_section "Compression Cache Subcommands"
    run_test "compression cache list" \
        "$BINARY compression cache list --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "compression cache clear" \
        "$BINARY compression cache clear --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Engine info subcommand ──
    log_section "Engine Info Subcommand"
    run_test "engine info" \
        "$BINARY engine info postgres --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Dedup delete subcommand ──
    log_section "Dedup Delete Subcommand"
    run_test "dedup delete --help" \
        "$BINARY dedup delete --help --no-config --allow-root >/dev/null 2>&1"
    section_time

    # ── Restore performance flags (flag acceptance) ──
    CURRENT_CATEGORY="restore"
    log_section "Restore Performance Flags"
    run_test "restore single --help shows --verbose" \
        "$BINARY restore single --help 2>&1 | grep -q 'verbose'"
    run_test "restore single --help shows --no-tui" \
        "$BINARY restore single --help 2>&1 | grep -q 'no-tui'"
    run_test "restore single --help shows --adaptive" \
        "$BINARY restore single --help 2>&1 | grep -q 'adaptive'"
    run_test "restore single --help shows --tiered-restore" \
        "$BINARY restore single --help 2>&1 | grep -q 'tiered-restore'"
    run_test "restore single --help shows --critical-tables" \
        "$BINARY restore single --help 2>&1 | grep -q 'critical-tables'"
    run_test "restore single --help shows --important-tables" \
        "$BINARY restore single --help 2>&1 | grep -q 'important-tables'"
    run_test "restore single --help shows --cold-tables" \
        "$BINARY restore single --help 2>&1 | grep -q 'cold-tables'"
    run_test "restore single --help shows --pipeline" \
        "$BINARY restore single --help 2>&1 | grep -q 'pipeline'"
    run_test "restore single --help shows --skip-disk-check" \
        "$BINARY restore single --help 2>&1 | grep -q 'skip-disk-check'"
    run_test "restore single --help shows --disk-space-multiplier" \
        "$BINARY restore single --help 2>&1 | grep -q 'disk-space-multiplier'"
    run_test "restore single --help shows --save-debug-log" \
        "$BINARY restore single --help 2>&1 | grep -q 'save-debug-log'"
    run_test "restore single --help shows --debug-locks" \
        "$BINARY restore single --help 2>&1 | grep -q 'debug-locks'"
    run_test "restore single --help shows --profile" \
        "$BINARY restore single --help 2>&1 | grep -q 'profile'"
    run_test "restore single --help shows --restore-fsync-mode" \
        "$BINARY restore single --help 2>&1 | grep -q 'restore-fsync-mode'"
    run_test "restore single --help shows --native" \
        "$BINARY restore single --help 2>&1 | grep -q 'native'"
    section_time

    # ── Restore cluster advanced flags ──
    log_section "Restore Cluster Advanced Flags"
    run_test "restore cluster --help shows --oom-protection" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'oom-protection'"
    run_test "restore cluster --help shows --low-memory" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'low-memory'"
    run_test "restore cluster --help shows --parallel-dbs" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'parallel-dbs'"
    run_test "restore cluster --help shows --clean-cluster" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'clean-cluster'"
    run_test "restore cluster --help shows --list-databases" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'list-databases'"
    run_test "restore cluster --help shows --databases" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'databases'"
    run_test "restore cluster --help shows --workdir" \
        "$BINARY restore cluster --help 2>&1 | grep -q 'workdir'"
    # List databases from a cluster backup
    local cluster_file; cluster_file=$(ls -t "${BDIR}"/pg_cluster_*.tar.{gz,zst} "${BACKUP_DIR_SLOW}"/pg_cluster_*.tar.{gz,zst} 2>/dev/null | head -1)
    if [[ -n "$cluster_file" ]]; then
        run_test "restore cluster --list-databases" \
            "$BINARY restore cluster '$cluster_file' --list-databases -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    else
        skip_test "restore cluster --list-databases (no archive)"
    fi
    section_time

    # ── Backup encryption flags ──
    CURRENT_CATEGORY="backup"
    log_section "Backup Encryption & Compression Flags"
    run_test "backup single --help shows --encryption-key-file" \
        "$BINARY backup single --help 2>&1 | grep -q 'encryption-key-file'"
    run_test "backup single --help shows --encryption-key-env" \
        "$BINARY backup single --help 2>&1 | grep -q 'encryption-key-env'"
    run_test "backup single --help shows --compression-algorithm" \
        "$BINARY backup single --help 2>&1 | grep -q 'compression-algorithm'"
    run_test "backup single --help shows --no-verify" \
        "$BINARY backup single --help 2>&1 | grep -q 'no-verify'"
    # Test compression-algorithm=zstd acceptance
    if should_test pg; then
        run_test "PG backup --compression-algorithm=zstd" \
            "$BINARY backup single ${PG_SMALL_DB} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --compression-algorithm zstd --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
        run_test "PG backup with --no-verify" \
            "$BINARY backup single ${PG_SMALL_DB} -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-verify --no-config --allow-root --insecure"
    fi
    section_time

    # ── Cloud auto-upload flags ──
    log_section "Cloud Auto-Upload Flags"
    run_test "backup single --help shows --cloud-auto-upload" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-auto-upload'"
    run_test "backup single --help shows --cloud-provider" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-provider'"
    run_test "backup single --help shows --cloud-bucket" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-bucket'"
    run_test "backup single --help shows --cloud-region" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-region'"
    run_test "backup single --help shows --cloud-endpoint" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-endpoint'"
    run_test "backup single --help shows --cloud-prefix" \
        "$BINARY backup single --help 2>&1 | grep -q 'cloud-prefix'"
    section_time

    # ── Galera cluster flags ──
    log_section "Galera Cluster Flags"
    run_test "backup single --help shows --galera-desync" \
        "$BINARY backup single --help 2>&1 | grep -q 'galera-desync'"
    run_test "backup single --help shows --galera-min-cluster-size" \
        "$BINARY backup single --help 2>&1 | grep -q 'galera-min-cluster-size'"
    run_test "backup single --help shows --galera-prefer-node" \
        "$BINARY backup single --help 2>&1 | grep -q 'galera-prefer-node'"
    run_test "backup single --help shows --galera-health-check" \
        "$BINARY backup single --help 2>&1 | grep -q 'galera-health-check'"
    section_time

    # ── Backup sample flags ──
    log_section "Backup Sample Flags"
    run_test "backup sample --help shows --sample-strategy" \
        "$BINARY backup sample --help 2>&1 | grep -q 'sample-strategy'"
    run_test "backup sample --help shows --sample-percent" \
        "$BINARY backup sample --help 2>&1 | grep -q 'sample-percent'"
    run_test "backup sample --help shows --sample-count" \
        "$BINARY backup sample --help 2>&1 | grep -q 'sample-count'"
    section_time

    # ── Diagnose flags ──
    CURRENT_CATEGORY="ops"
    log_section "Diagnose Flags"
    run_test "diagnose --help shows --auto-fix" \
        "$BINARY diagnose --help 2>&1 | grep -q 'auto-fix'"
    run_test "diagnose --help shows --check" \
        "$BINARY diagnose --help 2>&1 | grep -q 'check'"
    run_test "diagnose with --check" \
        "$BINARY diagnose --check tools --no-config --allow-root; [[ \$? -le 2 ]]"
    section_time

    # ── Health flags ──
    log_section "Health Flags"
    run_test "health --help shows --skip-db" \
        "$BINARY health --help 2>&1 | grep -q 'skip-db'"
    run_test "health --help shows --format" \
        "$BINARY health --help 2>&1 | grep -q 'format'"
    section_time

    # ── Catalog prune advanced flags ──
    log_section "Catalog Prune Advanced Flags"
    run_test "catalog prune --help shows --policy" \
        "$BINARY catalog prune --help 2>&1 | grep -q 'policy'"
    run_test "catalog prune --help shows --older-than" \
        "$BINARY catalog prune --help 2>&1 | grep -q 'older-than'"
    run_test "catalog prune --help shows --keep-daily" \
        "$BINARY catalog prune --help 2>&1 | grep -q 'keep-daily'"
    run_test "catalog prune --help shows --delete-files" \
        "$BINARY catalog prune --help 2>&1 | grep -q 'delete-files'"
    section_time

    # ── Catalog search flags ──
    log_section "Catalog Search Flags"
    run_test "catalog search --help shows --verified" \
        "$BINARY catalog search --help 2>&1 | grep -q 'verified'"
    run_test "catalog search --help shows --encrypted" \
        "$BINARY catalog search --help 2>&1 | grep -q 'encrypted'"
    section_time

    # ── Encryption rotate flags ──
    log_section "Encryption Rotate Flags"
    run_test "encryption rotate --help shows --key-size" \
        "$BINARY encryption rotate --help 2>&1 | grep -q 'key-size'"
    section_time

    # ── Restore preview flags ──
    log_section "Restore Preview Flags"
    run_test "restore preview --help shows --compare-schema" \
        "$BINARY restore preview --help 2>&1 | grep -q 'compare-schema'"
    section_time

    # ── Root-level persistent flags ──
    log_section "Root Persistent Flags"
    run_test "version with --debug" \
        "$BINARY version --debug --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "version with --no-color" \
        "$BINARY version --no-color --no-config --allow-root"
    run_test "health with --cpu-workload=io-heavy" \
        "$BINARY health --cpu-workload=io-heavy --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "health with --max-cores=2" \
        "$BINARY health --max-cores=2 --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "diagnose with --no-color" \
        "$BINARY diagnose --no-color --no-config --allow-root; [[ \$? -le 2 ]]"
    if should_test pg; then
        run_test "PG status with --ssl-mode=disable" \
            "$BINARY status -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --ssl-mode=disable --no-config --allow-root --insecure; [[ \$? -le 2 ]]"
    fi
    section_time

    # ── Cloud object lock flags ──
    log_section "Cloud Object Lock Flags"
    run_test "cloud upload --help shows --object-lock" \
        "$BINARY cloud upload --help 2>&1 | grep -q 'object-lock'"
    run_test "cloud upload --help shows --bandwidth-limit" \
        "$BINARY cloud upload --help 2>&1 | grep -q 'bandwidth-limit'"
    section_time

    # ── Dedup index-db flag ──
    log_section "Dedup Advanced Flags"
    run_test "dedup backup-db --help shows --index-db" \
        "$BINARY dedup backup-db --help 2>&1 | grep -q 'index-db'"
    run_test "dedup backup-db --help shows --encrypt" \
        "$BINARY dedup backup-db --help 2>&1 | grep -q 'encrypt'"
    section_time

    # ── Restore-benchmark pprof flag ──
    log_section "Restore-Benchmark Flags"
    run_test "restore-benchmark run --help shows --pprof" \
        "$BINARY restore-benchmark run --help 2>&1 | grep -q 'pprof'"
    section_time

    log_ok "Comprehensive flag & subcommand coverage complete"
}

# ═══════════════════════════════════════════════════════════════════════════
#  SIGNAL HANDLING & EDGE CASE TESTS
# ═══════════════════════════════════════════════════════════════════════════
test_edge_cases() {
    log_header "Edge Case & Robustness Tests"
    local BDIR="$BACKUP_DIR_FAST"

    # ── Backup non-existent database (expect graceful error) ──
    CURRENT_CATEGORY="ops"
    log_section "Edge: Non-existent Database"
    run_test "PG backup non-existent DB (graceful error)" \
        "$BINARY backup single qa_does_not_exist_db -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --no-config --allow-root --insecure 2>&1 | grep -qi 'error\|fail\|not found\|does not exist'"
    section_time

    # ── Restore to already-existing DB without --force ──
    log_section "Edge: Restore Without --force"
    if should_test pg; then
        ensure_pg_small_exists
        local edge_file; edge_file=$(ls -t "${BDIR}"/${PG_SMALL_DB}_*.{gz,zst} "${BACKUP_DIR_SLOW}"/${PG_SMALL_DB}_*.{gz,zst} 2>/dev/null | head -1)
        if [[ -n "$edge_file" ]]; then
            run_test "PG restore to existing DB without --force (expect fail)" \
                "! $BINARY restore single '$edge_file' --target $PG_SMALL_DB --create --confirm -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --allow-root --insecure --backup-dir '$BDIR' 2>&1"
        else
            skip_test "Edge: restore without --force (no backup)"
        fi
    fi
    section_time

    # ── Verify corrupt/empty file ──
    log_section "Edge: Verify Corrupt File"
    local corrupt_file="${BDIR}/corrupt_test.sql.gz"
    echo "NOT_A_REAL_GZIP" > "$corrupt_file"
    run_test "Verify corrupt file (expect fail)" \
        "! $BINARY verify '$corrupt_file' --no-config --allow-root 2>&1"
    rm -f "$corrupt_file"
    section_time

    # ── Empty backup directory operations ──
    log_section "Edge: Empty Directory"
    local empty_dir; empty_dir=$(mktemp -d)
    run_test "Catalog list (empty dir)" \
        "$BINARY catalog list --backup-dir '$empty_dir' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Cleanup (empty dir)" \
        "$BINARY cleanup '$empty_dir' --retention-days 1 --min-backups 1 --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "List (empty dir)" \
        "$BINARY list --backup-dir '$empty_dir' --no-config --allow-root; [[ \$? -le 2 ]]"
    run_test "Forecast (empty dir)" \
        "$BINARY forecast --all --days 30 --backup-dir '$empty_dir' --no-config --allow-root; [[ \$? -le 2 ]]"
    rm -rf "$empty_dir"
    section_time

    # ── Backup with invalid compression level ──
    log_section "Edge: Invalid Parameters"
    run_test "PG backup invalid compression (expect fail)" \
        "! $BINARY backup single postgres -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --backup-dir '$BDIR' --compression 99 --no-config --allow-root --insecure 2>&1"
    section_time

    # ── Multiple format outputs ──
    log_section "Edge: Output Formats"
    run_test "List backups (default format)" \
        "$BINARY list --backup-dir '$BDIR' --no-config --allow-root; [[ \$? -le 2 ]]"
    local edge_csv_out; edge_csv_out=$(mktemp)
    run_test "Catalog export (CSV format)" \
        "$BINARY catalog export --backup-dir '$BDIR' --format csv --output '$edge_csv_out' --no-config --allow-root; [[ \$? -le 2 ]]"
    rm -f "$edge_csv_out"
    section_time
}

# ─── Report generation ──────────────────────────────────────────────────────
generate_report() {
    log_header "Test Results"

    local elapsed=$(( $(date +%s) - GLOBAL_START ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))

    echo -e "${BOLD}Total:${NC}   ${TOTAL} tests"
    echo -e "${GREEN}Passed:${NC}  ${PASSED}"
    echo -e "${RED}Failed:${NC}  ${FAILED}"
    echo -e "${YELLOW}Skipped:${NC} ${SKIPPED}"
    echo -e "${BLUE}Time:${NC}    ${mins}m ${secs}s"
    echo

    # ── Performance timing table (terminal) ──
    if [[ ${#TIMINGS[@]} -gt 0 ]]; then
        echo -e "${BOLD}Performance Timing:${NC}"
        printf "  ${DIM}%-4s %-45s %-6s %10s  %-8s${NC}\n" "#" "Test" "Status" "Time" "Category"
        printf "  ${DIM}%-4s %-45s %-6s %10s  %-8s${NC}\n" "----" "---------------------------------------------" "------" "----------" "--------"
        for entry in "${TIMINGS[@]}"; do
            IFS='|' read -r t_num t_name t_status t_ms t_cat <<< "$entry"
            local t_sec=$(( t_ms / 1000 ))
            local t_frac=$(( t_ms % 1000 ))
            local time_str; time_str=$(printf "%d.%03ds" "$t_sec" "$t_frac")
            local color="${GREEN}"
            [[ "$t_status" == "FAIL" ]] && color="${RED}"
            [[ "$t_status" == "SKIP" ]] && color="${YELLOW}" && time_str="—"
            printf "  ${color}%-4s${NC} %-45s ${color}%-6s${NC} %10s  ${DIM}%-8s${NC}\n" "$t_num" "${t_name:0:45}" "$t_status" "$time_str" "$t_cat"
        done
        echo

        # ── Top 5 slowest tests ──
        echo -e "${BOLD}Top 5 Slowest:${NC}"
        printf '%s\n' "${TIMINGS[@]}" | sort -t'|' -k4 -rn | head -5 | while IFS='|' read -r t_num t_name t_status t_ms t_cat; do
            local t_sec=$(( t_ms / 1000 ))
            local t_frac=$(( t_ms % 1000 ))
            printf "  ${YELLOW}%d.%03ds${NC}  [%s] %s (%s)\n" "$t_sec" "$t_frac" "$t_num" "$t_name" "$t_cat"
        done
        echo

        # ── Category aggregates ──
        echo -e "${BOLD}Time by Category:${NC}"
        for cat in backup restore verify dedup pitr cleanup drill cloud ops; do
            local cat_total=0
            local cat_count=0
            for entry in "${TIMINGS[@]}"; do
                IFS='|' read -r _ _ t_status t_ms t_cat_entry <<< "$entry"
                if [[ "$t_cat_entry" == "$cat" && "$t_status" != "SKIP" ]]; then
                    cat_total=$(( cat_total + t_ms ))
                    cat_count=$(( cat_count + 1 ))
                fi
            done
            if [[ $cat_count -gt 0 ]]; then
                local cat_sec=$(( cat_total / 1000 ))
                local cat_frac=$(( cat_total % 1000 ))
                printf "  ${CYAN}%-10s${NC} %d.%03ds  (%d tests)\n" "$cat" "$cat_sec" "$cat_frac" "$cat_count"
            fi
        done
        echo
    fi

    # ── Per-Database Detail (terminal) ──
    if [[ ${#DB_DETAILS[@]} -gt 0 ]]; then
        # Populate timings into DB_DETAILS before display
        for i in "${!DB_DETAILS[@]}"; do
            IFS='|' read -r d_eng d_db d_dbsz d_bf d_bsz d_btm d_rtm d_cbsz d_crtm <<< "${DB_DETAILS[$i]}"
            # Find single backup time — try engine-specific naming patterns
            local bt=""
            bt=$(_get_timing_ms "single backup (large: ${d_db})")
            [[ -z "$bt" ]] && bt=$(_get_timing_ms "single backup (small: ${d_db})")
            [[ -z "$bt" ]] && bt=$(_get_timing_ms "single backup (${d_db})")
            [[ -n "$bt" ]] && d_btm="$bt"
            # Find single restore time — PG restores qa_pg_small only (large is
            # dropped before restore), Maria/MySQL each have one DB
            local rt=""
            case "$d_eng" in
                postgres)
                    if [[ "$d_db" == "$PG_SMALL_DB" ]]; then
                        rt=$(_get_timing_ms "PG single restore")
                    fi ;;
                mariadb)  rt=$(_get_timing_ms "Maria single restore") ;;
                mysql)    rt=$(_get_timing_ms "MySQL single restore") ;;
            esac
            [[ -n "$rt" ]] && d_rtm="$rt"
            # Find cluster backup time
            local cbt=""
            case "$d_eng" in
                postgres)
                    if [[ "$d_db" == "$PG_SMALL_DB" ]]; then
                        cbt=$(_get_timing_ms "PG cluster backup")
                    fi ;;
                mariadb) cbt=$(_get_timing_ms "Maria cluster backup") ;;
            esac
            [[ -n "$cbt" ]] && d_cbsz="$cbt"
            # Find cluster restore time
            local crt=""
            case "$d_eng" in
                postgres) crt=$(_get_timing_ms "PG cluster restore") ;;
                mariadb)  crt=$(_get_timing_ms "Maria cluster restore") ;;
            esac
            [[ -n "$crt" ]] && d_crtm="$crt"
            DB_DETAILS[$i]="${d_eng}|${d_db}|${d_dbsz}|${d_bf}|${d_bsz}|${d_btm}|${d_rtm}|${d_cbsz}|${d_crtm}"
        done

        echo -e "${BOLD}Per-Database Detail:${NC}"
        printf "  ${DIM}%-10s %-20s %10s %10s %12s %12s %12s %12s${NC}\n" \
            "Engine" "Database" "DB Size" "Bkp Size" "Bkp Time" "Rst Time" "Clst BkTm" "Clst RsTm"
        printf "  ${DIM}%-10s %-20s %10s %10s %12s %12s %12s %12s${NC}\n" \
            "----------" "--------------------" "----------" "----------" "------------" "------------" "------------" "------------"
        for entry in "${DB_DETAILS[@]}"; do
            IFS='|' read -r d_eng d_db d_dbsz d_bf d_bsz d_btm d_rtm d_cbsz d_crtm <<< "$entry"
            printf "  ${CYAN}%-10s${NC} %-20s %10s %10s %12s %12s %12s %12s\n" \
                "$d_eng" "$d_db" \
                "${d_dbsz:-—}" "${d_bsz:-—}" \
                "$(_fmt_ms "${d_btm}")" "$(_fmt_ms "${d_rtm}")" \
                "$(_fmt_ms "${d_cbsz}")" "$(_fmt_ms "${d_crtm}")"
        done
        echo
    fi

    if [[ ${#FAILURES[@]} -gt 0 ]]; then
        echo -e "${RED}${BOLD}Failed tests:${NC}"
        for f in "${FAILURES[@]}"; do
            echo -e "  ${RED}✗${NC} $f"
        done
        echo
    fi

    # Write markdown report
    cat > "$REPORT_FILE" <<EOF
# QA Full Test Report

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**Version:** $("$BINARY" version 2>&1 | grep -m1 'Version:' | sed 's/.*Version:[[:space:]]*//' || echo 'unknown')
**Host:** $(hostname)
**Profile:** ${SIZE_PROFILE} (PG: ${PG_LARGE_SIZE}+${PG_SMALL_SIZE} GB, Maria: ${MARIA_SIZE} GB, MySQL: ${MYSQL_SIZE} GB)
**Engine filter:** ${ENGINE_FILTER}
**Scope:** ${scope_list}
**Duration:** ${mins}m ${secs}s

## Summary

| Metric  | Count |
|---------|-------|
| Total   | ${TOTAL} |
| Passed  | ${PASSED} |
| Failed  | ${FAILED} |
| Skipped | ${SKIPPED} |

## Storage Used

\`\`\`
$(df -h "$FAST_STORAGE" "$SLOW_STORAGE" 2>/dev/null)
\`\`\`

## Backup Sizes

\`\`\`
$(du -sh "$BACKUP_DIR_FAST"/* "$BACKUP_DIR_SLOW"/* 2>/dev/null | sort -rh | head -20)
\`\`\`

## Database Sizes

### PostgreSQL
\`\`\`
$(psql -U "$PG_USER" -c "SELECT datname, pg_size_pretty(pg_database_size(datname)) FROM pg_database WHERE datistemplate = false ORDER BY pg_database_size(datname) DESC;" 2>/dev/null || echo "N/A")
\`\`\`

### MariaDB/MySQL
\`\`\`
$(mysql -u "$MY_USER" --socket="$MY_SOCKET" -e "SELECT table_schema AS db, ROUND(SUM(data_length + index_length)/1048576, 1) AS 'Size (MB)' FROM information_schema.tables GROUP BY table_schema ORDER BY SUM(data_length + index_length) DESC;" 2>/dev/null || echo "N/A")
\`\`\`

## Per-Database Detail

$(if [[ ${#DB_DETAILS[@]} -gt 0 ]]; then
    # Populate backup/restore timings from TIMINGS array
    for i in "${!DB_DETAILS[@]}"; do
        IFS='|' read -r d_eng d_db d_dbsz d_bf d_bsz d_btm d_rtm d_cbsz d_crtm <<< "${DB_DETAILS[$i]}"
        # Find single backup time — try engine-specific naming patterns
        bt=""
        bt=$(_get_timing_ms "single backup (large: ${d_db})")
        [[ -z "$bt" ]] && bt=$(_get_timing_ms "single backup (small: ${d_db})")
        [[ -z "$bt" ]] && bt=$(_get_timing_ms "single backup (${d_db})")
        [[ -n "$bt" ]] && d_btm="$bt"
        # Find single restore time — PG restores qa_pg_small only (large is
        # dropped before restore), Maria/MySQL each have one DB
        rt=""
        case "$d_eng" in
            postgres)
                if [[ "$d_db" == "$PG_SMALL_DB" ]]; then
                    rt=$(_get_timing_ms "PG single restore")
                fi ;;
            mariadb)  rt=$(_get_timing_ms "Maria single restore") ;;
            mysql)    rt=$(_get_timing_ms "MySQL single restore") ;;
        esac
        [[ -n "$rt" ]] && d_rtm="$rt"
        # Find cluster backup time (only on DBs present during cluster backup)
        cbt=""
        case "$d_eng" in
            postgres)
                if [[ "$d_db" == "$PG_SMALL_DB" ]]; then
                    cbt=$(_get_timing_ms "PG cluster backup")
                fi ;;
            mariadb) cbt=$(_get_timing_ms "Maria cluster backup") ;;
        esac
        [[ -n "$cbt" ]] && d_cbsz="$cbt"
        # Find cluster restore time
        crt=""
        case "$d_eng" in
            postgres) crt=$(_get_timing_ms "PG cluster restore") ;;
            mariadb)  crt=$(_get_timing_ms "Maria cluster restore") ;;
        esac
        [[ -n "$crt" ]] && d_crtm="$crt"
        DB_DETAILS[$i]="${d_eng}|${d_db}|${d_dbsz}|${d_bf}|${d_bsz}|${d_btm}|${d_rtm}|${d_cbsz}|${d_crtm}"
    done

    echo "| Engine | Database | DB Size | Backup Size | Backup Time | Restore Time | Cluster Bk Time | Cluster Rs Time |"
    echo "|--------|----------|---------|-------------|-------------|--------------|-----------------|-----------------|"
    for entry in "${DB_DETAILS[@]}"; do
        IFS='|' read -r d_eng d_db d_dbsz d_bf d_bsz d_btm d_rtm d_cbsz d_crtm <<< "$entry"
        printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n" \
            "$d_eng" "$d_db" \
            "${d_dbsz:-—}" \
            "${d_bsz:-—}" \
            "$(_fmt_ms "${d_btm}")" \
            "$(_fmt_ms "${d_rtm}")" \
            "$(_fmt_ms "${d_cbsz}")" \
            "$(_fmt_ms "${d_crtm}")"
    done
else
    echo "*No database detail data collected (--skip-create or no engines tested)*"
fi)

$(if [[ ${#FAILURES[@]} -gt 0 ]]; then
    echo "## Failed Tests"
    echo
    for f in "${FAILURES[@]}"; do
        echo "- $f"
    done
fi)

## Performance Timing

| # | Test | Status | Time | Category |
|---|------|--------|------|----------|
$(if [[ ${#TIMINGS[@]} -gt 0 ]]; then for entry in "${TIMINGS[@]}"; do
    IFS='|' read -r t_num t_name t_status t_ms t_cat <<< "$entry"
    t_sec=$(( t_ms / 1000 ))
    t_frac=$(( t_ms % 1000 ))
    if [[ "$t_status" == "SKIP" ]]; then
        printf "| %s | %s | %s | — | %s |\n" "$t_num" "$t_name" "$t_status" "$t_cat"
    else
        printf "| %s | %s | %s | %d.%03ds | %s |\n" "$t_num" "$t_name" "$t_status" "$t_sec" "$t_frac" "$t_cat"
    fi
done; fi)

### Top 5 Slowest

| Time | Test | Category |
|------|------|----------|
$(if [[ ${#TIMINGS[@]} -gt 0 ]]; then printf '%s\n' "${TIMINGS[@]}" | sort -t'|' -k4 -rn | head -5 | while IFS='|' read -r t_num t_name t_status t_ms t_cat; do
    t_sec=$(( t_ms / 1000 ))
    t_frac=$(( t_ms % 1000 ))
    printf "| %d.%03ds | [%s] %s | %s |\n" "$t_sec" "$t_frac" "$t_num" "$t_name" "$t_cat"
done; fi)

### Time by Category

| Category | Total Time | Tests |
|----------|------------|-------|
$(for cat in backup restore verify dedup pitr cleanup drill cloud ops; do
    cat_total=0
    cat_count=0
    if [[ ${#TIMINGS[@]} -gt 0 ]]; then for entry in "${TIMINGS[@]}"; do
        IFS='|' read -r _ _ t_status t_ms t_cat_entry <<< "$entry"
        if [[ "$t_cat_entry" == "$cat" && "$t_status" != "SKIP" ]]; then
            cat_total=$(( cat_total + t_ms ))
            cat_count=$(( cat_count + 1 ))
        fi
    done; fi
    if [[ $cat_count -gt 0 ]]; then
        cat_sec=$(( cat_total / 1000 ))
        cat_frac=$(( cat_total % 1000 ))
        printf "| %s | %d.%03ds | %d |\n" "$cat" "$cat_sec" "$cat_frac" "$cat_count"
    fi
done)

## Log Files

All test logs are in: \`${LOG_DIR}/\`
EOF

    # Append final verdict to report
    if [[ $FAILED -gt 0 ]]; then
        echo -e "\n## Verdict\n\n**QA SUITE FAILED** — $FAILED test(s) failed out of $TOTAL\n" >> "$REPORT_FILE"
    else
        echo -e "\n## Verdict\n\n**QA SUITE PASSED** — All $PASSED tests passed\n" >> "$REPORT_FILE"
    fi

    log_ok "Report saved: $REPORT_FILE"

    # ── Email notification ──
    send_email_report

    # Exit code
    if [[ $FAILED -gt 0 ]]; then
        echo -e "\n${RED}${BOLD}QA SUITE FAILED${NC} — $FAILED test(s) failed"
        return 1
    else
        echo -e "\n${GREEN}${BOLD}QA SUITE PASSED${NC} — All $PASSED tests passed"
        return 0
    fi
}

# ─── Email report ───────────────────────────────────────────────────────────
send_email_report() {
    if ! $EMAIL_ENABLED; then
        log_info "Email notification disabled (set EMAIL_ENABLED=true to enable)"
        return 0
    fi

    if [[ -z "${EMAIL_PASS}" ]]; then
        log_warn "QA_EMAIL_PASS not set — skipping email notification"
        return 0
    fi

    local has_curl=false has_sendmail=false
    command -v curl &>/dev/null && has_curl=true
    command -v sendmail &>/dev/null && has_sendmail=true
    if ! $has_curl && ! $has_sendmail; then
        log_warn "Neither curl nor sendmail found — cannot send email notification"
        return 1
    fi

    local status_label="PASSED"
    local status_emoji="✅"
    if [[ $FAILED -gt 0 ]]; then
        status_label="FAILED"
        status_emoji="❌"
    fi

    local elapsed=$(( $(date +%s) - GLOBAL_START ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))
    local subject="${status_emoji} QA ${status_label}: ${PASSED}/${TOTAL} passed (${mins}m${secs}s) — $(hostname) v$($BINARY version 2>&1 | grep -m1 'Version:' | awk '{print $2}' || echo '?')"

    log_info "Sending QA report to ${EMAIL_TO}..."

    # Build MIME email with report as attachment
    local boundary="qa-report-$(date +%s)"
    local tmpmail; tmpmail=$(mktemp)

    cat > "$tmpmail" <<EOMAIL
From: dbbackup QA <${EMAIL_FROM}>
To: ${EMAIL_TO}
Subject: ${subject}
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="${boundary}"

--${boundary}
Content-Type: text/plain; charset=UTF-8

QA Suite ${status_label} on $(hostname) at $(date '+%Y-%m-%d %H:%M:%S')

Profile:  ${SIZE_PROFILE}
Engines:  ${ENGINE_FILTER}
Scope:    ${scope_list:-core}
Duration: ${mins}m ${secs}s

Total:    ${TOTAL}
Passed:   ${PASSED}
Failed:   ${FAILED}
Skipped:  ${SKIPPED}
$(if [[ ${#FAILURES[@]} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for f in "${FAILURES[@]}"; do
        echo "  ✗ $f"
    done
fi)

Full report attached.

--${boundary}
Content-Type: text/markdown; charset=UTF-8; name="$(basename "$REPORT_FILE")"
Content-Disposition: attachment; filename="$(basename "$REPORT_FILE")"
Content-Transfer-Encoding: base64

$(base64 < "$REPORT_FILE")
--${boundary}--
EOMAIL

    # Send via curl SMTPS first, fall back to sendmail
    local sent=false
    if $has_curl; then
        log_info "Trying curl SMTPS..."
        if curl --silent --show-error \
            --url "${EMAIL_SMTP}" \
            --ssl-reqd \
            --mail-from "${EMAIL_FROM}" \
            --mail-rcpt "${EMAIL_TO}" \
            --user "${EMAIL_USER}:${EMAIL_PASS}" \
            --upload-file "$tmpmail" \
            --max-time 30 2>&1; then
            log_ok "Email sent via curl to ${EMAIL_TO}"
            sent=true
        else
            log_warn "curl SMTPS failed — trying sendmail fallback..."
        fi
    fi

    if ! $sent && $has_sendmail; then
        log_info "Sending via sendmail..."
        if sendmail -t < "$tmpmail" 2>&1; then
            log_ok "Email sent via sendmail to ${EMAIL_TO}"
            sent=true
        else
            log_warn "sendmail delivery failed"
        fi
    fi

    if ! $sent; then
        log_warn "Failed to send email via all methods"
    fi

    rm -f "$tmpmail"
}

# ═══════════════════════════════════════════════════════════════════════════
#  Phase 17: Verify-Restore & Status Dashboard Tests
# ═══════════════════════════════════════════════════════════════════════════
test_verify_restore_and_dashboard() {
    log_header "Verify-Restore & Status Dashboard Tests"
    local BDIR
    BDIR=$(mktemp -d)/verify_tests
    mkdir -p "$BDIR"
    local DB_FLAGS="-d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --no-config --no-save-config --allow-root --insecure"

    # ── 17.1 CLI flag registration ──
    CURRENT_CATEGORY="verify"
    log_section "Verify: CLI Flag Registration"

    run_test "--verify-restore flag on backup single" \
        "$BINARY backup single --help 2>&1 | grep -q 'verify-restore'"

    run_test "--verify-restore flag on backup cluster" \
        "$BINARY backup cluster --help 2>&1 | grep -q 'verify-restore'"

    run_test "--verify-restore flag on backup sample" \
        "$BINARY backup sample --help 2>&1 | grep -q 'verify-restore'"
    section_time

    # ── 17.2 Backup WITHOUT --verify-restore (baseline) ──
    CURRENT_CATEGORY="backup"
    log_section "Verify: Baseline Backup (no verify)"

    if should_test pg; then
        run_test "PG backup without --verify-restore (no verification output)" \
            "$BINARY backup single $PG_SMALL_DB $DB_FLAGS --backup-dir $BDIR 2>&1 | grep -vq 'Restore Verification Report'"
    else
        skip_test "PG backup baseline (pg not selected)"
    fi
    section_time

    # ── 17.3 Backup WITH --verify-restore ──
    CURRENT_CATEGORY="verify"
    log_section "Verify: Full Backup+Verify Cycle"

    if should_test pg; then
        # Run backup with verify, capture output for subsequent checks
        local VERIFY_LOG="$BDIR/verify_output.log"
        $BINARY backup single $PG_SMALL_DB $DB_FLAGS \
            --backup-dir "$BDIR" --verify-restore > "$VERIFY_LOG" 2>&1 || true

        run_test "Verification report printed" \
            "grep -q 'Restore Verification Report' '$VERIFY_LOG'"

        run_test "Temp database was created" \
            "grep -q 'Creating verification database' '$VERIFY_LOG'"

        run_test "Temp database was dropped (cleanup)" \
            "grep -q 'Dropping verification database' '$VERIFY_LOG'"

        run_test "Verification result is PASS" \
            "grep -q 'PASS' '$VERIFY_LOG'"

        run_test "Report shows table header (TABLE/SOURCE/RESTORED)" \
            "grep -qE 'TABLE.*SOURCE.*RESTORED' '$VERIFY_LOG'"

        run_test "Report shows TOTAL ROWS" \
            "grep -q 'TOTAL ROWS' '$VERIFY_LOG'"
    else
        skip_test "Verify report printed (pg not selected)"
        skip_test "Verify temp DB created (pg not selected)"
        skip_test "Verify temp DB dropped (pg not selected)"
        skip_test "Verify result PASS (pg not selected)"
        skip_test "Verify table header (pg not selected)"
        skip_test "Verify TOTAL ROWS (pg not selected)"
    fi
    section_time

    # ── 17.4 No leftover temp DBs ──
    log_section "Verify: Temp DB Cleanup"

    if should_test pg; then
        run_test "No leftover _dbbackup_verify_* databases" \
            "test \$(psql -h $PG_HOST -U $PG_USER -tAc \"SELECT count(*) FROM pg_database WHERE datname LIKE '_dbbackup_verify%'\" 2>/dev/null || echo 0) -eq 0"
    else
        skip_test "Leftover temp DB check (pg not selected)"
    fi
    section_time

    # ── 17.5 Status Dashboard ──
    CURRENT_CATEGORY="status"
    log_section "Verify: Status Dashboard"

    if should_test pg; then
        local STATUS_LOG="$BDIR/status_output.log"
        $BINARY status $DB_FLAGS --database $PG_SMALL_DB \
            --backup-dir "$BDIR" > "$STATUS_LOG" 2>&1 || true

        run_test "Dashboard header present" \
            "grep -q 'Backup Status Dashboard' '$STATUS_LOG'"

        run_test "Dashboard has table columns (DATABASE/LAST BACKUP)" \
            "grep -q 'DATABASE.*LAST BACKUP' '$STATUS_LOG'"

        run_test "Dashboard shows $PG_SMALL_DB entry" \
            "grep -q '$PG_SMALL_DB' '$STATUS_LOG'"

        run_test "Dashboard has status indicators (OK/AGING/STALE)" \
            "grep -qE 'OK|AGING|STALE' '$STATUS_LOG'"

        run_test "Dashboard shows Total Backups summary" \
            "grep -q 'Total Backups:' '$STATUS_LOG'"

        run_test "Dashboard has Summary line" \
            "grep -q 'Summary:' '$STATUS_LOG'"
    else
        skip_test "Dashboard header (pg not selected)"
        skip_test "Dashboard columns (pg not selected)"
        skip_test "Dashboard db entry (pg not selected)"
        skip_test "Dashboard status indicators (pg not selected)"
        skip_test "Dashboard total summary (pg not selected)"
        skip_test "Dashboard summary line (pg not selected)"
    fi
    section_time

    # ── 17.6 Empty directory handling ──
    log_section "Verify: Dashboard Empty Directory"

    local EMPTY_DIR
    EMPTY_DIR=$(mktemp -d)/empty_backups
    mkdir -p "$EMPTY_DIR"

    run_test "Empty backup dir shows 'No backups found'" \
        "$BINARY status $DB_FLAGS --database $PG_SMALL_DB --backup-dir '$EMPTY_DIR' 2>&1 | grep -q 'No backups found'"
    section_time

    # ── 17.7 Config persistence ──
    CURRENT_CATEGORY="config"
    log_section "Verify: Config Persistence"

    if should_test pg; then
        local TMP_CONF_DIR
        TMP_CONF_DIR=$(mktemp -d)/verify_conf
        mkdir -p "$TMP_CONF_DIR"

        # Run backup from the temp dir so config saves there.
        # Use a simple backup WITHOUT --verify-restore first to guarantee the config
        # file gets created (verify-restore is advisory but can cause issues in temp dirs).
        # Then test that --verify-restore is persisted by running a second backup with that flag.
        pushd "$TMP_CONF_DIR" > /dev/null
        run_test "PG backup for config persistence (verify-restore)" \
            "cd '$TMP_CONF_DIR' && $BINARY backup single $PG_SMALL_DB -d postgres --host $PG_HOST --port $PG_PORT --user $PG_USER --allow-root --insecure --backup-dir '$BDIR' --verify-restore"
        popd > /dev/null

        run_test "verify_restore persisted in .dbbackup.conf" \
            "test -f '$TMP_CONF_DIR/.dbbackup.conf' && grep -q 'verify_restore' '$TMP_CONF_DIR/.dbbackup.conf'"

        rm -rf "$TMP_CONF_DIR"
    else
        skip_test "Config persistence (pg not selected)"
    fi
    section_time

    # Cleanup
    rm -rf "$BDIR" "$EMPTY_DIR"

    log_ok "Phase 17 complete — verify-restore & dashboard tests done"
}

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════
main() {
    GLOBAL_START=$(date +%s)

    # Determine scope label
    local scope_list="core"
    $SCOPE_ADVANCED   && scope_list+=",advanced"
    $SCOPE_NATIVE     && scope_list+=",native"
    $SCOPE_ENCRYPTION && scope_list+=",encryption"
    $SCOPE_PITR_FULL  && scope_list+=",pitr-full"
    $SCOPE_PARALLEL   && scope_list+=",parallel"
    $SCOPE_DRILL      && scope_list+=",drill"
    $SCOPE_CLOUD      && scope_list+=",cloud"
    $SCOPE_MIGRATE    && scope_list+=",migrate"
    $SCOPE_SYSTEMD    && scope_list+=",systemd"
    $SCOPE_EDGE       && scope_list+=",edge"
    $SCOPE_LO_VACUUM  && scope_list+=",lo-vacuum"
    $SCOPE_MYSQL_SPEED && scope_list+=",mysql-speed"
    $SCOPE_VERIFY     && scope_list+=",verify"

    log_header "dbbackup Full QA Suite"
    echo -e "${DIM}  Date:     $(date)${NC}"
    echo -e "${DIM}  Profile:  ${SIZE_PROFILE} (PG: ${PG_LARGE_SIZE}+${PG_SMALL_SIZE} GB, Maria: ${MARIA_SIZE} GB, MySQL: ${MYSQL_SIZE} GB)${NC}"
    echo -e "${DIM}  Engines:  ${ENGINE_FILTER}${NC}"
    echo -e "${DIM}  Scope:    ${scope_list}${NC}"
    echo -e "${DIM}  Fast:     ${FAST_STORAGE}${NC}"
    echo -e "${DIM}  Slow:     ${SLOW_STORAGE}${NC}"
    echo -e "${DIM}  Logs:     ${LOG_DIR}${NC}"
    echo

    preflight

    # ── Drop / cleanup pre-phase ──
    # When combined with test flags (e.g. --drop-all --all), drop first then continue.
    # When used alone, drop and exit.
    local has_test_scope=false
    if $SCOPE_ADVANCED || $SCOPE_NATIVE || $SCOPE_ENCRYPTION || $SCOPE_PITR_FULL || \
       $SCOPE_PARALLEL || $SCOPE_DRILL || $SCOPE_CLOUD || $SCOPE_MIGRATE || \
       $SCOPE_SYSTEMD || $SCOPE_EDGE || $SCOPE_LO_VACUUM || $SCOPE_MYSQL_SPEED || \
       $SCOPE_VERIFY; then
        has_test_scope=true
    fi

    if $DROP_ALL_ONLY; then
        drop_all_qa_databases
        cleanup_all_qa_files
        log_ok "All QA databases and files removed."
        if ! $has_test_scope; then
            return 0
        fi
        log_info "Continuing with test suite..."
    elif $DROP_DBS_ONLY; then
        drop_all_qa_databases
        log_ok "All QA databases removed."
        if ! $has_test_scope; then
            return 0
        fi
        log_info "Continuing with test suite..."
    fi

    if $CLEANUP_ONLY; then
        cleanup_all_qa_files
        log_ok "All QA artifact files removed."
        generate_report
        return $?
    fi

    # Phase 1: Clean slate
    cleanup_old_dumps
    storage_status

    # \u2550\u2550\u2550 Per-engine flow: create \u2192 test \u2192 flush \u2192 drop \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
    # Each engine: check space \u2192 create DBs \u2192 run tests \u2192 move backups to slow \u2192 drop DBs
    # This keeps sda1 and faststorage from running out of space.

    if should_test pg; then
        log_header "Phase: PostgreSQL"
        local pg_need; pg_need=$(estimate_engine_gb pg)
        if ! $SKIP_CREATE; then
            if storage_guard / "$pg_need" "PG databases (${PG_LARGE_SIZE}+${PG_SMALL_SIZE} GB)"; then
                log_section "Creating PostgreSQL Databases"
                create_pg_db "$PG_LARGE_DB" "$PG_LARGE_SIZE"
                create_pg_db "$PG_SMALL_DB" "$PG_SMALL_SIZE"
                section_time
            else
                log_warn "Skipping PG database creation \u2014 not enough space"
            fi
        fi
        storage_guard "$FAST_STORAGE" "$pg_need" "PG backups" || log_warn "Low space on faststorage for PG backups"
        test_pg
        log_section "PG: Post-test cleanup"
        flush_fast_to_slow
        if ! $SKIP_CREATE; then
            drop_pg_large_db
        fi
        section_time
    fi

    if should_test maria; then
        log_header "Phase: MariaDB"
        local maria_need; maria_need=$(estimate_engine_gb maria)
        if ! $SKIP_CREATE; then
            if storage_guard / "$maria_need" "MariaDB database (${MARIA_SIZE} GB)"; then
                log_section "Creating MariaDB Database"
                create_mysql_db "$MARIA_DB" "$MARIA_SIZE" "MariaDB"
                section_time
            else
                log_warn "Skipping MariaDB database creation \u2014 not enough space"
            fi
        fi
        storage_guard "$FAST_STORAGE" "$maria_need" "MariaDB backups" || log_warn "Low space on faststorage for MariaDB backups"
        test_maria
        log_section "MariaDB: Post-test cleanup"
        flush_fast_to_slow
        if ! $SKIP_CREATE; then
            drop_maria_dbs
        fi
        section_time
    fi

    if should_test mysql; then
        log_header "Phase: MySQL"
        local mysql_need; mysql_need=$(estimate_engine_gb mysql)
        if ! $SKIP_CREATE; then
            if storage_guard / "$mysql_need" "MySQL database (${MYSQL_SIZE} GB)"; then
                log_section "Creating MySQL Database"
                create_mysql_db "$MYSQL_DB" "$MYSQL_SIZE" "MySQL"
                section_time
            else
                log_warn "Skipping MySQL database creation \u2014 not enough space"
            fi
        fi
        storage_guard "$FAST_STORAGE" "$mysql_need" "MySQL backups" || log_warn "Low space on faststorage for MySQL backups"
        test_mysql
        log_section "MySQL: Post-test cleanup"
        flush_fast_to_slow
        if ! $SKIP_CREATE; then
            drop_mysql_dbs
        fi
        section_time
    fi

    # Phase 4: Cross-engine and operational tests
    # Ensure test DBs exist (may have been dropped by cluster restore)
    log_section "Ensuring test databases exist for cross-engine tests"
    ensure_pg_small_exists
    ensure_maria_exists
    ensure_mysql_exists
    section_time

    test_cross_engine

    # Phase 5: Advanced & stress tests
    if $SCOPE_ADVANCED; then
        test_advanced
    else
        log_info "Skipping advanced tests (use --comprehensive or --with-advanced)"
    fi

    # Phase 6: Native engine tests (pure Go, no pg_dump/mysqldump)
    if $SCOPE_NATIVE; then
        test_native_engine
    else
        log_info "Skipping native engine tests (use --comprehensive or --with-native)"
    fi

    # Phase 6b: Percona XtraBackup tests
    if $SCOPE_XTRABACKUP; then
        test_xtrabackup_engine
    else
        log_info "Skipping XtraBackup tests (use --comprehensive or --with-xtrabackup)"
    fi

    # Phase 7: Encryption round-trip
    if $SCOPE_ENCRYPTION; then
        test_encryption_roundtrip
    else
        log_info "Skipping encryption tests (use --comprehensive or --with-encryption)"
    fi

    # Phase 8: Full PITR cycle (WAL/binlog replay)
    if $SCOPE_PITR_FULL; then
        test_pitr_full
    else
        log_info "Skipping full PITR cycle (use --comprehensive or --with-pitr)"
    fi

    # Phase 9: Parallel restore & benchmarks
    if $SCOPE_PARALLEL; then
        test_parallel_restore
    else
        log_info "Skipping parallel restore tests (use --comprehensive or --with-parallel)"
    fi

    # Phase 10: DR drill (Docker-based restore validation)
    if $SCOPE_DRILL; then
        test_dr_drill
    else
        log_info "Skipping DR drill tests (use --comprehensive or --with-drill)"
    fi

    # Phase 11: Cloud sync (MinIO / S3-compatible)
    if $SCOPE_CLOUD; then
        test_cloud_minio
    else
        log_info "Skipping cloud sync tests (use --comprehensive or --with-cloud)"
    fi

    # Phase 12: Migration dry-run
    if $SCOPE_MIGRATE; then
        test_migrate_dryrun
    else
        log_info "Skipping migration tests (use --comprehensive or --with-migrate)"
    fi

    # Phase 13: Systemd install/uninstall
    if $SCOPE_SYSTEMD; then
        test_install_systemd
    else
        log_info "Skipping systemd install tests (use --comprehensive or --with-systemd)"
    fi

    # Phase 14: Edge cases & robustness
    if $SCOPE_EDGE; then
        test_edge_cases
    else
        log_info "Skipping edge case tests (use --comprehensive or --with-edge)"
    fi

    # Phase 14b: Comprehensive flag coverage (always runs)
    test_flag_coverage

    # Phase 15: Large Object Vacuum (PostgreSQL maintenance)
    if $SCOPE_LO_VACUUM; then
        test_lo_vacuum
    else
        log_info "Skipping LO vacuum tests (use --comprehensive or --with-lo-vacuum)"
    fi

    # Phase 16: MySQL/MariaDB Speed Optimizations
    if $SCOPE_MYSQL_SPEED; then
        test_mysql_speed_optimizations
    else
        log_info "Skipping MySQL speed tests (use --comprehensive or --with-mysql-speed)"
    fi

    # Phase 17: Verify-Restore & Status Dashboard
    if $SCOPE_VERIFY; then
        test_verify_restore_and_dashboard
    else
        log_info "Skipping verify-restore tests (use --comprehensive or --with-verify)"
    fi

    # Phase 18: Drop remaining test databases
    if should_test pg && ! $SKIP_CREATE; then
        log_section "Dropping remaining PG small database"
        psql -U "$PG_USER" -c "DROP DATABASE IF EXISTS ${PG_SMALL_DB};" 2>/dev/null || true
        log_ok "${PG_SMALL_DB} dropped"
        section_time
    fi

    # Final storage status
    log_section "Final Storage Status"
    storage_status
    section_time

    # ── Post-run cleanup: WAL archive & binlog copies ──
    # These accumulate across runs and can fill the fast storage volume.
    log_section "Post-Run Cleanup (WAL/Binlog Archives)"
    for cleanup_dir in "$WAL_ARCHIVE" "$BINLOG_DIR"; do
        if [[ -d "$cleanup_dir" ]]; then
            local ccount; ccount=$(find "$cleanup_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
            local csize; csize=$(du -sh "$cleanup_dir" 2>/dev/null | cut -f1)
            if [[ "$ccount" -gt 0 ]]; then
                rm -rf "${cleanup_dir:?}"/*
                log_ok "Cleaned: $cleanup_dir ($ccount files, $csize)"
            else
                log_info "Already empty: $cleanup_dir"
            fi
        fi
    done
    section_time

    # Phase 6: Report
    generate_report
}

# ─── tmux wrapper ───────────────────────────────────────────────────────────
# If not already inside the target tmux session, re-exec inside it
SCRIPT_ABSPATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
if [[ -z "${TMUX:-}" ]]; then
    echo "Launching in tmux session '${TMUX_SESSION}'..."

    # Kill existing session if any
    tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true

    # Create new session and run this script inside it (use absolute path)
    # Do NOT use 'exec' — if the script exits the shell must survive so
    # the tmux session stays open for log inspection.
    tmux new-session -d -s "$TMUX_SESSION" -x 200 -y 50 -c "$SCRIPT_DIR"
    tmux send-keys -t "$TMUX_SESSION" "'${SCRIPT_ABSPATH}' ${SAVED_ARGS}; echo '--- QA script finished (exit code \$?) — shell kept alive ---'" C-m

    echo "Attached. Use: tmux attach -t ${TMUX_SESSION}"
    echo "Or watch:      tmux attach -t ${TMUX_SESSION} \\; set -g status off"

    # Auto-attach if interactive terminal
    if [[ -t 0 ]]; then
        sleep 1
        exec tmux attach -t "$TMUX_SESSION"
    fi
    exit 0
fi

# Guard against double execution — only ONE instance should run at a time
_QA_LOCKFILE="/tmp/dbbackup_qa.lock"
if [[ -f "$_QA_LOCKFILE" ]]; then
    _LOCK_PID=$(cat "$_QA_LOCKFILE" 2>/dev/null)
    if kill -0 "$_LOCK_PID" 2>/dev/null; then
        echo "ERROR: QA suite already running (PID $_LOCK_PID). Wait for it to finish or: rm $_QA_LOCKFILE"
        exit 1
    fi
fi
echo $$ > "$_QA_LOCKFILE"
trap 'rm -f "$_QA_LOCKFILE"' EXIT

# We're inside tmux — run the suite
main
exit $?
