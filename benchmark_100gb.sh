#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# DBBackup 100GB Benchmark Suite
# ═══════════════════════════════════════════════════════════════════════════════
#
# Sequential benchmark of PostgreSQL, MariaDB, and MySQL with 100GB databases.
# Creates database -> Backup -> Restore -> Drop -> Next engine
#
# Usage:
#   ./benchmark_100gb.sh [OPTIONS]
#
# Options:
#   --size SIZE      Database size in GB (default: 100)
#   --skip-pg        Skip PostgreSQL benchmark
#   --skip-maria     Skip MariaDB benchmark
#   --skip-mysql     Skip MySQL benchmark
#   --output DIR     Output directory (default: /tmp/dbbackup_benchmark)
#                    Reports saved to DIR/benchmarks/, backups to DIR/backups/
#   --help           Show this help
#
# Requirements:
#   - PostgreSQL, MariaDB, MySQL running (or skip engines not available)
#   - dbbackup binary built
#   - Sufficient disk space (3x database size recommended)
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

SIZE_GB="${SIZE_GB:-100}"
DBBACKUP="${DBBACKUP:-./bin/dbbackup_linux_amd64}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/dbbackup_benchmark}"
BACKUP_DIR="${OUTPUT_DIR}/backups"
REPORT_DIR="${OUTPUT_DIR}/benchmarks"
RESULTS_FILE="${REPORT_DIR}/benchmark_results.txt"
JSON_RESULTS="${REPORT_DIR}/benchmark_results.json"

SKIP_PG=false
SKIP_MARIA=false
SKIP_MYSQL=false

# Database configurations
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"

MARIA_HOST="${MARIA_HOST:-localhost}"
MARIA_PORT="${MARIA_PORT:-3306}"
MARIA_USER="${MARIA_USER:-root}"
MARIA_PASS="${MARIA_PASS:-}"

MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-}"

# ─── Colors ───────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Helper Functions ─────────────────────────────────────────────────────────

log_header() {
    echo ""
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${MAGENTA}  $1${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[✓]${NC}       $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[✗]${NC}       $1"; }
log_timing()  { echo -e "${CYAN}[TIMING]${NC}  $1"; }

format_duration() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    if [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $minutes $secs
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $secs
    else
        printf "%ds" $secs
    fi
}

format_throughput() {
    local size_gb=$1
    local seconds=$2
    if [ "$seconds" -gt 0 ]; then
        local mbps=$(echo "scale=2; ($size_gb * 1024) / $seconds" | bc)
        echo "${mbps} MB/s"
    else
        echo "N/A"
    fi
}

get_timestamp() {
    date +%s
}

# ─── Parse Arguments ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case $1 in
        --size)
            SIZE_GB="$2"
            shift 2
            ;;
        --skip-pg)
            SKIP_PG=true
            shift
            ;;
        --skip-maria)
            SKIP_MARIA=true
            shift
            ;;
        --skip-mysql)
            SKIP_MYSQL=true
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            BACKUP_DIR="${OUTPUT_DIR}/backups"
            REPORT_DIR="${OUTPUT_DIR}/benchmarks"
            RESULTS_FILE="${REPORT_DIR}/benchmark_results.txt"
            JSON_RESULTS="${REPORT_DIR}/benchmark_results.json"
            shift 2
            ;;
        --help)
            head -25 "$0" | tail -20
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ─── Setup ────────────────────────────────────────────────────────────────────

mkdir -p "$OUTPUT_DIR" "$BACKUP_DIR" "$REPORT_DIR"

# Results tracking
declare -A RESULTS

# ─── System Info ──────────────────────────────────────────────────────────────

print_system_info() {
    log_header "System Information"
    echo "  Date:       $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Hostname:   $(hostname)"
    echo "  OS:         $(uname -s) $(uname -m)"
    echo "  Kernel:     $(uname -r)"
    echo "  CPU Cores:  $(nproc)"
    if command -v lscpu &>/dev/null; then
        echo "  CPU Model:  $(lscpu | grep 'Model name' | sed 's/.*:\s*//' | head -1)"
    fi
    echo "  RAM:        $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo 'N/A')"
    echo "  Disk Free:  $(df -h "$OUTPUT_DIR" 2>/dev/null | tail -1 | awk '{print $4}')"
    echo "  dbbackup:   $DBBACKUP"
    if [ -x "$DBBACKUP" ]; then
        echo "  Version:    $($DBBACKUP version 2>/dev/null | head -1 || echo 'unknown')"
    fi
    echo ""
}

# ─── PostgreSQL Functions ─────────────────────────────────────────────────────

pg_create_database() {
    local db_name="$1"
    local size_gb="$2"

    local script_dir
    script_dir="$(dirname "$(realpath "$0")")"
    local fakedb="$script_dir/fakedbcreator.sh"

    if [ ! -f "$fakedb" ]; then
        log_error "fakedbcreator.sh not found at $fakedb"
        exit 1
    fi
    chmod +x "$fakedb"

    log_info "Creating PostgreSQL database '$db_name' (${size_gb}GB) via fakedbcreator.sh..."
    log_info "Includes: BYTEA blobs, pg_largeobject, JSONB, exotic types, partitions, materialized views"

    # Pre-drop so fakedbcreator.sh does not hit the interactive prompt
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS \"$db_name\";" 2>/dev/null || true

    local start_time
    start_time=$(get_timestamp)

    bash "$fakedb" "$size_gb" "$db_name"

    local end_time
    end_time=$(get_timestamp)
    local duration=$(( end_time - start_time ))

    local actual_size
    actual_size=$(sudo -u postgres psql -d "$db_name" -t -c \
        "SELECT pg_size_pretty(pg_database_size('$db_name'));" | tr -d ' ')

    log_success "PostgreSQL database ready: $actual_size in $(format_duration $duration)"

    RESULTS["pg_create_time"]=$duration
    RESULTS["pg_actual_size"]="$actual_size"
}

pg_drop_all_user_databases() {
    log_info "Dropping all user databases (clean slate)..."
    local dbs
    dbs=$(sudo -u postgres psql -t -c \
        "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres');" \
        2>/dev/null | tr -d ' ' | grep -v '^$' || true)
    if [ -n "$dbs" ]; then
        while IFS= read -r db; do
            [ -z "$db" ] && continue
            log_info "  Dropping: $db"
            sudo -u postgres psql -c "DROP DATABASE IF EXISTS \"$db\";" 2>/dev/null || true
        done <<< "$dbs"
        log_success "All user databases dropped"
    else
        log_info "  No user databases found — already clean"
    fi
}

pg_drop_database() {
    local db_name="$1"
    log_info "Dropping PostgreSQL database '$db_name'..."
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS \"$db_name\";" 2>/dev/null || true
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS \"${db_name}_restored\";" 2>/dev/null || true
    log_success "PostgreSQL database dropped"
}

# ─── MariaDB Functions ────────────────────────────────────────────────────────

maria_create_database() {
    local db_name="$1"
    local size_gb="$2"
    
    log_info "Creating MariaDB database '$db_name' with ${size_gb}GB of data..."
    
    local maria_cmd="mariadb"
    if [ -n "$MARIA_PASS" ]; then
        maria_cmd="mariadb -h $MARIA_HOST -P $MARIA_PORT -u $MARIA_USER -p$MARIA_PASS"
    else
        maria_cmd="mariadb -h $MARIA_HOST -P $MARIA_PORT -u $MARIA_USER"
    fi
    
    # Drop and create
    $maria_cmd -e "DROP DATABASE IF EXISTS \`$db_name\`;" 2>/dev/null || true
    $maria_cmd -e "CREATE DATABASE \`$db_name\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    # Create table
    $maria_cmd "$db_name" << 'SCHEMA_SQL'
CREATE TABLE large_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid CHAR(36) DEFAULT (UUID()),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    title VARCHAR(500),
    content TEXT,
    metadata JSON,
    binary_data BLOB,
    numeric_val DECIMAL(20,4),
    tags TEXT
) ENGINE=InnoDB;

CREATE INDEX idx_created ON large_data(created_at);
SCHEMA_SQL

    # Calculate rows
    local rows_per_gb=100000
    local total_rows=$((size_gb * rows_per_gb))
    local batch_size=5000
    local batches=$((total_rows / batch_size))
    
    log_info "Inserting $total_rows rows in $batches batches..."
    
    local start_time=$(get_timestamp)
    
    for ((i=1; i<=batches; i++)); do
        $maria_cmd "$db_name" << EOF
INSERT INTO large_data (title, content, metadata, binary_data, numeric_val, tags)
SELECT 
    CONCAT('Title ', FLOOR(RAND() * 1000000)),
    REPEAT(MD5(RAND()), 100),
    JSON_OBJECT('id', FLOOR(RAND() * 1000000), 'active', RAND() > 0.5, 'priority', ELT(FLOOR(RAND() * 3) + 1, 'low', 'medium', 'high')),
    UNHEX(REPEAT(MD5(RAND()), 64)),
    RAND() * 1000000,
    CONCAT(MD5(RAND()), ',', MD5(RAND()))
FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
    FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
    LIMIT $batch_size
) nums;
EOF
        if [ $((i % 100)) -eq 0 ]; then
            local pct=$((i * 100 / batches))
            log_info "Progress: $pct% ($i/$batches batches)"
        fi
    done
    
    # Optimize
    $maria_cmd "$db_name" -e "ANALYZE TABLE large_data;"
    
    local end_time=$(get_timestamp)
    local duration=$((end_time - start_time))
    
    local actual_size=$($maria_cmd "$db_name" -N -e "SELECT CONCAT(ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 2), ' GB') FROM information_schema.tables WHERE table_schema='$db_name';")
    
    log_success "MariaDB database created: $actual_size in $(format_duration $duration)"
    
    RESULTS["maria_create_time"]=$duration
    RESULTS["maria_actual_size"]="$actual_size"
}

maria_drop_database() {
    local db_name="$1"
    log_info "Dropping MariaDB database '$db_name'..."
    
    local maria_cmd="mariadb"
    if [ -n "$MARIA_PASS" ]; then
        maria_cmd="mariadb -h $MARIA_HOST -P $MARIA_PORT -u $MARIA_USER -p$MARIA_PASS"
    else
        maria_cmd="mariadb -h $MARIA_HOST -P $MARIA_PORT -u $MARIA_USER"
    fi
    
    $maria_cmd -e "DROP DATABASE IF EXISTS \`$db_name\`;" 2>/dev/null || true
    $maria_cmd -e "DROP DATABASE IF EXISTS \`${db_name}_restored\`;" 2>/dev/null || true
    log_success "MariaDB database dropped"
}

# ─── MySQL Functions ──────────────────────────────────────────────────────────

mysql_create_database() {
    local db_name="$1"
    local size_gb="$2"
    
    log_info "Creating MySQL database '$db_name' with ${size_gb}GB of data..."
    
    local mysql_cmd="mysql"
    if [ -n "$MYSQL_PASS" ]; then
        mysql_cmd="mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASS"
    else
        mysql_cmd="mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER"
    fi
    
    # Drop and create
    $mysql_cmd -e "DROP DATABASE IF EXISTS \`$db_name\`;" 2>/dev/null || true
    $mysql_cmd -e "CREATE DATABASE \`$db_name\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    # Create table
    $mysql_cmd "$db_name" << 'SCHEMA_SQL'
CREATE TABLE large_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid CHAR(36) DEFAULT (UUID()),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    title VARCHAR(500),
    content TEXT,
    metadata JSON,
    binary_data BLOB,
    numeric_val DECIMAL(20,4),
    tags TEXT
) ENGINE=InnoDB;

CREATE INDEX idx_created ON large_data(created_at);
SCHEMA_SQL

    # Calculate rows
    local rows_per_gb=100000
    local total_rows=$((size_gb * rows_per_gb))
    local batch_size=5000
    local batches=$((total_rows / batch_size))
    
    log_info "Inserting $total_rows rows in $batches batches..."
    
    local start_time=$(get_timestamp)
    
    for ((i=1; i<=batches; i++)); do
        $mysql_cmd "$db_name" << EOF
INSERT INTO large_data (title, content, metadata, binary_data, numeric_val, tags)
SELECT 
    CONCAT('Title ', FLOOR(RAND() * 1000000)),
    REPEAT(MD5(RAND()), 100),
    JSON_OBJECT('id', FLOOR(RAND() * 1000000), 'active', CAST(RAND() > 0.5 AS JSON), 'priority', ELT(FLOOR(RAND() * 3) + 1, 'low', 'medium', 'high')),
    UNHEX(REPEAT(MD5(RAND()), 64)),
    RAND() * 1000000,
    CONCAT(MD5(RAND()), ',', MD5(RAND()))
FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
    FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
    LIMIT $batch_size
) nums;
EOF
        if [ $((i % 100)) -eq 0 ]; then
            local pct=$((i * 100 / batches))
            log_info "Progress: $pct% ($i/$batches batches)"
        fi
    done
    
    # Optimize
    $mysql_cmd "$db_name" -e "ANALYZE TABLE large_data;"
    
    local end_time=$(get_timestamp)
    local duration=$((end_time - start_time))
    
    local actual_size=$($mysql_cmd "$db_name" -N -e "SELECT CONCAT(ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 2), ' GB') FROM information_schema.tables WHERE table_schema='$db_name';")
    
    log_success "MySQL database created: $actual_size in $(format_duration $duration)"
    
    RESULTS["mysql_create_time"]=$duration
    RESULTS["mysql_actual_size"]="$actual_size"
}

mysql_drop_database() {
    local db_name="$1"
    log_info "Dropping MySQL database '$db_name'..."
    
    local mysql_cmd="mysql"
    if [ -n "$MYSQL_PASS" ]; then
        mysql_cmd="mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASS"
    else
        mysql_cmd="mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER"
    fi
    
    $mysql_cmd -e "DROP DATABASE IF EXISTS \`$db_name\`;" 2>/dev/null || true
    $mysql_cmd -e "DROP DATABASE IF EXISTS \`${db_name}_restored\`;" 2>/dev/null || true
    log_success "MySQL database dropped"
}

# ─── Backup/Restore Functions ─────────────────────────────────────────────────

run_backup() {
    local db_type="$1"
    local db_name="$2"
    local backup_path="$3"
    
    log_info "Starting $db_type cluster backup..."
    
    rm -rf "$backup_path"
    mkdir -p "$backup_path"
    
    local start_time=$(get_timestamp)
    
    case "$db_type" in
        postgres)
            $DBBACKUP backup cluster \
                --db-type postgres \
                --host "$PG_HOST" \
                --port "$PG_PORT" \
                --user "$PG_USER" \
                --backup-dir "$backup_path" \
                --allow-root \
                --no-config \
                --native \
                --fallback-tools \
                --compression-algorithm zstd \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_backup.log"
            ;;
        mariadb)
            # Use MYSQL_PWD env var — --password flag is rejected by dbbackup for security
            MYSQL_PWD="$MARIA_PASS" $DBBACKUP backup cluster \
                --db-type mariadb \
                --host "$MARIA_HOST" \
                --port "$MARIA_PORT" \
                --user "$MARIA_USER" \
                --backup-dir "$backup_path" \
                --allow-root \
                --no-config \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_backup.log"
            ;;
        mysql)
            # Use MYSQL_PWD env var — --password flag is rejected by dbbackup for security
            MYSQL_PWD="$MYSQL_PASS" $DBBACKUP backup cluster \
                --db-type mysql \
                --host "$MYSQL_HOST" \
                --port "$MYSQL_PORT" \
                --user "$MYSQL_USER" \
                --backup-dir "$backup_path" \
                --allow-root \
                --no-config \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_backup.log"
            ;;
    esac
    
    local end_time=$(get_timestamp)
    local duration=$((end_time - start_time))
    
    local backup_size=$(du -sh "$backup_path" | cut -f1)
    
    log_timing "$db_type BACKUP: $(format_duration $duration) | Size: $backup_size | Throughput: $(format_throughput $SIZE_GB $duration)"
    
    RESULTS["${db_type}_backup_time"]=$duration
    RESULTS["${db_type}_backup_size"]="$backup_size"
}

run_restore() {
    local db_type="$1"
    local db_name="$2"
    local backup_path="$3"
    
    log_info "Starting $db_type cluster restore..."
    
    # Find the latest backup archive (supports .tar.zst, .tar.gz, .tar.xz, etc.)
    local backup_file=$(ls -t "$backup_path"/*.tar.* 2>/dev/null | head -1)
    
    if [ -z "$backup_file" ]; then
        log_error "No backup archive found in $backup_path"
        return 1
    fi
    
    local start_time=$(get_timestamp)
    
    case "$db_type" in
        postgres)
            $DBBACKUP restore cluster "$backup_file" \
                --db-type postgres \
                --host "$PG_HOST" \
                --port "$PG_PORT" \
                --user "$PG_USER" \
                --allow-root \
                --no-config \
                --confirm \
                --clean-cluster \
                --profile turbo \
                --restore-mode balanced \
                --no-tui \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_restore.log"
            ;;
        mariadb)
            MYSQL_PWD="$MARIA_PASS" $DBBACKUP restore cluster "$backup_file" \
                --db-type mariadb \
                --host "$MARIA_HOST" \
                --port "$MARIA_PORT" \
                --user "$MARIA_USER" \
                --allow-root \
                --no-config \
                --confirm \
                --no-tui \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_restore.log"
            ;;
        mysql)
            MYSQL_PWD="$MYSQL_PASS" $DBBACKUP restore cluster "$backup_file" \
                --db-type mysql \
                --host "$MYSQL_HOST" \
                --port "$MYSQL_PORT" \
                --user "$MYSQL_USER" \
                --allow-root \
                --no-config \
                --confirm \
                --no-tui \
                --jobs "$(nproc)" \
                2>&1 | tee "${OUTPUT_DIR}/${db_type}_restore.log"
            ;;
    esac
    
    local end_time=$(get_timestamp)
    local duration=$((end_time - start_time))
    
    log_timing "$db_type RESTORE: $(format_duration $duration) | Throughput: $(format_throughput $SIZE_GB $duration)"
    
    RESULTS["${db_type}_restore_time"]=$duration
}

# ─── Benchmark Runner ─────────────────────────────────────────────────────────

run_engine_benchmark() {
    local db_type="$1"
    local db_name="benchmark_${SIZE_GB}gb"
    local backup_path="${BACKUP_DIR}/${db_type}"
    
    log_header "${db_type^^} Benchmark (${SIZE_GB}GB)"

    local engine_start
    engine_start=$(get_timestamp)

    # PostgreSQL: drop all user databases for a clean slate before creating the test DB
    if [ "$db_type" = "postgres" ]; then
        pg_drop_all_user_databases
    fi

    # Create database
    case "$db_type" in
        postgres)
            pg_create_database "$db_name" "$SIZE_GB"
            ;;
        mariadb)
            maria_create_database "$db_name" "$SIZE_GB"
            ;;
        mysql)
            mysql_create_database "$db_name" "$SIZE_GB"
            ;;
    esac
    
    # Run backup
    run_backup "$db_type" "$db_name" "$backup_path"
    
    # Run restore
    run_restore "$db_type" "$db_name" "$backup_path"
    
    # Drop database
    case "$db_type" in
        postgres)
            pg_drop_database "$db_name"
            ;;
        mariadb)
            maria_drop_database "$db_name"
            ;;
        mysql)
            mysql_drop_database "$db_name"
            ;;
    esac
    
    # Clean up backup files
    rm -rf "$backup_path"
    
    local engine_end=$(get_timestamp)
    local total_time=$((engine_end - engine_start))
    
    RESULTS["${db_type}_total_time"]=$total_time
    
    log_success "${db_type^^} benchmark complete in $(format_duration $total_time)"
}

# ─── Results Output ───────────────────────────────────────────────────────────

print_results() {
    log_header "BENCHMARK RESULTS SUMMARY"
    
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
    printf "${BOLD}%-12s %12s %12s %12s %12s %12s${NC}\n" "ENGINE" "DB SIZE" "BACKUP" "RESTORE" "THROUGHPUT" "TOTAL"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    
    for engine in postgres mariadb mysql; do
        if [[ -v RESULTS["${engine}_backup_time"] ]]; then
            local backup_time=${RESULTS["${engine}_backup_time"]}
            local restore_time=${RESULTS["${engine}_restore_time"]}
            local total_time=${RESULTS["${engine}_total_time"]}
            local db_size=${RESULTS["${engine}_actual_size"]:-"${SIZE_GB}GB"}
            local throughput=$(format_throughput $SIZE_GB $backup_time)
            
            printf "%-12s %12s %12s %12s %12s %12s\n" \
                "${engine^^}" \
                "$db_size" \
                "$(format_duration $backup_time)" \
                "$(format_duration $restore_time)" \
                "$throughput" \
                "$(format_duration $total_time)"
        fi
    done
    
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo ""
    
    # Save results to file
    {
        echo "DBBackup 100GB Benchmark Results"
        echo "================================"
        echo "Date: $(date)"
        echo "Size: ${SIZE_GB}GB per database"
        echo ""
        for engine in postgres mariadb mysql; do
            if [[ -v RESULTS["${engine}_backup_time"] ]]; then
                echo "${engine^^}:"
                echo "  Database Size: ${RESULTS["${engine}_actual_size"]:-N/A}"
                echo "  Backup Time:   $(format_duration ${RESULTS["${engine}_backup_time"]})"
                echo "  Backup Size:   ${RESULTS["${engine}_backup_size"]:-N/A}"
                echo "  Restore Time:  $(format_duration ${RESULTS["${engine}_restore_time"]})"
                echo "  Total Time:    $(format_duration ${RESULTS["${engine}_total_time"]})"
                echo ""
            fi
        done
    } > "$RESULTS_FILE"
    
    # Save JSON results
    {
        echo "{"
        echo "  \"benchmark_date\": \"$(date -Iseconds)\","
        echo "  \"target_size_gb\": $SIZE_GB,"
        echo "  \"results\": {"
        local first=true
        for engine in postgres mariadb mysql; do
            if [[ -v RESULTS["${engine}_backup_time"] ]]; then
                if [ "$first" = true ]; then
                    first=false
                else
                    echo ","
                fi
                echo "    \"$engine\": {"
                echo "      \"actual_size\": \"${RESULTS["${engine}_actual_size"]:-N/A}\","
                echo "      \"backup_time_seconds\": ${RESULTS["${engine}_backup_time"]},"
                echo "      \"backup_size\": \"${RESULTS["${engine}_backup_size"]:-N/A}\","
                echo "      \"restore_time_seconds\": ${RESULTS["${engine}_restore_time"]},"
                echo "      \"total_time_seconds\": ${RESULTS["${engine}_total_time"]}"
                printf "    }"
            fi
        done
        echo ""
        echo "  }"
        echo "}"
    } > "$JSON_RESULTS"
    
    log_success "Results saved to:"
    echo "  Text:   $RESULTS_FILE"
    echo "  JSON:   $JSON_RESULTS"
    echo ""
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
    log_header "DBBackup ${SIZE_GB}GB Benchmark Suite"
    
    # Check prerequisites
    if [ ! -x "$DBBACKUP" ]; then
        log_error "dbbackup binary not found at $DBBACKUP"
        log_info "Build it with: make build"
        exit 1
    fi
    
    print_system_info
    
    local overall_start=$(get_timestamp)
    
    # Run PostgreSQL benchmark
    if [ "$SKIP_PG" = false ]; then
        if command -v psql &>/dev/null; then
            run_engine_benchmark "postgres"
        else
            log_warn "PostgreSQL client not found, skipping..."
        fi
    else
        log_info "Skipping PostgreSQL (--skip-pg)"
    fi
    
    # Run MariaDB benchmark
    if [ "$SKIP_MARIA" = false ]; then
        if command -v mariadb &>/dev/null; then
            run_engine_benchmark "mariadb"
        else
            log_warn "MariaDB client not found, skipping..."
        fi
    else
        log_info "Skipping MariaDB (--skip-maria)"
    fi
    
    # Run MySQL benchmark
    if [ "$SKIP_MYSQL" = false ]; then
        if command -v mysql &>/dev/null; then
            run_engine_benchmark "mysql"
        else
            log_warn "MySQL client not found, skipping..."
        fi
    else
        log_info "Skipping MySQL (--skip-mysql)"
    fi
    
    local overall_end=$(get_timestamp)
    local overall_time=$((overall_end - overall_start))
    
    print_results
    
    log_header "Benchmark Complete"
    echo "  Total Duration: $(format_duration $overall_time)"
    echo "  Output Dir:     $OUTPUT_DIR"
    echo "  Reports:        $REPORT_DIR"
    echo ""
    echo "  WE ARE FAST!"
    echo ""
}

main "$@"
